"""Step 1: Pipedrive API to raw.

Every API record is written verbatim to a single JSON column, `payload`, so raw does
not depend on dlt's normalization. Endpoints follow Pipedrive API v2 wherever a v2
list endpoint exists; users are only available in v1.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers.rest_client.auth import APIKeyAuth
from dlt.sources.helpers.rest_client.client import RESTClient
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    JSONResponseCursorPaginator,
    SinglePagePaginator,
)

from pipedrive_ingest import SOURCE_NAME
from pipedrive_ingest.timestamps import parse_utc, to_rfc3339

DEFAULT_BASE_URL = "https://api.pipedrive.com"
PAGE_SIZE = 500
"""Maximum `limit` accepted by v2 list endpoints."""

CURSOR_FIELD = "update_time"
"""Record field behind the `updated_since` filter of v2 list endpoints."""
CURSOR_STATE_KEY = "updated_since"

FULL_LISTINGS_TABLE = "_full_listings"
"""Raw table naming, per load, the entities whose listing in that load is complete.

Full-refresh entities are always listed completely; incremental entities only in
`--reconcile` runs. An entity with no records writes no file, so the bronze step relies
on this table to tell an empty listing from a skipped one: it empties full-refresh
tables and flags incremental records missing from a complete listing as deleted.
"""

RAW_COLUMNS: dict[str, Any] = {"payload": {"data_type": "json"}}


class Sync(StrEnum):
    INCREMENTAL = "incremental"
    FULL_REFRESH = "full_refresh"


@dataclass(frozen=True)
class Endpoint:
    path: str
    params: Mapping[str, str] = field(default_factory=dict)
    paginated: bool = True


@dataclass(frozen=True)
class Entity:
    name: str
    sync: Sync
    endpoints: tuple[Endpoint, ...]

    @property
    def cursor(self) -> str | None:
        return CURSOR_FIELD if self.sync is Sync.INCREMENTAL else None


# `status` must be explicit for deals: without it, deleted deals are omitted. Archived
# deals are only returned by the separate `/archived` endpoint.
_DEAL_PARAMS = {"status": "open,won,lost,deleted"}

ENTITIES: tuple[Entity, ...] = (
    Entity(
        "deals",
        Sync.INCREMENTAL,
        (Endpoint("api/v2/deals", _DEAL_PARAMS), Endpoint("api/v2/deals/archived", _DEAL_PARAMS)),
    ),
    Entity("persons", Sync.INCREMENTAL, (Endpoint("api/v2/persons"),)),
    Entity("organizations", Sync.INCREMENTAL, (Endpoint("api/v2/organizations"),)),
    Entity("activities", Sync.INCREMENTAL, (Endpoint("api/v2/activities"),)),
    Entity("pipelines", Sync.FULL_REFRESH, (Endpoint("api/v2/pipelines"),)),
    Entity("stages", Sync.FULL_REFRESH, (Endpoint("api/v2/stages"),)),
    Entity("users", Sync.FULL_REFRESH, (Endpoint("api/v1/users", paginated=False),)),
    Entity("deal_fields", Sync.FULL_REFRESH, (Endpoint("api/v2/dealFields"),)),
    Entity("person_fields", Sync.FULL_REFRESH, (Endpoint("api/v2/personFields"),)),
    Entity("organization_fields", Sync.FULL_REFRESH, (Endpoint("api/v2/organizationFields"),)),
)


@dlt.source(name=SOURCE_NAME, section=SOURCE_NAME, max_table_nesting=0)
def pipedrive_api(
    api_token: str = dlt.secrets.value,
    base_url: str = DEFAULT_BASE_URL,
    reconcile: bool = False,
) -> Iterator[DltResource]:
    """Raw resources, one per entity.

    Args:
        api_token: Pipedrive API token, sent in the `x-api-token` header.
        base_url: API host; `https://<company>.pipedrive.com` also works.
        reconcile: List incremental entities completely instead of from their cursor,
            so the bronze step can detect deletions.
    """
    client = RESTClient(
        base_url=base_url,
        auth=APIKeyAuth(name="x-api-token", api_key=api_token, location="header"),
    )
    for entity in ENTITIES:
        yield _raw_resource(client, entity, complete=reconcile)
    listed = [{"entity": e.name} for e in ENTITIES if reconcile or e.sync is Sync.FULL_REFRESH]
    yield dlt.resource(listed, name=FULL_LISTINGS_TABLE, write_disposition="append")


def _raw_resource(client: RESTClient, entity: Entity, *, complete: bool) -> DltResource:
    def records() -> Iterator[list[dict[str, Any]]]:
        state = dlt.current.resource_state()
        cursor: str | None = state.get(CURSOR_STATE_KEY)
        updated_since = None if complete or entity.cursor is None else cursor
        newest = parse_utc(cursor) if cursor else None

        for endpoint in entity.endpoints:
            for page in _pages(client, endpoint, updated_since):
                if entity.cursor is not None:
                    newest = _max_timestamp(newest, (r.get(entity.cursor) for r in page))
                yield [{"payload": record} for record in page]

        if entity.cursor is not None and newest is not None:
            state[CURSOR_STATE_KEY] = to_rfc3339(newest)

    return dlt.resource(records, name=entity.name, write_disposition="append", columns=RAW_COLUMNS)


def _pages(
    client: RESTClient, endpoint: Endpoint, updated_since: str | None
) -> Iterator[list[dict[str, Any]]]:
    params = dict(endpoint.params)
    paginator: BasePaginator
    if endpoint.paginated:
        params["limit"] = str(PAGE_SIZE)
        paginator = JSONResponseCursorPaginator(
            cursor_path="additional_data.next_cursor", cursor_param="cursor"
        )
    else:
        paginator = SinglePagePaginator()
    if updated_since is not None:
        # Inclusive bound: records updated in the cursor's second are fetched again and
        # deduplicated in bronze, so none are missed.
        params["updated_since"] = updated_since
    yield from client.paginate(
        endpoint.path, params=params, paginator=paginator, data_selector="data"
    )


def _max_timestamp(current: datetime | None, values: Iterable[Any]) -> datetime | None:
    for value in values:
        if value:
            parsed = parse_utc(value)
            current = parsed if current is None else max(current, parsed)
    return current
