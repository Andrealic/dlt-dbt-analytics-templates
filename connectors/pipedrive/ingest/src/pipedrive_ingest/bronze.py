"""Step 2: raw to bronze.

Bronze is derived only from raw. A cursor per entity, kept in the dlt source state,
records the newest raw load already applied; each run applies every newer raw load
in load order. The state is committed with the bronze load, so a failed load is
retried on the next run.

- Incremental entities are upserted on their primary key with the latest version of
  each record across the pending raw loads. When a pending load is a complete listing
  (see `FULL_LISTINGS_TABLE`), records absent from it are flagged `_is_deleted`.
- Full-refresh entities are replaced with the newest raw load, which may be empty.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from typing import Any

import dlt
import pyarrow.compute as pc
from dlt.sources import DltResource

from pipedrive_ingest import SOURCE_NAME
from pipedrive_ingest.raw import RawLoad, RawStore, load_order
from pipedrive_ingest.schemas import (
    BRONZE_TABLES,
    IS_DELETED,
    RAW_LOAD_ID,
    SOURCE_UPDATED_AT,
    BronzeTable,
)
from pipedrive_ingest.source import ENTITIES, Sync

CURSORS_STATE_KEY = "raw_load_cursors"

_EARLIEST = datetime.min.replace(tzinfo=UTC)

Row = dict[str, Any]


@dlt.source(
    name=SOURCE_NAME,
    section=SOURCE_NAME,
    max_table_nesting=0,
    schema_contract={"data_type": "freeze"},
)
def pipedrive_bronze(raw: RawStore) -> Iterator[DltResource]:
    """Bronze resources for every entity with raw loads not yet applied."""
    cursors: Mapping[str, str] = dlt.current.source_state().get(CURSORS_STATE_KEY, {})
    listings = raw.full_listings()

    for entity in ENTITIES:
        table = BRONZE_TABLES[entity.name]
        cursor = cursors.get(entity.name)
        listing_ids = {
            load_id
            for load_id in listings.get(entity.name, ())
            if cursor is None or load_order(load_id) > load_order(cursor)
        }
        pending = _with_empty_listings(raw.loads(entity.name, after=cursor), listing_ids)
        if not pending:
            continue
        if entity.sync is Sync.FULL_REFRESH:
            yield _resource(table, _snapshot(raw, table, pending[-1]), "replace")
        else:
            rows = _upserts(raw, table, pending, listing_ids, bronze_exists=cursor is not None)
            yield _resource(table, rows, {"disposition": "merge", "strategy": "upsert"})


def _resource(table: BronzeTable, rows: Iterator[list[Row]], disposition: Any) -> DltResource:
    return dlt.resource(
        rows,
        name=table.name,
        primary_key=table.primary_key,
        write_disposition=disposition,
        columns=table.column_schemas(),
    )


def _snapshot(raw: RawStore, table: BronzeTable, load: RawLoad) -> Iterator[list[Row]]:
    load_id, loaded_at = _current_load()
    latest: dict[Any, Row] = {}
    for line in raw.read(load):
        row = table.from_payload(
            line["payload"], load_id=load_id, raw_load_id=line["_dlt_load_id"], loaded_at=loaded_at
        )
        _keep_latest(latest, table, row)
    yield list(latest.values())
    _advance_cursor(table, load)


def _upserts(
    raw: RawStore,
    table: BronzeTable,
    pending: list[RawLoad],
    listing_ids: set[str],
    *,
    bronze_exists: bool,
) -> Iterator[list[Row]]:
    load_id, loaded_at = _current_load()
    stored = _stored_rows(table) if bronze_exists and listing_ids else {}
    latest: dict[Any, Row] = {}

    for load in pending:
        listed: set[Any] = set()
        for line in raw.read(load):
            row = table.from_payload(
                line["payload"],
                load_id=load_id,
                raw_load_id=line["_dlt_load_id"],
                loaded_at=loaded_at,
            )
            listed.add(row[table.primary_key])
            _keep_latest(latest, table, row)

        if load.load_id not in listing_ids:
            continue
        deleted = {RAW_LOAD_ID: load.load_id, IS_DELETED: True}
        for key, row in latest.items():
            if key not in listed and not row[IS_DELETED]:
                latest[key] = row | deleted
        for key, stored_row in stored.items():
            if key not in listed and key not in latest:
                latest[key] = table.from_bronze(stored_row, load_id=load_id, loaded_at=loaded_at)
                latest[key].update(deleted)

    yield list(latest.values())
    _advance_cursor(table, pending[-1])


def _keep_latest(latest: dict[Any, Row], table: BronzeTable, row: Row) -> None:
    """Keep the newest version per key: later raw loads win; within a load, the
    record with the later source update time wins."""
    key = row[table.primary_key]
    current = latest.get(key)
    if (
        current is None
        or current[RAW_LOAD_ID] != row[RAW_LOAD_ID]
        or (row[SOURCE_UPDATED_AT] or _EARLIEST) >= (current[SOURCE_UPDATED_AT] or _EARLIEST)
    ):
        latest[key] = row


def _stored_rows(table: BronzeTable) -> dict[Any, Row]:
    """Bronze rows not yet flagged as deleted, by primary key."""
    stored = dlt.current.pipeline().dataset()[table.name].arrow()
    if stored is None:
        return {}
    active = stored.filter(pc.invert(stored[IS_DELETED]))
    return {row[table.primary_key]: row for row in active.to_pylist()}


def _with_empty_listings(loads: list[RawLoad], listing_ids: set[str]) -> list[RawLoad]:
    """A complete listing with no records writes no entity file; it still lists nothing."""
    known = {load.load_id for load in loads}
    empty = [RawLoad(load_id) for load_id in listing_ids - known]
    return sorted([*loads, *empty], key=lambda load: load_order(load.load_id))


def _current_load() -> tuple[str, datetime]:
    return dlt.current.load_package_state()["load_id"], datetime.now(UTC)


def _advance_cursor(table: BronzeTable, load: RawLoad) -> None:
    dlt.current.source_state().setdefault(CURSORS_STATE_KEY, {})[table.name] = load.load_id
