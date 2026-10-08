from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

from fakes import API_TOKEN, FakePipedrive, deal, person, stage

from pipedrive_ingest.pipeline import lake_url, run_raw
from pipedrive_ingest.raw import RawStore
from pipedrive_ingest.source import ENTITIES, FULL_LISTINGS_TABLE, PAGE_SIZE, Sync

INCREMENTAL_PATHS = [
    endpoint.path for e in ENTITIES if e.sync is Sync.INCREMENTAL for endpoint in e.endpoints
]
FULL_REFRESH_PATHS = [
    endpoint.path for e in ENTITIES if e.sync is Sync.FULL_REFRESH for endpoint in e.endpoints
]


def test_raw_stores_verbatim_payloads_in_contract_layout(lake: Path, api: FakePipedrive) -> None:
    record = deal(1, "2026-02-01T10:00:00Z")
    api.set("api/v2/deals", [record])

    load_id = run_raw().loads_ids[0]

    files = [p.relative_to(lake) for p in (lake / "raw/pipedrive/deals").rglob("*.*")]
    load_date = datetime.fromtimestamp(float(load_id), UTC).date().isoformat()
    assert len(files) == 1
    assert re.fullmatch(
        rf"raw/pipedrive/deals/load_date={load_date}/{re.escape(load_id)}\.\w+\.jsonl\.gz",
        files[0].as_posix(),
    )
    store = RawStore.from_bucket_url(lake_url("raw"))
    [line] = store.read(store.loads("deals")[0])
    assert line["payload"] == record
    assert line["_dlt_load_id"] == load_id


def test_requests_authenticate_and_follow_cursor_pagination(lake: Path, api: FakePipedrive) -> None:
    api.set("api/v2/persons", [person(i) for i in range(PAGE_SIZE + 1)])

    run_raw()

    calls = api.calls("api/v2/persons")
    assert [c.get("cursor") for c in calls] == [None, str(PAGE_SIZE)]
    assert all(c["limit"] == str(PAGE_SIZE) for c in calls)
    assert all(headers["x-api-token"] == API_TOKEN for *_, headers in api.requests)
    store = RawStore.from_bucket_url(lake_url("raw"))
    assert sum(1 for _ in store.read(store.loads("persons")[0])) == PAGE_SIZE + 1


def test_deals_are_requested_with_all_statuses_including_archived(
    lake: Path, api: FakePipedrive
) -> None:
    run_raw()

    for path in ("api/v2/deals", "api/v2/deals/archived"):
        [call] = api.calls(path)
        assert call["status"] == "open,won,lost,deleted"


def test_incremental_entities_resume_from_newest_update_time(
    lake: Path, api: FakePipedrive
) -> None:
    api.set("api/v2/deals", [deal(1, "2026-02-01T10:00:00Z"), deal(2, "2026-02-03T08:30:00Z")])
    api.set("api/v2/deals/archived", [deal(3, "2026-02-02T12:00:00.000Z", is_archived=True)])
    api.set("api/v2/stages", [stage(1)])

    run_raw()
    run_raw()

    first, second = api.calls("api/v2/deals")
    assert "updated_since" not in first
    assert second["updated_since"] == "2026-02-03T08:30:00Z"
    assert api.calls("api/v2/deals/archived")[1]["updated_since"] == "2026-02-03T08:30:00Z"
    assert all("updated_since" not in c for p in FULL_REFRESH_PATHS for c in api.calls(p))


def test_full_listings_cover_full_refresh_and_reconciled_entities(
    lake: Path, api: FakePipedrive
) -> None:
    api.set("api/v2/persons", [person(1, "2026-02-01T10:00:00Z")])
    first = run_raw().loads_ids[0]

    load_id = run_raw(reconcile=True).loads_ids[0]

    assert all("updated_since" not in api.calls(path)[-1] for path in INCREMENTAL_PATHS)
    store = RawStore.from_bucket_url(lake_url("raw"))
    assert store.full_listings() == {
        e.name: {load_id} if e.sync is Sync.INCREMENTAL else {first, load_id} for e in ENTITIES
    }
    assert [load.load_id for load in store.loads(FULL_LISTINGS_TABLE)] == [first, load_id]
