from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dlt
import pytest
from fakes import FakePipedrive, deal, person, stage

from pipedrive_ingest.bronze import pipedrive_bronze
from pipedrive_ingest.pipeline import lake_url, run_raw
from pipedrive_ingest.raw import RawStore


@pytest.fixture
def bronze(tmp_path: Path, lake: Path) -> dlt.Pipeline:
    """Bronze pipeline on a local DuckDB destination; Iceberg is covered against the dev lake."""
    return dlt.pipeline(
        pipeline_name="pipedrive_bronze_test",
        destination=dlt.destinations.duckdb(str(tmp_path / "bronze.duckdb")),
        dataset_name="pipedrive",
    )


def run_bronze(pipeline: dlt.Pipeline) -> list[str]:
    """Same flow as `pipeline.run_bronze`, returning the names of the tables written."""
    pipeline.sync_destination()
    source = pipedrive_bronze(RawStore.from_bucket_url(lake_url("raw")))
    if source.resources:
        pipeline.run(source)
    return sorted(source.resources)


def rows(pipeline: dlt.Pipeline, table: str) -> dict[Any, dict[str, Any]]:
    data = pipeline.dataset()[table].arrow()
    key = "field_code" if table.endswith("_fields") else "id"
    return {row[key]: row for row in data.to_pylist()}


def test_bronze_keeps_latest_version_with_lineage(
    lake: Path, api: FakePipedrive, bronze: dlt.Pipeline
) -> None:
    api.set("api/v2/deals", [deal(1, "2026-02-01T10:00:00Z"), deal(2, "2026-02-01T11:00:00Z")])
    first_raw = run_raw().loads_ids[0]
    api.set("api/v2/deals", [deal(1, "2026-02-02T10:00:00Z", title="Renamed", value=250.5)])
    second_raw = run_raw().loads_ids[0]

    run_bronze(bronze)

    deals = rows(bronze, "deals")
    assert deals[1]["title"] == "Renamed"
    assert deals[1]["value"] == 250.5
    assert deals[1]["_raw_load_id"] == second_raw
    assert deals[2]["_raw_load_id"] == first_raw
    assert deals[1]["_source_updated_at"] == datetime(2026, 2, 2, 10, tzinfo=UTC)
    assert deals[1]["_is_deleted"] is False
    assert all(row["_load_id"] == row["_dlt_load_id"] for row in deals.values())
    assert all(row["_raw_load_id"] != row["_load_id"] for row in deals.values())


def test_bronze_applies_only_raw_loads_after_its_cursor(
    lake: Path, api: FakePipedrive, bronze: dlt.Pipeline
) -> None:
    api.set("api/v2/deals", [deal(1, "2026-02-01T10:00:00Z")])
    run_raw()
    assert "deals" in run_bronze(bronze)

    assert run_bronze(bronze) == []

    api.set("api/v2/deals", [deal(1, "2026-02-02T10:00:00Z", title="Renamed")])
    run_raw()
    written = run_bronze(bronze)
    assert "deals" in written
    assert "persons" not in written
    assert rows(bronze, "deals")[1]["title"] == "Renamed"


def test_bronze_flags_deals_deleted_in_pipedrive(
    lake: Path, api: FakePipedrive, bronze: dlt.Pipeline
) -> None:
    api.set("api/v2/deals", [deal(1, "2026-02-01T10:00:00Z", is_deleted=True, status="deleted")])
    run_raw()

    run_bronze(bronze)

    assert rows(bronze, "deals")[1]["_is_deleted"] is True


def test_full_refresh_tables_hold_the_newest_raw_load(
    lake: Path, api: FakePipedrive, bronze: dlt.Pipeline
) -> None:
    api.set("api/v2/stages", [stage(1), stage(2)])
    run_raw()
    api.set("api/v2/stages", [stage(1)])
    run_raw()

    run_bronze(bronze)
    assert set(rows(bronze, "stages")) == {1}

    # No new raw load: the table must not be replaced with an empty one.
    run_bronze(bronze)
    assert set(rows(bronze, "stages")) == {1}

    # A new raw load with no stages is an empty listing.
    api.set("api/v2/stages", [])
    run_raw()
    run_bronze(bronze)
    assert rows(bronze, "stages") == {}


def test_reconciliation_flags_records_missing_from_a_full_listing(
    lake: Path, api: FakePipedrive, bronze: dlt.Pipeline
) -> None:
    api.set("api/v2/persons", [person(1), person(2), person(3)])
    run_raw()
    run_bronze(bronze)

    api.set("api/v2/persons", [person(1), person(3), person(4, "2026-03-01T09:00:00Z")])
    run_raw()
    api.set("api/v2/persons", [person(1), person(3)])
    listing = run_raw(reconcile=True).loads_ids[0]
    run_bronze(bronze)

    persons = rows(bronze, "persons")
    assert {key for key, row in persons.items() if row["_is_deleted"]} == {2, 4}
    assert persons[2]["_raw_load_id"] == listing
    assert persons[2]["name"] == "Person 2"
    assert json.loads(persons[2]["emails"]) == person(2)["emails"]
