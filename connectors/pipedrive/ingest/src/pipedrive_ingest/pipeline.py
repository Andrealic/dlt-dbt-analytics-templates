"""Entrypoints for both ingest steps.

Environment-specific settings come only from dlt configuration (environment
variables or `.dlt/*.toml`):

- `lake_root`: lake root URL, e.g. `s3://<bucket>/lake`; raw and bronze live under it.
- `destination.filesystem.credentials`: object store credentials.
- `iceberg_catalog`: Iceberg REST catalog used for bronze.
- `sources.pipedrive.api_token`: Pipedrive API token.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.destinations import filesystem

from pipedrive_ingest import SOURCE_NAME
from pipedrive_ingest.bronze import pipedrive_bronze
from pipedrive_ingest.raw import RawStore
from pipedrive_ingest.source import pipedrive_api

RAW_LAYOUT = "{table_name}/load_date={YYYY}-{MM}-{DD}/{load_id}.{file_id}.{ext}"
"""dlt fills the date placeholders from the load package creation time, in UTC."""

logger = logging.getLogger(__name__)


def lake_url(layer: str) -> str:
    root: str = dlt.config["lake_root"]
    return f"{root.rstrip('/')}/{layer}"


def run_raw(*, reconcile: bool = False) -> LoadInfo:
    pipeline = dlt.pipeline(
        pipeline_name=f"{SOURCE_NAME}_raw",
        destination=filesystem(bucket_url=lake_url("raw"), layout=RAW_LAYOUT),
        dataset_name=SOURCE_NAME,
    )
    return pipeline.run(pipedrive_api(reconcile=reconcile), loader_file_format="jsonl")


def run_bronze() -> LoadInfo | None:
    pipeline = dlt.pipeline(
        pipeline_name=f"{SOURCE_NAME}_bronze",
        destination=filesystem(bucket_url=lake_url("bronze")),
        dataset_name=SOURCE_NAME,
    )
    # Restore the raw load cursors from the destination before selecting pending loads.
    pipeline.sync_destination()
    source = pipedrive_bronze(RawStore.from_bucket_url(lake_url("raw")))
    if not source.resources:
        logger.info("No raw loads pending; bronze is up to date.")
        return None
    return pipeline.run(source, table_format="iceberg")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="pipedrive-ingest", description=__doc__.splitlines()[0])
    steps = parser.add_subparsers(dest="step", required=True)
    raw = steps.add_parser("raw", help="step 1: Pipedrive API to raw JSONL")
    raw.add_argument(
        "--reconcile",
        action="store_true",
        help="list incremental entities completely so step 2 can flag deleted records",
    )
    steps.add_parser("bronze", help="step 2: raw to bronze Iceberg")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    info = run_raw(reconcile=args.reconcile) if args.step == "raw" else run_bronze()
    if info is not None:
        print(info)
