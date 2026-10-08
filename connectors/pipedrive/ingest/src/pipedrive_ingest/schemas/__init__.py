"""Bronze table contracts.

Each `<table>.yml` file in this package declares the source columns of one bronze
table, its primary key, and which payload fields feed the lake metadata columns.
Bronze tables carry exactly these columns plus the metadata columns defined here;
fields the API adds later stay in raw until they are declared.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from importlib import resources
from typing import Any, cast

import yaml
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.typing import TTableSchemaColumns

from pipedrive_ingest.timestamps import parse_utc

LOAD_ID = "_load_id"
RAW_LOAD_ID = "_raw_load_id"
LOADED_AT = "_loaded_at"
SOURCE_UPDATED_AT = "_source_updated_at"
IS_DELETED = "_is_deleted"

_DLT_COLUMN_PREFIX = "_dlt_"


def _metadata_columns(*, tracks_deletes: bool) -> TTableSchemaColumns:
    columns: TTableSchemaColumns = {
        LOAD_ID: {"data_type": "text", "nullable": False},
        RAW_LOAD_ID: {"data_type": "text", "nullable": False},
        LOADED_AT: {"data_type": "timestamp", "nullable": False},
        SOURCE_UPDATED_AT: {"data_type": "timestamp", "nullable": True},
    }
    if tracks_deletes:
        columns[IS_DELETED] = {"data_type": "bool", "nullable": False}
    return columns


@dataclass(frozen=True)
class BronzeTable:
    name: str
    primary_key: str
    columns: Mapping[str, TDataType]
    source_updated_at: str | None = None
    """Payload field holding the record's last-modified time, if the source has one."""
    deleted_flag: str | None = None
    """Payload field holding the record's deleted status. Tables without one have no
    `_is_deleted` column."""

    @property
    def tracks_deletes(self) -> bool:
        return self.deleted_flag is not None

    def column_schemas(self) -> TTableSchemaColumns:
        """dlt column hints: declared source columns followed by the metadata columns."""
        columns: TTableSchemaColumns = {
            name: {"data_type": data_type, "nullable": name != self.primary_key}
            for name, data_type in self.columns.items()
        }
        columns[self.primary_key]["primary_key"] = True
        columns.update(_metadata_columns(tracks_deletes=self.tracks_deletes))
        return columns

    def from_payload(
        self,
        payload: Mapping[str, Any],
        *,
        load_id: str,
        raw_load_id: str,
        loaded_at: datetime,
    ) -> dict[str, Any]:
        """Project a verbatim API payload onto the bronze columns."""
        row = {name: payload.get(name) for name in self.columns}
        updated_at = payload.get(self.source_updated_at) if self.source_updated_at else None
        row[LOAD_ID] = load_id
        row[RAW_LOAD_ID] = raw_load_id
        row[LOADED_AT] = loaded_at
        row[SOURCE_UPDATED_AT] = parse_utc(updated_at) if updated_at else None
        if self.deleted_flag is not None:
            row[IS_DELETED] = bool(payload.get(self.deleted_flag))
        return row

    def from_bronze(
        self, stored: Mapping[str, Any], *, load_id: str, loaded_at: datetime
    ) -> dict[str, Any]:
        """Rebuild a writable row from one read back from bronze."""
        row = {
            name: json.loads(value)
            if self.columns.get(name) == "json" and isinstance(value, str)
            else value
            for name, value in stored.items()
            if not name.startswith(_DLT_COLUMN_PREFIX)
        }
        row[LOAD_ID] = load_id
        row[LOADED_AT] = loaded_at
        return row


def _load(name: str) -> BronzeTable:
    spec = yaml.safe_load(resources.files(__package__).joinpath(name).read_text())
    return BronzeTable(
        name=spec["name"],
        primary_key=spec["primary_key"],
        columns=cast(dict[str, TDataType], spec["columns"]),
        source_updated_at=spec.get("source_updated_at"),
        deleted_flag=spec.get("deleted_flag"),
    )


BRONZE_TABLES: Mapping[str, BronzeTable] = {
    table.name: table
    for table in (
        _load(entry.name)
        for entry in sorted(resources.files(__package__).iterdir(), key=lambda e: e.name)
        if entry.name.endswith(".yml")
    )
}
