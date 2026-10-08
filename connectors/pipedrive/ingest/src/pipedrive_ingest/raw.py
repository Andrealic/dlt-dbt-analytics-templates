"""Read access to the raw layer.

Raw files follow `<root>/<source>/<table>/load_date=YYYY-MM-DD/<load_id>.<file_id>.jsonl.gz`.
dlt may split one table's load into several files; they share the `load_id`.
"""

from __future__ import annotations

import gzip
import json
import posixpath
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from dlt.common.configuration import known_sections, resolve_configuration
from dlt.common.storages import FilesystemConfiguration, fsspec_from_config

from pipedrive_ingest import SOURCE_NAME
from pipedrive_ingest.source import FULL_LISTINGS_TABLE

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

RAW_FILE_SUFFIX = ".jsonl.gz"


def load_order(load_id: str) -> Decimal:
    """dlt load ids are Unix timestamps with a fractional part; compare them numerically."""
    return Decimal(load_id)


@dataclass(frozen=True)
class RawLoad:
    load_id: str
    files: tuple[str, ...] = ()


class RawStore:
    def __init__(self, fs: AbstractFileSystem, root: str) -> None:
        self._fs = fs
        self._root = root.rstrip("/")

    @classmethod
    def from_bucket_url(cls, bucket_url: str) -> RawStore:
        """Open raw with the same credentials the raw destination uses."""
        config = resolve_configuration(
            FilesystemConfiguration(bucket_url=bucket_url),
            sections=(known_sections.DESTINATION, "filesystem"),
        )
        fs, root = fsspec_from_config(config)
        return cls(fs, root)

    def loads(self, table: str, after: str | None = None) -> list[RawLoad]:
        """Raw loads of `table` newer than `after`, oldest first."""
        pattern = f"{self._root}/{SOURCE_NAME}/{table}/load_date=*/*{RAW_FILE_SUFFIX}"
        files: dict[str, list[str]] = defaultdict(list)
        for path in self._fs.glob(pattern):
            files[_load_id_from_path(path)].append(path)
        return [
            RawLoad(load_id, tuple(sorted(paths)))
            for load_id, paths in sorted(files.items(), key=lambda item: load_order(item[0]))
            if after is None or load_order(load_id) > load_order(after)
        ]

    def read(self, load: RawLoad) -> Iterator[dict[str, Any]]:
        """Raw lines of one load: `payload` plus dlt's load columns."""
        for path in load.files:
            with self._fs.open(path, "rb") as handle, gzip.open(handle, "rt") as lines:
                for line in lines:
                    yield json.loads(line)

    def full_listings(self) -> dict[str, set[str]]:
        """Load ids holding a complete listing, per entity."""
        listings: dict[str, set[str]] = defaultdict(set)
        for load in self.loads(FULL_LISTINGS_TABLE):
            for line in self.read(load):
                listings[line["entity"]].add(line["_dlt_load_id"])
        return dict(listings)


def _load_id_from_path(path: str) -> str:
    stem = posixpath.basename(path).removesuffix(RAW_FILE_SUFFIX)
    load_id, _file_id = stem.rsplit(".", 1)
    return load_id
