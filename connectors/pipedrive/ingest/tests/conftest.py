from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
import requests_mock
from fakes import API_TOKEN, FakePipedrive

from pipedrive_ingest.source import DEFAULT_BASE_URL, ENTITIES


@pytest.fixture
def lake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Local lake root and isolated dlt working directory."""
    root = tmp_path / "lake"
    monkeypatch.setenv("LAKE_ROOT", root.as_uri())
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setenv("SOURCES__PIPEDRIVE__API_TOKEN", API_TOKEN)
    monkeypatch.setenv("RUNTIME__LOG_LEVEL", "WARNING")
    return root


@pytest.fixture
def api() -> Iterator[FakePipedrive]:
    fake = FakePipedrive()
    with requests_mock.Mocker() as mocker:
        for entity in ENTITIES:
            for endpoint in entity.endpoints:
                mocker.get(f"{DEFAULT_BASE_URL}/{endpoint.path}", json=fake.respond)
        yield fake
