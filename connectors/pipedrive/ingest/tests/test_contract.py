"""The manifest, the ingest code, the bronze schemas, and the dbt sources must agree."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from pipedrive_ingest import SOURCE_NAME
from pipedrive_ingest.schemas import BRONZE_TABLES
from pipedrive_ingest.source import ENTITIES

CONNECTOR_DIR = Path(__file__).resolve().parents[2]
MODELS_DIR = CONNECTOR_DIR / "transform" / "models"


def _yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text())


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    data: dict[str, Any] = _yaml(CONNECTOR_DIR / "connector.yml")
    return data


@pytest.fixture(scope="module")
def dbt_sources() -> dict[str, list[str]]:
    [source] = _yaml(MODELS_DIR / "staging" / "_pipedrive__sources.yml")["sources"]
    assert source["name"] == SOURCE_NAME
    assert source["schema"] == SOURCE_NAME
    return {t["name"]: [c["name"] for c in t["columns"]] for t in source["tables"]}


def test_manifest_matches_ingest_entities(manifest: dict[str, Any]) -> None:
    assert manifest["name"] == SOURCE_NAME
    declared = [
        (e["name"], e["sync"], e.get("cursor"), e["bronze_table"]) for e in manifest["entities"]
    ]
    implemented = [(e.name, e.sync.value, e.cursor, f"{SOURCE_NAME}.{e.name}") for e in ENTITIES]
    assert declared == implemented


def test_manifest_version_matches_package_and_changelog(manifest: dict[str, Any]) -> None:
    pyproject = (CONNECTOR_DIR / "ingest" / "pyproject.toml").read_text()
    changelog = (CONNECTOR_DIR / "CHANGELOG.md").read_text()
    assert f'version = "{manifest["version"]}"' in pyproject
    assert f"## [{manifest['version']}]" in changelog


def test_every_entity_has_a_bronze_schema() -> None:
    assert sorted(BRONZE_TABLES) == sorted(e.name for e in ENTITIES)


def test_dbt_sources_declare_the_bronze_columns(dbt_sources: dict[str, list[str]]) -> None:
    assert sorted(dbt_sources) == sorted(BRONZE_TABLES)
    for name, table in BRONZE_TABLES.items():
        assert dbt_sources[name] == list(table.column_schemas()), name


def test_manifest_models_read_the_bronze_table(manifest: dict[str, Any]) -> None:
    sql = {path.stem: path.read_text() for path in MODELS_DIR.rglob("*.sql")}
    for entity in manifest["entities"]:
        source_ref = re.compile(rf"pipedrive_source\('{entity['name']}'\)")
        readers = {model for model, text in sql.items() if source_ref.search(text)}
        listed = set(entity["models"])
        # Custom field decoding in staging reads the *_fields tables through a macro.
        decode = re.compile(rf"pipedrive_decode_custom_fields\('{entity['name']}'")
        decoders = {model for model, text in sql.items() if decode.search(text)}
        assert listed == readers | decoders, entity["name"]
