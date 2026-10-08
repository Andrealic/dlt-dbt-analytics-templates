"""Test doubles and record builders for the Pipedrive API."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlparse

from pipedrive_ingest.timestamps import parse_utc

API_TOKEN = "test-token"


@dataclass
class FakePipedrive:
    """In-memory Pipedrive API serving the endpoints the connector uses."""

    records: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    requests: list[tuple[str, dict[str, str], dict[str, str]]] = field(default_factory=list)

    def set(self, path: str, records: list[dict[str, Any]]) -> None:
        self.records[path] = records

    def calls(self, path: str) -> list[dict[str, str]]:
        """Query parameters of every request made to `path`."""
        return [params for called, params, _ in self.requests if called == path]

    def respond(self, request: Any, context: Any) -> dict[str, Any]:
        path = urlparse(request.url).path.lstrip("/")
        params = {key: values[0] for key, values in parse_qs(urlparse(request.url).query).items()}
        self.requests.append((path, params, dict(request.headers)))
        if request.headers.get("x-api-token") != API_TOKEN:
            context.status_code = 401
            return {"success": False}

        records = self.records.get(path, [])
        if "updated_since" in params:
            since = parse_utc(params["updated_since"])
            records = [r for r in records if parse_utc(r["update_time"]) >= since]
        if path.startswith("api/v1/"):
            return {"success": True, "data": records}

        start = int(params.get("cursor", 0))
        end = start + int(params.get("limit", 100))
        next_cursor = str(end) if end < len(records) else None
        return {
            "success": True,
            "data": records[start:end],
            "additional_data": {"next_cursor": next_cursor},
        }


def deal(deal_id: int, update_time: str, **fields: Any) -> dict[str, Any]:
    return {
        "id": deal_id,
        "title": f"Deal {deal_id}",
        "owner_id": 1,
        "pipeline_id": 1,
        "stage_id": 1,
        "value": 1000,
        "currency": "EUR",
        "status": "open",
        "is_deleted": False,
        "is_archived": False,
        "add_time": "2026-01-01T09:00:00Z",
        "update_time": update_time,
        "label_ids": [3],
        "custom_fields": {"a" * 40: 12, "b" * 40: {"value": 50, "currency": "EUR"}},
        **fields,
    }


def person(person_id: int, update_time: str = "2026-01-01T09:00:00Z") -> dict[str, Any]:
    return {
        "id": person_id,
        "name": f"Person {person_id}",
        "owner_id": 1,
        "is_deleted": False,
        "emails": [{"value": f"p{person_id}@example.com", "primary": True, "label": "work"}],
        "add_time": "2026-01-01T09:00:00Z",
        "update_time": update_time,
        "custom_fields": {},
    }


def stage(stage_id: int) -> dict[str, Any]:
    return {
        "id": stage_id,
        "pipeline_id": 1,
        "name": f"Stage {stage_id}",
        "order_nr": stage_id,
        "is_deleted": False,
        "add_time": "2026-01-01T09:00:00Z",
        "update_time": "2026-01-01T09:00:00Z",
    }
