"""Timestamp parsing for Pipedrive payloads.

API v2 returns RFC 3339 timestamps (`2024-01-09T09:31:15Z`); API v1 returns
`YYYY-MM-DD HH:MM:SS`. Both are UTC.
"""

from datetime import UTC, datetime


def parse_utc(value: str) -> datetime:
    """Parse a Pipedrive timestamp, treating values without an offset as UTC."""
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def to_rfc3339(value: datetime) -> str:
    """Format as the RFC 3339 form accepted by `updated_since`, truncated to seconds."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
