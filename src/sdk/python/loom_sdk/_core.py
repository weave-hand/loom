"""Sans-IO request/response core shared by the sync and async clients.

Every function here is pure: no network I/O, no httpx dependency beyond the
types it hands back to the transport shells (`client.py`/`aclient.py`), which
are the only place sync/async duplication is allowed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class PreparedRequest:
    """A fully-formed, not-yet-sent HTTP request against one loom service."""

    method: str
    service: str  # "ingest" | "query"
    path: str
    params: dict[str, str]
    headers: dict[str, str]
    content: bytes | None


def login_request(username: str, password: str) -> PreparedRequest:
    """Build the `POST /auth/login` request (routed to the query service)."""
    content = json.dumps({"username": username, "password": password}).encode("utf-8")
    return PreparedRequest(
        method="POST",
        service="query",
        path="/auth/login",
        params={},
        headers={"Content-Type": "application/json"},
        content=content,
    )


def parse_login(body: bytes) -> str:
    """Extract the bearer token from a `POST /auth/login` 200 response body."""
    data = json.loads(body)
    return str(data["token"])


def resolve_base(service: str, ingest_url: str | None, query_url: str | None) -> str:
    """Pick the base URL for `service` (`"ingest"` or `"query"`).

    Raises `ValueError` if the service is unknown, or if the base URL it
    needs was never configured (neither the single-`url` form nor the
    per-service one).
    """
    if service == "ingest":
        base = ingest_url
    elif service == "query":
        base = query_url
    else:
        raise ValueError(f"unknown service: {service!r}")
    if base is None:
        raise ValueError(f"no base URL configured for the {service!r} service")
    return base


def build_headers(headers: dict[str, str], token: str | None) -> dict[str, str]:
    """Merge request headers with the bearer `Authorization` header, if any."""
    merged = dict(headers)
    if token is not None:
        merged["Authorization"] = f"Bearer {token}"
    return merged
