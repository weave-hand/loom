"""Sans-IO request/response core shared by the sync and async clients.

Every function here is pure: no network I/O, no httpx dependency beyond the
types it hands back to the transport shells (`client.py`/`aclient.py`), which
are the only place sync/async duplication is allowed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .models import LandAck, ModelLandAck


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


_ARROW_STREAM_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


def land_dataset_request(
    schema: str,
    table: str,
    ipc: bytes,
    *,
    mode: str | None = None,
    buckets: int | None = None,
    model_gate: list[dict] | None = None,
    run_id: str | None = None,
) -> PreparedRequest:
    """Build `POST /datasets/{schema}/{table}` (routed to the ingest service).

    `model_gate` (when given) is sent as the `X-Loom-Model` header —
    `{"columns": model_gate}` — required for zero-row bootstrap of
    date/timestamp columns, which Arrow-schema inference alone rejects.
    """
    params: dict[str, str] = {}
    if mode is not None:
        params["mode"] = mode
    if buckets is not None:
        params["buckets"] = str(buckets)

    headers = {"Content-Type": _ARROW_STREAM_CONTENT_TYPE}
    if model_gate is not None:
        headers["X-Loom-Model"] = json.dumps({"columns": model_gate})
    if run_id is not None:
        headers["X-Loom-Run-Id"] = run_id

    return PreparedRequest(
        method="POST",
        service="ingest",
        path=f"/datasets/{schema}/{table}",
        params=params,
        headers=headers,
        content=ipc,
    )


def land_model_request(
    type_name: str,
    ipc: bytes,
    *,
    identity: str | None = None,
    mode: str | None = None,
    buckets: int | None = None,
    merge_engine: str | None = None,
) -> PreparedRequest:
    """Build `POST /models/{type_name}` (routed to the ingest service)."""
    params: dict[str, str] = {}
    if identity is not None:
        params["identity"] = identity
    if mode is not None:
        params["mode"] = mode
    if buckets is not None:
        params["buckets"] = str(buckets)
    if merge_engine is not None:
        params["merge_engine"] = merge_engine

    return PreparedRequest(
        method="POST",
        service="ingest",
        path=f"/models/{type_name}",
        params=params,
        headers={"Content-Type": _ARROW_STREAM_CONTENT_TYPE},
        content=ipc,
    )


def parse_land_ack(body: bytes) -> LandAck:
    """Parse a `POST /datasets/{schema}/{table}` 200 response body."""
    data = json.loads(body)
    return LandAck(snapshot_id=int(data["snapshot_id"]), dataset=str(data["dataset"]))


def parse_model_ack(body: bytes) -> ModelLandAck:
    """Parse a `POST /models/{type}` 200 response body (wire key is `type`)."""
    data = json.loads(body)
    return ModelLandAck(snapshot_id=int(data["snapshot_id"]), type_name=str(data["type"]))
