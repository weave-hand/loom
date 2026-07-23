"""Sans-IO request/response core shared by the sync and async clients.

Every function here is pure: no network I/O, no httpx dependency beyond the
types it hands back to the transport shells (`client.py`/`aclient.py`), which
are the only place sync/async duplication is allowed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .models import (
    ColumnView,
    DatasetDetail,
    DatasetEntry,
    GrantEntry,
    LandAck,
    LinkView,
    ModelLandAck,
    Preview,
    PropertyView,
    TableRef,
    TypeDetail,
)


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


def ontology_types_request() -> PreparedRequest:
    """Build `GET /ontology/types` (routed to the query service)."""
    return PreparedRequest(
        method="GET",
        service="query",
        path="/ontology/types",
        params={},
        headers={},
        content=None,
    )


def parse_ontology_types(body: bytes) -> list[str]:
    """Parse a `GET /ontology/types` 200 response body."""
    data = json.loads(body)
    return [str(name) for name in data["types"]]


def ontology_type_request(name: str) -> PreparedRequest:
    """Build `GET /ontology/types/{name}` (routed to the query service)."""
    return PreparedRequest(
        method="GET",
        service="query",
        path=f"/ontology/types/{name}",
        params={},
        headers={},
        content=None,
    )


def _parse_table_ref(data: dict) -> TableRef:
    return TableRef(schema=str(data["schema"]), name=str(data["name"]))


def _parse_property_view(data: dict) -> PropertyView:
    return PropertyView(
        name=str(data["name"]),
        ty=str(data["ty"]),
        required=bool(data["required"]),
        description=data.get("description"),
    )


def _parse_link_view(data: dict) -> LinkView:
    return LinkView(
        name=str(data["name"]),
        from_type=str(data["from"]),
        to_type=str(data["to"]),
        cardinality=str(data["cardinality"]).lower(),
        description=data.get("description"),
    )


def parse_type_detail(body: bytes) -> TypeDetail:
    """Parse a `GET /ontology/types/{name}` 200 response body.

    `from`/`to` JSON keys on each link map to `from_type`/`to_type`;
    `cardinality` is normalized to lowercase.
    """
    data = json.loads(body)
    return TypeDetail(
        name=str(data["name"]),
        table=_parse_table_ref(data["table"]),
        identity=data.get("identity"),
        properties=[_parse_property_view(p) for p in data["properties"]],
        links=[_parse_link_view(link) for link in data["links"]],
        links_to=[_parse_link_view(link) for link in data["links_to"]],
        description=data.get("description"),
    )


def list_datasets_request() -> PreparedRequest:
    """Build `GET /datasets` (routed to the query service)."""
    return PreparedRequest(
        method="GET",
        service="query",
        path="/datasets",
        params={},
        headers={},
        content=None,
    )


def parse_dataset_list(body: bytes) -> list[DatasetEntry]:
    """Parse a `GET /datasets` 200 response body."""
    data = json.loads(body)
    return [
        DatasetEntry(
            schema=str(entry["schema"]),
            name=str(entry["name"]),
            project=str(entry["project"]),
            updated=str(entry["updated"]),
            kind=str(entry["kind"]),
            base=entry.get("base"),
        )
        for entry in data["datasets"]
    ]


def get_dataset_request(
    schema: str,
    table: str,
    *,
    as_of: str | None = None,
    as_of_snapshot: int | None = None,
) -> PreparedRequest:
    """Build `GET /datasets/{schema}/{table}` (routed to the query service).

    `as_of` (an RFC3339 timestamp) and `as_of_snapshot` (a snapshot id) are
    mutually exclusive: passing both raises `ValueError` before any request
    is built.
    """
    if as_of is not None and as_of_snapshot is not None:
        raise ValueError("as_of and as_of_snapshot are mutually exclusive")
    params: dict[str, str] = {}
    if as_of is not None:
        params["as_of"] = as_of
    if as_of_snapshot is not None:
        params["as_of_snapshot"] = str(as_of_snapshot)
    return PreparedRequest(
        method="GET",
        service="query",
        path=f"/datasets/{schema}/{table}",
        params=params,
        headers={},
        content=None,
    )


def parse_dataset_detail(body: bytes) -> DatasetDetail:
    """Parse a `GET /datasets/{schema}/{table}` 200 response body."""
    data = json.loads(body)
    return DatasetDetail(
        table=_parse_table_ref(data["table"]),
        snapshot_id=int(data["snapshot_id"]),
        snapshot_time=str(data["snapshot_time"]),
        columns=[
            ColumnView(name=str(c["name"]), ty=str(c["ty"]), nullable=bool(c["nullable"]))
            for c in data["columns"]
        ],
        kind=str(data["kind"]),
        base=data.get("base"),
    )


_VALID_CARDINALITIES = ("One", "Many")


def model_payload(
    name: str,
    schema: str,
    table: str,
    identity: str | None,
    properties: list[tuple[str, str, bool]],
    description: str | None = None,
) -> dict:
    """Build a `POST /admin/models` JSON body.

    `properties` is `(name, ty, required)` triples; `identity` is always
    emitted (even `null`) since the wire body always carries the key,
    `description` is omitted entirely when `None`.
    """
    payload: dict = {
        "name": name,
        "table": {"schema": schema, "name": table},
        "identity": identity,
        "properties": [
            {"name": prop_name, "ty": ty, "required": required} for prop_name, ty, required in properties
        ],
    }
    if description is not None:
        payload["description"] = description
    return payload


def fk_link_payload(
    name: str,
    from_type: str,
    to_type: str,
    from_column: str,
    to_column: str,
    cardinality: str = "One",
    description: str | None = None,
) -> dict:
    """Build a `POST /admin/links` JSON body with `ForeignKey` backing.

    `cardinality` defaults to `"One"` — an FK field on the declaring type
    points at exactly one target (the tree's canonical
    `LinkDef::fk("customer", "Order", "Customer", Cardinality::One, ...)`
    shape). Must be `"One"` or `"Many"`; `description` is omitted entirely
    when `None`.
    """
    if cardinality not in _VALID_CARDINALITIES:
        raise ValueError(f"cardinality must be one of {_VALID_CARDINALITIES!r}, got {cardinality!r}")
    payload: dict = {
        "name": name,
        "from": from_type,
        "to": to_type,
        "cardinality": cardinality,
        "backing": {"ForeignKey": {"from_column": from_column, "to_column": to_column}},
    }
    if description is not None:
        payload["description"] = description
    return payload


def define_model_request(payload: dict) -> PreparedRequest:
    """Build `POST /admin/models` (routed to the query service).

    `payload` is passed through verbatim as the JSON body — dict-level API;
    use `model_payload` to build a wire-conformant body.
    """
    content = json.dumps(payload).encode("utf-8")
    return PreparedRequest(
        method="POST",
        service="query",
        path="/admin/models",
        params={},
        headers={"Content-Type": "application/json"},
        content=content,
    )


def parse_define_model_ack(body: bytes) -> str:
    """Extract the type name from a `POST /admin/models` 201 response body."""
    data = json.loads(body)
    return str(data["name"])


def grant_payload(action: str, type: str | None, table: tuple[str, str] | None) -> dict:
    """Build the shared grant/revoke JSON body (`POST`/`DELETE .../grants`).

    Exactly one of `type` (an ontology type name) or `table` (a
    `(schema, name)` tuple) must be set — zero or both raises `ValueError`
    before any request is built, mirroring the server's 400. `action` is
    passed through verbatim (the server rejects anything but `read`/`write`).
    """
    if (type is None) == (table is None):
        raise ValueError("exactly one of type or table must be set")
    payload: dict = {"action": action}
    if type is not None:
        payload["type"] = type
    else:
        payload["table"] = {"schema": table[0], "name": table[1]}
    return payload


def parse_role_list(body: bytes) -> list[str]:
    """Parse a `{"roles": [...]}` body (role list and user-role list share it)."""
    data = json.loads(body)
    return [str(role) for role in data["roles"]]


def parse_create_role(body: bytes) -> str:
    """Extract the role id from a `POST /admin/roles` 201 response body."""
    data = json.loads(body)
    return str(data["role"])


def create_role_request(role: str) -> PreparedRequest:
    """Build `POST /admin/roles` (routed to the query service)."""
    return PreparedRequest(
        method="POST",
        service="query",
        path="/admin/roles",
        params={},
        headers={"Content-Type": "application/json"},
        content=json.dumps({"role": role}).encode("utf-8"),
    )


def list_roles_request() -> PreparedRequest:
    """Build `GET /admin/roles` (routed to the query service)."""
    return PreparedRequest(
        method="GET", service="query", path="/admin/roles", params={}, headers={}, content=None
    )


def delete_role_request(role: str) -> PreparedRequest:
    """Build `DELETE /admin/roles/{role}` (routed to the query service)."""
    return PreparedRequest(
        method="DELETE",
        service="query",
        path=f"/admin/roles/{role}",
        params={},
        headers={},
        content=None,
    )


def assign_role_request(role: str, username: str) -> PreparedRequest:
    """Build `PUT /admin/users/{username}/roles/{role}` (routed to the query service)."""
    return PreparedRequest(
        method="PUT",
        service="query",
        path=f"/admin/users/{username}/roles/{role}",
        params={},
        headers={},
        content=None,
    )


def user_roles_request(username: str) -> PreparedRequest:
    """Build `GET /admin/users/{username}/roles` (routed to the query service)."""
    return PreparedRequest(
        method="GET",
        service="query",
        path=f"/admin/users/{username}/roles",
        params={},
        headers={},
        content=None,
    )


def unassign_role_request(role: str, username: str) -> PreparedRequest:
    """Build `DELETE /admin/users/{username}/roles/{role}` (routed to the query service)."""
    return PreparedRequest(
        method="DELETE",
        service="query",
        path=f"/admin/users/{username}/roles/{role}",
        params={},
        headers={},
        content=None,
    )


def grant_request(
    role: str, action: str, type: str | None, table: tuple[str, str] | None
) -> PreparedRequest:
    """Build `POST /admin/roles/{role}/grants` (routed to the query service).

    Exactly one of `type`/`table` — validated by `grant_payload`, which
    raises `ValueError` before the request is built.
    """
    return PreparedRequest(
        method="POST",
        service="query",
        path=f"/admin/roles/{role}/grants",
        params={},
        headers={"Content-Type": "application/json"},
        content=json.dumps(grant_payload(action, type, table)).encode("utf-8"),
    )


def grant_list_request(role: str) -> PreparedRequest:
    """Build `GET /admin/roles/{role}/grants` (routed to the query service)."""
    return PreparedRequest(
        method="GET",
        service="query",
        path=f"/admin/roles/{role}/grants",
        params={},
        headers={},
        content=None,
    )


def revoke_request(
    role: str, action: str, type: str | None, table: tuple[str, str] | None
) -> PreparedRequest:
    """Build `DELETE /admin/roles/{role}/grants` (routed to the query service).

    Same body as `grant_request`; revoking an absent grant is a server-side no-op.
    """
    return PreparedRequest(
        method="DELETE",
        service="query",
        path=f"/admin/roles/{role}/grants",
        params={},
        headers={"Content-Type": "application/json"},
        content=json.dumps(grant_payload(action, type, table)).encode("utf-8"),
    )


def parse_grants(body: bytes) -> list[GrantEntry]:
    """Parse a `GET /admin/roles/{role}/grants` 200 response body.

    Each grant's `target` is the `PolicyTarget` serde shape: `{"Type": name}`
    or `{"Table": {"schema", "name"}}`. An unrecognized variant raises
    `ValueError` naming the shape rather than guessing.
    """
    data = json.loads(body)
    grants: list[GrantEntry] = []
    for row in data["grants"]:
        target = row["target"]
        if "Type" in target:
            grants.append(
                GrantEntry(
                    action=str(row["action"]),
                    effect=str(row["effect"]),
                    type=str(target["Type"]),
                    table=None,
                )
            )
        elif "Table" in target:
            tbl = target["Table"]
            grants.append(
                GrantEntry(
                    action=str(row["action"]),
                    effect=str(row["effect"]),
                    type=None,
                    table=TableRef(schema=str(tbl["schema"]), name=str(tbl["name"])),
                )
            )
        else:
            raise ValueError(f"unrecognized grant target shape: {target!r}")
    return grants


def define_link_request(payload: dict) -> PreparedRequest:
    """Build `POST /admin/links` (routed to the query service).

    `payload` is passed through verbatim as the JSON body — dict-level API;
    use `fk_link_payload` to build a wire-conformant `ForeignKey`-backed body.
    The 201 response is plain text (`"defined"`); there is nothing to parse.
    """
    content = json.dumps(payload).encode("utf-8")
    return PreparedRequest(
        method="POST",
        service="query",
        path="/admin/links",
        params={},
        headers={"Content-Type": "application/json"},
        content=content,
    )


def preview_dataset_request(schema: str, table: str, *, limit: int | None = None) -> PreparedRequest:
    """Build `GET /datasets/{schema}/{table}/preview` (routed to the query service)."""
    params: dict[str, str] = {}
    if limit is not None:
        params["limit"] = str(limit)
    return PreparedRequest(
        method="GET",
        service="query",
        path=f"/datasets/{schema}/{table}/preview",
        params=params,
        headers={},
        content=None,
    )


def parse_preview(body: bytes) -> Preview:
    """Parse a `GET /datasets/{schema}/{table}/preview` 200 response body."""
    data = json.loads(body)
    return Preview(
        columns=[str(c) for c in data["columns"]],
        rows=[[str(cell) for cell in row] for row in data["rows"]],
        sampled=bool(data["sampled"]),
    )
