"""Wire DTOs for loom_sdk: write acknowledgements, read views, and reports.

Plain dataclasses, no pydantic (the pydantic-based ontology layer is a
separate opt-in module).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class LandAck:
    """200 response from `POST /datasets/{schema}/{table}`."""

    snapshot_id: int
    dataset: str


@dataclass
class ModelLandAck:
    """200 response from `POST /models/{type}` (wire key is `type`)."""

    snapshot_id: int
    type_name: str


@dataclass
class TableRef:
    """A `{schema, name}` physical table reference."""

    schema: str
    name: str


@dataclass
class PropertyView:
    """One ontology-type property, as rendered by `GET /ontology/types/{name}`.

    `description` is `None` when the wire response omits the field (never
    sent as JSON `null`).
    """

    name: str
    ty: str
    required: bool
    description: str | None = None


@dataclass
class LinkView:
    """One ontology link, as rendered by `GET /ontology/types/{name}`.

    Wire JSON keys `from`/`to` map to `from_type`/`to_type`; `cardinality` is
    normalized to lowercase (`"one"`/`"many"`). `description` is `None` when
    the wire response omits the field.
    """

    name: str
    from_type: str
    to_type: str
    cardinality: str
    description: str | None = None


@dataclass
class TypeDetail:
    """200 response from `GET /ontology/types/{name}`."""

    name: str
    table: TableRef
    identity: str | None
    properties: list[PropertyView]
    links: list[LinkView]
    links_to: list[LinkView]
    description: str | None = None


@dataclass
class DatasetEntry:
    """One entry of the `GET /datasets` 200 response body.

    `base` is `None` for a physical table entry (`kind == "table"`); present
    for a view entry (`kind == "view"`).
    """

    schema: str
    name: str
    project: str
    updated: str
    kind: str
    base: str | None = None


@dataclass
class ColumnView:
    """One physical column, as rendered by `GET /datasets/{schema}/{table}`."""

    name: str
    ty: str
    nullable: bool


@dataclass
class DatasetDetail:
    """200 response from `GET /datasets/{schema}/{table}`."""

    table: TableRef
    snapshot_id: int
    snapshot_time: str
    columns: list[ColumnView]
    kind: str
    base: str | None = None


@dataclass
class ApplyReport:
    """Result of `client.ontology.apply(*models)` (the pydantic extra).

    `created` / `unchanged` hold ontology type names; `links_created` holds
    `<from>_<field>` link names. A type name appears in exactly one of
    `created`/`unchanged` per `apply` call.
    """

    created: list[str]
    unchanged: list[str]
    links_created: list[str]


@dataclass
class Preview:
    """200 response from `GET /datasets/{schema}/{table}/preview`.

    Every cell is a display string on the wire (nulls render as `""`).
    """

    columns: list[str]
    rows: list[list[str]]
    sampled: bool
