"""Sans-IO algorithm for `client.ontology.apply` / `client.models.land_instances`.

Every function here is pure: no network I/O. It duck-types off a
`LoomModel` subclass's `__loom_table__`/`__loom_identity__`/
`__loom_properties__`/`__loom_links__` class attributes (never `import
pydantic`, matching `_mapping.py`'s existing convention) and off an
instance's plain attribute access — so it composes cleanly with either the
sync (`client.py`) or async (`aclient.py`) transport shell, which are the
only places sync/async duplication is allowed. Each shell:

1. builds a `PreparedRequest` via a function here and sends it,
2. feeds the parsed response (or a caught `NotFoundError`) back into a pure
   comparison/filter function here,
3. accumulates the `ApplyReport`/`ModelLandAck` fields.

See `client._OntologyNamespace.apply` / `client._ModelsNamespace.land_instances`
for the short orchestration loop each shell drives this with (the loop is
duplicated sync/async by design and kept line-identical across the two shells).
"""

from __future__ import annotations

import typing

from .. import _core
from .._arrow import empty_ipc, to_ipc
from ..errors import OntologyDriftError
from ._mapping import arrow_schema, model_gate

if typing.TYPE_CHECKING:
    from .._core import PreparedRequest
    from ..models import TypeDetail
    from . import _LinkSpec


def type_check_request(cls: type) -> PreparedRequest:
    """Build the `GET /ontology/types/{name}` request for `cls` (step 1)."""
    return _core.ontology_type_request(cls.__name__)


def diff_type(cls: type, detail: TypeDetail) -> None:
    """Compare an existing ontology type's shape against `cls`'s declaration.

    Checked in order: identity, properties (as `(name, ty, required)`
    sets), table. Raises `OntologyDriftError` naming the first difference
    found; returns `None` (i.e. "unchanged") when everything matches.
    """
    if detail.identity != cls.__loom_identity__:
        raise OntologyDriftError(
            f"{cls.__name__}: identity drift — server has {detail.identity!r}, "
            f"declared {cls.__loom_identity__!r}"
        )

    declared_properties = set(cls.__loom_properties__)
    server_properties = {(p.name, p.ty, p.required) for p in detail.properties}
    if server_properties != declared_properties:
        raise OntologyDriftError(
            f"{cls.__name__}: properties drift — server has {sorted(server_properties)!r}, "
            f"declared {sorted(declared_properties)!r}"
        )

    declared_table = tuple(cls.__loom_table__)
    server_table = (detail.table.schema, detail.table.name)
    if server_table != declared_table:
        raise OntologyDriftError(
            f"{cls.__name__}: table drift — server has {server_table!r}, declared {declared_table!r}"
        )


def dataset_check_request(cls: type) -> PreparedRequest:
    """Build the `GET /datasets/{schema}/{table}` request for `cls` (step 2)."""
    schema, table = cls.__loom_table__
    return _core.get_dataset_request(schema, table)


def land_bootstrap_request(cls: type) -> PreparedRequest:
    """Build the zero-row bootstrap `POST /datasets/{schema}/{table}` request.

    Ships an empty Arrow IPC stream carrying `cls`'s Arrow schema, gated by
    `cls`'s `X-Loom-Model` header — required so date/timestamp columns
    survive (Arrow-schema inference alone rejects them).
    """
    schema, table = cls.__loom_table__
    ipc = empty_ipc(arrow_schema(cls))
    return _core.land_dataset_request(schema, table, ipc, model_gate=model_gate(cls))


def define_model_request_for(cls: type) -> PreparedRequest:
    """Build the `POST /admin/models` request registering `cls` (step 3)."""
    schema, table = cls.__loom_table__
    payload = _core.model_payload(
        cls.__name__,
        schema,
        table,
        cls.__loom_identity__,
        cls.__loom_properties__,
    )
    return _core.define_model_request(payload)


def link_name(from_cls: type, spec: _LinkSpec) -> str:
    """The `<from>_<field>` name a link spec registers under."""
    return f"{from_cls.__name__}_{spec.field}"


def links_to_create(from_cls: type, current_link_names: list[str]) -> list[_LinkSpec]:
    """`from_cls.__loom_links__` entries not already present in `current_link_names`.

    `current_link_names` is either the `from`-type's current `links` (from
    a matching `GET /ontology/types/{name}` 200) or `[]` for a type just
    created by this same `apply` call.
    """
    return [spec for spec in from_cls.__loom_links__ if link_name(from_cls, spec) not in current_link_names]


def define_link_request_for(from_cls: type, spec: _LinkSpec) -> PreparedRequest:
    """Build the `POST /admin/links` request for one link spec (step 4).

    `from_column` is the FK column on `from_cls`; `to_column` is the target
    type's identity property. Cardinality is `fk_link_payload`'s default
    `"One"` — an FK field on the declaring type points at exactly one
    target.
    """
    payload = _core.fk_link_payload(
        link_name(from_cls, spec),
        from_cls.__name__,
        spec.target.__name__,
        spec.fk_column,
        spec.target.__loom_identity__,
    )
    return _core.define_link_request(payload)


def _field_for_property(cls: type) -> dict[str, str]:
    """Map each `__loom_properties__` name to the instance attribute that holds it.

    Link-backed properties (FK columns) are read off the link's pydantic
    *field* name (e.g. `customer_id` <- `instance.customer`), not the
    column name itself; plain properties are read off their own name.
    """
    mapping = {spec.fk_column: spec.field for spec in cls.__loom_links__}
    for name, _ty, _required in cls.__loom_properties__:
        mapping.setdefault(name, name)
    return mapping


def instance_row(instance: object) -> dict[str, object]:
    """Flatten one `LoomModel` instance to a `{column_name: value}` row.

    FK link fields are written under their `fk_column` name (per
    `_field_for_property`), not the pydantic field name used to construct
    the instance.
    """
    cls = type(instance)
    return {prop_name: getattr(instance, field_name) for prop_name, field_name in _field_for_property(cls).items()}


def land_instances_request(instances: typing.Sequence[object]) -> PreparedRequest:
    """Build the `POST /models/{type}` request landing `instances`.

    Raises `ValueError` if `instances` is empty or mixes classes — all
    instances must be the exact same `LoomModel` subclass.
    """
    if not instances:
        raise ValueError("land_instances requires at least one instance")
    cls = type(instances[0])
    if any(type(instance) is not cls for instance in instances):
        raise ValueError("land_instances requires all instances to be the same LoomModel subclass")

    schema = arrow_schema(cls)
    rows = [instance_row(instance) for instance in instances]
    ipc = to_ipc(rows, schema=schema)
    return _core.land_model_request(cls.__name__, ipc)
