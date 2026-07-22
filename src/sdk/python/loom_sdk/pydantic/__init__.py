"""Pydantic-based ontology declaration layer for loom_sdk (the `pydantic`
opt-in extra — `pip install loom-sdk[pydantic]`).

The class declaration *is* the ontology type:

```python
class Customer(LoomModel, table=("crm", "customers")):
    customer_id: Identity[int]
    name: str

class Order(LoomModel, table=("crm", "orders")):
    order_id: Identity[int]
    customer: Link[Customer]  # FK-backed link; lands as customer_id
```

`table=` is a required class keyword argument; subclasses must declare
exactly one `Identity[T]` field. All `__loom_*__` derived attributes are
computed once, synchronously, during the declaring subclass's own
class-creation — by `LoomModel.__pydantic_init_subclass__`, which pydantic
calls after it has finished collecting `model_fields` for that subclass.
`LoomModel` itself is exempt from the `table=` requirement: the hook only
ever fires for subclasses, never for the class that defines it.

This top-level package (and its `_mapping` submodule) require the
`pydantic` extra; importing it without pydantic installed raises a clear
`ImportError` here rather than a confusing one from deep inside pydantic's
own import machinery.
"""

from __future__ import annotations

import dataclasses
import typing

try:
    import pydantic
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError("install loom-sdk[pydantic]") from exc

from ._mapping import arrow_schema, loom_type, model_gate

__all__ = [
    "Identity",
    "Link",
    "LoomModel",
    "arrow_schema",
    "loom_type",
    "model_gate",
]

T = typing.TypeVar("T")

LOOM_IDENTITY = object()
"""Sentinel placed in `Annotated` metadata by `Identity[T]`."""

Identity = typing.Annotated[T, LOOM_IDENTITY]
"""Marks a `LoomModel` field as the type's identity property.

`customer_id: Identity[int]` is `Annotated[int, LOOM_IDENTITY]`. Exactly one
field per `LoomModel` subclass must carry this marker — zero or two-or-more
raises `TypeError` at class-creation time.
"""


@dataclasses.dataclass(frozen=True)
class _LinkSpec:
    """One resolved FK-backed link on a `LoomModel` subclass."""

    field: str
    target: type["LoomModel"]
    fk_column: str


class Link:
    """`Annotated` metadata marking a field as an FK-backed link to `target`.

    `customer: Link[Customer]` resolves eagerly — at the point the
    annotation expression is evaluated, i.e. during the *declaring* class's
    own class-creation — to `Annotated[<Customer's identity python type>,
    Link(Customer)]`. The FK column defaults to the target's identity
    property name; override it with `Link[Customer, "other_customer_id"]`
    when two links on the same class target the same type (otherwise their
    FK columns collide and class-creation raises `TypeError`).
    """

    def __init__(self, target: type["LoomModel"], column: str | None = None) -> None:
        self.target = target
        self.column = column

    def __class_getitem__(cls, params: object) -> object:
        target, column = params if isinstance(params, tuple) else (params, None)
        identity_name = getattr(target, "__loom_identity__", None)
        if identity_name is None:
            raise TypeError(
                f"Link target {target!r} has no loom identity "
                "(not an applied LoomModel subclass with an Identity field)"
            )
        identity_ty = target.model_fields[identity_name].annotation
        return typing.Annotated[identity_ty, cls(target, column)]


class LoomModel(pydantic.BaseModel):
    """Base class for pydantic-declared ontology types.

    Subclasses MUST pass `table=(schema, table_name)` as a class keyword
    argument. This base class itself is exempt — `__pydantic_init_subclass__`
    only fires for subclasses, never for the class defining it.
    """

    __loom_table__: typing.ClassVar[tuple[str, str]]
    __loom_identity__: typing.ClassVar[str]
    __loom_properties__: typing.ClassVar[list[tuple[str, str, bool]]]
    __loom_links__: typing.ClassVar[list[_LinkSpec]]

    def __init_subclass__(cls, **kwargs: object) -> None:
        # Python's own class-kwarg machinery (via ABCMeta.__new__, which
        # pydantic's metaclass delegates to) routes `table=...` here before
        # `__pydantic_init_subclass__` below ever runs — and the default
        # `object.__init_subclass__` rejects unknown kwargs outright. Swallow
        # it here; `__pydantic_init_subclass__` receives the same `table`
        # value separately (pydantic invokes it after `model_fields` are
        # collected) and does the real class-machinery work.
        super().__init_subclass__()

    @classmethod
    def __pydantic_init_subclass__(cls, *, table: tuple[str, str] | None = None, **kwargs: object) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if table is None:
            raise TypeError(f"{cls.__name__} must declare table=(schema, name)")

        properties: list[tuple[str, str, bool]] = []
        links: list[_LinkSpec] = []
        identity_fields: list[str] = []
        seen_fk_columns: set[str] = set()

        for field_name, field_info in cls.model_fields.items():
            link_marker = next(
                (meta for meta in field_info.metadata if isinstance(meta, Link)),
                None,
            )
            is_identity = any(meta is LOOM_IDENTITY for meta in field_info.metadata)

            try:
                ty, required = loom_type(field_info.annotation)
            except TypeError as exc:
                raise TypeError(f"{cls.__name__}.{field_name}: {exc}") from exc

            if link_marker is not None:
                fk_column = link_marker.column or link_marker.target.__loom_identity__
                if fk_column in seen_fk_columns:
                    raise TypeError(
                        f"{cls.__name__}: duplicate FK column {fk_column!r} "
                        f"(field {field_name!r}); pass Link[Target, \"other_column\"] "
                        "to disambiguate"
                    )
                seen_fk_columns.add(fk_column)
                properties.append((fk_column, ty, required))
                links.append(_LinkSpec(field=field_name, target=link_marker.target, fk_column=fk_column))
                property_name = fk_column
            else:
                properties.append((field_name, ty, required))
                property_name = field_name

            if is_identity:
                identity_fields.append(property_name)

        if len(identity_fields) != 1:
            raise TypeError(
                f"{cls.__name__} must declare exactly one Identity field, found {len(identity_fields)}"
            )

        cls.__loom_table__ = table
        cls.__loom_identity__ = identity_fields[0]
        cls.__loom_properties__ = properties
        cls.__loom_links__ = links
