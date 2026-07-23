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
    from pydantic.fields import FieldInfo
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError("install loom-sdk[pydantic]") from exc

from ._mapping import arrow_schema, loom_type, model_gate, optional_metadata

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


@dataclasses.dataclass(frozen=True)
class _PendingLink:
    """A string-target `Link[...]` awaiting second-pass resolution.

    Placed in `Annotated` metadata by `Link.__class_getitem__` when the link
    target is written as a string (`Link["Node", "parent_id"]`, the
    self-referential spelling). Carries the string target name and the
    optional explicit FK column; resolved in
    `LoomModel.__pydantic_init_subclass__`'s second pass, once the declaring
    class's identity is known.
    """

    name: str
    column: str | None


class Link:
    """`Annotated` metadata marking a field as an FK-backed link to `target`.

    `customer: Link[Customer]` resolves eagerly — at the point the
    annotation expression is evaluated, i.e. during the *declaring* class's
    own class-creation — to `Annotated[<Customer's identity python type>,
    Link(Customer)]`. The FK column defaults to the target's identity
    property name; override it with `Link[Customer, "other_customer_id"]`
    when two links on the same class target the same type (otherwise their
    FK columns collide and class-creation raises `TypeError`). `Link[X] |
    None` (a nullable FK) is supported: the FK property is emitted with
    `required=False` and the link spec is still emitted.

    **Ordering constraint (v1 limitation):** because resolution is eager,
    a *class-typed* `target` must already be a fully-defined `LoomModel`
    subclass (with its own `__loom_identity__` computed) at the point the
    declaring class's annotations are evaluated — in practice, declared
    textually before the class that links to it.

    **Self-referential links use the string spelling** —
    `parent: Link["Node", "parent_id"]` inside `class Node(LoomModel, ...)`.
    A string target is deferred to a second resolution pass in
    `LoomModel.__pydantic_init_subclass__` (once the class's identity is
    known); an explicit FK column is required because the default column (the
    target's identity name) always collides with the class's own identity.
    A *bare-class* self-link (`Link[Node]`) and general forward references (a
    string naming a class defined later, or a whole-string annotation like
    `parent: "Link[Node] | None"`) stay rejected with a `TypeError` naming
    the constraint — tracked as `fut-python-sdk-link-forward-refs`.
    """

    def __init__(self, target: type["LoomModel"], column: str | None = None) -> None:
        self.target = target
        self.column = column

    def __class_getitem__(cls, params: object) -> object:
        target, column = params if isinstance(params, tuple) else (params, None)
        if isinstance(target, (str, typing.ForwardRef)):
            name = target.__forward_arg__ if isinstance(target, typing.ForwardRef) else target
            # Deferred: the target might be the class currently under
            # construction (a self-referential link), whose identity isn't
            # known yet. `LoomModel.__pydantic_init_subclass__`'s second pass
            # resolves it (or rejects a non-self forward reference there).
            return typing.Annotated[typing.Any, _PendingLink(str(name), column)]
        if getattr(target, "__loom_building__", False):
            raise TypeError(
                f"Link target {target.__name__!r} is self-referential; write it "
                'as a string with an explicit FK column — Link["'
                f'{target.__name__}", "<fk_column>"] — a bare-class self-link is '
                "not supported in v1"
            )
        identity_name = getattr(target, "__loom_identity__", None)
        if identity_name is None:
            raise TypeError(
                f"Link target {target!r} has no loom identity "
                "(not an applied LoomModel subclass with an Identity field)"
            )
        identity_ty = target.model_fields[identity_name].annotation
        return typing.Annotated[identity_ty, cls(target, column)]


def _record_link(
    declaring_cls: type["LoomModel"],
    target: type["LoomModel"],
    column: str | None,
    field_name: str,
    ty: str,
    required: bool,
    *,
    properties: list[tuple[str, str, bool]],
    links: list[_LinkSpec],
    seen_properties: dict[str, tuple[str, str | None]],
) -> str:
    """Record one resolved FK link into `properties`/`links`, guarding the FK
    column against collision with an already-seen property (Finding 3).
    Returns the resolved FK column name.

    Shared by the eager (`Link[Class]`) and pending (`Link["Self", col]`)
    resolution paths — both arrive here with a concrete `target` and the FK
    column type already reduced to `(ty, required)`.
    """
    fk_column = column or target.__loom_identity__
    if fk_column in seen_properties:
        other_field, _ = seen_properties[fk_column]
        # A self-link's target is the class currently under construction, so
        # the bare-class spelling (Link[Target, ...]) is unwritable there —
        # referencing the class inside its own body raises NameError. Only
        # the string spelling (Link["Target", ...]) is actually usable.
        hint = (
            f'Link["{target.__name__}", "other_column"]'
            if target is declaring_cls
            else f'Link[{target.__name__}, "other_column"]'
        )
        raise TypeError(
            f"{declaring_cls.__name__}: property {fk_column!r} is declared by both "
            f"{other_field!r} and {field_name!r}; pass "
            f"{hint} to disambiguate"
        )
    seen_properties[fk_column] = (field_name, target.__name__)
    properties.append((fk_column, ty, required))
    links.append(_LinkSpec(field=field_name, target=target, fk_column=fk_column))
    return fk_column


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
    __loom_building__: typing.ClassVar[bool]
    """Internal: `True` from `__init_subclass__` until `__pydantic_init_subclass__`
    finishes computing the `__loom_*__` attributes above. Lets `Link.__class_getitem__`
    detect a self-referential link — the target resolves to this very class, still
    under construction — and raise a clear error instead of a misleading one."""

    def __init_subclass__(cls, **kwargs: object) -> None:
        # Python's own class-kwarg machinery (via ABCMeta.__new__, which
        # pydantic's metaclass delegates to) routes `table=...` here before
        # `__pydantic_init_subclass__` below ever runs — and the default
        # `object.__init_subclass__` rejects unknown kwargs outright. Swallow
        # it here; `__pydantic_init_subclass__` receives the same `table`
        # value separately (pydantic invokes it after `model_fields` are
        # collected) and does the real class-machinery work.
        #
        # This also fires early enough (before pydantic resolves this
        # subclass's own field annotations) to mark the class as
        # under-construction for self-referential-link detection.
        cls.__loom_building__ = True
        super().__init_subclass__()

    @classmethod
    def __pydantic_init_subclass__(cls, *, table: tuple[str, str] | None = None, **kwargs: object) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if table is None:
            raise TypeError(f"{cls.__name__} must declare table=(schema, name)")

        properties: list[tuple[str, str, bool]] = []
        links: list[_LinkSpec] = []
        identity_fields: list[str] = []
        # property name -> (declaring field name, link target name or None).
        seen_properties: dict[str, tuple[str, str | None]] = {}
        # (field name, marker, is_optional) — string-target links deferred to pass 2.
        pending: list[tuple[str, _PendingLink, bool]] = []

        # Pass 1: plain properties, eager links, identity discovery. A pending
        # (string-target) link is deferred — its FK column can only be checked
        # after every plain/eager property is in `seen_properties`, and its FK
        # type needs `__loom_identity__` (self-link ⇒ the class's own identity).
        for field_name, field_info in cls.model_fields.items():
            if isinstance(field_info.annotation, typing.ForwardRef):
                forward_str = field_info.annotation.__forward_arg__
                if "Link[" in forward_str:
                    raise TypeError(
                        f"{cls.__name__}.{field_name}: link target could not be "
                        f"resolved ({forward_str!r}); link targets must be "
                        "fully-defined LoomModel classes declared before the class "
                        "that links to them — forward references and "
                        "self-referential links are not supported in v1"
                    )

            optional_meta = optional_metadata(field_info.annotation)
            metadata = (*field_info.metadata, *optional_meta)

            top_pending = next((m for m in field_info.metadata if isinstance(m, _PendingLink)), None)
            opt_pending = next((m for m in optional_meta if isinstance(m, _PendingLink)), None)
            if top_pending is not None or opt_pending is not None:
                marker = top_pending if top_pending is not None else opt_pending
                pending.append((field_name, marker, opt_pending is not None))
                continue

            link_marker = next((meta for meta in metadata if isinstance(meta, Link)), None)
            is_identity = any(meta is LOOM_IDENTITY for meta in metadata)

            try:
                ty, required = loom_type(field_info.annotation)
            except TypeError as exc:
                raise TypeError(f"{cls.__name__}.{field_name}: {exc}") from exc

            if is_identity and not required:
                raise TypeError(f"{cls.__name__}.{field_name}: identity property cannot be optional")

            if link_marker is not None:
                property_name = _record_link(
                    cls, link_marker.target, link_marker.column, field_name, ty, required,
                    properties=properties, links=links, seen_properties=seen_properties,
                )
            else:
                if field_name in seen_properties:
                    other_field, other_target = seen_properties[field_name]
                    raise TypeError(
                        f"{cls.__name__}: property {field_name!r} is declared by both "
                        f"{other_field!r} and {field_name!r}; pass "
                        f"Link[{other_target or 'Target'}, \"other_column\"] to disambiguate "
                        f"the {other_field!r} link"
                    )
                seen_properties[field_name] = (field_name, None)
                properties.append((field_name, ty, required))
                property_name = field_name

            if is_identity:
                identity_fields.append(property_name)

        if len(identity_fields) != 1:
            raise TypeError(
                f"{cls.__name__} must declare exactly one Identity field, found {len(identity_fields)}"
            )
        identity_name = identity_fields[0]
        # Set identity BEFORE pass 2: a self-link resolves `target = cls`, and
        # `_record_link`'s default FK column reads `target.__loom_identity__`.
        # For the one-arg `Link["Node"]` case this must be set so the lookup
        # yields "node_id" (→ collision TypeError), not AttributeError.
        cls.__loom_identity__ = identity_name

        # Pass 2: resolve string-target links now that identity is known. Only a
        # self-reference (the string names the class under construction) is
        # supported; any other name is an unsupported forward reference.
        repaired: list[tuple[str, object]] = []
        for field_name, marker, is_optional in pending:
            if marker.name != cls.__name__:
                raise TypeError(
                    f"{cls.__name__}.{field_name}: link target {marker.name!r} is a "
                    "forward reference; only self-referential string links (naming the "
                    "class under construction) are supported — link targets must "
                    "otherwise be fully-defined LoomModel classes declared before the "
                    "class that links to them"
                )
            identity_ty = cls.model_fields[identity_name].annotation
            ty, _ = loom_type(identity_ty)
            _record_link(
                cls, cls, marker.column, field_name, ty, not is_optional,
                properties=properties, links=links, seen_properties=seen_properties,
            )
            resolved: object = typing.Annotated[identity_ty, Link(cls, marker.column)]
            if is_optional:
                resolved = typing.Optional[resolved]
            repaired.append((field_name, resolved))

        cls.__loom_table__ = table
        # __loom_identity__ was set before pass 2 (above).
        cls.__loom_properties__ = properties
        cls.__loom_links__ = links
        cls.__loom_building__ = False

        # Annotation repair: swap each self-link's placeholder `Any` FieldInfo
        # for the real FK type, then rebuild the validator so pydantic type-checks
        # the FK value. No weakly-typed FK survives class creation.
        #
        # `model_rebuild(force=True)` rebuilds the core schema from
        # `cls.__pydantic_fields__` (pydantic 2.13's `complete_model_class` →
        # `GenerateSchema._model_schema` reads the FieldInfos directly; it does
        # NOT re-collect from `__annotations__`, and it does NOT re-invoke
        # `__pydantic_init_subclass__`, so this is not re-entrant).
        if repaired:
            for field_name, resolved in repaired:
                existing = cls.__pydantic_fields__[field_name]
                cls.__pydantic_fields__[field_name] = FieldInfo.from_annotated_attribute(
                    resolved, existing.default
                )
            cls.model_rebuild(force=True)
