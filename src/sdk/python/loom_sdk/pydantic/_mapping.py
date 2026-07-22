"""Python-type -> loom-ontology-type mapping and derived Arrow/wire artifacts.

Pure functions only — no pydantic import here. `loom_type` is used by
`loom_sdk.pydantic`'s `LoomModel.__pydantic_init_subclass__` hook to turn
each field's annotation into a `(loom_ty, required)` pair; `arrow_schema`
and `model_gate` are derived purely from a `LoomModel` subclass's
`__loom_properties__` (no re-inspection of pydantic internals needed).
"""

from __future__ import annotations

import datetime
import types
import typing

import pyarrow as pa

# Locked python -> loom type mapping (see the plan's Global Constraints).
_PYTHON_TO_LOOM: dict[type, str] = {
    int: "long",
    str: "string",
    float: "double",
    bool: "boolean",
    datetime.datetime: "timestamp",
    datetime.date: "date",
}

# Locked loom -> Arrow type mapping (see the plan's Global Constraints).
_LOOM_TO_ARROW: dict[str, pa.DataType] = {
    "long": pa.int64(),
    "string": pa.string(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
}


def _strip_annotated(annotation: object) -> object:
    """Unwrap one level of `Annotated[X, ...]`, returning `X` unchanged
    otherwise. `Link[Customer]` / `Identity[int]` resolve to `Annotated`
    types whose metadata pydantic only lifts out of `FieldInfo.metadata`
    when `Annotated` is the outermost wrapper — inside a Union arm (i.e. an
    Optional field) it survives unstripped, so callers that recurse into
    Union arms need to unwrap it themselves.
    """
    if typing.get_origin(annotation) is typing.Annotated:
        return typing.get_args(annotation)[0]
    return annotation


def loom_type(annotation: object) -> tuple[str, bool]:
    """Map a python type annotation to `(loom_ty, required)`.

    `X | None` (`typing.Optional[X]` / `types.UnionType`) maps to
    `(loom_type(X)[0], False)` — including when `X` is itself an
    `Annotated` type (e.g. `Link[Customer] | None`, `Identity[int] | None`),
    which is unwrapped before recursing. Any other unmappable annotation
    raises `TypeError`.
    """
    origin = typing.get_origin(annotation)
    if origin is typing.Union or origin is types.UnionType:
        args = typing.get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(args) == 2 and len(non_none) == 1:
            ty, _ = loom_type(_strip_annotated(non_none[0]))
            return ty, False
        raise TypeError(f"unmappable annotation: {annotation!r}")

    annotation = _strip_annotated(annotation)
    if isinstance(annotation, type) and annotation in _PYTHON_TO_LOOM:
        return _PYTHON_TO_LOOM[annotation], True

    raise TypeError(f"unmappable annotation: {annotation!r}")


def optional_metadata(annotation: object) -> tuple[object, ...]:
    """Recover the `Annotated` metadata tuple hiding inside an `X | None`
    annotation, e.g. `Annotated[int, MARKER] | None` -> `(MARKER,)`.

    Pydantic only lifts `Annotated` metadata into `FieldInfo.metadata` when
    `Annotated` is the outermost wrapper; `Link[X] | None` / `Identity[T] |
    None` keep their marker nested inside the Optional union arm instead
    (`FieldInfo.annotation` is the whole `Optional[Annotated[...]]`,
    `FieldInfo.metadata` is empty). `loom_sdk.pydantic`'s class machinery
    calls this alongside `FieldInfo.metadata` to recover the marker either
    way. Returns `()` when `annotation` isn't an Optional-wrapped
    `Annotated` type.
    """
    origin = typing.get_origin(annotation)
    if origin is not typing.Union and origin is not types.UnionType:
        return ()
    args = typing.get_args(annotation)
    non_none = [a for a in args if a is not type(None)]
    if len(args) != 2 or len(non_none) != 1:
        return ()
    (inner,) = non_none
    if typing.get_origin(inner) is not typing.Annotated:
        return ()
    return typing.get_args(inner)[1:]


def arrow_schema(cls: type) -> pa.Schema:
    """Build the Arrow schema for a `LoomModel` subclass from `__loom_properties__`."""
    return pa.schema(
        [
            pa.field(name, _LOOM_TO_ARROW[ty], nullable=not required)
            for name, ty, required in cls.__loom_properties__
        ]
    )


def model_gate(cls: type) -> list[dict]:
    """Build the `X-Loom-Model` header's `columns` list for a `LoomModel` subclass."""
    return [
        {"name": name, "ty": ty, "required": required} for name, ty, required in cls.__loom_properties__
    ]
