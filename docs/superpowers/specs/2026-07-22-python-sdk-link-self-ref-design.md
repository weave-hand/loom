# Python SDK Self-Referential `Link` Design

> **Status:** planned (`road-python-sdk-link-self-ref`, promoted 2026-07-22 from
> `fut-python-sdk-link-forward-refs`, deliberately narrowed — see scope
> decision). Follow-on slice to the v1 SDK pydantic layer
> (`2026-07-21-python-sdk-v1-design.md`, shipped as `road-python-sdk-v1`).

## Problem

`Link[Other]` resolves eagerly at the declaring class's own class-creation
(`loom_sdk/pydantic/__init__.py::Link.__class_getitem__` needs the target's
identity python type to build the `Annotated[<identity_ty>, Link(target)]`
wrapper pydantic validates against). So `target` must be a fully-defined
`LoomModel` subclass declared textually before the class that links to it.
Self-referential links — the tree/hierarchy case, e.g.

```python
class Node(LoomModel, table=("graph", "nodes")):
    node_id: Identity[int]
    parent: Link["Node", "parent_id"] | None
```

— are impossible to write: `Node` does not exist yet inside its own body, and
the string form is rejected with a `TypeError` naming the v1 constraint
(`__class_getitem__`'s `str`/`ForwardRef` check plus the `__loom_building__`
guard and `__pydantic_init_subclass__`'s ForwardRef scan).

Note the ontology/server side needs **nothing**: `apply()` already registers
links in a second pass after all types exist, and `POST /admin/links` accepts
`from == to` today. The limitation is purely Python class-creation mechanics.

## Decision (operator, 2026-07-22): self-referential only

- **Support `Link["Node"]` where the string names the class under
  construction** — resolvable within the declaring class's own
  `__pydantic_init_subclass__`, since by then pydantic has collected
  `model_fields` and the identity field is discoverable.
- **General forward references stay rejected** (a string naming a class defined
  later, and mutual A↔B cycles). Lifting them needs a whole deferred-resolution
  point (resolve at `apply()`/first-use time), moving failures from class
  creation to first use and leaving FK fields weakly typed in between. The
  remaining gap is re-recorded in FUTURE as `fut-python-sdk-link-forward-refs`
  (narrowed prose) rather than silently dropped.
- **A self-link requires the explicit two-arg column form**
  (`Link["Node", "parent_id"]`). The default FK column — the target's identity
  property name — *always* collides with the class's own identity property for
  a self-link, so a one-arg `Link["Node"]` raises `TypeError` telling the user
  to pass an explicit column. No second auto-derivation naming rule (e.g.
  `<field>_<identity>`) is introduced; the existing default rule stays the only
  rule and the error makes the fix obvious.

## Mechanism: two-pass resolution inside class creation

1. **`Link.__class_getitem__` accepts a `str` target**: instead of raising, it
   returns `Annotated[typing.Any, _PendingLink(name, column)]` (a new private
   marker dataclass carrying the string target and optional column). Non-string
   targets keep the existing eager path unchanged. The `__loom_building__`
   self-reference guard becomes unreachable for the string form and stays for
   the (still-broken) bare-class form.
2. **`__pydantic_init_subclass__` runs two passes** over `model_fields`:
   - *Pass 1 (unchanged logic):* plain properties, eager `Link` markers, and
     identity discovery — producing `__loom_identity__` before any pending link
     is looked at.
   - *Pass 2:* each `_PendingLink` resolves: if `name == cls.__name__`, the
     target is `cls` itself (identity now known from pass 1); any other name
     raises the existing constraint-naming `TypeError` (general forward refs
     unsupported). A missing explicit column raises the collision `TypeError`
     above. The resolved link then flows through the existing collision checks,
     `__loom_properties__`, and `__loom_links__` exactly like an eager
     `_LinkSpec` (with `target = cls`).
3. **Annotation repair + rebuild:** after pass 2, each pending field's
   annotation is rewritten from `Any` to the real
   `Annotated[<identity_ty>, Link(cls, column)]` (preserving the `| None`
   wrapper — self-links are almost always nullable for the root row; the
   existing `optional_metadata` path handles the metadata lift) and
   `cls.model_rebuild(force=True)` is called, restoring full pydantic
   validation of the FK value. **No weakly-typed FK survives class creation.**
4. The `ForwardRef` scan in `__pydantic_init_subclass__` (which today rejects
   whole-string annotations mentioning `Link[`) stays, still rejecting
   `parent: "Link[Node] | None"`-style whole-annotation strings — only the
   `Link["Node", ...]` subscript form is the supported spelling, keeping one
   way to write it.

## Downstream invariants (regression surface, no code change expected)

- `_apply.define_link_request_for` with `spec.target is from_cls` produces a
  `from == to` FK link payload; `apply()`'s second-pass ordering already
  guarantees the type exists before its self-link is registered.
- `diff_type` / `links_to_create` idempotency on re-`apply` of a self-linked
  model (the link name `Node_parent` appears in the type's `links`).
- `instance_row` flattens the self-link field to its `fk_column` like any other
  link (`instance.parent` → `parent_id`).
- Referential integrity of the FK *value* is the caller's problem, as with
  every loom FK link today (advisory, no server-side enforcement) — a `None`
  root vs. land-ordering concerns are not the SDK's to solve; the spec notes
  this, no code addresses it.

## Testing

Unit tests only (no e2e needed — nothing new crosses the wire):

- `pydantic_mapping_test.py` additions: `Link["Node", "parent_id"] | None`
  class-creation succeeds with correct `__loom_properties__` /
  `__loom_links__` / Arrow schema; pydantic validation post-rebuild rejects a
  non-int `parent_id` (proving the annotation repair worked); one-arg
  `Link["Node"]` raises the collision `TypeError`; `Link["SomethingElse"]`
  raises the forward-ref `TypeError`; whole-string annotation still raises;
  non-optional self-link also builds (nullability is orthogonal).
- `pydantic_apply_test.py` additions: `define_link_request_for` emits
  `from == to` payload; `links_to_create` skips an already-registered
  self-link; `instance_row` flattens `parent` → `parent_id` (incl. `None`).
- RE-pinned like all python tests.
