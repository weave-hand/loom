# Lineage Table→Type ACL fallback (+ UI dataset-ref fix) — design

## Problem

The UI lineage panel rendered only the current dataset for every catalog entry.
Live debugging on a `dev-up` stack found **two independent blockers**:

1. **Wrong URL namespace.** loom keys every physical dataset in the lineage graph
   as `{namespace: "loom", name: "<schema>.<table>"}` (`DatasetId::dataset_ref`),
   but the UI queried `/lineage/datasets/{schema}/{table}/…` — a
   `{namespace: schema, name: table}` ref that matches no stored edge, ever.
2. **ACL denial of table-namespaced nodes.** Even with the correct ref form, the
   closure came back empty: lineage refs for physical datasets resolve to
   `PolicyTarget::Table`, `Acl::check` is deliberately exact-match (no
   type→table resolution, no admin-all, default Deny), and real deployments —
   including the dev-up seed — grant `Read` on ontology *types* only. The
   `LineageVisibility` seed gate therefore denied every table node. Proven live:
   granting table-reads made the correct-form URL return the closure while the
   old form stayed empty.

## Decision

**Resolve type-backed table refs to their backing type for the lineage ACL check**
(option 2a of the debugging write-up), in the query-api governor — not in
`Acl::check`, whose exact-match contract ("P4 never resolves a `Type` to its
backing `Table`") stays untouched.

`LineageVisibility::is_readable`, `Table` arm:

1. `Acl::check(Read, Table)` — Allow ⇒ readable (unchanged).
2. Otherwise, for each ontology type **backed by** that table:
   `Acl::check(Read, Type)` — any Allow ⇒ readable.
3. No binding, no allow ⇒ not readable (unchanged: denied *and cut*).

The table→types map is built lazily per request (`tokio::sync::OnceCell`), on the
first Table-target Deny, from one unbounded `Ontology::list_types` read (ontology
metadata is deployment-sized — the same argument as the governor's unbounded
one-hop pages). Requests whose Table checks all allow never pay for it.

### Why the widening is sound

A `Read` grant on a type already discloses the backing table's rows through the
governed object read (`/objects/{type}`). Seeing the table's lineage *node*
(existence + edges) discloses strictly less. Corollary: the fallback is
allow-oriented — an explicit Table-target `Deny` does not veto a type Allow,
consistent with the object read, which consults only the Type target.

### Visible-set change (tests)

`define_type` emits a table→type binding lineage edge. With the fallback, a type
grant now also reveals the backing-table node that edge reaches, so closures
legitimately gain `<schema>.<table>` nodes next to their `loom:type` nodes. The
existing lineage-visibility unit tests and lineage-acl e2e expectations were
updated accordingly (the *cut* semantics — a denied node hides everything only
reachable through it — are unchanged and still asserted).

## Untyped physical-transform outputs

A physical transform's output is a fresh table bound to **no** type, so the
fallback cannot apply and the node (and the whole closure, via seed gating, when
it is the seed) stays invisible. Rather than widen ACL semantics further, the UI
transform define flow now **self-grants** `read` on the physical output table to
the reserved `admin` role (`output_table_grant` → `POST
/admin/roles/admin/grants`): the define surface is `require_admin`-gated, so the
defining caller necessarily holds that role; table grants are existence-unchecked
and upsert, so granting the not-yet-created output at define time is safe and
idempotent. Typed outputs need no grant (governed by their type's grants + the
fallback).

## Out of scope / deferred

- **Catalog↔lineage governance asymmetry.** `list_datasets` / `get_dataset` /
  `dataset_preview` are auth-only (`_subject` unused) while lineage is per-ref
  ACL-gated — a subject can preview a dataset whose lineage node is hidden.
  Registered as an ISSUES item; needs its own design (either per-dataset catalog
  gating or a documented coarse/fine split).
- **Server-side output grants** (define-time grant inside
  `define_transform_route`, or creator-scoped grants) — the UI self-grant covers
  the admin surface; non-UI callers of `/admin/transforms` must grant manually.
- Cross-target deny propagation (Table Deny vetoing Type Allow) — would need the
  same treatment on the object-read path first.
