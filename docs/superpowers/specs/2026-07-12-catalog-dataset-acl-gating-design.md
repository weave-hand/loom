# Per-dataset catalog ACL gating Design

> **Status:** design (direction). This spec makes
> `iss-catalog-lineage-acl-asymmetry` build-ready. The item stays in ISSUES
> (a defect in shipped governance posture); a separate work agent writes the
> implementation plan from it and builds it.

## Problem

The catalog reads and the lineage reads enforce two different governance
models over the same objects:

- `list_datasets` / `get_dataset` / `dataset_preview`
  (`query-api/src/http.rs:228-252, 275-315, 374-406`) take `_subject`
  **unused** — any authenticated subject lists every table, reads every
  schema, and previews raw rows (`SELECT *` via `fetch_rows`, no row filter,
  no column mask).
- `/lineage` reads are per-ref ACL-gated through `LineageVisibility`
  (`query-api/src/lineage_filter.rs`): seed-gated, cut-not-skip closure,
  with the Table→Type fallback (`is_readable`, `lineage_filter.rs:104-132`)
  landed by `2026-07-09-lineage-table-type-acl-fallback-design`.

So a subject can preview a dataset whose lineage node is invisible to them —
metadata and data disclosure are *inverted* relative to the governance
model. A second edge rides along: non-UI callers of `POST /admin/transforms`
get no read grant on a physical output table (the UI best-effort self-grants
to `admin`, `ui/src/main.rs:695-704` / `ui/src/transforms.rs:296-304`),
leaving such outputs invisible in lineage until manually granted.

## Decision record

**Operator decision, 2026-07-12 (committed): full per-dataset gating** — all
three catalog reads adopt the same readable predicate lineage uses, rather
than gating preview only or documenting the split. Unreadable refs 404 on
the point reads (no existence oracle), and `list_datasets` filters.

## Context — what ships today (verified)

- **The readable predicate exists and is proven.** `is_readable`
  (`lineage_filter.rs:104-132`): `PolicyTarget::Type` → `Acl::check(Read)`;
  `PolicyTarget::Table` → Table allow **or** any backing type's allow
  (`types_backed_by` lazy map from one `list_types()` read,
  `lineage_filter.rs:148-169`); Unresolvable → deny; External → allow. Its
  soundness argument (fallback spec lines 39-46): a Type Read grant already
  discloses the backing table's rows via the governed object read, so
  Table∨Type is allow-oriented and sound. `Acl::check` itself stays
  exact-match (`core/src/acl.rs:66-72,402-409`) — resolution lives in
  query-api, by design.
- **Catalog handlers** run over `st.cp.catalog()` +
  `st.serving.fetch_rows` (preview: ungoverned `SELECT *` compiled at
  `http.rs:396-401`); auth is bearer-only.
- **The governed read path** (`resolve_governed`, `governed.rs:59-88`)
  enforces Type-level ACL + row filters/masks for `/objects` — untouched
  here.
- **The lineage posture for unreadable seeds** is an empty page, not
  403/404 (`lineage_filter.rs:211-213`) — deliberately not an existence
  oracle.

## Design

### Shared governor extraction

Lift the readable predicate out of `LineageVisibility` into a reusable
helper in the same crate — `dataset_acl::DatasetVisibility` (name final at
plan time) — owning: the Table/Type `Acl::check` calls, the lazy
`types_backed_by` map, and the Table∨Type fallback. `LineageVisibility`
delegates to it (behavior-preserving refactor, its tests pin that);
the catalog handlers consume it. One resolution, one soundness argument,
two consumers.

### The three handlers

- **`list_datasets`**: after listing, retain only refs where
  `visibility.is_readable(Table(ref))` — per-row checks amortized by the
  shared lazy map (one `list_types` + policy reads per request). Page
  shape unchanged; a subject with no grants sees an empty list.
- **`get_dataset`** / **`dataset_preview`**: unreadable → **404** with the
  same body as a nonexistent dataset — the point-read analog of lineage's
  empty-page seed gate. (403 would confirm existence; 404 keeps the
  non-oracle posture.)
- Preview keeps its `SELECT *` semantics for readable datasets. **Row-filter
  /column-mask parity on preview is explicitly out of scope** — when this
  lands, `fut-ui-dataset-preview-acl` narrows to that residue (fine-grained
  row/column governance on preview) rather than closing.

### Server-side transform-output grant

Move the UI's best-effort self-grant into the define path: when
`POST /admin/transforms` (`runtime/src/admin.rs`) defines a transform with a
**physical** output table, grant `Read` on `PolicyTarget::Table(output)` to
the `admin` role as part of the define flow (same grant body the UI builds,
`ui/src/transforms.rs:296-304`; typed outputs need none — their visibility
rides the bound type, exactly the UI's current split). Delete the UI copy
(`output_table_grant` + the `main.rs:695-704` POST). Non-UI callers then get
visible outputs for free, and the grant is no longer best-effort.

Idempotency/atomicity: the grant rides the define handler after the
transform upsert; a duplicate grant is a no-op upsert in the ACL store.
Cross-concern atomicity (define + grant in one tx) is the known
`#fut-auth-acl-provisioning-tx` gap — a mid-sequence failure leaves a
defined-but-ungranted transform, recoverable by re-POST, same posture as
user provisioning. Not solved here.

### UI impact

The Catalog surface (list + preview) now renders only readable datasets for
non-admin subjects. The bootstrap admin self-grants broadly today; admin
workflows are unchanged. This is the intended behavior change, not a
regression.

## Non-regression

- Lineage behavior byte-identical (delegation refactor pinned by the
  existing lineage-filter suite).
- `/objects` governance untouched.
- Admin-role subjects (the operator norm) keep seeing everything they could
  before, via their grants.
- No migration; ACL store and `PolicyTarget` unchanged.

## Testing

Extend the query-api e2e suite (reuse `e2e-support` seed/ACL helpers —
`grant_read`, `subject_with_role`):

- **list filters** — subjects with: no grants (empty), a Type grant
  (backing table listed), a Table grant (listed), admin (all).
- **get/preview 404** — ungranted subject on an existing dataset gets the
  same 404 as a nonexistent one (assert body equality — the oracle test).
- **Type-backed fallback** — Type grant alone makes the backing dataset
  listable/previewable (the lineage-symmetry case from the issue: lineage
  node visible ⇔ preview allowed).
- **Lineage regression** — `lineage_filter` suite green post-extraction.
- **Transform-output grant** — non-UI `POST /admin/transforms` with a
  physical output: the output is immediately visible in list/lineage to
  admin-role subjects; typed output gets no table grant; re-POST idempotent.
- **UI** — self-grant code deleted; the Transforms surface still shows the
  output post-define (now via the server-side grant).

## Global constraints (loom-specific, carry into the plan)

- Strict clippy (pedantic + restriction); `#[expect(lint, reason)]` locally.
- `buck2 run //tools:prek -- run --all-files` before every commit.
- Tests are `rust_test`/`loom_fixture_test` targets only; reuse
  `//src/services/query-api:e2e-support` rather than copying helpers.

## Out of scope (deferred)

- **Row-filter/mask parity on preview** — the narrowed
  `fut-ui-dataset-preview-acl` residue.
- **Per-subject OpenAPI catalog filtering** — `#fut-openapi-per-subject-catalog`.
- **Cross-concern define+grant Tx** — `#fut-auth-acl-provisioning-tx`.
- **A dedicated catalog-metadata ACL action** (finer than Read) — YAGNI
  until a consumer distinguishes metadata-read from data-read.

## Acceptance

1. A subject sees a dataset in `/datasets`, `/datasets/{s}/{t}`, preview,
   AND its lineage node under exactly the same predicate — the asymmetry is
   gone.
2. Ungranted point reads are indistinguishable from nonexistent (404 body
   equality).
3. Physical transform outputs are visible to admins regardless of the
   defining client.
4. Existing suites green (`buck2 test //src/...`).

## Interfaces (names the plan consumes)

- Consumes: `is_readable` / `types_backed_by` / `LineageVisibility`
  (`query-api/src/lineage_filter.rs:49-169`); `Acl::check` +
  `PolicyTarget` (`core/src/acl.rs:68-72,402-409`); the three handlers
  (`query-api/src/http.rs:228-406`); the UI grant builder
  (`ui/src/transforms.rs:296-304`, `ui/src/main.rs:690-704`); the
  transforms define handler (`runtime/src/admin.rs`).
- Produces: the extracted shared visibility governor; gated
  `list_datasets`/`get_dataset`/`dataset_preview`; the server-side
  physical-output grant in the define path; deletion of the UI self-grant.
