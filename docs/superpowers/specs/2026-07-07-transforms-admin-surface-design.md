# Transforms admin surface — design

_Date: 2026-07-07_

## Summary

Build the **Transforms admin surface** in loom's Yew/WASM UI — the first real
consumer of the [`SqlEditor`](2026-07-06-reusable-sql-editor-design.md)
component. It is a list+drawer surface over the eight `/admin/transforms`
control-plane routes (define/redefine, list, get, delete, run-saved,
run-ad-hoc, run-history, get-run), letting an admin browse, author, run, and
delete both **physical** (SQL over tables) and **typed** (ontology-vocabulary)
transforms, with the `SqlEditor` fed a **schema built from each transform's
declared inputs**.

This spec folds in `#fut-ui-transforms-surface` and
`#fut-ui-sql-editor-schema-wiring` — the schema wiring lands here, with the
consumer that needs it.

Scope of *this* spec:

- Rename the `Pipelines` nav surface to `Transforms` (teal accent kept) and
  build `src/ui/src/surfaces/transforms.rs`.
- The eight `net.rs` HTTP calls, with a new `FetchError::Forbidden` (403).
- An **epoch-guarded** fetch pattern for the surface's selection-keyed effects.
- A **resizable drawer** in the shared `Shell` (drag handle + width clamp,
  persisted) — needed because the editor form is wider than existing drawers.
- A pure, `rust_test`'d `loom_ui_core` module: response parsers, form → request
  builders, input-scoped completion-schema builders, display mappers, and the
  drawer-width clamp.
- Gallery + browser verification.

Explicitly **out of scope** (stay/return as `docs/FUTURE.md` items): EXPLAIN +
diagnostics in the editor (`#fut-ui-sql-editor-diagnostics`), the standalone SQL
query console (`#fut-ui-sql-query-console`), the `SqlEditor` completion-polish
proper fix (`#fut-ui-sql-completion-polish` — this spec *works around* the
mount-time-prop-capture limitation, it does not close it), any server-side batch
metadata endpoint, and live/streaming run updates (history is fetch-on-demand).

## Context

- **UI stack:** Yew 0.21 → `wasm32-unknown-unknown`, buck2 genrules +
  wasm-bindgen `--target web` (no bundler), `stylist` styling, `gloo-net` HTTP.
  All app state lives in one stateful `Workspace` (`src/ui/src/main.rs`);
  surfaces and `loom_ui_components` primitives are stateless/controlled.
- **Pure/testable split:** view-glue (`html!`/`css!`) is thin and unverifiable
  by unit test (no DOM in buck2 `rust_test`); logic lives in the lint-clean,
  `rust_test`'d `loom_ui_core`. Components are verified by eye in `:gallery`.
- **Server contract** (`src/services/runtime/src/admin.rs`, control-plane
  `transforms.rs`):

  | Method | Path | Purpose | Body / result |
  |---|---|---|---|
  | POST | `/admin/transforms` | define/redefine | `TransformDef` → `201 "defined"` / `400` |
  | GET | `/admin/transforms` | list | → `{transforms:[TransformDefView]}` |
  | POST | `/admin/transforms/run` | run ad-hoc | `TransformBody` → `202 {run_id}` / `400` |
  | GET | `/admin/transforms/:name` | get one (+`next_run_at`) | → `TransformDefView` / `404` |
  | DELETE | `/admin/transforms/:name` | delete (idempotent) | → `200 {deleted}` |
  | POST | `/admin/transforms/:name/run` | run saved now | → `202 {run_id}` / `404` |
  | GET | `/admin/transforms/:name/runs` | run history (newest first) | → `{runs:[TransformRunView]}` / `404` |
  | GET | `/admin/runs/:run_id` | get one run | → `TransformRunView` / `400`/`404` |

- **Wire shapes:**
  - `TransformDef` = `{name: string, body: TransformBody, schedule?: string,
    on_input_commit?: bool}`.
  - `TransformBody` is internally tagged on `kind`:
    - `{"kind":"physical", inputs:[{schema,name}], output:{schema,name}, sql,
      output_mode?}`
    - `{"kind":"typed", inputs:[string], output:string, sql, output_mode?}`
    - `output_mode` ∈ `"append"` (default) | `"overwrite"`.
  - `TransformDefView` = `{name, body(json), schedule?, on_input_commit,
    next_run_at?}`.
  - `TransformRunView` = `{run_id, transform?, trigger, state, body(json),
    queued_at, started_at?, finished_at?, snapshot_id?, error?}`.
  - `trigger` ∈ `manual|schedule|data-trigger|ad-hoc`; `state` ∈
    `queued|running|succeeded|failed`.
- **Auth:** every `/admin/*` call needs `Authorization: Bearer <token>` (an
  existing login session token) **and** the subject must hold the `admin` ACL
  role. Missing/invalid token → `401`; valid token without `admin` → `403`.

## Decisions (from brainstorming)

1. **v1 scope: full read + write.** All eight routes wired: list, drawer
   (definition view + run history), define/redefine form, run-now, run-ad-hoc,
   delete.
2. **Admin gating: reuse session, surface 403.** Send the existing session's
   Bearer token as-is; no new login/token concept and no capability probe. On
   `403` the surface renders a clear "requires admin" empty-state. The nav item
   is always visible. Aligns with loom's least-privilege bootstrap (no standing
   separate admin token).
3. **Nav slot: rename `Pipelines` → `Transforms`.** Keeps the nav at five
   surfaces (`Surface::all()` stays `[Surface;5]`) and the teal `#2bb0a0`
   accent; the lower-friction path.
4. **Schema wiring: input-scoped lazy.** The `CompletionSchema` is built only
   from the transform's *declared inputs* — physical: `GET
   /datasets/{schema}/{table}` per chosen input `TableRef`; typed: the chosen
   input ontology types' properties (reuse the Ontology surface's type-detail
   fetch). Fires when the inputs list changes. No new endpoint, no whole-catalog
   N+1, and completion matches exactly the tables the SQL can reference.

## Architecture

### Surface layout

The surface uses the existing `Shell` list+drawer split (`Shell` renders the
nav from `Surface::all()`).

- **List slot** — the transform list (`GET /admin/transforms`): one `DataTable`
  row per definition — name, kind `Badge` (physical/typed), schedule (or
  "manual"), an `on_input_commit` `StatusDot`. A **"＋ New transform"** `Button`
  at the top opens the editor form.
- **Drawer** — context-sensitive, showing exactly one of two mutually-exclusive
  views (so **at most one `SqlEditor` is mounted at a time** — sidesteps the
  double-registration limitation):
  - **Definition view** (a transform is selected) — two `Tabs`:
    - **Definition**: the body SQL in a **read-only** `SqlEditor`, plus
      inputs/output/output-mode/schedule/`on_input_commit` metadata, and
      **Edit** / **Run** / **Delete** action buttons.
    - **Runs**: `GET /:name/runs` history — a `DataTable` of state `Badge`,
      trigger, started/finished, error, snapshot_id.
  - **Editor form** ("New", or "Edit" of the selected transform) — name
    `Input`, kind toggle (physical/typed), inputs multi-select, output field,
    the schema-fed `SqlEditor`, `schedule` cron `Input`, `on_input_commit`
    toggle, output-mode toggle, and **Define** + **Run ad-hoc** buttons.

**Inputs picker drives completion.** Inputs are multi-selected from the fetched
dataset list (physical) or ontology type list (typed) — the structured input set
the input-scoped schema fetch consumes. Output is a free-text `schema`+`name`
(physical) or type name (typed), since the output table often does not yet
exist.

### Resizable drawer (shared `Shell`)

The editor form is wider than the Catalog/Ontology drawers, so `Shell` gains a
**horizontally resizable drawer**: a drag handle on the drawer's left edge sets
its width, clamped to `[min, max]` and persisted (localStorage). The clamp is
pure (`clamp_drawer_width` in `loom_ui_core`); the pointer-drag glue lives in
`shell.rs`. All surfaces inherit the resizable drawer — a net improvement, not a
Transforms-only hack.

### Data flow & fetch

New `net.rs` calls, each sending `Authorization: Bearer {token}` like the
existing governed reads:

- `list_transforms(base, token)` → `GET /admin/transforms`
- `get_transform(base, token, name)` → `GET /admin/transforms/{name}`
- `define_transform(base, token, def_json)` → `POST /admin/transforms`
- `delete_transform(base, token, name)` → `DELETE /admin/transforms/{name}`
- `run_transform(base, token, name)` → `POST /admin/transforms/{name}/run`
- `run_adhoc(base, token, body_json)` → `POST /admin/transforms/run`
- `list_runs(base, token, name)` → `GET /admin/transforms/{name}/runs`
- reuse the existing dataset-list, dataset-detail, and ontology-type calls for
  the input-scoped completion schema.

**403 handling.** Extend `FetchError` with a `Forbidden` variant (today `403`
collapses into `Server(403)`); `fetch_status_err` maps `403 → Forbidden`. The
surface renders a distinct **"Transforms requires the admin role."**
empty-state on `Forbidden`; `401` still fails closed to logout as everywhere
else.

**Epoch-guarded effects.** The surface's selection-keyed fetches (definition,
runs, input-scoped columns) use a **generation counter** captured into each
`spawn_local`, compared before any state `set` — so a slow in-flight fetch from
a prior selection cannot stale the current view. This is the fix pattern named
in `iss-ui-async-fetch-race`; introduce a small shared epoch helper here rather
than repeat the racy pattern. Non-401 fetch errors surface a visible error line
rather than being swallowed (the spirit of `iss-ui-swallowed-fetch-errors` for
the new surface).

**Write-then-refetch.** After Define / Delete / Run succeeds, re-fetch the list
(and runs, after Run) rather than mutating local state optimistically — keeps
the view authoritative and simple.

### `SqlEditor` integration

`SqlEditor` captures `schema`/`read_only` at mount (the `#fut-ui-sql-completion-polish`
limitation). The input-scoped schema changes *after* mount as inputs are added,
so make fresh completions take effect by **remounting the editor via a Yew
`key`** bound to a fingerprint of the input set (the sorted input list). Because
`value` is controlled from the form state, remount **preserves the SQL text** —
only cursor/undo history resets, acceptable for a deliberate, infrequent action
(adding/removing an input). The definition view mounts it `read_only=true`; the
edit form `read_only=false`; they are mutually exclusive so only one editor
registers Monaco's provider at a time. This spec **works around** the polish
limitation with `key` and does **not** close `#fut-ui-sql-completion-polish`.

## Components / units

### `loom_ui_core` — pure, `rust_test`'d (`src/ui/src/transforms.rs`)

One clear responsibility: turn wire JSON ⇄ local view/form values, with no Yew
dependency.

- **Response parsers** (mirroring `parse_datasets`):
  - `parse_transform_list(&Value) -> Vec<TransformSummary>` — name, kind,
    schedule, on_input_commit.
  - `parse_transform_def(&Value) -> TransformDefView` — full definition incl.
    the decoded `TransformBody`.
  - `parse_runs(&Value) -> Vec<RunRow>` — run_id, trigger, state, timestamps,
    snapshot_id, error.
- **Form → request:**
  - `TransformForm` value struct: `{kind, name, inputs, output, sql, schedule,
    on_input_commit, output_mode}`.
  - `form_to_def(&TransformForm) -> Result<Value, Vec<FieldError>>` — builds the
    exact tagged `TransformDef`/`TransformBody` JSON. Client-side light
    validation: name non-empty and ≠ `"run"`; ≥1 input; output & sql non-empty;
    cron arity (5 fields) when scheduled. The **server is the authoritative
    validator** — a `400` body still surfaces beside the form.
  - `form_to_body(&TransformForm) -> Result<Value, Vec<FieldError>>` — the
    ad-hoc-run body (the `TransformBody` alone).
- **Completion-schema builders:**
  - `schema_from_dataset_details(&[Value]) -> CompletionSchema` — physical:
    `columns:[{name,ty}]` per input table → `CompletionTable`.
  - `schema_from_types(&[Value]) -> CompletionSchema` — typed: type properties →
    columns.
- **Display mappers:** run `state` → `BadgeTone` / `StatusDot` `Status`;
  `trigger` → label; `TransformBody` kind → badge label.
- **Drawer width:** `clamp_drawer_width(px, min, max) -> u32`.

### `app` binary — view glue (`src/ui/src/surfaces/transforms.rs`)

Thin Yew components rendering the list, definition view, runs table, and editor
form from the values above; added to the explicit `app` `srcs` list in
`src/ui/BUCK` and `mod`/`pub use`'d in `src/ui/src/surfaces/mod.rs`. The
`Workspace` (`main.rs`) gains the Transforms state hooks and epoch-guarded
effects and a `Surface::Transforms => …` render arm.

### `loom_ui_components` — `Shell` change (`src/ui/src/components/shell.rs`)

Resizable-drawer drag glue (glob'd `srcs`, no BUCK edit).

## Error handling

- `Forbidden` → the admin empty-state in the list slot.
- `400` from Define → the server's validation message rendered beside the form
  (do not swallow); client-side `FieldError`s render inline before submit.
- Network / non-401 errors → a visible retry line (not a stuck "Loading…").
- `401` anywhere → `on_logout` (existing fail-closed behavior).

## Testing

- **Pure logic** (`loom_ui_core`): a `tests/<name>.rs` `rust_test` per unit,
  table-driven — valid/invalid `TransformForm`s; each `TransformBody` variant
  round-tripping to the correct tagged JSON; parser edge cases (missing optional
  fields, ad-hoc runs with no `transform`); `schema_from_*` builders;
  `clamp_drawer_width` bounds. Each new test file gets a `rust_test(...)` target
  in `src/ui/BUCK`.
- **View glue**: verified in the **gallery + browser** (the `SqlEditor`
  precedent) — no headless component tests exist in this harness.
- **Verification gate:** whole-tree `buck2 build //src/...` + `buck2 test
  //src/...` green, plus a browser walkthrough of define → run → history →
  delete, the input-scoped completion, and the `Forbidden` state.

## Register bookkeeping (at land)

- Remove `#fut-ui-transforms-surface` and `#fut-ui-sql-editor-schema-wiring`
  from `docs/FUTURE.md` (folded into this spec).
- Mint `road-ui-transforms-surface` in `docs/ROADMAP.md` under a new `## ui`
  section: `status:planned area:ui
  spec:2026-07-07-transforms-admin-surface-design`, its prose naming the two
  removed `#fut-…` ids as code spans.
