# Transforms Admin Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Transforms admin surface in loom's Yew/WASM UI — a list+drawer surface over the eight `/admin/transforms` control-plane routes, the first real consumer of the shipped `SqlEditor`, fed an input-scoped completion schema.

**Architecture:** All pure logic (wire JSON ⇄ view/form values, completion-schema builders, display mappers, drawer-width clamp) lands in the lint-clean, `rust_test`'d `loom_ui_core` crate as a new `src/transforms.rs` module. The HTTP calls (7 new `fetch`s + a `Forbidden`/`Rejected` `FetchError` extension) land in `net.rs`. View glue (list, definition-view tabs, runs table, editor form) lands in a new `src/surfaces/transforms.rs`; the `Workspace` in `main.rs` gains the state hooks, epoch-guarded fetches, and the `Surface::Transforms` render arm. The shared `Shell` gains a resizable drawer. Only one `SqlEditor` is ever mounted at a time (definition view *or* editor form, never both), and it is remounted via a Yew `key` bound to the input-set fingerprint so the input-scoped schema takes effect despite the editor's mount-time prop capture.

**Tech Stack:** Rust (edition 2024), Yew 0.21 → `wasm32-unknown-unknown`, `stylist` CSS-in-Rust, `gloo-net` HTTP, `serde_json`, buck2 (`//src/ui` cell), Monaco-backed `SqlEditor`.

## Global Constraints

- **Rust edition 2024**; all UI targets cross-compile to `//platforms:wasm`.
- **No inline `#[cfg(test)]`/`#[test]` in first-party `src/**.rs`** — the `no-inline-tests` prek hook fails the build. Every unit test is a `tests/<name>.rs` file wired as its own `rust_test` target in `src/ui/BUCK`.
- **`loom_ui_core` (crate root `src/ui/src/lib.rs`) is strictly lint-clean** under the whole `clippy::pedantic` + `clippy::restriction` groups. In `src/transforms.rs`: no `unwrap()`/`expect()`, no `[]` indexing, no `panic!`/`todo!`, no `dbg!`. Use `.and_then`/`.map`/`.unwrap_or_default`/`str::split_once`/`u32::try_from(..).unwrap_or(..)`. Mark pure returns `#[must_use]` (mirror the existing parsers). `serde_json::json!` is acceptable (used across the tree); if the strict gate flags a specific macro expansion, wrap the fn with `#[expect(clippy::<lint>, reason = "...")]`.
- **`net.rs`, `main.rs`, `surfaces/*.rs`, `components/*.rs` are NOT strictly lint-gated** — the crate roots (`main.rs`, `components/mod.rs`) carry `#![allow(clippy::pedantic, clippy::restriction)]`, which covers every module in those crates (that is why existing `net.rs` uses `.to_string()`/`.unwrap_or_default()` freely). Match the surrounding style; do not add per-fn `#[expect]` there.
- **Auth:** every `/admin/*` call sends `Authorization: Bearer {token}` (the existing session token, passed into `Workspace` as `AttrValue`, converted with `.to_string()` before each `spawn_local`). `401` anywhere → `on_logout.emit(())` (fail-closed). `403` → the "requires admin" empty-state.
- **`Surface::all()` stays `[Surface; 5]`** — this is a *rename* of `Pipelines`, not a new surface. Teal accent `#2bb0a0` is kept.
- **Verified builds:** `buck2 build -v0 --console none //src/...` (silent on success) and `buck2 test --console none //src/ui/...` (prints `Tests finished: Pass N. Fail 0`). Never pipe `buck2 test` through `tail`/`head`.
- **New `.rs` files:** `loom_ui_core` and `:app` have **explicit `srcs` lists** in `src/ui/BUCK` — a new file there needs a BUCK edit. `loom_ui_components` (`src/components/**/*.rs`) is glob'd — no BUCK edit.

---

### Task 1: Rename the `Pipelines` nav surface to `Transforms`

**Files:**
- Modify: `src/ui/src/lib.rs` (the `Surface` enum + its `all()`/`label()`/`accent()`/`is_live()` impls, ~lines 233-284)
- Modify: `src/ui/tests/surface.rs` (existing `surface` rust_test)
- Possibly modify: `src/ui/src/main.rs` (only if it names `Surface::Pipelines` explicitly)

**Interfaces:**
- Produces: `loom_ui_core::Surface::Transforms` (replacing `Surface::Pipelines`), keeping `Surface::all() -> [Surface; 5]` and the teal `#2bb0a0` accent. `is_live()` still returns `true` only for `Catalog`/`Ontology` after this task (the real render arm arrives in Task 9).

- [ ] **Step 1: Update the failing test first**

Rewrite `src/ui/tests/surface.rs` to expect `Transforms` where it currently expects `Pipelines`:

```rust
use loom_ui_core::Surface;

#[test]
fn accents_match_the_design_tokens() {
    assert_eq!(Surface::Catalog.accent(), "#3b82f6");
    assert_eq!(Surface::Transforms.accent(), "#2bb0a0");
    assert_eq!(Surface::Ontology.accent(), "#8b5cf6");
    assert_eq!(Surface::Workbooks.accent(), "#2da44e");
    assert_eq!(Surface::Dashboards.accent(), "#d29922");
}

#[test]
fn only_catalog_and_ontology_are_live() {
    let live: Vec<&str> = Surface::all()
        .into_iter()
        .filter(|s| s.is_live())
        .map(Surface::label)
        .collect();
    assert_eq!(live, vec!["Catalog", "Ontology"]);
}

#[test]
fn all_lists_five_surfaces_in_nav_order() {
    let labels: Vec<&str> = Surface::all().into_iter().map(Surface::label).collect();
    assert_eq!(
        labels,
        vec!["Catalog", "Transforms", "Ontology", "Workbooks", "Dashboards"]
    );
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/ui:surface`
Expected: FAIL — `no variant named Transforms found for enum Surface` (compile error) or an assertion mismatch on the label list.

- [ ] **Step 3: Rename the variant in `lib.rs`**

In `src/ui/src/lib.rs`, change the `Surface` enum's second variant `Pipelines` → `Transforms`, and update every match arm that names it:
- `all()` — the array literal: `[Surface::Catalog, Surface::Transforms, Surface::Ontology, Surface::Workbooks, Surface::Dashboards]` (still `[Surface; 5]`).
- `label()` — the arm `Surface::Pipelines => "Pipelines"` becomes `Surface::Transforms => "Transforms"`.
- `accent()` — the arm returning `"#2bb0a0"` now matches `Surface::Transforms`.
- `is_live()` — leave `matches!(self, Surface::Catalog | Surface::Ontology)` unchanged (Transforms is not yet live; Task 9 flips it).

- [ ] **Step 4: Fix any remaining `Surface::Pipelines` references**

Run: `grep -rn "Pipelines" src/ui/src` — fix every hit (e.g. if `main.rs` names it). The dispatch `match *surface` in `main.rs` uses an `other =>` catch-all for the non-live surfaces, so Transforms will fall through to `StubView` until Task 9 — that is expected and compiles.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/ui:surface`
Expected: `Tests finished: Pass 3. Fail 0`.

- [ ] **Step 6: Verify the wasm crate still builds**

Run: `buck2 build -v0 --console none //src/ui:app`
Expected: exit 0 (silent).

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/surface.rs src/ui/src/main.rs
git commit -m "feat(ui): rename Pipelines nav surface to Transforms"
```

---

### Task 2: Transforms module scaffold + response parsers (`loom_ui_core`)

**Files:**
- Create: `src/ui/src/transforms.rs`
- Modify: `src/ui/src/lib.rs` (add `mod transforms;` + re-export; make `str_field` `pub(crate)`)
- Modify: `src/ui/BUCK` (add `src/transforms.rs` to the `:ui-core` `srcs`; add the `transforms-parse` `rust_test`)
- Test: `src/ui/tests/transforms_parse.rs`

**Interfaces:**
- Consumes: the existing private helper `str_field(v: &Value, k: &str) -> String` in `lib.rs` (promote to `pub(crate)`), and `serde_json::Value`.
- Produces (all re-exported from `loom_ui_core`):
  - `enum TransformKind { Physical, Typed }` — `Copy`, `Default` (`Physical`), with `fn as_str(self) -> &'static str`.
  - `enum OutputMode { Append, Overwrite }` — `Copy`, `Default` (`Append`), `fn as_str(self) -> &'static str`, `fn from_str_opt(s: &str) -> Self`.
  - `struct TableRef { schema: String, name: String }` — `Default`.
  - `enum TransformIo { Physical { inputs: Vec<TableRef>, output: TableRef }, Typed { inputs: Vec<String>, output: String } }`.
  - `struct TransformBody { io: TransformIo, sql: String, output_mode: OutputMode }` with `fn kind(&self) -> TransformKind`.
  - `struct TransformSummary { name: String, kind: TransformKind, schedule: Option<String>, on_input_commit: bool }`.
  - `struct TransformDefView { name: String, body: TransformBody, schedule: Option<String>, on_input_commit: bool, next_run_at: Option<String> }`.
  - `struct RunRow { run_id: String, trigger: String, state: String, queued_at: String, started_at: Option<String>, finished_at: Option<String>, snapshot_id: Option<String>, error: Option<String> }` (raw `trigger`/`state` strings; interpreted by Task 5's display mappers).
  - `fn parse_transform_list(body: &Value) -> Vec<TransformSummary>`
  - `fn parse_transform_def(body: &Value) -> TransformDefView`
  - `fn parse_runs(body: &Value) -> Vec<RunRow>`

- [ ] **Step 1: Promote `str_field` and declare the module in `lib.rs`**

In `src/ui/src/lib.rs`:
- Change `fn str_field(v: &Value, k: &str) -> String {` to `pub(crate) fn str_field(v: &Value, k: &str) -> String {` (line ~317).
- Below the existing `mod completion;` / `pub use completion::{...};` block (lines ~6-10), add:

```rust
mod transforms;
pub use transforms::{
    OutputMode, RunRow, TableRef, TransformBody, TransformDefView, TransformIo, TransformKind,
    TransformSummary, parse_runs, parse_transform_def, parse_transform_list,
};
```

(Task 3, 4, 5 will extend this `pub use` list as they add public items.)

- [ ] **Step 2: Write the parser types + functions in `transforms.rs`**

Create `src/ui/src/transforms.rs`:

```rust
//! Pure wire JSON ⇄ view/form values for the Transforms admin surface.
//! No Yew dependency; strictly lint-clean (covered by `//src/ui:transforms-*` tests).

use serde_json::Value;

use crate::str_field;

/// Whether a transform operates on physical tables or ontology types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransformKind {
    #[default]
    Physical,
    Typed,
}

impl TransformKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Physical => "physical",
            Self::Typed => "typed",
        }
    }
}

/// The output write mode. Server default is `Append`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    #[default]
    Append,
    Overwrite,
}

impl OutputMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Overwrite => "overwrite",
        }
    }

    #[must_use]
    pub fn from_str_opt(s: &str) -> Self {
        if s == "overwrite" {
            Self::Overwrite
        } else {
            Self::Append
        }
    }
}

/// A physical `{schema, name}` table reference.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

/// Kind-specific inputs/output of a transform body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformIo {
    Physical {
        inputs: Vec<TableRef>,
        output: TableRef,
    },
    Typed {
        inputs: Vec<String>,
        output: String,
    },
}

/// A decoded `TransformBody` (the internally-tagged `body` object).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformBody {
    pub io: TransformIo,
    pub sql: String,
    pub output_mode: OutputMode,
}

impl TransformBody {
    #[must_use]
    pub fn kind(&self) -> TransformKind {
        match self.io {
            TransformIo::Physical { .. } => TransformKind::Physical,
            TransformIo::Typed { .. } => TransformKind::Typed,
        }
    }
}

/// A row in the transform list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformSummary {
    pub name: String,
    pub kind: TransformKind,
    pub schedule: Option<String>,
    pub on_input_commit: bool,
}

/// A full transform definition (drawer definition view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformDefView {
    pub name: String,
    pub body: TransformBody,
    pub schedule: Option<String>,
    pub on_input_commit: bool,
    pub next_run_at: Option<String>,
}

/// A row in the run-history table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRow {
    pub run_id: String,
    pub trigger: String,
    pub state: String,
    pub queued_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub snapshot_id: Option<String>,
    pub error: Option<String>,
}

fn opt_str(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(ToOwned::to_owned)
}

/// The `kind` field of a body object; anything but `"typed"` is treated as physical.
fn body_kind(body: &Value) -> TransformKind {
    match body.get("kind").and_then(Value::as_str) {
        Some("typed") => TransformKind::Typed,
        _ => TransformKind::Physical,
    }
}

fn parse_table_ref(v: &Value) -> TableRef {
    TableRef {
        schema: str_field(v, "schema"),
        name: str_field(v, "name"),
    }
}

/// Decode a `TransformBody` object. Total: missing fields → defaults.
fn parse_body(body: &Value) -> TransformBody {
    let output_mode = OutputMode::from_str_opt(
        body.get("output_mode").and_then(Value::as_str).unwrap_or("append"),
    );
    let io = match body_kind(body) {
        TransformKind::Physical => {
            let inputs = body
                .get("inputs")
                .and_then(Value::as_array)
                .map(|arr| arr.iter().map(parse_table_ref).collect())
                .unwrap_or_default();
            let output = body.get("output").map(parse_table_ref).unwrap_or_default();
            TransformIo::Physical { inputs, output }
        }
        TransformKind::Typed => {
            let inputs = body
                .get("inputs")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            let output = str_field(body, "output");
            TransformIo::Typed { inputs, output }
        }
    };
    TransformBody {
        io,
        sql: str_field(body, "sql"),
        output_mode,
    }
}

/// Decode `GET /admin/transforms` → `{transforms:[TransformDefView]}`. Total.
#[must_use]
pub fn parse_transform_list(body: &Value) -> Vec<TransformSummary> {
    body.get("transforms")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|d| TransformSummary {
                    name: str_field(d, "name"),
                    kind: d.get("body").map(body_kind).unwrap_or_default(),
                    schedule: opt_str(d, "schedule"),
                    on_input_commit: d
                        .get("on_input_commit")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Decode `GET /admin/transforms/{name}` → one `TransformDefView`. Total.
#[must_use]
pub fn parse_transform_def(body: &Value) -> TransformDefView {
    const NULL: Value = Value::Null;
    TransformDefView {
        name: str_field(body, "name"),
        body: parse_body(body.get("body").unwrap_or(&NULL)),
        schedule: opt_str(body, "schedule"),
        on_input_commit: body
            .get("on_input_commit")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        next_run_at: opt_str(body, "next_run_at"),
    }
}

/// Decode `GET /admin/transforms/{name}/runs` → `{runs:[TransformRunView]}`. Total.
#[must_use]
pub fn parse_runs(body: &Value) -> Vec<RunRow> {
    body.get("runs")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|r| RunRow {
                    run_id: str_field(r, "run_id"),
                    trigger: str_field(r, "trigger"),
                    state: str_field(r, "state"),
                    queued_at: str_field(r, "queued_at"),
                    started_at: opt_str(r, "started_at"),
                    finished_at: opt_str(r, "finished_at"),
                    snapshot_id: opt_str(r, "snapshot_id"),
                    error: opt_str(r, "error"),
                })
                .collect()
        })
        .unwrap_or_default()
}
```

Note on the `NULL` const (a compile blocker if done naively): a `let inner = body.get("body").unwrap_or(&Value::Null);` binding does **not** compile — E0716, because `serde_json::Value` has drop glue, so the `Value::Null` temporary is not rvalue-static-promoted; it is dropped at the end of the `let`, leaving `inner` dangling when used in the following struct expression. A fn-scope `const NULL: Value = Value::Null;` is `'static`, so `&NULL` lives long enough. `parse_body(&NULL)` is total (yields an empty physical body). `unwrap_or` is not the lint-gated `unwrap()`.

- [ ] **Step 3: Wire the module into the `:ui-core` build and add the test target**

In `src/ui/BUCK`, add `"src/transforms.rs"` to the `:ui-core` target's `srcs`:

```python
rust_library(
    name = "ui-core",
    crate = "loom_ui_core",
    srcs = ["src/lib.rs", "src/completion.rs", "src/transforms.rs"],
    crate_root = "src/lib.rs",
    edition = "2024",
    visibility = ["PUBLIC"],
    deps = [
        "//third-party:serde_json",
    ],
)
```

And add a new `rust_test` block alongside the others:

```python
rust_test(
    name = "transforms-parse",
    crate = "transforms_parse",
    srcs = ["tests/transforms_parse.rs"],
    crate_root = "tests/transforms_parse.rs",
    edition = "2024",
    deps = ["//third-party:serde_json", ":ui-core"],
)
```

- [ ] **Step 4: Write the parser tests**

Create `src/ui/tests/transforms_parse.rs`:

```rust
use loom_ui_core::{
    OutputMode, TransformIo, TransformKind, parse_runs, parse_transform_def, parse_transform_list,
};
use serde_json::json;

#[test]
fn list_decodes_kind_schedule_and_flag() {
    let body = json!({
        "transforms": [
            { "name": "daily_rollup", "body": {"kind": "physical"}, "schedule": "0 0 * * *",
              "on_input_commit": false },
            { "name": "enrich_customers", "body": {"kind": "typed"}, "on_input_commit": true },
        ]
    });
    let rows = parse_transform_list(&body);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].name, "daily_rollup");
    assert_eq!(rows[0].kind, TransformKind::Physical);
    assert_eq!(rows[0].schedule.as_deref(), Some("0 0 * * *"));
    assert!(!rows[0].on_input_commit);
    assert_eq!(rows[1].kind, TransformKind::Typed);
    assert_eq!(rows[1].schedule, None);
    assert!(rows[1].on_input_commit);
}

#[test]
fn list_missing_array_is_empty() {
    assert!(parse_transform_list(&json!({})).is_empty());
}

#[test]
fn def_decodes_physical_body() {
    let body = json!({
        "name": "join_orders",
        "body": {
            "kind": "physical",
            "inputs": [{"schema": "sales", "name": "orders"},
                       {"schema": "sales", "name": "line_items"}],
            "output": {"schema": "marts", "name": "order_totals"},
            "sql": "SELECT 1",
            "output_mode": "overwrite"
        },
        "on_input_commit": true,
        "next_run_at": "2026-07-08T00:00:00Z"
    });
    let def = parse_transform_def(&body);
    assert_eq!(def.name, "join_orders");
    assert_eq!(def.body.kind(), TransformKind::Physical);
    assert_eq!(def.body.sql, "SELECT 1");
    assert_eq!(def.body.output_mode, OutputMode::Overwrite);
    assert!(def.on_input_commit);
    assert_eq!(def.next_run_at.as_deref(), Some("2026-07-08T00:00:00Z"));
    match def.body.io {
        TransformIo::Physical { inputs, output } => {
            assert_eq!(inputs.len(), 2);
            assert_eq!(inputs[0].schema, "sales");
            assert_eq!(inputs[0].name, "orders");
            assert_eq!(output.schema, "marts");
            assert_eq!(output.name, "order_totals");
        }
        TransformIo::Typed { .. } => panic!("expected physical io"),
    }
}

#[test]
fn def_decodes_typed_body_and_defaults_mode_to_append() {
    let body = json!({
        "name": "enrich",
        "body": {
            "kind": "typed",
            "inputs": ["Customer", "Order"],
            "output": "EnrichedCustomer",
            "sql": "SELECT *"
        },
        "on_input_commit": false
    });
    let def = parse_transform_def(&body);
    assert_eq!(def.body.output_mode, OutputMode::Append);
    match def.body.io {
        TransformIo::Typed { inputs, output } => {
            assert_eq!(inputs, vec!["Customer".to_string(), "Order".to_string()]);
            assert_eq!(output, "EnrichedCustomer");
        }
        TransformIo::Physical { .. } => panic!("expected typed io"),
    }
}

#[test]
fn runs_decode_with_optional_fields() {
    let body = json!({
        "runs": [
            { "run_id": "r1", "trigger": "manual", "state": "succeeded",
              "queued_at": "t0", "started_at": "t1", "finished_at": "t2",
              "snapshot_id": "snap-9" },
            { "run_id": "r2", "trigger": "ad-hoc", "state": "failed",
              "queued_at": "t0", "error": "boom" },
        ]
    });
    let runs = parse_runs(&body);
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].run_id, "r1");
    assert_eq!(runs[0].state, "succeeded");
    assert_eq!(runs[0].snapshot_id.as_deref(), Some("snap-9"));
    assert_eq!(runs[0].error, None);
    assert_eq!(runs[1].error.as_deref(), Some("boom"));
    assert_eq!(runs[1].started_at, None);
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test --console none //src/ui:transforms-parse`
Expected: `Tests finished: Pass 5. Fail 0`.

- [ ] **Step 6: Confirm lint-clean (strict gate)**

Run: `buck2 build --console none '//src/ui:ui-core[clippy.txt]' --show-simple-output`, then `cat` the printed path.
Expected: the file is empty (no clippy findings for `transforms.rs`). Fix any finding by preferring the lint-safe idioms in Global Constraints; only if a `serde_json`/macro expansion is flagged, add a scoped `#[expect(..., reason = "...")]`.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/transforms.rs src/ui/src/lib.rs src/ui/BUCK src/ui/tests/transforms_parse.rs
git commit -m "feat(ui): transform wire types + response parsers in loom_ui_core"
```

---

### Task 3: Form → request builders (`loom_ui_core`)

**Files:**
- Modify: `src/ui/src/transforms.rs` (append the form types + builders)
- Modify: `src/ui/src/lib.rs` (extend the `pub use transforms::{...}`)
- Modify: `src/ui/BUCK` (add the `transforms-form` `rust_test`)
- Test: `src/ui/tests/transforms_form.rs`

**Interfaces:**
- Consumes: `TransformKind`, `OutputMode` (Task 2).
- Produces (re-exported):
  - `struct TransformForm { kind: TransformKind, name: String, inputs: Vec<String>, output: String, sql: String, schedule: String, on_input_commit: bool, output_mode: OutputMode }` — `Default`. For `Physical`, each `inputs` entry and `output` is `"schema.name"`; for `Typed`, each is a type name. Empty `schedule` means "no schedule".
  - `struct FieldError { field: String, message: String }`.
  - `fn form_to_def(form: &TransformForm) -> Result<serde_json::Value, Vec<FieldError>>` — builds the tagged `TransformDef` (`{name, body, on_input_commit, schedule?}`).
  - `fn form_to_body(form: &TransformForm) -> Result<serde_json::Value, Vec<FieldError>>` — builds the `TransformBody` alone (ad-hoc run).

- [ ] **Step 1: Extend the `lib.rs` re-export**

Add `FieldError, TransformForm, form_to_body, form_to_def` to the `pub use transforms::{...}` list in `src/ui/src/lib.rs`.

- [ ] **Step 2: Append the form builders to `transforms.rs`**

Append to `src/ui/src/transforms.rs`:

```rust
use serde_json::json;

/// The editor-form value. `inputs`/`output` are `"schema.name"` for physical
/// transforms and bare type names for typed transforms. Empty `schedule` = none.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransformForm {
    pub kind: TransformKind,
    pub name: String,
    pub inputs: Vec<String>,
    pub output: String,
    pub sql: String,
    pub schedule: String,
    pub on_input_commit: bool,
    pub output_mode: OutputMode,
}

/// A client-side validation failure, keyed by form field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

impl FieldError {
    fn new(field: &str, message: &str) -> Self {
        Self {
            field: field.to_owned(),
            message: message.to_owned(),
        }
    }
}

/// Split a `"schema.name"` ref on the first `.`; no `.` → `("", whole)`.
fn split_ref(s: &str) -> (&str, &str) {
    match s.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("", s),
    }
}

fn table_ref_json(s: &str) -> Value {
    let (schema, name) = split_ref(s);
    json!({ "schema": schema, "name": name })
}

/// Validation shared by define and ad-hoc-run (everything but name/schedule).
fn validate_body(form: &TransformForm, errors: &mut Vec<FieldError>) {
    if form.inputs.is_empty() {
        errors.push(FieldError::new("inputs", "select at least one input"));
    }
    if form.output.trim().is_empty() {
        errors.push(FieldError::new("output", "output is required"));
    }
    if form.sql.trim().is_empty() {
        errors.push(FieldError::new("sql", "SQL is required"));
    }
    if form.kind == TransformKind::Physical {
        if form.inputs.iter().any(|i| !i.contains('.')) {
            errors.push(FieldError::new("inputs", "physical inputs must be schema.name"));
        }
        if !form.output.trim().is_empty() && !form.output.contains('.') {
            errors.push(FieldError::new("output", "physical output must be schema.name"));
        }
    }
}

/// Build the `TransformBody` JSON (used by both define and ad-hoc run).
fn build_body(form: &TransformForm) -> Value {
    match form.kind {
        TransformKind::Physical => {
            let inputs: Vec<Value> = form.inputs.iter().map(|s| table_ref_json(s)).collect();
            json!({
                "kind": "physical",
                "inputs": inputs,
                "output": table_ref_json(&form.output),
                "sql": form.sql,
                "output_mode": form.output_mode.as_str(),
            })
        }
        TransformKind::Typed => json!({
            "kind": "typed",
            "inputs": form.inputs,
            "output": form.output,
            "sql": form.sql,
            "output_mode": form.output_mode.as_str(),
        }),
    }
}

/// Build the full `TransformDef` (`POST /admin/transforms`). Client-side light
/// validation; the server is the authoritative validator.
///
/// # Errors
/// Returns the accumulated [`FieldError`]s when the form is not submittable.
pub fn form_to_def(form: &TransformForm) -> Result<Value, Vec<FieldError>> {
    let mut errors = Vec::new();
    let name = form.name.trim();
    if name.is_empty() {
        errors.push(FieldError::new("name", "name is required"));
    } else if name == "run" {
        errors.push(FieldError::new("name", "\"run\" is reserved"));
    }
    validate_body(form, &mut errors);
    let schedule = form.schedule.trim();
    if !schedule.is_empty() && schedule.split_whitespace().count() != 5 {
        errors.push(FieldError::new("schedule", "cron needs 5 space-separated fields"));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let mut def = json!({
        "name": name,
        "body": build_body(form),
        "on_input_commit": form.on_input_commit,
    });
    if !schedule.is_empty() {
        def["schedule"] = json!(schedule);
    }
    Ok(def)
}

/// Build the ad-hoc-run body (`POST /admin/transforms/run`) — the `TransformBody` alone.
///
/// # Errors
/// Returns the accumulated [`FieldError`]s when the body is not submittable.
pub fn form_to_body(form: &TransformForm) -> Result<Value, Vec<FieldError>> {
    let mut errors = Vec::new();
    validate_body(form, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(build_body(form))
}
```

Note: `def["schedule"] = ...` uses `serde_json::Value`'s `IndexMut`, which inserts into the object — this is `serde_json`'s API, not slice indexing, so it does not trip `clippy::indexing_slicing`. If the strict gate disagrees, build the object with `json!` including schedule conditionally instead.

- [ ] **Step 3: Add the test target to `src/ui/BUCK`**

```python
rust_test(
    name = "transforms-form",
    crate = "transforms_form",
    srcs = ["tests/transforms_form.rs"],
    crate_root = "tests/transforms_form.rs",
    edition = "2024",
    deps = ["//third-party:serde_json", ":ui-core"],
)
```

- [ ] **Step 4: Write the form tests**

Create `src/ui/tests/transforms_form.rs`:

```rust
use loom_ui_core::{OutputMode, TransformForm, TransformKind, form_to_body, form_to_def};
use serde_json::json;

fn physical_form() -> TransformForm {
    TransformForm {
        kind: TransformKind::Physical,
        name: "join_orders".into(),
        inputs: vec!["sales.orders".into(), "sales.line_items".into()],
        output: "marts.order_totals".into(),
        sql: "SELECT 1".into(),
        schedule: String::new(),
        on_input_commit: true,
        output_mode: OutputMode::Overwrite,
    }
}

#[test]
fn physical_def_builds_tagged_json() {
    let def = form_to_def(&physical_form()).expect("valid form");
    assert_eq!(def["name"], json!("join_orders"));
    assert_eq!(def["on_input_commit"], json!(true));
    assert_eq!(def.get("schedule"), None);
    let body = &def["body"];
    assert_eq!(body["kind"], json!("physical"));
    assert_eq!(body["inputs"][0], json!({"schema": "sales", "name": "orders"}));
    assert_eq!(body["output"], json!({"schema": "marts", "name": "order_totals"}));
    assert_eq!(body["output_mode"], json!("overwrite"));
}

#[test]
fn typed_def_builds_string_inputs_and_output() {
    let form = TransformForm {
        kind: TransformKind::Typed,
        name: "enrich".into(),
        inputs: vec!["Customer".into()],
        output: "EnrichedCustomer".into(),
        sql: "SELECT *".into(),
        schedule: "0 0 * * *".into(),
        on_input_commit: false,
        output_mode: OutputMode::Append,
    };
    let def = form_to_def(&form).expect("valid form");
    assert_eq!(def["schedule"], json!("0 0 * * *"));
    let body = &def["body"];
    assert_eq!(body["kind"], json!("typed"));
    assert_eq!(body["inputs"], json!(["Customer"]));
    assert_eq!(body["output"], json!("EnrichedCustomer"));
    assert_eq!(body["output_mode"], json!("append"));
}

#[test]
fn reserved_name_run_is_rejected() {
    let mut form = physical_form();
    form.name = "run".into();
    let errs = form_to_def(&form).expect_err("run is reserved");
    assert!(errs.iter().any(|e| e.field == "name"));
}

#[test]
fn empty_name_and_no_inputs_and_no_sql_all_error() {
    let form = TransformForm {
        kind: TransformKind::Typed,
        ..TransformForm::default()
    };
    let errs = form_to_def(&form).expect_err("empty form");
    assert!(errs.iter().any(|e| e.field == "name"));
    assert!(errs.iter().any(|e| e.field == "inputs"));
    assert!(errs.iter().any(|e| e.field == "output"));
    assert!(errs.iter().any(|e| e.field == "sql"));
}

#[test]
fn physical_inputs_must_be_schema_dot_name() {
    let mut form = physical_form();
    form.inputs = vec!["orders".into()];
    let errs = form_to_def(&form).expect_err("bad input ref");
    assert!(errs.iter().any(|e| e.field == "inputs"));
}

#[test]
fn bad_cron_arity_is_rejected() {
    let mut form = physical_form();
    form.schedule = "0 0 *".into();
    let errs = form_to_def(&form).expect_err("cron arity");
    assert!(errs.iter().any(|e| e.field == "schedule"));
}

#[test]
fn body_builder_skips_name_and_schedule_validation() {
    // Ad-hoc run needs no name; a valid body still passes even with empty name.
    let mut form = physical_form();
    form.name = String::new();
    let body = form_to_body(&form).expect("valid body");
    assert_eq!(body["kind"], json!("physical"));
}
```

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/ui:transforms-form`
Expected: `Tests finished: Pass 7. Fail 0`.

- [ ] **Step 6: Confirm lint-clean**

Run: `buck2 build --console none '//src/ui:ui-core[clippy.txt]' --show-simple-output`, then `cat` the printed path. Expected: empty.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/transforms.rs src/ui/src/lib.rs src/ui/BUCK src/ui/tests/transforms_form.rs
git commit -m "feat(ui): transform form → request builders with client validation"
```

---

### Task 4: Input-scoped completion-schema builders (`loom_ui_core`)

**Files:**
- Modify: `src/ui/src/transforms.rs` (append the schema builders)
- Modify: `src/ui/src/lib.rs` (extend the `pub use`)
- Modify: `src/ui/BUCK` (add `transforms-schema` `rust_test`)
- Test: `src/ui/tests/transforms_schema.rs`

**Interfaces:**
- Consumes: `TableRef` (Task 2); existing `loom_ui_core` types `CompletionSchema`, `CompletionTable`, `CompletionColumn`, `DatasetDetail` (`{snapshot_time, columns: Vec<SchemaCol{name, ty, nullable}>}`), `TypeDetail` (`{properties: Vec<PropRow{name, ty, required}>, ...}`).
- Produces (re-exported):
  - `fn schema_from_dataset_details(inputs: &[(TableRef, DatasetDetail)]) -> CompletionSchema`
  - `fn schema_from_types(types: &[(String, TypeDetail)]) -> CompletionSchema`

> **Deviation from spec, justified:** the spec sketches `schema_from_dataset_details(&[Value])` / `schema_from_types(&[Value])`. The detail responses do **not** carry their own table/type identifier (`parse_dataset_detail` yields only `{snapshot_time, columns}`; `parse_type_detail` yields no self-name), so a `CompletionTable.name` cannot be recovered from the `Value` alone. The builders therefore take the already-decoded detail **paired with the identity the caller holds** (the input `TableRef`, or the type name). This is the same "decoded structs, not raw `Value`" shape as the rest of `loom_ui_core`'s consumers.

- [ ] **Step 1: Extend the `lib.rs` re-export**

Add `schema_from_dataset_details, schema_from_types` to the `pub use transforms::{...}` list. Also confirm `transforms.rs` can name the completion/detail types — add at the top of `transforms.rs`:

```rust
use crate::{CompletionColumn, CompletionSchema, CompletionTable, DatasetDetail, TypeDetail};
```

- [ ] **Step 2: Append the builders to `transforms.rs`**

```rust
/// Build an input-scoped completion schema from each physical input's dataset detail.
#[must_use]
pub fn schema_from_dataset_details(inputs: &[(TableRef, DatasetDetail)]) -> CompletionSchema {
    CompletionSchema {
        tables: inputs
            .iter()
            .map(|(tref, detail)| CompletionTable {
                schema: Some(tref.schema.clone()),
                name: tref.name.clone(),
                columns: detail
                    .columns
                    .iter()
                    .map(|c| CompletionColumn {
                        name: c.name.clone(),
                        ty: c.ty.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// Build an input-scoped completion schema from each typed input's ontology properties.
#[must_use]
pub fn schema_from_types(types: &[(String, TypeDetail)]) -> CompletionSchema {
    CompletionSchema {
        tables: types
            .iter()
            .map(|(name, detail)| CompletionTable {
                schema: None,
                name: name.clone(),
                columns: detail
                    .properties
                    .iter()
                    .map(|p| CompletionColumn {
                        name: p.name.clone(),
                        ty: p.ty.clone(),
                    })
                    .collect(),
            })
            .collect(),
    }
}
```

- [ ] **Step 3: Add the test target to `src/ui/BUCK`**

```python
rust_test(
    name = "transforms-schema",
    crate = "transforms_schema",
    srcs = ["tests/transforms_schema.rs"],
    crate_root = "tests/transforms_schema.rs",
    edition = "2024",
    deps = [":ui-core"],
)
```

(No `serde_json` dep — this test builds structs directly.)

- [ ] **Step 4: Write the tests**

Create `src/ui/tests/transforms_schema.rs`:

```rust
use loom_ui_core::{
    DatasetDetail, PropRow, SchemaCol, TableRef, TypeDetail, schema_from_dataset_details,
    schema_from_types,
};

#[test]
fn physical_schema_carries_schema_name_and_columns() {
    let inputs = vec![(
        TableRef { schema: "sales".into(), name: "orders".into() },
        DatasetDetail {
            snapshot_time: "t".into(),
            columns: vec![
                SchemaCol { name: "id".into(), ty: "int64".into(), nullable: false },
                SchemaCol { name: "total".into(), ty: "float64".into(), nullable: true },
            ],
        },
    )];
    let schema = schema_from_dataset_details(&inputs);
    assert_eq!(schema.tables.len(), 1);
    let t = &schema.tables[0];
    assert_eq!(t.schema.as_deref(), Some("sales"));
    assert_eq!(t.name, "orders");
    assert_eq!(t.columns.len(), 2);
    assert_eq!(t.columns[0].name, "id");
    assert_eq!(t.columns[0].ty, "int64");
}

#[test]
fn typed_schema_uses_type_name_and_properties() {
    let types = vec![(
        "Customer".to_string(),
        TypeDetail {
            properties: vec![
                PropRow { name: "id".into(), ty: "int64".into(), required: true },
                PropRow { name: "email".into(), ty: "utf8".into(), required: false },
            ],
            ..TypeDetail::default()
        },
    )];
    let schema = schema_from_types(&types);
    assert_eq!(schema.tables.len(), 1);
    let t = &schema.tables[0];
    assert_eq!(t.schema, None);
    assert_eq!(t.name, "Customer");
    assert_eq!(t.columns.len(), 2);
    assert_eq!(t.columns[1].name, "email");
    assert_eq!(t.columns[1].ty, "utf8");
}

#[test]
fn empty_inputs_yield_empty_schema() {
    assert!(schema_from_dataset_details(&[]).tables.is_empty());
    assert!(schema_from_types(&[]).tables.is_empty());
}
```

This test uses `PropRow`, `SchemaCol`, `TypeDetail`, `DatasetDetail` — confirm all four are `pub` re-exports of `loom_ui_core` (they are: defined in `lib.rs`). `TypeDetail` derives `Default`; `DatasetDetail` derives `Default` — construct explicitly as shown.

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/ui:transforms-schema`
Expected: `Tests finished: Pass 3. Fail 0`.

- [ ] **Step 6: Confirm lint-clean**

Run: `buck2 build --console none '//src/ui:ui-core[clippy.txt]' --show-simple-output`, then `cat`. Expected: empty.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/transforms.rs src/ui/src/lib.rs src/ui/BUCK src/ui/tests/transforms_schema.rs
git commit -m "feat(ui): input-scoped completion-schema builders"
```

---

### Task 5: Display mappers + drawer-width clamp (`loom_ui_core`)

**Files:**
- Modify: `src/ui/src/transforms.rs` (append the display + clamp helpers)
- Modify: `src/ui/src/lib.rs` (extend the `pub use`)
- Modify: `src/ui/BUCK` (add `transforms-display` `rust_test`)
- Test: `src/ui/tests/transforms_display.rs`

**Interfaces:**
- Consumes: `TransformKind` (Task 2); existing `loom_ui_core` token enums `BadgeTone` (`{Neutral, Info, Pii, Success, Warning, Danger}`) and `Status` (`{Ok, Warn, Error}`).
- Produces (re-exported):
  - `fn run_state_tone(state: &str) -> BadgeTone`
  - `fn run_state_status(state: &str) -> Status`
  - `fn trigger_label(trigger: &str) -> &'static str`
  - `fn kind_badge_label(kind: TransformKind) -> &'static str`
  - `fn clamp_drawer_width(px: i32, min: u32, max: u32) -> u32`

- [ ] **Step 1: Extend the `lib.rs` re-export**

Add `clamp_drawer_width, kind_badge_label, run_state_status, run_state_tone, trigger_label` to `pub use transforms::{...}`. Add to the top of `transforms.rs`:

```rust
use crate::{BadgeTone, Status};
```

- [ ] **Step 2: Append the helpers to `transforms.rs`**

```rust
/// Map a run `state` string to a badge tone.
#[must_use]
pub fn run_state_tone(state: &str) -> BadgeTone {
    match state {
        "succeeded" => BadgeTone::Success,
        "failed" => BadgeTone::Danger,
        "running" => BadgeTone::Info,
        _ => BadgeTone::Neutral,
    }
}

/// Map a run `state` string to a status-dot status.
#[must_use]
pub fn run_state_status(state: &str) -> Status {
    match state {
        "succeeded" => Status::Ok,
        "failed" => Status::Error,
        _ => Status::Warn,
    }
}

/// Human label for a run `trigger` string.
#[must_use]
pub fn trigger_label(trigger: &str) -> &'static str {
    match trigger {
        "manual" => "Manual",
        "schedule" => "Schedule",
        "data-trigger" => "Data trigger",
        "ad-hoc" => "Ad-hoc",
        _ => "Unknown",
    }
}

/// Badge label for a transform kind.
#[must_use]
pub fn kind_badge_label(kind: TransformKind) -> &'static str {
    match kind {
        TransformKind::Physical => "Physical",
        TransformKind::Typed => "Typed",
    }
}

/// Clamp a proposed drawer width (integer px, may be negative) into `[min, max]`.
#[must_use]
pub fn clamp_drawer_width(px: i32, min: u32, max: u32) -> u32 {
    u32::try_from(px).unwrap_or(0).clamp(min, max)
}
```

`u32::try_from(px)` returns `Err` for negative `px` → `.unwrap_or(0)` (not the lint-gated `unwrap()`), which then clamps up to `min`. No casts, no `unwrap()`.

- [ ] **Step 3: Add the test target to `src/ui/BUCK`**

```python
rust_test(
    name = "transforms-display",
    crate = "transforms_display",
    srcs = ["tests/transforms_display.rs"],
    crate_root = "tests/transforms_display.rs",
    edition = "2024",
    deps = [":ui-core"],
)
```

- [ ] **Step 4: Write the tests**

Create `src/ui/tests/transforms_display.rs`:

```rust
use loom_ui_core::{
    BadgeTone, Status, TransformKind, clamp_drawer_width, kind_badge_label, run_state_status,
    run_state_tone, trigger_label,
};

#[test]
fn state_maps_to_tone() {
    assert_eq!(run_state_tone("succeeded"), BadgeTone::Success);
    assert_eq!(run_state_tone("failed"), BadgeTone::Danger);
    assert_eq!(run_state_tone("running"), BadgeTone::Info);
    assert_eq!(run_state_tone("queued"), BadgeTone::Neutral);
    assert_eq!(run_state_tone("weird"), BadgeTone::Neutral);
}

#[test]
fn state_maps_to_status() {
    assert_eq!(run_state_status("succeeded"), Status::Ok);
    assert_eq!(run_state_status("failed"), Status::Error);
    assert_eq!(run_state_status("running"), Status::Warn);
    assert_eq!(run_state_status("queued"), Status::Warn);
}

#[test]
fn trigger_labels() {
    assert_eq!(trigger_label("manual"), "Manual");
    assert_eq!(trigger_label("data-trigger"), "Data trigger");
    assert_eq!(trigger_label("ad-hoc"), "Ad-hoc");
    assert_eq!(trigger_label("nope"), "Unknown");
}

#[test]
fn kind_labels() {
    assert_eq!(kind_badge_label(TransformKind::Physical), "Physical");
    assert_eq!(kind_badge_label(TransformKind::Typed), "Typed");
}

#[test]
fn drawer_width_clamps_both_ends_and_negatives() {
    assert_eq!(clamp_drawer_width(600, 360, 900), 600);
    assert_eq!(clamp_drawer_width(100, 360, 900), 360);
    assert_eq!(clamp_drawer_width(1200, 360, 900), 900);
    assert_eq!(clamp_drawer_width(-50, 360, 900), 360);
}
```

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/ui:transforms-display`
Expected: `Tests finished: Pass 5. Fail 0`.

- [ ] **Step 6: Confirm lint-clean**

Run: `buck2 build --console none '//src/ui:ui-core[clippy.txt]' --show-simple-output`, then `cat`. Expected: empty.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/transforms.rs src/ui/src/lib.rs src/ui/BUCK src/ui/tests/transforms_display.rs
git commit -m "feat(ui): run/kind display mappers + drawer-width clamp"
```

---

### Task 6: HTTP layer — `FetchError::Forbidden`/`Rejected` + 7 transform fetches (`net.rs`)

**Files:**
- Modify: `src/ui/src/net.rs`

**Interfaces:**
- Consumes: `loom_ui_core::{TransformSummary, TransformDefView, RunRow, parse_transform_list, parse_transform_def, parse_runs}`; existing `url` helper; `serde_json::Value`.
- Produces (all `pub async fn`, first two params `base: &str, token: &str`):
  - `FetchError` extended with `Forbidden` and `Rejected(String)`.
  - `list_transforms(base, token) -> Result<Vec<TransformSummary>, FetchError>`
  - `get_transform(base, token, name: &str) -> Result<TransformDefView, FetchError>`
  - `list_runs(base, token, name: &str) -> Result<Vec<RunRow>, FetchError>`
  - `define_transform(base, token, def: &Value) -> Result<(), FetchError>`
  - `delete_transform(base, token, name: &str) -> Result<(), FetchError>`
  - `run_transform(base, token, name: &str) -> Result<String, FetchError>` (returns `run_id`)
  - `run_adhoc(base, token, body: &Value) -> Result<String, FetchError>` (returns `run_id`)

> **Route reconciliation (7 of 8 backed):** Decision 1 says "all eight routes wired," but the spec's own "Data flow & fetch" list omits the 8th, `GET /admin/runs/{run_id}` (get-run). The Runs tab renders the full `TransformRunView` set from `list_runs` (`GET /admin/transforms/{name}/runs`), which **subsumes** single-run get-run — there is no UI need for a separate single-run fetch in v1 (no live/streaming run polling; history is fetch-on-demand). So this plan wires 7 net calls and deliberately does not add a `get_run`, matching the spec's data-flow list. If a reviewer insists on literal 8/8, add a `get_run(base, token, run_id) -> Result<RunRow, FetchError>` and call it to refresh the just-started run after `run_transform`; it is otherwise dead code.

> `net.rs` is inside the `:app` crate (crate root `main.rs` carries `#![allow(clippy::pedantic, clippy::restriction)]`), so this code follows existing `net.rs` style and is **verified by wasm compile**, not a `rust_test` (there is no DOM/HTTP test harness). The spec asks only for a `Forbidden` variant; `Rejected(String)` is added because the Error-handling section requires the 400 validation message be surfaced beside the form, and `Server(400)` discards the body.

- [ ] **Step 1: Extend `FetchError` and its `Display`**

In `src/ui/src/net.rs`, change the enum (lines ~57-66) to:

```rust
/// Why a governed request failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// The bearer token is missing/expired (HTTP 401) — the caller should log out.
    Unauthorized,
    /// Authenticated but lacking the required role (HTTP 403).
    Forbidden,
    /// The request never completed (transport / decode failure).
    Network,
    /// The server rejected the request with a message (e.g. HTTP 400 validation).
    Rejected(String),
    /// The server responded with an unexpected status.
    Server(u16),
}
```

And its `Display` impl:

```rust
impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized => write!(f, "your session has expired — please sign in again"),
            Self::Forbidden => write!(f, "this action requires the admin role"),
            Self::Rejected(msg) => write!(f, "{msg}"),
            Self::Server(c) => write!(f, "server error ({c})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}
```

- [ ] **Step 2: Map 403 in `fetch_status_err`**

```rust
fn fetch_status_err(status: u16) -> FetchError {
    match status {
        401 => FetchError::Unauthorized,
        403 => FetchError::Forbidden,
        s => FetchError::Server(s),
    }
}
```

- [ ] **Step 3: Add a write-status helper that captures the 400 body**

Add near `fetch_status_err`:

```rust
/// Map a non-success write response to an error, reading the body on 400 so the
/// server's validation message can be surfaced.
async fn write_status_err(resp: gloo_net::http::Response) -> FetchError {
    match resp.status() {
        401 => FetchError::Unauthorized,
        403 => FetchError::Forbidden,
        400 => {
            let msg = resp.text().await.unwrap_or_default();
            if msg.is_empty() {
                FetchError::Rejected("request rejected".to_string())
            } else {
                FetchError::Rejected(msg)
            }
        }
        s => FetchError::Server(s),
    }
}
```

- [ ] **Step 4: Add the imports**

Extend the `use loom_ui_core::{...}` block at the top of `net.rs` with `RunRow, TransformDefView, TransformSummary, parse_runs, parse_transform_def, parse_transform_list` (keep it alphabetized with the existing imports). Ensure `serde_json::Value` is in scope (add `use serde_json::Value;` if not already).

- [ ] **Step 5: Add the read fetches**

```rust
/// GET /admin/transforms with the bearer token.
pub async fn list_transforms(base: &str, token: &str) -> Result<Vec<TransformSummary>, FetchError> {
    let resp = Request::get(&url(base, "/admin/transforms"))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_transform_list(&body))
}

/// GET /admin/transforms/{name} with the bearer token.
pub async fn get_transform(
    base: &str,
    token: &str,
    name: &str,
) -> Result<TransformDefView, FetchError> {
    let resp = Request::get(&url(base, &format!("/admin/transforms/{name}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_transform_def(&body))
}

/// GET /admin/transforms/{name}/runs with the bearer token (newest first).
pub async fn list_runs(base: &str, token: &str, name: &str) -> Result<Vec<RunRow>, FetchError> {
    let resp = Request::get(&url(base, &format!("/admin/transforms/{name}/runs")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 200 {
        return Err(fetch_status_err(resp.status()));
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(parse_runs(&body))
}
```

- [ ] **Step 6: Add the write fetches**

```rust
/// POST /admin/transforms — define/redefine (expects 201).
pub async fn define_transform(base: &str, token: &str, def: &Value) -> Result<(), FetchError> {
    let resp = Request::post(&url(base, "/admin/transforms"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(def)
        .map_err(|_| FetchError::Network)?
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() == 201 {
        Ok(())
    } else {
        Err(write_status_err(resp).await)
    }
}

/// DELETE /admin/transforms/{name} — idempotent delete (expects 200).
pub async fn delete_transform(base: &str, token: &str, name: &str) -> Result<(), FetchError> {
    let resp = Request::delete(&url(base, &format!("/admin/transforms/{name}")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() == 200 {
        Ok(())
    } else {
        Err(fetch_status_err(resp.status()))
    }
}

/// POST /admin/transforms/{name}/run — run a saved transform now (expects 202 {run_id}).
pub async fn run_transform(base: &str, token: &str, name: &str) -> Result<String, FetchError> {
    let resp = Request::post(&url(base, &format!("/admin/transforms/{name}/run")))
        .header("Authorization", &format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 202 {
        return Err(write_status_err(resp).await);
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body.get("run_id").and_then(Value::as_str).unwrap_or_default().to_string())
}

/// POST /admin/transforms/run — run an ad-hoc body (expects 202 {run_id}).
pub async fn run_adhoc(base: &str, token: &str, body_json: &Value) -> Result<String, FetchError> {
    let resp = Request::post(&url(base, "/admin/transforms/run"))
        .header("Authorization", &format!("Bearer {token}"))
        .json(body_json)
        .map_err(|_| FetchError::Network)?
        .send()
        .await
        .map_err(|_| FetchError::Network)?;
    if resp.status() != 202 {
        return Err(write_status_err(resp).await);
    }
    let body: Value = resp.json().await.map_err(|_| FetchError::Network)?;
    Ok(body.get("run_id").and_then(Value::as_str).unwrap_or_default().to_string())
}
```

Note: `Request::post(..).json(def)` returns `Result<Request, _>` (serialization) — hence the `.map_err(|_| FetchError::Network)?` before `.send()`. Mirror `gloo_net`'s builder exactly; if the installed `gloo-net` version's `.json()` signature differs, follow whatever the existing POST call in `net.rs` (`login`, lines ~36-47) does.

- [ ] **Step 7: Verify the wasm crate builds**

Run: `buck2 build -v0 --console none //src/ui:app`
Expected: exit 0. (No `dead_code` warnings escalate to errors here, but every new fn is consumed by Task 9; if building this task in isolation flags unused fns, that is expected until Task 9 wires them and is not a failure — the strict gate does not apply to `:app`.)

- [ ] **Step 8: Commit**

```bash
git add src/ui/src/net.rs
git commit -m "feat(ui): admin transform HTTP calls + Forbidden/Rejected FetchError"
```

---

### Task 7: Resizable drawer in the shared `Shell`

**Files:**
- Modify: `src/ui/src/components/shell.rs`

**Interfaces:**
- Consumes: `loom_ui_core::clamp_drawer_width` (Task 5).
- Produces: a `Shell` whose drawer width is user-draggable via a handle on the drawer's left edge, clamped to `[MIN, MAX]` and persisted in `localStorage` (key `loom_drawer_width`). No `ShellProps` change — all surfaces inherit it. Constants `const DRAWER_MIN: u32 = 360; const DRAWER_MAX: u32 = 900; const DRAWER_DEFAULT: u32 = 428;`.

> View glue in the glob'd `loom_ui_components` crate (no BUCK edit). Not `rust_test`-able (no DOM); verified by gallery + browser. The crate root carries `#![allow(clippy::pedantic, clippy::restriction)]`.

- [ ] **Step 1: Add width state, drag handlers, and persistence to `Shell`**

In `src/ui/src/components/shell.rs`, inside the `Shell` component fn (before the returned `html!`), add:

```rust
const DRAWER_MIN: u32 = 360;
const DRAWER_MAX: u32 = 900;
const DRAWER_DEFAULT: u32 = 428;

// Persisted drawer width (localStorage), clamped on load.
let width = use_state(|| {
    web_sys::window()
        .and_then(|w| w.local_storage().ok().flatten())
        .and_then(|s| s.get_item("loom_drawer_width").ok().flatten())
        .and_then(|v| v.parse::<i32>().ok())
        .map_or(DRAWER_DEFAULT, |px| clamp_drawer_width(px, DRAWER_MIN, DRAWER_MAX))
});
let dragging = use_state(|| false);

let on_handle_down = {
    let dragging = dragging.clone();
    Callback::from(move |_e: MouseEvent| dragging.set(true))
};
let on_mouse_move = {
    let width = width.clone();
    let dragging = dragging.clone();
    Callback::from(move |e: MouseEvent| {
        if !*dragging {
            return;
        }
        // Drawer is right-anchored: width = viewport_right - cursor_x.
        if let Some(win) = web_sys::window() {
            let vw = win.inner_width().ok().and_then(|v| v.as_f64()).unwrap_or(0.0);
            let proposed = (vw - f64::from(e.client_x())).round() as i32;
            let clamped = clamp_drawer_width(proposed, DRAWER_MIN, DRAWER_MAX);
            width.set(clamped);
        }
    })
};
let on_mouse_up = {
    let width = width.clone();
    let dragging = dragging.clone();
    Callback::from(move |_e: MouseEvent| {
        if *dragging {
            dragging.set(false);
            if let Some(store) = web_sys::window().and_then(|w| w.local_storage().ok().flatten()) {
                let _ = store.set_item("loom_drawer_width", &width.to_string());
            }
        }
    })
};
```

Add `use loom_ui_core::clamp_drawer_width;` to the file's imports. `web_sys` is already a `:app`/`:ui-components` dep (confirm `web-sys` is in the `:ui-components` deps in `src/ui/BUCK`; the `sql_editor` component already uses `web_sys`, so it is present). The `proposed as i32` cast is fine here — this crate is not strict-lint-gated.

- [ ] **Step 2: Apply the width and render the drag handle in the drawer markup**

Change the list+drawer split (currently lines ~73-78) to bind an inline width style, attach the window-level move/up handlers on the `shell-body`, and render a drag handle on the drawer's left edge:

```rust
html! {
    <div class={classes!("shell-body")}
         onmousemove={on_mouse_move} onmouseup={on_mouse_up}
         onmouseleave={on_mouse_up.clone()}>
        <div class="shell-list">{ props.list.clone() }</div>
        if !is_empty(&props.drawer) {
            <div class="shell-drawer" style={format!("width: {}px", *width)}>
                <div class="shell-drawer-handle" onmousedown={on_handle_down}></div>
                { props.drawer.clone() }
            </div>
        }
    </div>
}
```

(Keep the rest of the `Shell` `html!` — nav, `search`, `avatar` — as-is; only the `shell-body` block changes. If `Shell` does not already `use yew::prelude::*` bringing `classes!`, use the existing class-attribute style in the file.)

- [ ] **Step 3: Update the `Shell` CSS**

In the component's `css!`, replace the fixed `.shell-drawer` rule and add the handle + a drag affordance. The drawer's `width` is now driven by the inline style, so drop the hard-coded `width: 428px`:

```css
.shell-drawer {
    flex: none;
    position: relative;
    background: var(--loom-panel);
    border-left: 1px solid var(--loom-border);
}
.shell-drawer-handle {
    position: absolute;
    left: -3px;
    top: 0;
    width: 6px;
    height: 100%;
    cursor: col-resize;
    z-index: 2;
}
.shell-drawer-handle:hover {
    background: var(--loom-accent);
    opacity: 0.4;
}
```

Keep the existing `shell-*` class-prefixing discipline (the file's comment about `Panel`'s `.body`/`.title` leaking — do not introduce unprefixed structural classes).

- [ ] **Step 4: Build the app and the gallery**

Run: `buck2 build -v0 --console none //src/ui:app //src/ui:gallery`
Expected: exit 0.

- [ ] **Step 5: Verify by eye in the browser**

Build + serve the gallery (`buck2 build //src/ui:gallery-bundle` then `buck2 run //src/ui:gallery-serve`) OR, if the gallery does not exercise a drawer, defer visual verification to the Task 9 browser walkthrough. Confirm: drag handle resizes the drawer, width clamps at both ends, and a reload restores the persisted width. Record the outcome in the task notes.

- [ ] **Step 6: Commit**

```bash
git add src/ui/src/components/shell.rs
git commit -m "feat(ui): resizable, persisted drawer in the shared Shell"
```

---

### Task 8: Transforms surface view glue (`surfaces/transforms.rs`)

**Files:**
- Create: `src/ui/src/surfaces/transforms.rs`
- Modify: `src/ui/src/surfaces/mod.rs`
- Modify: `src/ui/BUCK` (add `src/surfaces/transforms.rs` to the `:app` `srcs`)

**Interfaces:**
- Consumes: `loom_ui_core::{TransformSummary, TransformDefView, RunRow, TransformForm, TransformKind, OutputMode, FieldError, TransformIo, CompletionSchema, kind_badge_label, run_state_tone, trigger_label}`; `loom_ui_components::{Panel, DataTable, Column, TableRow, Button, ButtonVariant, Badge, StatusDot, Input, Tabs, TabItem, SqlEditor}`; `loom_ui_core::Align`.
- Produces the presentational components (all state lives in `Workspace`, Task 9):
  - `TransformsList` — props `{ rows: Vec<TransformSummary>, status: LoadStatus, selected: Option<usize>, on_row: Callback<usize>, on_new: Callback<()>, forbidden: bool }`.
  - `TransformDrawer` — props `{ def: TransformDefView, active_tab: AttrValue, on_tab: Callback<AttrValue>, runs: Vec<RunRow>, runs_status: LoadStatus, on_edit: Callback<()>, on_run: Callback<()>, on_delete: Callback<()> }`.
  - `TransformEditor` — props `{ form: TransformForm, schema: CompletionSchema, editing: bool, dataset_options: Vec<String>, type_options: Vec<String>, errors: Vec<FieldError>, server_error: Option<AttrValue>, on_change: Callback<TransformForm>, on_submit: Callback<()>, on_run_adhoc: Callback<()>, on_cancel: Callback<()> }`.
- Reuses `LoadStatus` from `surfaces::ontology` (already `pub`).

> View glue — verified by wasm compile + browser (Task 9), no `rust_test`. Keep components controlled/stateless: they render from props and emit callbacks; the `Workspace` owns all state.

- [ ] **Step 1: Declare the module**

In `src/ui/src/surfaces/mod.rs`:

```rust
mod catalog;
mod ontology;
mod transforms;

pub use catalog::{CatalogDrawer, CatalogList};
pub use ontology::{LoadStatus, OntologyDrawer, OntologyList, OntologyTypeRow};
pub use transforms::{TransformDrawer, TransformEditor, TransformsList};
```

- [ ] **Step 2: Add the file to the `:app` `srcs` in `src/ui/BUCK`**

```python
    srcs = ["src/main.rs", "src/surfaces/mod.rs", "src/surfaces/catalog.rs", "src/surfaces/ontology.rs", "src/surfaces/transforms.rs", "src/net.rs", "src/session.rs"],
```

- [ ] **Step 3: Write `TransformsList`**

Create `src/ui/src/surfaces/transforms.rs` starting with the list. Model a private row type implementing `TableRow` (mirror `catalog.rs`'s `CatalogRow`). The `forbidden` prop renders the admin empty-state instead of the table:

```rust
use loom_ui_components::{
    Badge, Button, ButtonVariant, Column, DataTable, Input, Panel, SqlEditor, StatusDot, TableRow,
    TabItem, Tabs,
};
use loom_ui_core::{
    Align, BadgeTone, CompletionSchema, FieldError, OutputMode, RunRow, Status, TransformDefView,
    TransformForm, TransformIo, TransformKind, TransformSummary, kind_badge_label, run_state_tone,
    trigger_label,
};
use yew::prelude::*;

use crate::surfaces::LoadStatus;

// `DataTable<R>` bounds `R: PartialEq + Clone + TableRow + 'static`, so the
// derives are mandatory (mirror `CatalogRow`/`OntologyTypeRow`).
#[derive(Clone, PartialEq)]
struct TransformListRow {
    name: String,
    kind: TransformKind,
    schedule: String,
    on_input_commit: bool,
}

impl TableRow for TransformListRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { { &self.name } },
            html! { <Badge label={kind_badge_label(self.kind)} tone={BadgeTone::Info} /> },
            html! { { &self.schedule } },
            html! { <StatusDot status={ if self.on_input_commit { Status::Ok } else { Status::Warn } } /> },
        ]
    }
}

fn list_columns() -> Vec<Column> {
    vec![
        Column { label: "Name".into(), align: Align::Start },
        Column { label: "Kind".into(), align: Align::Start },
        Column { label: "Schedule".into(), align: Align::Start },
        Column { label: "On commit".into(), align: Align::End },
    ]
}

#[derive(Properties, PartialEq)]
pub struct TransformsListProps {
    pub rows: Vec<TransformSummary>,
    pub status: LoadStatus,
    pub selected: Option<usize>,
    pub on_row: Callback<usize>,
    pub on_new: Callback<()>,
    pub forbidden: bool,
}

#[function_component(TransformsList)]
pub fn transforms_list(props: &TransformsListProps) -> Html {
    if props.forbidden {
        return html! {
            <Panel title="Transforms">
                <p>{ "Transforms requires the admin role." }</p>
            </Panel>
        };
    }
    let on_new = props.on_new.clone();
    let new_click = Callback::from(move |_| on_new.emit(()));
    let body = match &props.status {
        LoadStatus::Loading => html! { <p>{ "Loading…" }</p> },
        LoadStatus::Error(e) => html! { <p class="error">{ e }</p> },
        LoadStatus::Idle => {
            let rows: Vec<TransformListRow> = props
                .rows
                .iter()
                .map(|r| TransformListRow {
                    name: r.name.clone(),
                    kind: r.kind,
                    schedule: r.schedule.clone().unwrap_or_else(|| "manual".to_string()),
                    on_input_commit: r.on_input_commit,
                })
                .collect();
            html! {
                <DataTable<TransformListRow>
                    columns={list_columns()} rows={rows}
                    selected={props.selected} onrow={props.on_row.clone()} />
            }
        }
    };
    html! {
        <Panel title="Transforms">
            <Button variant={ButtonVariant::Primary} onclick={new_click}>{ "＋ New transform" }</Button>
            { body }
        </Panel>
    }
}
```

(Adjust `DataTable`/`Badge`/`StatusDot`/`Column`/`Button` prop names to the exact signatures verified in the component library; `DataTable<R>` needs the turbofish as shown. `Panel`/`p.error` class reuse follows `catalog.rs`.)

- [ ] **Step 4: Write `TransformDrawer` (definition + runs tabs)**

Append the drawer. Two tabs (`definition`, `runs`). The definition tab renders a **read-only** `SqlEditor` (keyed on the input fingerprint so it is stable), the inputs/output/mode/schedule metadata, and Edit/Run/Delete buttons. The runs tab renders a `DataTable` of state `Badge` + trigger + timestamps + error + snapshot.

```rust
#[derive(Clone, PartialEq)]
struct RunTableRow {
    state: String,
    trigger: String,
    started: String,
    finished: String,
    snapshot: String,
    error: String,
}

impl TableRow for RunTableRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <Badge label={self.state.clone()} tone={run_state_tone(&self.state)} /> },
            html! { { trigger_label(&self.trigger) } },
            html! { { &self.started } },
            html! { { &self.finished } },
            html! { { &self.snapshot } },
            html! { { &self.error } },
        ]
    }
}

fn run_columns() -> Vec<Column> {
    vec![
        Column { label: "State".into(), align: Align::Start },
        Column { label: "Trigger".into(), align: Align::Start },
        Column { label: "Started".into(), align: Align::Start },
        Column { label: "Finished".into(), align: Align::Start },
        Column { label: "Snapshot".into(), align: Align::Start },
        Column { label: "Error".into(), align: Align::Start },
    ]
}

/// A stable fingerprint of a transform's inputs, used as the SqlEditor remount key.
fn io_fingerprint(io: &TransformIo) -> String {
    match io {
        TransformIo::Physical { inputs, .. } => {
            let mut parts: Vec<String> =
                inputs.iter().map(|t| format!("{}.{}", t.schema, t.name)).collect();
            parts.sort();
            format!("physical:{}", parts.join(","))
        }
        TransformIo::Typed { inputs, .. } => {
            let mut parts = inputs.clone();
            parts.sort();
            format!("typed:{}", parts.join(","))
        }
    }
}

/// Render the definition-view metadata rows (inputs/output/mode/schedule/on-commit)
/// required by the spec's Surface layout, mirroring `catalog.rs`'s field-row style.
fn def_metadata(def: &TransformDefView) -> Html {
    let (inputs, output) = match &def.body.io {
        TransformIo::Physical { inputs, output } => (
            inputs.iter().map(|t| format!("{}.{}", t.schema, t.name)).collect::<Vec<_>>().join(", "),
            format!("{}.{}", output.schema, output.name),
        ),
        TransformIo::Typed { inputs, output } => (inputs.join(", "), output.clone()),
    };
    let schedule = def.schedule.clone().unwrap_or_else(|| "manual".to_string());
    let row = |label: &str, value: String| html! {
        <div class="tf-meta-row"><span class="tf-meta-key">{ label }</span><span>{ value }</span></div>
    };
    html! {
        <div class="tf-meta">
            { row("Kind", kind_badge_label(def.body.kind()).to_string()) }
            { row("Inputs", inputs) }
            { row("Output", output) }
            { row("Output mode", def.body.output_mode.as_str().to_string()) }
            { row("Schedule", schedule) }
            { row("On input commit", def.on_input_commit.to_string()) }
            { row("Next run", def.next_run_at.clone().unwrap_or_default()) }
        </div>
    }
}

#[derive(Properties, PartialEq)]
pub struct TransformDrawerProps {
    pub def: TransformDefView,
    pub active_tab: AttrValue,
    pub on_tab: Callback<AttrValue>,
    pub runs: Vec<RunRow>,
    pub runs_status: LoadStatus,
    pub on_edit: Callback<()>,
    pub on_run: Callback<()>,
    pub on_delete: Callback<()>,
}

#[function_component(TransformDrawer)]
pub fn transform_drawer(props: &TransformDrawerProps) -> Html {
    let tabs = vec![
        TabItem { id: "definition".into(), label: "Definition".into() },
        TabItem { id: "runs".into(), label: "Runs".into() },
    ];
    let no_op = Callback::from(|_: String| {});
    let on_edit = props.on_edit.clone();
    let on_run = props.on_run.clone();
    let on_delete = props.on_delete.clone();
    let body = match props.active_tab.as_str() {
        "runs" => match &props.runs_status {
            LoadStatus::Loading => html! { <p>{ "Loading…" }</p> },
            LoadStatus::Error(e) => html! { <p class="error">{ e }</p> },
            LoadStatus::Idle => {
                let rows: Vec<RunTableRow> = props
                    .runs
                    .iter()
                    .map(|r| RunTableRow {
                        state: r.state.clone(),
                        trigger: r.trigger.clone(),
                        started: r.started_at.clone().unwrap_or_default(),
                        finished: r.finished_at.clone().unwrap_or_default(),
                        snapshot: r.snapshot_id.clone().unwrap_or_default(),
                        error: r.error.clone().unwrap_or_default(),
                    })
                    .collect();
                html! { <DataTable<RunTableRow> columns={run_columns()} rows={rows} /> }
            }
        },
        _ => {
            let key = io_fingerprint(&props.def.body.io);
            html! {
                <>
                    { def_metadata(&props.def) }
                    <SqlEditor key={key} value={props.def.body.sql.clone()}
                               on_change={no_op} read_only={true} />
                    <div class="shell-drawer-actions">
                        <Button variant={ButtonVariant::Secondary}
                                onclick={Callback::from(move |_| on_edit.emit(()))}>{ "Edit" }</Button>
                        <Button variant={ButtonVariant::Primary}
                                onclick={Callback::from(move |_| on_run.emit(()))}>{ "Run" }</Button>
                        <Button variant={ButtonVariant::Ghost}
                                onclick={Callback::from(move |_| on_delete.emit(()))}>{ "Delete" }</Button>
                    </div>
                </>
            }
        }
    };
    html! {
        <Panel title={props.def.name.clone()}>
            <Tabs tabs={tabs} active={props.active_tab.clone()} onselect={props.on_tab.clone()} />
            { body }
        </Panel>
    }
}
```

- [ ] **Step 5: Write `TransformEditor` (the schema-fed form)**

Append the editor form. It renders: name `Input`, kind toggle, inputs multi-select (from `dataset_options`/`type_options`), output field, the **writable** `SqlEditor` keyed on the input fingerprint of the *form* inputs, schedule `Input`, `on_input_commit` toggle, output-mode toggle, inline `errors`, an optional `server_error`, and **Define** + **Run ad-hoc** buttons. Every field edit clones the `form`, mutates one field, and emits `on_change`.

**Use a checkbox list for the inputs multi-select, NOT `<select multiple>`.** Reading a `<select multiple>`'s selection requires `web_sys::HtmlSelectElement`, which is **not** in `src/ui/Cargo.toml`'s `web-sys` feature list (only `HtmlInputElement` is). A checkbox per option (`<input type="checkbox">`) stays within the already-enabled `HtmlInputElement`, so no `Cargo.toml`/`buckify.sh` change is needed. Each checkbox's `on_change` adds/removes its option string from `form.inputs`. The boolean toggles (`on_input_commit`) and the two segmented toggles (kind, output-mode) are also `<input type="checkbox">` / a pair of `<Button>`s — all within `HtmlInputElement`/existing primitives.

```rust
fn form_fingerprint(form: &TransformForm) -> String {
    let mut inputs = form.inputs.clone();
    inputs.sort();
    format!("{}:{}", form.kind.as_str(), inputs.join(","))
}

#[derive(Properties, PartialEq)]
pub struct TransformEditorProps {
    pub form: TransformForm,
    pub schema: CompletionSchema,
    pub editing: bool,
    pub dataset_options: Vec<String>,
    pub type_options: Vec<String>,
    pub errors: Vec<FieldError>,
    pub server_error: Option<AttrValue>,
    pub on_change: Callback<TransformForm>,
    pub on_submit: Callback<()>,
    pub on_run_adhoc: Callback<()>,
    pub on_cancel: Callback<()>,
}

#[function_component(TransformEditor)]
pub fn transform_editor(props: &TransformEditorProps) -> Html {
    let form = props.form.clone();
    let emit = props.on_change.clone();

    // One representative field wiring (name); mirror for output/schedule/sql/toggles.
    let on_name = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: InputEvent| {
            let value = input_value(&e);
            let mut next = form.clone();
            next.name = value;
            emit.emit(next);
        })
    };
    let on_sql = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |value: String| {
            let mut next = form.clone();
            next.sql = value;
            emit.emit(next);
        })
    };
    // Kind toggle: switching kind CLEARS inputs/output (their meaning changes:
    // "schema.name" for physical vs a type name for typed).
    let set_kind = {
        let form = form.clone();
        let emit = emit.clone();
        move |kind: TransformKind| {
            let mut next = form.clone();
            next.kind = kind;
            next.inputs.clear();
            next.output.clear();
            emit.emit(next);
        }
    };
    let on_physical = { let f = set_kind.clone(); Callback::from(move |_| f(TransformKind::Physical)) };
    let on_typed = { let f = set_kind.clone(); Callback::from(move |_| f(TransformKind::Typed)) };
    // The options offered by the checkbox list depend on the selected kind.
    let options: Vec<String> = match props.form.kind {
        TransformKind::Physical => props.dataset_options.clone(),
        TransformKind::Typed => props.type_options.clone(),
    };
    // One checkbox per option; toggling adds/removes the option string from `inputs`.
    let input_checkboxes: Html = options
        .into_iter()
        .map(|opt| {
            let checked = props.form.inputs.contains(&opt);
            let on_toggle = {
                let form = form.clone();
                let emit = emit.clone();
                let opt = opt.clone();
                Callback::from(move |_e: Event| {
                    let mut next = form.clone();
                    if next.inputs.contains(&opt) {
                        next.inputs.retain(|i| i != &opt);
                    } else {
                        next.inputs.push(opt.clone());
                    }
                    emit.emit(next);
                })
            };
            html! {
                <label class="tf-check">
                    <input type="checkbox" checked={checked} onchange={on_toggle} />
                    { opt }
                </label>
            }
        })
        .collect();
    // output / schedule text fields (mirror `on_name`, mutating `next.output` / `next.schedule`).
    let on_output = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: InputEvent| {
            let mut next = form.clone();
            next.output = input_value(&e);
            emit.emit(next);
        })
    };
    let on_schedule = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: InputEvent| {
            let mut next = form.clone();
            next.schedule = input_value(&e);
            emit.emit(next);
        })
    };
    // Boolean/segmented toggles (on_input_commit, output_mode) follow the same
    // clone-mutate-emit shape; each is an `<input type="checkbox">` reading `.checked()`.
    let on_commit_toggle = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: Event| {
            use wasm_bindgen::JsCast;
            let checked = e
                .target()
                .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
                .map(|el| el.checked())
                .unwrap_or(false);
            let mut next = form.clone();
            next.on_input_commit = checked;
            emit.emit(next);
        })
    };
    let on_mode_toggle = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: Event| {
            use wasm_bindgen::JsCast;
            let checked = e
                .target()
                .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
                .map(|el| el.checked())
                .unwrap_or(false);
            let mut next = form.clone();
            next.output_mode = if checked { OutputMode::Overwrite } else { OutputMode::Append };
            emit.emit(next);
        })
    };

    let title = if props.editing { "Edit transform" } else { "New transform" };
    let key = form_fingerprint(&props.form);
    let submit = props.on_submit.clone();
    let run_adhoc = props.on_run_adhoc.clone();
    let cancel = props.on_cancel.clone();

    html! {
        <Panel title={title}>
            <Input value={props.form.name.clone()} placeholder="name" oninput={on_name}
                   disabled={props.editing} />
            <div class="tf-kind">
                <Button variant={ if props.form.kind == TransformKind::Physical { ButtonVariant::Primary } else { ButtonVariant::Ghost } }
                        onclick={on_physical}>{ "Physical" }</Button>
                <Button variant={ if props.form.kind == TransformKind::Typed { ButtonVariant::Primary } else { ButtonVariant::Ghost } }
                        onclick={on_typed}>{ "Typed" }</Button>
            </div>
            <div class="tf-inputs">{ input_checkboxes }</div>
            <Input value={props.form.output.clone()} placeholder="output (schema.name or Type)"
                   oninput={on_output} />
            <SqlEditor key={key} value={props.form.sql.clone()} on_change={on_sql}
                       schema={props.schema.clone()} read_only={false} />
            <Input value={props.form.schedule.clone()} placeholder="cron schedule (optional)"
                   oninput={on_schedule} />
            <label class="tf-check">
                <input type="checkbox" checked={props.form.on_input_commit} onchange={on_commit_toggle} />
                { "Run on input commit" }
            </label>
            <label class="tf-check">
                <input type="checkbox"
                       checked={props.form.output_mode == OutputMode::Overwrite}
                       onchange={on_mode_toggle} />
                { "Overwrite output (else append)" }
            </label>
            { for props.errors.iter().map(|e| html! {
                <p class="error">{ format!("{}: {}", e.field, e.message) }</p> }) }
            if let Some(msg) = &props.server_error {
                <p class="error">{ msg }</p>
            }
            <div class="shell-drawer-actions">
                <Button variant={ButtonVariant::Primary}
                        onclick={Callback::from(move |_| submit.emit(()))}>{ "Define" }</Button>
                <Button variant={ButtonVariant::Secondary}
                        onclick={Callback::from(move |_| run_adhoc.emit(()))}>{ "Run ad-hoc" }</Button>
                <Button variant={ButtonVariant::Ghost}
                        onclick={Callback::from(move |_| cancel.emit(()))}>{ "Cancel" }</Button>
            </div>
        </Panel>
    }
}

/// Read an `<input>`'s value from an `InputEvent` (mirror the login form's helper).
fn input_value(e: &InputEvent) -> String {
    use wasm_bindgen::JsCast;
    e.target()
        .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
        .map(|el| el.value())
        .unwrap_or_default()
}
```

The `// ...` comments mark the remaining fields the implementer wires by repeating the `on_name`/`on_sql` clone-mutate-emit pattern for each `TransformForm` field. Reuse the exact `InputEvent`→value idiom already in `main.rs`'s `Login`/`Input` usage rather than the helper above if one already exists. The multi-select must set `form.inputs` to the selected `"schema.name"` (physical) or type-name (typed) strings; switching `kind` clears `inputs`/`output`.

- [ ] **Step 6: Build the app**

Run: `buck2 build -v0 --console none //src/ui:app`
Expected: exit 0. (Unused-in-isolation components are fine until Task 9 mounts them.)

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/surfaces/transforms.rs src/ui/src/surfaces/mod.rs src/ui/BUCK
git commit -m "feat(ui): Transforms surface view components (list, drawer, editor)"
```

---

### Task 9: `Workspace` wiring — state, epoch-guarded fetches, render arm

**Files:**
- Modify: `src/ui/src/main.rs`
- Modify: `src/ui/src/lib.rs` (`Surface::is_live` — add `Transforms`)
- Modify: `src/ui/tests/surface.rs` (update the `is_live` expectation)

**Interfaces:**
- Consumes: everything above — the `net::{list_transforms, get_transform, list_runs, define_transform, delete_transform, run_transform, run_adhoc, FetchError}` calls, the `surfaces::{TransformsList, TransformDrawer, TransformEditor}` components, and the `loom_ui_core` form/schema/parse types.
- Produces: a fully wired `Surface::Transforms` render arm returning `(list, drawer)`, with an epoch generation guard shared across the surface's selection-keyed fetches; `is_live()` now includes `Transforms`.

> Verified by wasm compile + a browser walkthrough. `main.rs` carries the crate-level lint allow.

- [ ] **Step 1: Flip `is_live` and update its test**

In `src/ui/src/lib.rs`, change `is_live` to `matches!(self, Surface::Catalog | Surface::Ontology | Surface::Transforms)`. In `src/ui/tests/surface.rs`, update `only_catalog_and_ontology_are_live` → rename and assert `vec!["Catalog", "Transforms", "Ontology"]`. Run `buck2 test --console none //src/ui:surface` → expect pass.

- [ ] **Step 2: Add the Transforms state hooks to `Workspace`**

Add alongside the existing Catalog/Ontology state blocks in `main.rs`:

```rust
// Transforms
let transforms = use_state(Vec::<TransformSummary>::new);
let tf_status = use_state(|| LoadStatus::Idle);
let tf_forbidden = use_state(|| false);
let tf_selected = use_state(|| Option::<usize>::None);
let tf_def = use_state(|| Option::<TransformDefView>::None);
let tf_drawer_tab = use_state(|| AttrValue::from("definition"));
let tf_runs = use_state(Vec::<RunRow>::new);
let tf_runs_status = use_state(|| LoadStatus::Idle);
// Editor form: Some(form) when the editor is open (New or Edit), None when viewing.
let tf_editing = use_state(|| Option::<TransformForm>::None);
// When editing an EXISTING def, the name being redefined; None for a New transform.
// Drives the editor title + disabled name field + the redefine target.
let tf_edit_name = use_state(|| Option::<String>::None);
let tf_schema = use_state(CompletionSchema::default);
let tf_errors = use_state(Vec::<FieldError>::new);
let tf_server_error = use_state(|| Option::<AttrValue>::None);
let tf_dataset_options = use_state(Vec::<String>::new);
let tf_type_options = use_state(Vec::<String>::new);
// Per-stream epoch generation guards. One SHARED counter is wrong: two effects
// keyed on the same selection each bump-then-capture, so the later effect's bump
// invalidates the earlier effect's in-flight fetch. Each independent fetch stream
// gets its own counter so a stream only cancels its own stale in-flight requests.
let tf_def_gen = use_mut_ref(|| 0u64);
let tf_runs_gen = use_mut_ref(|| 0u64);
let tf_schema_gen = use_mut_ref(|| 0u64);
```

- [ ] **Step 3: Add the list fetch (mount-keyed) with 403 → empty-state**

```rust
{
    let transforms = transforms.clone();
    let tf_status = tf_status.clone();
    let tf_forbidden = tf_forbidden.clone();
    let token = props.token.to_string();
    let base = api_base();
    let on_logout = props.on_logout.clone();
    use_effect_with((), move |()| {
        tf_status.set(LoadStatus::Loading);
        wasm_bindgen_futures::spawn_local(async move {
            match net::list_transforms(&base, &token).await {
                Ok(rows) => {
                    tf_forbidden.set(false);
                    transforms.set(rows);
                    tf_status.set(LoadStatus::Idle);
                }
                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                Err(net::FetchError::Forbidden) => {
                    tf_forbidden.set(true);
                    tf_status.set(LoadStatus::Idle);
                }
                Err(e) => tf_status.set(LoadStatus::Error(e.to_string())),
            }
        });
        || ()
    });
}
```

- [ ] **Step 4: Add the selection-keyed def fetch with the epoch guard**

Bump the generation on each new selection, capture it, and refuse to `set` if a newer selection has superseded it:

```rust
{
    let tf_def = tf_def.clone();
    let tf_drawer_tab = tf_drawer_tab.clone();
    let tf_editing = tf_editing.clone();
    let tf_edit_name = tf_edit_name.clone();
    let tf_runs = tf_runs.clone();
    let transforms = transforms.clone();
    let tf_def_gen = tf_def_gen.clone();
    let token = props.token.to_string();
    let base = api_base();
    let on_logout = props.on_logout.clone();
    let selected = *tf_selected;
    use_effect_with(selected, move |selected| {
        *tf_def_gen.borrow_mut() += 1;
        let my_gen = *tf_def_gen.borrow();
        tf_def.set(None);
        tf_editing.set(None);
        tf_edit_name.set(None);
        tf_runs.set(Vec::new()); // force the runs tab to refetch for the new selection
        tf_drawer_tab.set(AttrValue::from("definition"));
        if let Some(idx) = *selected {
            if let Some(row) = transforms.get(idx) {
                let name = row.name.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    match net::get_transform(&base, &token, &name).await {
                        Ok(def) => {
                            if *tf_def_gen.borrow() == my_gen {
                                tf_def.set(Some(def));
                            }
                        }
                        Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                        Err(_) => {
                            if *tf_def_gen.borrow() == my_gen {
                                tf_def.set(None);
                            }
                        }
                    }
                });
            }
        }
        || ()
    });
}
```

Add a parallel runs fetch keyed on `(*tf_selected, (*tf_drawer_tab).clone())` that only fires when the tab is `"runs"` and `tf_runs` is empty — mirror the Catalog lazy-preview effect (`main.rs` lines ~151-181), guarding the `set` with its **own** `tf_runs_gen` counter (bump-and-capture inside this effect, exactly as the def effect does with `tf_def_gen`). Populate `tf_dataset_options` from `net::fetch_datasets` (map its `DatasetRow`s to `"schema.name"` strings) and `tf_type_options` from `net::fetch_types` (mount-keyed, once) so the editor's checkbox list has options.

- [ ] **Step 5: Add the input-scoped completion-schema fetch (editor open)**

When the editor is open (`tf_editing` is `Some`), key an effect on the form's sorted input fingerprint: for physical, `net::fetch_dataset_detail` per `"schema.name"` input → pair with its `TableRef` → `schema_from_dataset_details`; for typed, `net::fetch_type_detail` per type → `schema_from_types`. Set `tf_schema`, guarded by `tf_gen`. Because the `SqlEditor` remounts on the form fingerprint `key` (Task 8), the fresh schema takes effect.

```rust
{
    let tf_schema = tf_schema.clone();
    let tf_editing = tf_editing.clone();
    let tf_schema_gen = tf_schema_gen.clone();
    let token = props.token.to_string();
    let base = api_base();
    let fp = tf_editing.as_ref().map(|f| (f.kind, {
        let mut xs = f.inputs.clone(); xs.sort(); xs
    }));
    use_effect_with(fp, move |fp| {
        if let Some((kind, inputs)) = fp.clone() {
            *tf_schema_gen.borrow_mut() += 1;
            let my_gen = *tf_schema_gen.borrow();
            wasm_bindgen_futures::spawn_local(async move {
                let schema = build_input_schema(&base, &token, kind, &inputs).await;
                if *tf_schema_gen.borrow() == my_gen {
                    tf_schema.set(schema);
                }
            });
        }
        || ()
    });
}
```

Add a free helper in `main.rs`:

```rust
async fn build_input_schema(
    base: &str,
    token: &str,
    kind: TransformKind,
    inputs: &[String],
) -> CompletionSchema {
    match kind {
        TransformKind::Physical => {
            let mut pairs = Vec::new();
            for input in inputs {
                let (schema, name) = input.split_once('.').unwrap_or(("", input.as_str()));
                if let Ok(detail) = net::fetch_dataset_detail(base, token, schema, name).await {
                    pairs.push((TableRef { schema: schema.to_string(), name: name.to_string() }, detail));
                }
            }
            schema_from_dataset_details(&pairs)
        }
        TransformKind::Typed => {
            let mut pairs = Vec::new();
            for ty in inputs {
                if let Ok(detail) = net::fetch_type_detail(base, token, ty).await {
                    pairs.push((ty.clone(), detail));
                }
            }
            schema_from_types(&pairs)
        }
    }
}
```

- [ ] **Step 6: Add the `Surface::Transforms` render arm**

In the `match *surface` dispatch, add an arm returning `(list, drawer)`:

```rust
Surface::Transforms => {
    let on_row = { let s = tf_selected.clone(); Callback::from(move |i| s.set(Some(i))) };
    let on_new = {
        let editing = tf_editing.clone();
        let edit_name = tf_edit_name.clone();
        let selected = tf_selected.clone();
        let errors = tf_errors.clone();
        let server_error = tf_server_error.clone();
        Callback::from(move |()| {
            selected.set(None);
            edit_name.set(None); // New, not Edit
            errors.set(Vec::new());
            server_error.set(None);
            editing.set(Some(TransformForm::default()));
        })
    };
    let list = html! {
        <TransformsList rows={(*transforms).clone()} status={(*tf_status).clone()}
            selected={*tf_selected} on_row={on_row} on_new={on_new} forbidden={*tf_forbidden} />
    };
    // Reusable "refetch the list into state" async closure builder.
    let reload_list = {
        let transforms = transforms.clone();
        let tf_status = tf_status.clone();
        let on_logout = props.on_logout.clone();
        let token = props.token.to_string();
        let base = api_base();
        move || {
            let (transforms, tf_status, on_logout) =
                (transforms.clone(), tf_status.clone(), on_logout.clone());
            let (token, base) = (token.clone(), base.clone());
            wasm_bindgen_futures::spawn_local(async move {
                match net::list_transforms(&base, &token).await {
                    Ok(rows) => { transforms.set(rows); tf_status.set(LoadStatus::Idle); }
                    Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => tf_status.set(LoadStatus::Error(e.to_string())),
                }
            });
        }
    };
    let drawer = if let Some(form) = (*tf_editing).clone() {
        let on_change = {
            let e = tf_editing.clone();
            Callback::from(move |f| e.set(Some(f)))
        };
        let editing = tf_edit_name.is_some();
        // Define (or redefine): validate client-side, POST, then close + refetch list.
        let on_submit = {
            let form = form.clone();
            let editing_state = tf_editing.clone();
            let errors = tf_errors.clone();
            let server_error = tf_server_error.clone();
            let on_logout = props.on_logout.clone();
            let token = props.token.to_string();
            let base = api_base();
            let reload_list = reload_list.clone();
            Callback::from(move |()| {
                errors.set(Vec::new());
                server_error.set(None);
                match form_to_def(&form) {
                    Err(errs) => errors.set(errs),
                    Ok(def_json) => {
                        let (editing_state, server_error, on_logout) =
                            (editing_state.clone(), server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        let reload_list = reload_list.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            match net::define_transform(&base, &token, &def_json).await {
                                Ok(()) => { editing_state.set(None); reload_list(); }
                                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                                Err(e) => server_error.set(Some(AttrValue::from(e.to_string()))),
                            }
                        });
                    }
                }
            })
        };
        // Ad-hoc run: validate the body, POST, close the editor (result shows in Runs on reselect).
        let on_run_adhoc = {
            let form = form.clone();
            let editing_state = tf_editing.clone();
            let errors = tf_errors.clone();
            let server_error = tf_server_error.clone();
            let on_logout = props.on_logout.clone();
            let token = props.token.to_string();
            let base = api_base();
            Callback::from(move |()| {
                errors.set(Vec::new());
                server_error.set(None);
                match form_to_body(&form) {
                    Err(errs) => errors.set(errs),
                    Ok(body_json) => {
                        let (editing_state, server_error, on_logout) =
                            (editing_state.clone(), server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        wasm_bindgen_futures::spawn_local(async move {
                            match net::run_adhoc(&base, &token, &body_json).await {
                                Ok(_run_id) => editing_state.set(None),
                                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                                Err(e) => server_error.set(Some(AttrValue::from(e.to_string()))),
                            }
                        });
                    }
                }
            })
        };
        let on_cancel = { let e = tf_editing.clone(); Callback::from(move |()| e.set(None)) };
        html! {
            <TransformEditor form={form} schema={(*tf_schema).clone()}
                editing={editing}
                dataset_options={(*tf_dataset_options).clone()}
                type_options={(*tf_type_options).clone()}
                errors={(*tf_errors).clone()} server_error={(*tf_server_error).clone()}
                on_change={on_change} on_submit={on_submit}
                on_run_adhoc={on_run_adhoc} on_cancel={on_cancel} />
        }
    } else if let Some(def) = (*tf_def).clone() {
        let on_tab = { let t = tf_drawer_tab.clone(); Callback::from(move |id| t.set(id)) };
        // Edit: seed the form from the def, record the edit target name.
        let on_edit = {
            let editing = tf_editing.clone();
            let edit_name = tf_edit_name.clone();
            let errors = tf_errors.clone();
            let server_error = tf_server_error.clone();
            let def = def.clone();
            Callback::from(move |()| {
                errors.set(Vec::new());
                server_error.set(None);
                edit_name.set(Some(def.name.clone()));
                editing.set(Some(form_from_def(&def)));
            })
        };
        // Run saved now → refetch runs (clear tf_runs so the runs effect refires) + open Runs tab.
        let on_run = {
            let name = def.name.clone();
            let tf_runs = tf_runs.clone();
            let tf_drawer_tab = tf_drawer_tab.clone();
            let on_logout = props.on_logout.clone();
            let token = props.token.to_string();
            let base = api_base();
            Callback::from(move |()| {
                let (name, tf_runs, tf_drawer_tab, on_logout) =
                    (name.clone(), tf_runs.clone(), tf_drawer_tab.clone(), on_logout.clone());
                let (token, base) = (token.clone(), base.clone());
                wasm_bindgen_futures::spawn_local(async move {
                    match net::run_transform(&base, &token, &name).await {
                        Ok(_run_id) => { tf_runs.set(Vec::new()); tf_drawer_tab.set(AttrValue::from("runs")); }
                        Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                        Err(_e) => tf_drawer_tab.set(AttrValue::from("runs")),
                    }
                });
            })
        };
        // Delete → clear selection + refetch list.
        let on_delete = {
            let name = def.name.clone();
            let tf_selected = tf_selected.clone();
            let tf_def = tf_def.clone();
            let on_logout = props.on_logout.clone();
            let token = props.token.to_string();
            let base = api_base();
            let reload_list = reload_list.clone();
            Callback::from(move |()| {
                let (name, tf_selected, tf_def, on_logout) =
                    (name.clone(), tf_selected.clone(), tf_def.clone(), on_logout.clone());
                let (token, base) = (token.clone(), base.clone());
                let reload_list = reload_list.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    match net::delete_transform(&base, &token, &name).await {
                        Ok(()) => { tf_selected.set(None); tf_def.set(None); reload_list(); }
                        Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                        Err(_e) => {}
                    }
                });
            })
        };
        html! {
            <TransformDrawer def={def} active_tab={(*tf_drawer_tab).clone()}
                on_tab={on_tab}
                runs={(*tf_runs).clone()} runs_status={(*tf_runs_status).clone()}
                on_edit={on_edit} on_run={on_run} on_delete={on_delete} />
        }
    } else {
        Html::default()
    };
    (list, drawer)
}
```

The `reload_list` closure is captured by value into the define/delete callbacks; because it is `Clone` (it clones its own captured handles), `.clone()` it into each. Add `fn form_from_def(def: &TransformDefView) -> TransformForm` in `main.rs` inverting `parse_transform_def` for the Edit path:

```rust
fn form_from_def(def: &TransformDefView) -> TransformForm {
    let (kind, inputs, output) = match &def.body.io {
        TransformIo::Physical { inputs, output } => (
            TransformKind::Physical,
            inputs.iter().map(|t| format!("{}.{}", t.schema, t.name)).collect(),
            format!("{}.{}", output.schema, output.name),
        ),
        TransformIo::Typed { inputs, output } => {
            (TransformKind::Typed, inputs.clone(), output.clone())
        }
    };
    TransformForm {
        kind,
        name: def.name.clone(),
        inputs,
        output,
        sql: def.body.sql.clone(),
        schedule: def.schedule.clone().unwrap_or_default(),
        on_input_commit: def.on_input_commit,
        output_mode: def.body.output_mode,
    }
}
```

Add all new imports to `main.rs`: `loom_ui_core::{TransformSummary, TransformDefView, TransformIo, RunRow, TransformForm, FieldError, CompletionSchema, TransformKind, TableRef, form_to_def, form_to_body, schema_from_dataset_details, schema_from_types}` and `crate::surfaces::{TransformsList, TransformDrawer, TransformEditor}`.

- [ ] **Step 7: Build the whole tree**

Run: `buck2 build -v0 --console none //src/...`
Expected: exit 0.

- [ ] **Step 8: Run the full UI test sweep**

Run: `buck2 test --console none //src/ui/...`
Expected: `Tests finished: Pass N. Fail 0` (all `transforms-*`, `surface`, and existing UI tests green).

- [ ] **Step 9: Browser walkthrough (verification gate)**

Boot the composite (per `src/ui/CLAUDE.md`: the all-in-one/standalone path, or `buck2 run //src/ui:serve` against a running backend) and walk: list renders → New transform → pick inputs (completion offers only those tables' columns) → Define → Run → Runs history shows the run → Delete → the `Forbidden` empty-state (log in as a non-admin). Record the outcome; capture a screenshot if useful.

- [ ] **Step 10: Commit**

```bash
git add src/ui/src/main.rs src/ui/src/lib.rs src/ui/tests/surface.rs
git commit -m "feat(ui): wire Transforms surface into Workspace with epoch-guarded fetches"
```

---

## Register bookkeeping (at finish, not an implementation task)

At branch finish (via `loom-docs-update`): close `road-ui-transforms-surface` in `docs/ROADMAP.md` and fold the capability into `docs/system-capabilities/`. Confirm `#fut-ui-transforms-surface` and `#fut-ui-sql-editor-schema-wiring` are already absent from `docs/FUTURE.md` (they were removed when this road item was minted in PR #382); if either lingers, remove it. Keep `#fut-ui-sql-completion-polish`, `#fut-ui-sql-editor-diagnostics`, `#fut-ui-sql-query-console`, and `iss-ui-async-fetch-race` as-is — this surface works *around* the completion-polish limitation (via the remount `key`) and does not close it; it introduces the epoch-guard pattern only for its own fetches.

## Self-Review

- **Spec coverage:** Task 1 = Pipelines→Transforms rename (spec Decisions §3). Tasks 2-5 = the `loom_ui_core` pure module (parsers, form builders, schema builders, display mappers, clamp — spec "Components / units"). Task 6 = the 7 net calls + `Forbidden` (+`Rejected` for the 400-message requirement) (spec "Data flow & fetch", "Error handling"). Task 7 = resizable `Shell` drawer (spec "Resizable drawer"). Task 8 = the surface view glue incl. the two mutually-exclusive drawer views and the single at-a-time `SqlEditor` remounted by `key` (spec "Surface layout", "SqlEditor integration"). Task 9 = `Workspace` wiring, epoch guard (spec "Epoch-guarded effects"), input-scoped schema fetch (spec Decisions §4), and write-then-refetch (spec "Write-then-refetch"). Testing per spec "Testing" (pure `rust_test`, view by browser).
- **Placeholder scan:** after the plan-review pass, Task 9 Step 6's write→refetch callbacks (define / redefine / run / delete / run-adhoc / edit / cancel / on_tab) are all written out concretely (no `Callback::noop()` markers remain); the New-vs-Edit distinction is tracked in `tf_edit_name`; the editor's kind toggle, checkbox multi-select, and boolean toggles are spelled out (Task 8 Step 5). Tasks 2-6 are fully concrete with complete code + tests. The only intentional "mirror the sibling effect" prose is the runs-fetch effect (Task 9 Step 4), which has a direct analogue in the def effect shown immediately above it and the Catalog lazy-preview effect.
- **Plan-review fixes folded in:** E0716 `const NULL` fix (parse_transform_def); `#[derive(Clone, PartialEq)]` on both `DataTable` row structs; checkbox multi-select (avoids the un-enabled `HtmlSelectElement` web-sys feature); definition-view metadata block (`def_metadata`); per-stream epoch counters (`tf_def_gen`/`tf_runs_gen`/`tf_schema_gen`) replacing the racy single shared counter; `Rejected(m)` → `AttrValue::from(e.to_string())` coercion; 7-of-8 route reconciliation note.
- **Type consistency:** `TransformForm`/`FieldError`/`TransformKind`/`OutputMode`/`TableRef`/`TransformIo`/`TransformBody`/`TransformDefView`/`TransformSummary`/`RunRow` names and fields are identical across Tasks 2-9; `form_to_def`/`form_to_body`/`parse_*`/`schema_from_*`/`clamp_drawer_width`/`run_state_tone`/`trigger_label`/`kind_badge_label` signatures match their call sites; `FetchError` variants (`Unauthorized`/`Forbidden`/`Network`/`Rejected`/`Server`) are consistent between Task 6 and Task 9's error mapping. `DataTable<R>` turbofish, `Column{label,align}`, `Badge{label,tone}`, `Button{variant,onclick,children}`, `Input{value,placeholder,oninput,disabled}`, `Tabs{tabs,active,onselect}`, `SqlEditor{value,on_change,schema,read_only,height}` match the verified component signatures.
