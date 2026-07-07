# Reusable `SqlEditor` component — design

_Date: 2026-07-06_

## Summary

Build a **reusable, schema-fed SQL editor** for loom's Yew/WASM UI, wrapping
[Monaco](https://github.com/siku2/rust-monaco) and proven in isolation via the
component gallery. This is the foundational, highest-risk piece of a larger arc
(a Transforms admin surface, and eventually a governed SQL query console) — so
it is built and de-risked **first, on its own**, with the downstream consumers
deferred to their own specs.

Scope of *this* spec:

- A `SqlEditor` component in `loom_ui_components` (Monaco highlighting + a
  completion provider fed by a caller-supplied schema).
- A **pure, unit-tested completion engine** in `loom_ui_core`.
- The **build integration** to make Monaco load in loom's hermetic, no-bundler
  buck2 + wasm-bindgen build — gated behind a spike.
- Gallery verification.

Explicitly **out of scope** (deferred, tracked in `docs/FUTURE.md`): the
Transforms admin surface, wiring the live `/datasets` schema into the editor,
any diagnostics (client-side or backend), and any SQL-execution endpoint /
query console. The north star is full server-side **EXPLAIN + diagnostics**;
this spec deliberately ships neither.

## Context

- **UI stack:** Yew 0.21 → `wasm32-unknown-unknown`, bundled by **buck2
  genrules + wasm-bindgen `--target web`** (no Trunk/webpack), `stylist`
  styling, `gloo-net` HTTP. State lives in one stateful `Workspace`; surfaces
  and every `loom_ui_components` primitive are **stateless/controlled**.
- **Convention (pure/testable split):** untestable view-glue (`html!`/`css!`)
  is thin; logic lives in the lint-clean, `rust_test`'d `loom_ui_core`.
  Components are verified by eye in `:gallery` (buck2's `rust_test` has no DOM).
- **rust-monaco reality:** the crate vendors Monaco's JS internally and exposes
  it via `wasm-bindgen` module **snippets** — **no CDN, no network at build**.
  Its `yew-components` feature pins **yew 0.23**, incompatible with loom's
  0.21 — so we use the **`api` feature only** and write our own yew-0.21 wrapper.
- **Schema source (for later, not this spec):** `GET /datasets` (table list) +
  `GET /datasets/{schema}/{table}` (`columns: [{name, ty, nullable}]`).

## Architecture — the pure / interop split

### `loom_ui_core` (pure, lint-clean, `rust_test`'d) — the completion engine

Data types the caller populates, and the function that computes suggestions:

```rust
pub struct CompletionSchema { pub tables: Vec<CompletionTable> }
pub struct CompletionTable {
    pub schema: Option<String>,   // "public" (physical) | None (typed vocab)
    pub name: String,             // table or ontology-type name
    pub columns: Vec<CompletionColumn>,
}
pub struct CompletionColumn { pub name: String, pub ty: String }

pub enum SuggestionKind { Table, Column, Keyword }
pub struct Suggestion {
    pub label: String,
    pub kind: SuggestionKind,
    pub detail: Option<String>,   // e.g. the column's logical type
    pub insert_text: String,
}

/// Pure. No DOM, no monaco. `qualifier` is the `t` in `t.<prefix>`.
pub fn sql_completions(
    schema: &CompletionSchema,
    prefix: &str,
    qualifier: Option<&str>,
) -> Vec<Suggestion>;
```

Behaviour (v1, bounded):

- **`qualifier = Some(t)`** → columns of the table/type whose name (or
  `schema.name`) matches `t`, filtered by `prefix`; each carries `detail = ty`.
  Unknown qualifier → empty.
- **`qualifier = None`** → union of: table/type names, all column names, and a
  static **SQL-keyword** list (`SELECT`, `FROM`, `WHERE`, `JOIN`, `GROUP BY`,
  …), all filtered by `prefix` (case-insensitive).
- Deterministic ordering (keywords last, or a stable sort) so tests and the
  gallery are predictable.

This is where the real behaviour lives, and it is exhaustively unit-tested.

### `loom_ui_components` — `src/components/sql_editor.rs` (thin interop)

A yew-0.21 `#[styled_component(SqlEditor)]` (new file, auto-globbed by
`components/**/*.rs`, no BUCK change; add a `pub use` in `mod.rs`). It:

1. On node-ref **mount**, creates a Monaco editor bound to the container node;
   on **unmount**, disposes it (and disposes the registered provider).
2. Registers a **`CompletionItemProvider`** whose callback reads the model text
   + cursor position, extracts `(prefix, qualifier)` (the `word` under the
   cursor and, if the char before the word-start is `.`, the identifier before
   it), calls `sql_completions`, and maps `Suggestion` → Monaco JS completion
   items (`kind`/`detail`/`insertText`).
3. Applies a **`loom-dark`** Monaco theme derived from the `--loom-*` tokens
   (background/foreground/accent), so the editor matches the Foundry palette.

The prefix/qualifier *extraction* from a `(text, offset)` pair is itself a pure
function in `loom_ui_core` (`cursor_context(text, offset) -> (prefix,
qualifier)`), unit-tested; the wrapper only does the JS ↔ Rust marshalling.

## Component API (controlled, matching the library convention)

```rust
#[derive(Properties, PartialEq)]
pub struct SqlEditorProps {
    pub value: AttrValue,              // current SQL text
    pub on_change: Callback<String>,   // fired on edit
    pub schema: CompletionSchema,      // caller-supplied completion source
    #[prop_or_default] pub read_only: bool,
    #[prop_or_default] pub height: Option<AttrValue>,  // default ~320px
}
```

**Controlled-with-guard.** Monaco owns a mutable text buffer, so we do *not*
re-set it on every render (that fights the editor and moves the cursor).
Instead: initialise the model from `value` on mount, emit `on_change` on edits,
and only force-set the model when an incoming `value` prop **differs from the
editor's current text** — supporting external resets ("load this saved SQL")
without disrupting normal typing. `schema` is a plain prop; the caller (a future
`Workspace`) is responsible for fetching and passing it. The component fetches
nothing and holds no app state.

## Build integration — the gated spike (highest risk)

Monaco surfaces via wasm-bindgen module **snippets** (vendored in the crate; no
CDN, no network). The plan's **step 0 is a spike that must go green before any
component logic is written**:

1. Add `monaco` (the crate) to `src/ui/Cargo.toml` with **default features
   only** (`api` + `workers`; **not** `yew-components`); refresh the lock;
   `./tools/buckify.sh`. Confirm `monaco` and its deps resolve under
   `//platforms:wasm` in `reindeer.toml` (js-sys/web-sys/wasm-bindgen are
   wasm-native; no tokio/socket2-style leak is expected, but verify).
2. Teach the **`:bundle` (and `:gallery-bundle`) genrule to copy wasm-bindgen's
   emitted `snippets/` dir into `dist/`** — today it copies only
   `app.js`/`app_bg.wasm`/`index.html`/`config.js`. Monaco's JS + language
   workers live under `snippets/`.
3. Render an **empty Monaco editor in the gallery** over
   `buck2 build //src/ui:gallery-bundle` + `gallery-serve`, confirming the
   editor mounts and its language workers load (watch the console for
   `MonacoEnvironment`/worker-URL failures).

**Decision gate:** if the editor cannot be made to load hermetically (worker
loading, snippet paths, or reindeer wasm resolution prove intractable), we stop
and surface it rather than pushing on — the fallback would be a lighter editor
(CodeMirror) or a plain styled `<textarea>` behind the same `SqlEditor` API, so
the completion engine and consumers are unaffected. Bundle-size note: Monaco is
several MB of JS under `snippets/`; acceptable for an internal admin UI, but
worth confirming it loads at an acceptable speed.

## Verification

- **`loom_ui_core` `rust_test`** (new `tests/sql_completions.rs`): exhaustive
  coverage of `sql_completions` (unqualified → tables + keywords + columns;
  `t.` → that table's columns; prefix filter, case-insensitivity; empty schema;
  unknown qualifier) and `cursor_context` (word extraction, `.`-qualifier
  detection, cursor at string boundaries).
- **Gallery** (`src/gallery.rs`): render `SqlEditor` with a hardcoded 2-table
  sample `CompletionSchema` — the eyeball acceptance surface for highlighting +
  `Ctrl+Space` completion. Per the library's stated limit, component *rendering*
  is not `rust_test`-able; the gallery + (later) the fantoccini e2e layer are
  the DOM checks. No new e2e in this spec.
- No `net.rs`/backend changes — the component is proven in isolation.

## What this deliberately defers (tracked in `docs/FUTURE.md`)

- **Transforms admin surface** — the first real consumer: CRUD/list/run/history
  + schedule/data-trigger fields over the 7 `/admin/transforms` routes, wiring
  the live `/datasets` (and ontology-type) schema into `SqlEditor`.
- **SQL query console + governed execution endpoint** — the second consumer;
  needs the deferred external-SQL-wire work.
- **Editor diagnostics — the north star: full server-side EXPLAIN +
  diagnostics.** Client-side identifier squiggles are an intermediate step;
  the target is real parse/EXPLAIN feedback surfaced as Monaco markers, gated on
  a backend validate endpoint.

## Non-goals

- No real language server / LSP over the wire (in-browser Monaco providers only).
- No backend changes of any kind in this spec.
- No router / URL state for any future surface (consistent with the existing
  no-router SPA).
