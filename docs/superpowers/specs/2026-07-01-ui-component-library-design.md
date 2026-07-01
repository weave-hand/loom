# loom UI component library — foundational slice (design)

**Status:** approved for planning
**Date:** 2026-07-01
**Crate area:** `src/ui/`
**Depends on:** the yew-wasm UI experiment (`2026-06-30-yew-wasm-ui-experiment-design.md`) and the UI login slice (`2026-06-30-ui-login-design.md`), both landed.

## Goal

Establish the foundation of loom's web UI design system: a **design-token layer**, a
small set of **reusable primitive components** authored with `stylist` (scoped
CSS-in-Rust), and a **dev-only gallery bundle** that renders every primitive in
isolation. This is the vocabulary the eventual Foundry-style screens (catalog
explorer, master-detail drawer, lineage DAG — see the wireframe studies) compose
from; those screens are **out of scope** for this slice and land later, each in its
own slice.

The current UI has **no styling at all** (`src/ui/src/main.rs` emits raw HTML;
`index.html` links no CSS), so this slice is genuinely greenfield for styling.

## Non-goals (this slice)

- No real screens (catalog table, lineage graph, drawers) — the gallery uses static
  demo data only.
- No backend wiring, no routing dependency (`yew-router`), no network.
- No headless-browser render tests (buck2 `rust_test` has no DOM — see **Testing**).
- No changes to the login flow's behaviour. Adopting a primitive or two inside the
  login `app` is optional polish, not a requirement of this slice.

## Global constraints (copied verbatim from repo conventions)

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]`
  (the `no-inline-tests` prek hook enforces it). Pure logic tests live in
  `src/ui/tests/*.rs` wired as `rust_test` targets.
- **Strict clippy** (pedantic + restriction) runs on `//src`. The `html!` macro is
  not lint-clean under that gate, so any crate containing `html!` carries a
  crate-level `#![allow(clippy::pedantic, clippy::restriction, reason = "…")]`
  (`allow`, not `expect` — which group members fire depends on macro expansion).
  `loom_ui_core` stays pure and lint-clean (no allow).
- **Wasm target platform.** Every wasm rule sets
  `default_target_platform = "//platforms:wasm"` so the native `buck2 build //src/...`
  sweep cross-compiles rather than native-builds it.
- **New third-party dep discipline.** Adding `stylist` means: edit
  `src/ui/Cargo.toml`, refresh the lock (`cargo generate-lockfile` via
  `eval "$(./tools/env.sh)"`, or `reindeer update`), run `./tools/buckify.sh`, then
  run the **full** `buck2 test //src/...` — the reindeer `[platform]` config is
  graph-global, so a wasm cross-compile leak surfaces tree-wide, not just in `//src/ui`.
- **Pinned versions.** yew `0.21` (features `["csr"]`) — components use
  `#[function_component]` (the `0.21` attribute; master's `#[component]` does not
  exist here). `stylist` pinned to its yew-0.21-compatible line (`0.13`, feature
  `yew_integration` / `yew`), verified during implementation.

## Architecture

```
//src/ui
  :ui-core        loom_ui_core        pure Rust, lint-clean, rust_test'd
  :ui-components  loom_ui_components   NEW wasm rust_library — stylist primitives
  :app            app (login)          may depend on :ui-components (optional)
  :gallery        NEW wasm rust_binary composes every primitive
  :gallery-bundle NEW genrule          wasm-bindgen → dist/ (dev-only, no config.js)
```

Data/ownership split:

- **`loom_ui_core`** (existing lib) gains the **pure, testable** pieces: the shared
  token *enums* (`ButtonVariant`, `BadgeTone`, `Status`, `Align`) with their
  string/`css-var` mappings, and `format_count(u64) -> String`. No `web-sys`, no
  `yew` — compiles for the host, covered by `rust_test`.
- **`loom_ui_components`** (new lib) holds the `stylist` components. It depends on
  `loom_ui_core` (for the enums), `yew`, `stylist`, and `web-sys`/`wasm-bindgen` as
  needed. Carries the crate-level pedantic/restriction allow.
- **`:gallery`** (new binary) depends on `:ui-components` + `:ui-core`, renders the
  kitchen sink, and ships via **`:gallery-bundle`** (a genrule mirroring `:bundle`,
  minus `config.js` — the gallery has no backend). This keeps the gallery out of the
  production login bundle entirely.

### Token layer

A `GlobalStyles` component (stylist `<Global>`) is rendered once at the root of both
`:app` and `:gallery`. It injects a `:root { --loom-* }` custom-property block plus
base `body`/font rules. Every component's `css!` references `var(--loom-*)`, so the
whole theme swaps by editing one component. Tokens (extracted from the wireframe):

```
--loom-bg:        #0b0e14   app background (near-black navy)
--loom-panel:     #161b22   cards / drawers / tree pane
--loom-panel-2:   #1c2230   raised rows, hover
--loom-border:    #232a35   hairline dividers
--loom-text:      #e6edf3   primary text
--loom-text-mut:  #8b949e   secondary / labels
--loom-accent:    #3b82f6   primary button, active tab, selected row
--loom-accent-fg: #ffffff   text on accent
--loom-ok:        #3fb950   health green
--loom-warn:      #d29922   health amber
--loom-danger:    #f85149   health red
--loom-radius:    6px
--loom-radius-sm: 4px
--loom-space:     4px base scale (multiples: 4/8/12/16/24)
font: system-ui / Inter stack; 13px base, 12px small; tabular-nums in tables
```

## Components (this slice)

Enums (in `loom_ui_core`, so they are pure and testable):

```rust
pub enum ButtonVariant { Primary, Secondary, Ghost }
pub enum BadgeTone     { Neutral, Info, Pii, Success, Warning, Danger }
pub enum Status        { Ok, Warn, Error }
pub enum Align         { Start, End }
// each exposes a pure mapping used by both the components and the tests, e.g.
impl Status { pub fn css_var(self) -> &'static str { /* "--loom-ok" | … */ } }
```

Components (in `loom_ui_components`):

| Component | Key props | Variants / notes |
|---|---|---|
| `GlobalStyles` | — | injects `:root` tokens + base body/font |
| `Button` | `variant: ButtonVariant`, `disabled: bool`, `onclick: Callback<MouseEvent>`, `children` | Primary / Secondary / Ghost |
| `Input` | `value: String`, `placeholder: String`, `input_type: InputKind`, `oninput: Callback<InputEvent>`, `disabled: bool` | `InputKind::{Text, Password, Search}`; Search shows a leading magnifier + `⌘K` hint |
| `Badge` | `label: String`, `tone: BadgeTone` | pill; lowercase tag look (`pii`, `finance`, `certified`) |
| `StatusDot` | `status: Status` | colored dot (health column) |
| `Tabs` | `tabs: Vec<TabItem>`, `active: AttrValue`, `onselect: Callback<AttrValue>` | underline-active (`Preview / Schema / Lineage / History`) |
| `Panel` | `title: Option<AttrValue>`, `children` | titled container (cards, metadata blocks) |
| `DataTable<R>` | see below | dense, selectable-row table, **generic** over the row type |
| `TopNav` | `items: Vec<NavItem>`, `on_select: Callback<AttrValue>`, `search: Html` (slot), `avatar: AttrValue` | app-shell bar (brand + nav + search + avatar) |

### `DataTable<R>` — generic over the row type

Yew 0.21 supports generic function components. Props must be `PartialEq`, so per-column
render *closures* cannot live in props (closures aren't `PartialEq`). We sidestep this
with a tiny trait the row type implements — the row maps *itself* to cells — and a
`Callback` for selection (`Callback` is `PartialEq` by identity):

```rust
// loom_ui_components
pub trait TableRow {
    fn cells(&self) -> Vec<Html>;   // caller maps its domain struct → row cells
}

#[derive(Clone, PartialEq)]
pub struct Column { pub label: AttrValue, pub align: Align }

#[derive(Properties, PartialEq)]
pub struct DataTableProps<R: PartialEq> {
    pub columns:  Vec<Column>,
    pub rows:     Vec<R>,            // R: PartialEq + Clone + TableRow
    #[prop_or_default] pub selected: Option<usize>,
    #[prop_or_default] pub onrow:    Callback<usize>,
}

#[function_component]
pub fn DataTable<R>(props: &DataTableProps<R>) -> Html
where R: PartialEq + Clone + TableRow + 'static { /* … */ }

// caller / gallery instantiate with a concrete row type:
//   html! { <DataTable<DatasetRow> columns={cols} rows={rows}
//                                   selected={Some(0)} onrow={cb} /> }
```

Callers pass **domain structs** (e.g. the gallery's demo
`DatasetRow { name, rows, owner, health }`) and `impl TableRow` for them — type-safe,
selection by index, no pre-rendered `Html` threaded through props. The exact
`#[function_component]` generic form is verified against yew 0.21 during
implementation.

## Gallery

`:gallery`'s `main` renders `GlobalStyles`, a realistic `TopNav`, then one `Panel`
section per primitive, each showing all variants:

- **Buttons** — Primary / Secondary / Ghost / disabled
- **Inputs** — Text / Password / Search
- **Badges** — one per `BadgeTone`
- **Status** — Ok / Warn / Error dots
- **Tabs** — a 4-tab strip with local `use_state` active index
- **Panel** — a titled panel (shown by containing the other sections)
- **DataTable** — a `DatasetRow` demo table (name / rows via `format_count` / owner /
  health `StatusDot`), one row `selected`

Static, no network, no routing. Served over HTTP like `:bundle` (wasm needs `fetch`,
so `file://` won't load it): `buck2 build //src/ui:gallery-bundle` → `dist/`, served
by any static server (`buck2 run //src/ui:serve` pattern, or the eventual all-in-one
binary via `LOOM_UI_DIR`).

## Error handling

Components are pure render functions over props; there is nothing to fail. The gallery
is static. `format_count` is total over `u64`. No `Result`s, no fallbacks, no panics
introduced.

## Testing

- **Pure logic** → real `rust_test` coverage in `src/ui/tests/`:
  - `format_count`: `2_410_000 → "2.41M"`, `18_200 → "18.2K"`, `880_000 → "880K"`,
    `9_700 → "9.7K"`, `142_000 → "142K"`, plus `< 1000 → "N"` and a `0` case.
  - enum mappings: each `Status`/`BadgeTone`/`ButtonVariant`/`Align` variant maps to
    its expected css-var / class token (guards against a silent mis-wire).
- **Component rendering (`html!`)** → **not** unit-tested: buck2's `rust_test` has no
  DOM/browser, so wasm component rendering cannot execute there. Verified **by eye in
  the gallery bundle**. This is stated plainly rather than faked with a green-but-empty
  test. (A future headless-wasm harness is a separate, deferred concern.)
- After adding `stylist`: full `buck2 test //src/...` (graph-global `[platform]`
  config), and `tools/clippy-all.sh` clean.

## Register updates

- `docs/ROADMAP.md`: add this slice as `planned` under a `ui` area, `spec:` this file.
- On completion, `loom-docs-update` closes it and records any deferrals (e.g. the
  headless-wasm render-test harness; the composite screens).

## File structure (created / modified)

- **Modify** `src/ui/src/lib.rs` — add `ButtonVariant`/`BadgeTone`/`Status`/`Align`
  enums + mappings and `format_count`.
- **Create** `src/ui/tests/components.rs` — `rust_test` for `format_count` + enum maps.
- **Create** `src/ui/src/components/…` — `global.rs`, `button.rs`, `input.rs`,
  `badge.rs`, `status.rs`, `tabs.rs`, `panel.rs`, `table.rs`, `topnav.rs`, `mod.rs`
  (the `loom_ui_components` crate root; exact file split finalized in the plan).
- **Create** `src/ui/src/gallery.rs` — the `:gallery` binary root.
- **Create** `src/ui/gallery.html` — the gallery bundle's HTML entrypoint.
- **Modify** `src/ui/BUCK` — add `:ui-components`, `:gallery`, `:gallery-bundle`
  targets + the `components.rs` `rust_test`; extend `:app` deps if it adopts a primitive.
- **Modify** `src/ui/Cargo.toml` — add `stylist`; **regenerate** `Cargo.lock` +
  `third-party/BUCK` via `./tools/buckify.sh`.
- **Modify** `src/ui/CLAUDE.md` — document the `:gallery` bundle + the component-library
  layout.
```
