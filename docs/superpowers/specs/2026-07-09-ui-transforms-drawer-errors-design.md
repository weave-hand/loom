# Transforms drawer action errors + Runs-tab refetch Design

> **Status:** design (direction). This spec makes `iss-ui-transforms-drawer-errors`
> build-ready (it currently points at the umbrella
> `2026-07-07-transforms-admin-surface-design`, whose whole-branch review surfaced
> it). Matching plan: `docs/superpowers/plans/2026-07-09-ui-transforms-drawer-errors.md`.

## Goal

Make the Transforms drawer's two action paths — **Run saved** and **Delete** —
loud on failure and correct on success:

1. A non-401 failure (400 `Rejected(body)`, 403, 404/`Server(status)`, network)
   surfaces as an error line rendered beside the drawer's action buttons,
   reusing the surface's existing `tf_server_error` state — instead of today's
   silence (a failed Delete looks like a no-op; a failed Run silently flips to
   the Runs tab).
2. A **Run** whose response lands while the Runs tab is already the active
   effect dep actually refetches the runs history, instead of clearing
   `tf_runs` and leaving a stale-empty Idle table.

## Context — defect mechanics (verified against the code)

All interactive state for the Transforms surface lives in `Workspace`
(`src/ui/src/main.rs`); `TransformDrawer` (`src/ui/src/surfaces/transforms.rs:235`)
is purely presentational.

- **No error slot on the drawer.** `TransformDrawerProps`
  (`surfaces/transforms.rs:221-231`) has `def / active_tab / on_tab / runs /
  runs_status / on_edit / on_run / on_delete` — nothing that can carry an action
  error. The Definition-tab body renders the metadata rows, the read-only
  `SqlEditor`, and the `.shell-drawer-actions` button row (Edit / Run / Delete,
  `surfaces/transforms.rs:278-285`); the action buttons exist **only** on the
  Definition tab.
- **`on_run` swallows the error** (`main.rs:773-799`): the `Err(_e)` arm is
  `tf_drawer_tab.set(AttrValue::from("runs"))` — it flips to the Runs tab
  showing the *old* runs, discarding the message. `net::run_transform`
  (`net.rs:320`) maps a non-202 through `write_status_err` (`net.rs:96`), so a
  400 carries the server's validation body as `FetchError::Rejected(msg)` —
  the message exists and is dropped.
- **`on_delete` swallows the error** (`main.rs:801-830`): the `Err(_e)` arm is
  `{}`. A 400/404/network failure on `net::delete_transform` (`net.rs:306`)
  changes nothing on screen — the row stays, the drawer stays, no feedback.
- **The editor form already does this right** (the pattern to reuse):
  `TransformEditorProps.server_error: Option<AttrValue>`
  (`surfaces/transforms.rs:331`) is rendered as `<p class="error">` above the
  form's action row (`surfaces/transforms.rs:515-517`), set from the
  `define_transform`/`run_adhoc` `Err(e)` arms via
  `server_error.set(Some(AttrValue::from(e.to_string())))` (`main.rs:697-699`,
  `:731-733`). Only the drawer actions lack the slot.
- **The Runs-tab refetch edge.** The lazy runs effect (`main.rs:427-464`) is
  keyed on `(*tf_selected, (*tf_drawer_tab).clone())` (`main.rs:436`) and
  guarded by `already_loaded = !tf_runs.is_empty()`. `on_run`'s success arm
  does `tf_runs.set(Vec::new()); tf_drawer_tab.set("runs")`. In the normal flow
  (Run clicked on the Definition tab, response arrives with the tab still
  `"definition"`) the tab flip changes the dep and the effect refetches. But if
  the user flips to the Runs tab while the POST is in flight, the response's
  `tf_drawer_tab.set("runs")` is a no-op — the dep tuple is unchanged, the
  effect never refires, and the cleared `tf_runs` renders as an Idle empty
  table (looks like the transform has no runs at all).

Both are low-frequency, admin-only paths. A `401` on either action already
fails closed to logout (`Err(net::FetchError::Unauthorized) => on_logout.emit(())`)
— that behavior is correct and must not change.

## Architecture

### Edge 1 — `action_error` prop on `TransformDrawer`

- Add `pub action_error: Option<AttrValue>` to `TransformDrawerProps`,
  mirroring `TransformEditorProps.server_error`.
- Render it in the Definition-tab body, immediately before the
  `.shell-drawer-actions` row (beside the buttons the user just clicked):

  ```rust
  if let Some(msg) = &props.action_error {
      <p class="error">{ msg }</p>
  }
  ```

  `class="error"` matches the surface's existing error lines (the list's and
  Runs tab's `LoadStatus::Error` branches use the same class); the Transforms
  surface deliberately has no scoped `css!` block, so no styling change rides
  along.
- Wire it in `Workspace` by **reusing `tf_server_error`** (`main.rs:112`) — the
  same state the editor form uses. This is safe because the editor and the
  drawer are mutually exclusive (`if let Some(form) = (*tf_editing) … else if
  let Some(def) = (*tf_def)`, `main.rs:661/:753`), and every editor entry point
  (`on_new` `main.rs:630`, `on_edit` `main.rs:767`) already resets it to `None`.
- Set it in the action handlers: clear at click (`server_error.set(None)`),
  set `Some(AttrValue::from(e.to_string()))` in the non-401 `Err` arms of
  `on_run` and `on_delete` (`FetchError`'s `Display`, `net.rs:74-84`, already
  produces the user-facing message incl. the `Rejected` body).
- **On Run failure, stay on the Definition tab** (drop the current
  `Err(_e) => tf_drawer_tab.set("runs")`): the error renders beside the button
  that failed; flipping tabs would hide it.
- Reset `tf_server_error` in the row-select effect's clear block
  (`main.rs:393-399`) so a stale action error never leaks onto a newly selected
  transform's drawer.

### Edge 2 — runs-generation bump on the Run success path

- Add a render-participating epoch: `let tf_runs_epoch = use_state(|| 0u64);`
  (a `use_state`, not a `use_mut_ref` — it must invalidate the effect dep;
  `tf_runs_gen` stays as the in-flight staleness guard, unchanged).
- Fold it into the runs effect's dep tuple:
  `(*tf_selected, (*tf_drawer_tab).clone(), *tf_runs_epoch)`.
- `on_run`'s success arm becomes: clear `tf_runs`, bump the epoch
  (`tf_runs_epoch.set(*tf_runs_epoch + 1)`), then open the Runs tab. Whatever
  the tab state at response time, the dep tuple now changes, `already_loaded`
  recomputes to `false` (runs were just cleared), and the effect refetches.
  The normal flow still fetches exactly once per Run (one re-render, one dep
  change); a mid-flight manual tab flip's earlier fetch is invalidated by the
  existing `tf_runs_gen` guard.

### The testable core — pure action-effect helpers in `loom_ui_core`

The repo has no DOM/wasm render harness (`fut-ui-browser-test-fixture` /
`fut-ui-component-test-fixture` are deferred); UI tests are pure-logic
`rust_test`s on `loom_ui_core` (`//src/ui:transforms-form`, `:transforms-display`,
…). So the decision logic — *what state transition does each action outcome
produce?* — is extracted as plain functions in `loom_ui_core::transforms`
(`src/ui/src/transforms.rs`, already glob-free listed in `:ui-core`'s `srcs`):

```rust
pub struct DrawerActionEffect {
    pub error: Option<String>,
    pub open_runs_tab: bool,
    pub refetch_runs: bool,    // clear tf_runs + bump the runs epoch
    pub clear_selection: bool, // clear tf_selected + tf_def, reload the list
}
pub fn run_action_effect(result: Result<(), String>) -> DrawerActionEffect;
pub fn delete_action_effect(result: Result<(), String>) -> DrawerActionEffect;
```

Truth table (pinned by the new `rust_test`):

| call | `error` | `open_runs_tab` | `refetch_runs` | `clear_selection` |
|---|---|---|---|---|
| `run_action_effect(Ok(()))` | `None` | true | true | false |
| `run_action_effect(Err(m))` | `Some(m)` | **false** | false | false |
| `delete_action_effect(Ok(()))` | `None` | false | false | true |
| `delete_action_effect(Err(m))` | `Some(m)` | false | false | false |

`Workspace`'s handlers route `Unauthorized` to logout first (unchanged), then
map the remainder through `.map(|_| ()).map_err(|e| e.to_string())` and apply
the effect fields mechanically. The prop plumbing and `html!` rendering are the
untestable shell, verified by wasm cross-compilation + clippy + a documented
manual check (the same posture as the sibling
`2026-07-07-ui-swallowed-fetch-errors` plan).

## Non-regression

- **401 fails closed to logout, unchanged** — both handlers keep
  `Err(net::FetchError::Unauthorized) => on_logout.emit(())` as the first,
  effect-free arm.
- **Editor-form paths unchanged** — Define / Run-ad-hoc already surface
  `Rejected(body)` via `server_error`; their handlers are not touched (they
  keep setting/clearing the same shared `tf_server_error`).
- **Normal Run flow unchanged in behavior** — Run from the Definition tab still
  flips to the Runs tab and fetches exactly once; only the dep tuple gains the
  epoch element.
- **Runs-tab `LoadStatus::Error` rendering, the list, Catalog and Ontology
  surfaces untouched.**
- **Existing `//src/ui` pure-logic tests and `//src/ui/e2e:login` unaffected**
  (no login/DOM-hook changes; `TransformDrawer` gains one optional-shaped prop,
  but yew props are not optional by default — the one call site in `main.rs` is
  updated in the same change).

## Testing

Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`
(the `no-inline-tests` prek hook fails the build otherwise). Component `html!`
rendering is not `rust_test`-able in this repo (no DOM in buck2's runner — see
`src/ui/CLAUDE.md`); the split below follows the established ui-core pattern.

- **New `rust_test` `//src/ui:transforms-actions`**
  (`src/ui/tests/transforms_actions.rs`, mirroring the `:transforms-display`
  target): the full `DrawerActionEffect` truth table, including the
  behavior-change assertions — a failed Run does **not** open the Runs tab, a
  failed Delete produces a pure error (no selection clear), and the error
  message passes through verbatim.
- **Wasm cross-compile + clippy** — `buck2 build --console none //src/ui:app`
  and its `[clippy.txt]`: proves the new prop, the render arm, the epoch dep,
  and the handler wiring all type-check under the strict gate.
- **Manual verification (documented step, not a test):** run the bundle against
  a backend, select a transform, Delete a transform the server rejects (or kill
  the backend) → the error line appears beside the buttons; press Run, flip to
  the Runs tab while the POST is in flight → the runs table still refetches.
- **Non-regression:** `buck2 test --console none //src/ui:` pure-logic targets
  stay green; `//src/ui/e2e:login` stays green.

## Global constraints (loom-specific, carry into the plan)

- **Tests are `rust_test` integration targets only** — the new test file lives
  at `src/ui/tests/transforms_actions.rs` with its own target in `src/ui/BUCK`;
  no inline `#[test]` anywhere.
- **Clippy is strict (pedantic + restriction)** on `loom_ui_core` (it carries
  no crate-level allow, unlike `:app`): the new helpers need `#[must_use]`
  where applicable and no panic-capable code. `src/ui/src/main.rs` and the
  surfaces ride under the app crate's existing
  `#![allow(clippy::pedantic, clippy::restriction)]` (the `html!` allowance).
- **`//src/ui:app` builds only under the wasm platform** — verify with
  `buck2 build --console none //src/ui:app` (its `default_target_platform`
  handles the cross-compile); `:ui-core` tests run native as usual.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files`
  (rustfmt is a separate hook; clippy-clean ≠ lint-clean). Markdown files end
  with exactly one trailing newline, no trailing whitespace.
- **No new dependencies, no BUCK `srcs` changes for `:app`** (all touched app
  files are already listed); `:ui-core`'s `srcs` already include
  `src/transforms.rs`.

## Non-goals (deferred)

- **A DOM/wasm component render harness** — stays deferred
  (`fut-ui-component-test-fixture`); this spec structures the change so the
  decision logic is testable without one.
- **Retry affordances / toasts / global error surface** — the fix is the
  surface-local error line, consistent with the editor form and the sibling
  Catalog fix.
- **Styling pass on the Transforms surface's `.error` class** — it renders
  unstyled (plain text) today across the surface; changing that is cosmetic
  scope creep.
- **Async job-failure reporting** — Run returns 202 with a `run_id`; a run that
  *starts* and then fails is already visible in the Runs table's Error column.
  This spec covers only the synchronous action response.
- **The sibling `iss-ui-swallowed-fetch-errors`** (Catalog drawer) — tracked
  and planned separately (`2026-07-07-ui-swallowed-fetch-errors`).

## Interfaces (names the plan consumes)

- Consumes: `TransformDrawerProps` + the Definition-tab action row
  (`src/ui/src/surfaces/transforms.rs:221-231`, `:278-285`);
  `TransformEditorProps.server_error` + its render (`:331`, `:515-517`) as the
  pattern; `tf_server_error` (`src/ui/src/main.rs:112`); the `on_run` /
  `on_delete` handlers (`main.rs:773-799`, `:801-830`) and `reload_list`
  (`main.rs:639-660`); the runs effect + dep (`main.rs:427-464`, `:436`) and
  `tf_runs_gen` (`main.rs:120`); the row-select clear block (`main.rs:393-399`);
  `net::run_transform`/`net::delete_transform`/`FetchError`
  (`src/ui/src/net.rs:320`, `:306`, `:61-84`); the `:transforms-display`
  `rust_test` target shape (`src/ui/BUCK:129-136`).
- Produces (the plan relies on these EXACT names/types):
  - `TransformDrawerProps.action_error: Option<AttrValue>` + its
    `<p class="error">` render before `.shell-drawer-actions`.
  - `pub struct DrawerActionEffect { pub error: Option<String>, pub
    open_runs_tab: bool, pub refetch_runs: bool, pub clear_selection: bool }`,
    `pub fn run_action_effect(result: Result<(), String>) -> DrawerActionEffect`,
    `pub fn delete_action_effect(result: Result<(), String>) -> DrawerActionEffect`
    in `loom_ui_core::transforms`, re-exported from `loom_ui_core`
    (`src/ui/src/lib.rs:13`).
  - `tf_runs_epoch: UseStateHandle<u64>` in `Workspace`, third element of the
    runs effect's dep tuple, bumped only by `run_action_effect(Ok(()))`'s
    `refetch_runs`.
  - `rust_test` target `//src/ui:transforms-actions`
    (`src/ui/tests/transforms_actions.rs`).
