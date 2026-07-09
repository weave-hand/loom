# Transforms drawer action errors + Runs-tab refetch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Surface non-401 failures of the Transforms drawer's **Run saved** and **Delete** actions as an error line beside the action buttons (today a failed Delete is a silent no-op and a failed Run silently flips tabs), and make a Run whose response lands while the Runs tab is already the active effect dep actually refetch the runs history (today it clears `tf_runs` and renders a stale-empty table).

**Architecture:** Per the spec (`docs/superpowers/specs/2026-07-09-ui-transforms-drawer-errors-design.md`): (1) a new `action_error: Option<AttrValue>` prop on `TransformDrawer`, rendered as `<p class="error">` before the `.shell-drawer-actions` row, fed from the surface's existing `tf_server_error` state; (2) a `tf_runs_epoch: UseStateHandle<u64>` folded into the runs effect's dep tuple and bumped on Run success. The decision logic lives in two pure `loom_ui_core` helpers (`run_action_effect` / `delete_action_effect` → `DrawerActionEffect`) pinned by a new `rust_test` — the only test shape this repo has for UI code (no DOM harness; `fut-ui-component-test-fixture` is deferred). The `html!`/hook wiring is verified by wasm cross-compile + clippy + a documented manual check, the same posture as the sibling `2026-07-07-ui-swallowed-fetch-errors` plan.

**Tech Stack:** Rust, Yew 0.21, buck2 `rust_test` (native) + wasm cross-compile for `:app`.

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` integration targets only — never inline `#[cfg(test)]`.** The new test is `src/ui/tests/transforms_actions.rs` with its own target in `src/ui/BUCK`, mirroring `:transforms-display`. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **Clippy is strict (pedantic + restriction) on `loom_ui_core`** — it carries no crate-level allow (unlike `:app`). New helpers: `#[must_use]`, no panic-capable code, doc comments. `main.rs` / `surfaces/transforms.rs` ride under the app crate's existing `#![allow(clippy::pedantic, clippy::restriction)]`.
- **401 fails closed to logout, unchanged** — `Err(net::FetchError::Unauthorized) => on_logout.emit(())` stays the first arm of both handlers; the effect helpers never see a 401.
- **Editor-form Define / Run-ad-hoc paths unchanged** — they already surface `Rejected(body)` via the shared `tf_server_error`; do not touch their handlers.
- **Normal Run flow stays single-fetch** — Run from the Definition tab must still produce exactly one runs fetch (one re-render: tab flip + epoch bump land in the same dep change).
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a separate hook; clippy-clean ≠ lint-clean; `git add` new files first — prek skips untracked files). Markdown ends with exactly one trailing newline, no trailing whitespace.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; scoped tests `buck2 test --console none //src/ui:<target>`. `:app` cross-compiles via its `default_target_platform` — a plain `buck2 build --console none //src/ui:app` is the wasm check.

---

## File Structure

**Create:**
- `src/ui/tests/transforms_actions.rs` — `DrawerActionEffect` truth-table `rust_test`.

**Modify:**
- `src/ui/src/transforms.rs` — `DrawerActionEffect` + `run_action_effect` + `delete_action_effect` (pure, `loom_ui_core`).
- `src/ui/src/lib.rs:13` — re-export the three new names.
- `src/ui/src/surfaces/transforms.rs` — `TransformDrawerProps.action_error` + render.
- `src/ui/src/main.rs` — `tf_runs_epoch` state; runs-effect dep; `on_run`/`on_delete` rewired through the helpers; row-select reset; drawer call site.
- `src/ui/BUCK` — new `transforms-actions` `rust_test` target.

---

## Task 1: Pure action-effect helpers in `loom_ui_core` (TDD)

**Files:**
- Create: `src/ui/tests/transforms_actions.rs`
- Modify: `src/ui/BUCK` (new target after `transforms-display`, line 136)
- Modify: `src/ui/src/transforms.rs` (helpers at the end, after `clamp_drawer_width`, line 481)
- Modify: `src/ui/src/lib.rs:13` (re-export)

**Interfaces:**
- Consumes: nothing (foundation).
- Produces (Task 2 relies on these EXACT names/types): `loom_ui_core::{DrawerActionEffect, run_action_effect, delete_action_effect}` — `DrawerActionEffect { error: Option<String>, open_runs_tab: bool, refetch_runs: bool, clear_selection: bool }`.

- [ ] **Step 1: Write the failing test**

Create `src/ui/tests/transforms_actions.rs`:

```rust
//! The Transforms drawer-action state transitions (Run saved / Delete outcomes)
//! — the DOM-free core of iss-ui-transforms-drawer-errors. External rust_test
//! (no inline #[cfg(test)]) — see CLAUDE.md.

use loom_ui_core::{DrawerActionEffect, delete_action_effect, run_action_effect};

#[test]
fn run_success_opens_runs_tab_and_refetches() {
    let eff = run_action_effect(Ok(()));
    assert_eq!(
        eff,
        DrawerActionEffect {
            error: None,
            open_runs_tab: true,
            refetch_runs: true,
            clear_selection: false,
        },
        "a successful Run clears the error, opens Runs, and forces a refetch \
         (epoch bump) even when the Runs tab is already active"
    );
}

#[test]
fn run_failure_surfaces_the_error_and_stays_on_definition() {
    let eff = run_action_effect(Err("bad transform: no such input".to_string()));
    assert_eq!(
        eff.error.as_deref(),
        Some("bad transform: no such input"),
        "the server message passes through verbatim"
    );
    assert!(
        !eff.open_runs_tab,
        "a failed Run must NOT flip to the Runs tab — the error renders beside \
         the Definition tab's action buttons (behavior change vs the old \
         silent tab flip)"
    );
    assert!(!eff.refetch_runs, "nothing ran; no refetch");
    assert!(!eff.clear_selection, "selection untouched");
}

#[test]
fn delete_success_clears_the_selection() {
    let eff = delete_action_effect(Ok(()));
    assert_eq!(
        eff,
        DrawerActionEffect {
            error: None,
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: true,
        },
        "a successful Delete clears selection + def and reloads the list"
    );
}

#[test]
fn delete_failure_is_loud_and_otherwise_a_no_op() {
    let eff = delete_action_effect(Err("server error (404)".to_string()));
    assert_eq!(
        eff,
        DrawerActionEffect {
            error: Some("server error (404)".to_string()),
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: false,
        },
        "a failed Delete keeps the row + drawer and surfaces the message \
         (behavior change vs the old silent no-op)"
    );
}
```

- [ ] **Step 2: Wire the test target**

In `src/ui/BUCK`, after the `transforms-display` target (line 136), mirror it:

```python
rust_test(
    name = "transforms-actions",
    crate = "transforms_actions",
    srcs = ["tests/transforms_actions.rs"],
    crate_root = "tests/transforms_actions.rs",
    edition = "2024",
    deps = [":ui-core"],
)
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/ui:transforms-actions`
Expected: FAIL (compile error — `DrawerActionEffect`/`run_action_effect`/`delete_action_effect` not found).

- [ ] **Step 4: Implement the helpers**

In `src/ui/src/transforms.rs`, after `clamp_drawer_width` (line 481), add:

```rust
/// The `Workspace` state transition applied when a drawer action (Run saved /
/// Delete) resolves. Pure and DOM-free so the drawer-action contract is
/// `rust_test`-able (component rendering is not — see `src/ui/CLAUDE.md`).
/// The caller routes `FetchError::Unauthorized` to logout BEFORE building the
/// `Result<(), String>` — a 401 never reaches these helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrawerActionEffect {
    /// The surface's server-error line (`tf_server_error`): `Some(msg)` renders
    /// beside the drawer's action buttons; `None` clears a previous error.
    pub error: Option<String>,
    /// Flip the drawer to the Runs tab.
    pub open_runs_tab: bool,
    /// Clear `tf_runs` and bump the runs-fetch epoch, so the runs effect
    /// refires even when the Runs tab is already part of its dep tuple.
    pub refetch_runs: bool,
    /// Clear the selection + loaded def and reload the transform list (the
    /// selected row no longer exists).
    pub clear_selection: bool,
}

/// The transition for a **Run saved** response. Success opens the Runs tab and
/// forces a refetch; failure surfaces the message and deliberately stays on
/// the Definition tab, where the error line renders beside the Run button.
#[must_use]
pub fn run_action_effect(result: Result<(), String>) -> DrawerActionEffect {
    match result {
        Ok(()) => DrawerActionEffect {
            error: None,
            open_runs_tab: true,
            refetch_runs: true,
            clear_selection: false,
        },
        Err(msg) => DrawerActionEffect {
            error: Some(msg),
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: false,
        },
    }
}

/// The transition for a **Delete** response. Success clears the selection
/// (the row is gone); failure surfaces the message and otherwise changes
/// nothing — the row and drawer remain.
#[must_use]
pub fn delete_action_effect(result: Result<(), String>) -> DrawerActionEffect {
    match result {
        Ok(()) => DrawerActionEffect {
            error: None,
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: true,
        },
        Err(msg) => DrawerActionEffect {
            error: Some(msg),
            open_runs_tab: false,
            refetch_runs: false,
            clear_selection: false,
        },
    }
}
```

In `src/ui/src/lib.rs`, add `DrawerActionEffect`, `delete_action_effect`, `run_action_effect` to the `pub use transforms::{…}` list (line 13, keep it alphabetized).

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/ui:transforms-actions`
Expected: PASS (4 tests). Also: `buck2 build --console none '//src/ui:ui-core[clippy.txt]'` and confirm the printed file is empty (`:ui-core` has no crate-level allow — the strict gate applies in full).

- [ ] **Step 6: Run prek + commit**

```bash
git add src/ui/tests/transforms_actions.rs src/ui/BUCK src/ui/src/transforms.rs src/ui/src/lib.rs
buck2 run //tools:prek -- run --all-files
git commit -m "feat(ui): pure drawer-action effect helpers for the Transforms surface

DrawerActionEffect + run_action_effect/delete_action_effect in loom_ui_core:
the rust_test-able truth table for iss-ui-transforms-drawer-errors (a failed
Run stays on the Definition tab with an error; a failed Delete is loud; a
successful Run forces a runs refetch). No behavior change yet — Workspace
still swallows the errors until the wiring task."
```

(Commit body carries the two required trailers.)

---

## Task 2: `action_error` prop + `Workspace` wiring (epoch bump, handler rewrite)

**Files:**
- Modify: `src/ui/src/surfaces/transforms.rs` (`TransformDrawerProps` line 221-231; Definition-tab body line 271-288)
- Modify: `src/ui/src/main.rs` (state line ~120; row-select reset line ~393-399; runs-effect dep line ~436; `on_run` line ~773-799; `on_delete` line ~801-830; drawer call site line ~832-835)

**Interfaces:**
- Consumes: `loom_ui_core::{run_action_effect, delete_action_effect}` (Task 1); `tf_server_error` (`main.rs:112`); `reload_list` (`main.rs:639`); `net::run_transform`/`net::delete_transform` (`net.rs:320`/`:306`).
- Produces: `TransformDrawerProps.action_error: Option<AttrValue>`; `tf_runs_epoch: UseStateHandle<u64>` as the runs-effect dep's third element.

- [ ] **Step 1: Add the prop and render the error line (surfaces/transforms.rs)**

In `TransformDrawerProps` (after `runs_status`, line 227), add:

```rust
    /// A non-401 Run/Delete failure, rendered beside the action buttons
    /// (mirrors `TransformEditorProps.server_error`). `None` = no error.
    pub action_error: Option<AttrValue>,
```

In the Definition-tab body (the `_ =>` arm, line 271-288), insert the error line between the `SqlEditor` and the `.shell-drawer-actions` div:

```rust
                    if let Some(msg) = &props.action_error {
                        <p class="error">{ msg }</p>
                    }
                    <div class="shell-drawer-actions">
```

(`class="error"` matches the surface's existing `LoadStatus::Error` lines; no `css!` block exists or is added on this surface.)

- [ ] **Step 2: Add the epoch state + fold it into the runs effect (main.rs)**

After `tf_runs_gen` (line 120), add:

```rust
    // Render-participating runs-fetch epoch: bumped on a successful Run so the
    // runs effect refires even when (selection, tab) is unchanged — i.e. when
    // the response lands with the Runs tab already active. (tf_runs_gen stays
    // the in-flight staleness guard; this is the dep invalidator.)
    let tf_runs_epoch = use_state(|| 0u64);
```

In the runs effect (line ~427-464): clone nothing extra (the dep captures the value), change the dep tuple (line 436) to:

```rust
        let dep = (*tf_selected, (*tf_drawer_tab).clone(), *tf_runs_epoch);
        use_effect_with(dep, move |(sel, tab, _epoch)| {
```

- [ ] **Step 3: Reset the shared error on row-select (main.rs)**

In the transform row-select effect's clear block (lines ~393-399), clone `tf_server_error` into the effect and add, alongside `tf_editing.set(None)`:

```rust
            tf_server_error.set(None); // a stale action error never leaks onto a new selection
```

- [ ] **Step 4: Rewire `on_run` through `run_action_effect` (main.rs ~773-799)**

Replace the handler body. Clone `tf_server_error` and `tf_runs_epoch` into it (alongside the existing `name`/`tf_runs`/`tf_drawer_tab`/`on_logout` clones); clear the error at click; keep the `Unauthorized` arm first; apply the effect to the remainder:

```rust
                let on_run = {
                    let name = def.name.clone();
                    let tf_runs = tf_runs.clone();
                    let tf_runs_epoch = tf_runs_epoch.clone();
                    let tf_drawer_tab = tf_drawer_tab.clone();
                    let server_error = tf_server_error.clone();
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
                    let base = net::api_base();
                    Callback::from(move |()| {
                        server_error.set(None);
                        let (name, tf_runs, tf_runs_epoch, tf_drawer_tab) = (
                            name.clone(),
                            tf_runs.clone(),
                            tf_runs_epoch.clone(),
                            tf_drawer_tab.clone(),
                        );
                        let (server_error, on_logout) = (server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        wasm_bindgen_futures::spawn_local(async move {
                            match net::run_transform(&base, &token, &name).await {
                                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                                other => {
                                    let eff = run_action_effect(
                                        other.map(|_run_id| ()).map_err(|e| e.to_string()),
                                    );
                                    server_error.set(eff.error.map(AttrValue::from));
                                    if eff.refetch_runs {
                                        tf_runs.set(Vec::new());
                                        tf_runs_epoch.set(*tf_runs_epoch + 1);
                                    }
                                    if eff.open_runs_tab {
                                        tf_drawer_tab.set(AttrValue::from("runs"));
                                    }
                                }
                            }
                        });
                    })
                };
```

(Note the behavior change baked into the helper: the old `Err(_e) => tf_drawer_tab.set("runs")` silent flip is gone — a failed Run stays on the Definition tab with the error line visible.)

- [ ] **Step 5: Rewire `on_delete` through `delete_action_effect` (main.rs ~801-830)**

Same shape — clone `tf_server_error` in, clear at click, `Unauthorized` first, then:

```rust
                        wasm_bindgen_futures::spawn_local(async move {
                            match net::delete_transform(&base, &token, &name).await {
                                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                                other => {
                                    let eff =
                                        delete_action_effect(other.map_err(|e| e.to_string()));
                                    server_error.set(eff.error.map(AttrValue::from));
                                    if eff.clear_selection {
                                        tf_selected.set(None);
                                        tf_def.set(None);
                                        reload_list();
                                    }
                                }
                            }
                        });
```

Add `run_action_effect` and `delete_action_effect` (and `DrawerActionEffect` is not needed by name) to `main.rs`'s `loom_ui_core` import list.

- [ ] **Step 6: Pass the prop at the drawer call site (main.rs ~832-835)**

```rust
                    <TransformDrawer def={def} active_tab={(*tf_drawer_tab).clone()}
                        on_tab={on_tab}
                        runs={(*tf_runs).clone()} runs_status={(*tf_runs_status).clone()}
                        action_error={(*tf_server_error).clone()}
                        on_edit={on_edit} on_run={on_run} on_delete={on_delete} />
```

- [ ] **Step 7: Cross-compile + clippy**

Run: `buck2 build --console none //src/ui:app`
Expected: clean wasm cross-compile — the prop, the render arm, the epoch dep, and both handlers agree.

Run: `buck2 build --console none '//src/ui:app[clippy.txt]'` — read the printed path in a separate step and confirm the file is empty.

- [ ] **Step 8: Run prek + commit**

```bash
git add src/ui/src/surfaces/transforms.rs src/ui/src/main.rs
buck2 run //tools:prek -- run --all-files
git commit -m "fix(ui): surface Transforms drawer Run/Delete errors; refetch runs from the Runs tab

Add TransformDrawerProps.action_error (rendered beside the action buttons,
fed from the shared tf_server_error) and rewire on_run/on_delete through the
Task-1 effect helpers: a failed Run now stays on the Definition tab with the
server message instead of silently flipping tabs; a failed Delete is no
longer a silent no-op. A tf_runs_epoch dep bump on Run success makes the
runs effect refire even when the Runs tab is already active. 401 still
fails closed to logout; the editor-form paths are untouched.

Closes iss-ui-transforms-drawer-errors."
```

(Commit body carries the two required trailers.)

---

## Task 3: Final verification (build, tests, bundle, manual check)

- [ ] **Step 1: Full first-party build**

Run: `buck2 build -v0 --console none //src/...`
Expected: silent success (exit 0). (Cloud sessions: `buck2 build -M none //src/...`.)

- [ ] **Step 2: UI test targets**

Run: `buck2 test --console none //src/ui:transforms-actions //src/ui:transforms-display //src/ui:transforms-form //src/ui:transforms-parse //src/ui:transforms-schema //src/ui:logic //src/ui:surface`
Expected: `Tests finished: Pass N. Fail 0`.

If on a host that can run the browser e2e (or on RE via the cloud shim), also run: `buck2 test --console none //src/ui/e2e:login` — the login flow and Shell DOM hooks are untouched, so it must stay green.

- [ ] **Step 3: Bundle builds**

Run: `buck2 build --console none //src/ui:bundle`
Expected: `dist/` builds (wasm-bindgen glue unaffected by the change).

- [ ] **Step 4: Manual verification (documented — no automated harness exists for drawer rendering)**

Serve the bundle against a live backend (e.g. the standalone binary with `LOOM_UI_DIR`, or `buck2 run //src/ui:serve` with `config.js` pointed at a running query-api). As an admin:

1. Select a transform → Definition tab → **Delete** a transform the server will reject (or stop the backend first) → the error line appears above the Edit/Run/Delete buttons; the row and drawer remain.
2. **Run** a valid transform; while the POST is in flight, click the **Runs** tab → after the 202 lands, the runs table shows the Loading state and then the refreshed history (not a stale-empty table).
3. **Run** from the Definition tab normally → still flips to Runs and fetches once.
4. Select a different transform after a failure → the error line is gone (row-select reset).

Record the outcome in the PR description (this is the rendering-wiring coverage the spec's Testing section documents in lieu of a DOM test).

- [ ] **Step 5: prek clean**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks pass with no files modified. If hooks rewrote anything, `git add` + amend/commit the fixups before opening the PR.
