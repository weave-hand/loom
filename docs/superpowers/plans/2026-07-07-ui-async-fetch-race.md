# Catalog drawer async-fetch race — epoch guard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop a slow in-flight drawer fetch from a previous dataset selection overwriting the current selection's Catalog drawer tab body, by guarding the detail/preview/lineage `spawn_local` fetches with a shared generation counter that is bumped on selection change.

**Architecture:** `Workspace` (`src/ui/src/main.rs`) resets drawer state synchronously on selection change, then `spawn_local`s a new fetch with no generation guard — so an out-of-order network arrival from a superseded selection can `.set()` stale content. Add a pure, testable `FetchGeneration` guard to `loom_ui_core`; hold it in a `use_mut_ref` in `Workspace`; bump it in the row-select effect; capture the generation into each of the three catalog fetch spawns; and only commit a fetch's result when the generation has not advanced. The Ontology surface eagerly loads all type details once on mount (no per-selection fetch), so it has no such race and is out of scope.

**Tech Stack:** Rust, Yew 0.21 (`use_mut_ref` → `Rc<RefCell<_>>`, already used in `sql_editor.rs`), `wasm_bindgen_futures::spawn_local`. Pure guard logic lives in `loom_ui_core` (host-compiled, `rust_test`-able); the Yew wiring is DOM-free-untestable and verified by cross-compilation (per `src/ui/CLAUDE.md`).

## Global Constraints

- **No DOM `#[test]`** — component/hook wiring is not `rust_test`-able (buck2 has no DOM; the `no-inline-tests` hook fails any inline `#[test]`). The only new test is a pure unit test of `FetchGeneration` in the existing `//src/ui:logic` target (`tests/logic.rs`, deps `:ui-core`).
- **`loom_ui_core` is lint-clean** (host-compiled, strict pedantic+restriction with NO crate-level allow) — the new type must carry `#[must_use]` on its non-mutating accessors and use no `unwrap`/indexing/panic.
- **`main.rs` (`app` crate) carries a crate-level `#![allow(clippy::pedantic, clippy::restriction)]`** already (for `html!`), so the wiring there is not lint-gated the same way — but keep it clean regardless.
- **The guard bumps on SELECTION change only** — a tab change for the same selection is a legitimate fresh fetch under the same generation and must be allowed to commit.
- **No behavior change to the 401 path** — `Err(FetchError::Unauthorized) => on_logout.emit(())` stays unguarded (logout is global, not selection-scoped).
- Commit messages end with the two required trailers; subjects follow Conventional Commits.

---

### Task 1: A testable `FetchGeneration` guard in `loom_ui_core`

**Files:**
- Modify: `src/ui/src/lib.rs` (add `FetchGeneration`)
- Test: `src/ui/tests/logic.rs` (unit tests for the guard)

**Interfaces:**
- Produces: `FetchGeneration` with `Default`, `fn current(&self) -> u32` (`#[must_use]`), `fn bump(&mut self) -> u32`, `fn is_current(&self, captured: u32) -> bool` (`#[must_use]`). Task 2 consumes this from `main.rs`.

- [ ] **Step 1: Write the failing unit test**

Append to `src/ui/tests/logic.rs`:

```rust
use loom_ui_core::FetchGeneration;

#[test]
fn fetch_generation_starts_at_zero_and_is_current() {
    let gen = FetchGeneration::default();
    assert_eq!(gen.current(), 0);
    assert!(gen.is_current(0), "the initial generation is current");
}

#[test]
fn fetch_generation_bump_advances_and_stales_prior() {
    let mut gen = FetchGeneration::default();
    let first = gen.bump(); // a fetch spawned for the first selection captures this
    assert_eq!(first, 1);
    assert!(gen.is_current(first));

    let second = gen.bump(); // selection changed: a new fetch captures this
    assert_eq!(second, 2);
    assert!(gen.is_current(second), "the latest generation is current");
    assert!(
        !gen.is_current(first),
        "the prior selection's fetch is now stale and must not commit"
    );
}
```

(`use loom_ui_core::FetchGeneration;` may need to fold into the existing top `use loom_ui_core::{…}` line — merge it there to avoid a duplicate import.)

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/ui:logic`
Expected: FAIL to compile — `FetchGeneration` is not defined in `loom_ui_core`.

- [ ] **Step 3: Add the guard type**

In `src/ui/src/lib.rs`, add (after the `url` function is a good spot):

```rust
/// A monotonic generation guard for selection-scoped async fetches in the
/// `Workspace` Catalog drawer. The drawer bumps the generation whenever the
/// selected dataset changes; each in-flight fetch captures the generation it was
/// spawned under and commits its result only if the generation has not advanced
/// since — so a fetch from a superseded selection cannot overwrite the current
/// drawer body (whichever fetch resolves last would otherwise win).
#[derive(Debug, Clone, Default)]
pub struct FetchGeneration(u32);

impl FetchGeneration {
    /// The current generation.
    #[must_use]
    pub fn current(&self) -> u32 {
        self.0
    }

    /// Advance to a new generation — call when the selection changes — and
    /// return the new value for the freshly-spawned fetch to capture.
    pub fn bump(&mut self) -> u32 {
        self.0 = self.0.wrapping_add(1);
        self.0
    }

    /// True if `captured` is still the current generation, i.e. the fetch that
    /// captured it may commit its result; false if the selection has advanced.
    #[must_use]
    pub fn is_current(&self, captured: u32) -> bool {
        self.0 == captured
    }
}
```

(`wrapping_add` avoids the `arithmetic_side_effects` restriction lint; the counter realistically never wraps, and `is_current` compares exact equality so even a wrap is harmless within one session.)

- [ ] **Step 4: Run the test to verify it passes (GREEN)**

Run: `buck2 test --console none //src/ui:logic`
Expected: `Pass N. Fail 0`.

- [ ] **Step 5: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/logic.rs
git commit -m "feat(ui): add FetchGeneration guard for selection-scoped fetches"
```

(Commit body carries the two required trailers.)

---

### Task 2: Guard the Catalog drawer fetches in `Workspace`

**Files:**
- Modify: `src/ui/src/main.rs` (add the `use_mut_ref` guard; bump in the row-select effect; capture + check in the detail, preview, and lineage spawns)

**Interfaces:**
- Consumes: `loom_ui_core::FetchGeneration` (Task 1); `yew::functional::use_mut_ref` (yields `Rc<RefCell<FetchGeneration>>`).

- [ ] **Step 1: Import the guard and add the `use_mut_ref`**

In `src/ui/src/main.rs`, add `FetchGeneration` to the **existing grouped** `use loom_ui_core::{…}` import at `main.rs:15-18` (fold it into that brace list — do NOT add a second standalone `use loom_ui_core::…` line). `use_mut_ref` is already in scope via `use yew::prelude::*;` (`main.rs:25`), the same import `sql_editor.rs` relies on — no new import needed for it.

Then, alongside the other `use_state` declarations in `workspace(...)` (after `let show_full_lineage = use_state(|| false);`, around line 85), add:

```rust
    // Generation guard for selection-scoped drawer fetches: bumped on dataset
    // selection change so a slow in-flight fetch from a prior selection cannot
    // overwrite the current selection's drawer tab body (see FetchGeneration).
    let fetch_gen = use_mut_ref(FetchGeneration::default);
```

- [ ] **Step 2: Bump on selection change and guard the detail fetch**

In the row-select effect (currently `src/ui/src/main.rs:112-145`), add `let fetch_gen = fetch_gen.clone();` to the clone-prelude (with the other `.clone()`s at the top of the block). Then, after the synchronous resets and before the `spawn_local`, bump the generation; and guard the `detail.set` in the spawn:

```rust
            detail.set(None);
            preview.set(None);
            preview_loading.set(false);
            lineage.set(None);
            show_full_lineage.set(false);
            catalog_tab.set(AttrValue::from("schema"));
            // Any in-flight fetch from the previous selection is now stale.
            let my_gen = fetch_gen.borrow_mut().bump();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_dataset_detail(&net::api_base(), &token, &ds.schema, &ds.name)
                    .await
                {
                    Ok(d) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            detail.set(Some(d));
                        }
                    }
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    // Best-effort: a failure leaves the Schema tab on its "Loading…"
                    // line rather than blocking the rest of the drawer.
                    Err(_) => {}
                }
            });
```

(`fetch_gen` is used by-ref for `bump()` then moved into the `async move` — valid in the effect's `FnOnce` closure, mirroring how the existing effects move their captured handles into `spawn_local`.)

- [ ] **Step 3: Guard the preview fetch**

In the lazy-preview effect (currently `src/ui/src/main.rs:151-181`), add `let fetch_gen = fetch_gen.clone();` to its clone-prelude. Capture the current generation at spawn time (this effect does NOT bump — it fetches for the current selection), and guard every state mutation so a stale resolution touches nothing:

```rust
            preview_loading.set(true);
            let my_gen = fetch_gen.borrow().current();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_preview(&net::api_base(), &token, &ds.schema, &ds.name, 50).await {
                    Ok(p) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            preview.set(Some(p));
                            preview_loading.set(false);
                        }
                    }
                    Err(FetchError::Unauthorized) => {
                        preview_loading.set(false);
                        on_logout.emit(());
                    }
                    Err(_) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            preview_loading.set(false);
                        }
                    }
                }
            });
```

(When the selection has advanced, the new selection's own row-select effect has already reset `preview`/`preview_loading`, so the stale fetch correctly leaves them alone.)

- [ ] **Step 4: Guard the lineage fetch**

In the lazy-lineage effect (currently `src/ui/src/main.rs:187-217`), add `let fetch_gen = fetch_gen.clone();` to its clone-prelude, capture the current generation, and guard the `lineage.set`:

```rust
            let my_gen = fetch_gen.borrow().current();
            wasm_bindgen_futures::spawn_local(async move {
                let base = net::api_base();
                let up = net::fetch_lineage(&base, &token, &ds.schema, &ds.name, "upstream").await;
                let down =
                    net::fetch_lineage(&base, &token, &ds.schema, &ds.name, "downstream").await;
                // A 401 on either leg fails closed to logout; any other error degrades
                // to an empty closure so the mini-DAG still renders the current node.
                if up.as_ref().err() == Some(&FetchError::Unauthorized)
                    || down.as_ref().err() == Some(&FetchError::Unauthorized)
                {
                    on_logout.emit(());
                    return;
                }
                if fetch_gen.borrow().is_current(my_gen) {
                    lineage.set(Some((up.unwrap_or_default(), down.unwrap_or_default())));
                }
            });
```

- [ ] **Step 5: Cross-compile the app to verify the wiring compiles**

Run: `buck2 build --console none //src/ui:app`
Expected: builds clean (cross-compiled to wasm via `default_target_platform = //platforms:wasm`). This is the compile-time proof of the hook wiring — there is no DOM test.

- [ ] **Step 6: Re-run the pure-logic tests and clippy on the touched crates**

Run: `buck2 test --console none //src/ui:logic` (guard tests still green).
Run clippy on the app + core: `buck2 build --console none '//src/ui:app[clippy.txt]' '//src/ui:ui-core[clippy.txt]'` and confirm the printed clippy files are empty (clean).

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/main.rs
git commit -m "fix(ui): guard Catalog drawer fetches against out-of-order selection races"
```

(Commit body carries the two required trailers.)
