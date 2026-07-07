# Catalog drawer swallowed fetch errors — surface an error line — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the Catalog drawer's Schema and Preview tabs from hanging on "Loading…" (Schema) or silently emptying (Preview) when a non-401 fetch fails — surface a small error line instead, consistent with the rest of the UI.

**Architecture:** `Workspace`'s detail and preview effects (`src/ui/src/main.rs`) swallow non-401 errors (`Err(_) => {}` / just clear the loading flag), leaving `detail`/`preview` at `None` so the drawer body renders its "Loading…" placeholder forever. Add `detail_error`/`preview_error` state, set it (epoch-guarded, like item 3) in the effects' error arms, reset it on selection change, pass it to `CatalogDrawer`, and render it as a `<p class="error">` line (styled with `--loom-danger`) that takes precedence over the loading placeholder. The Lineage tab already degrades gracefully and is untouched.

**Tech Stack:** Rust, Yew 0.21, `stylist` `css!`. This is a DOM-rendering change with no extractable pure logic, so — per `src/ui/CLAUDE.md`'s explicit no-DOM-test policy (component `html!` rendering is not `rust_test`-able) — it is verified by wasm cross-compilation + clippy, exactly as the existing untested `schema_body`/`preview_body` render functions are.

## Global Constraints

- **No new `rust_test`** — the change is pure `html!` rendering (no DOM test harness exists; `schema_body`/`preview_body` have no tests today). Verified by `buck2 build //src/ui:app` (wasm) + clippy. Do NOT add an inline `#[test]` (the `no-inline-tests` hook fails the build).
- **Error takes precedence over the loading placeholder** in both body functions — an error line replaces "Loading…", it does not stack with it.
- **Epoch-guard the error set** — set `detail_error`/`preview_error` only when `fetch_gen.borrow().is_current(my_gen)`, mirroring the item-3 guard already in these effects, so a superseded selection's late error cannot stale the current drawer.
- **401 path unchanged** — `Err(FetchError::Unauthorized) => on_logout.emit(())` stays as is (logout is global, not an in-drawer error).
- **`main.rs` carries a crate-level `#![allow(clippy::pedantic, clippy::restriction)]`** (for `html!`); `catalog.rs` is in the same `app` crate. Keep the code clean regardless.
- Commit message ends with the two required trailers; subject follows Conventional Commits.

---

### Task 1: Surface non-401 fetch errors on the Schema and Preview drawer tabs

**Files:**
- Modify: `src/ui/src/surfaces/catalog.rs` (`CatalogDrawerProps` + `schema_body`/`preview_body` + a `.error` css rule)
- Modify: `src/ui/src/main.rs` (`detail_error`/`preview_error` state; reset on select; set in the detail/preview error arms; pass to `CatalogDrawer`)

**Interfaces:**
- Produces: two new `CatalogDrawerProps` fields `detail_error: Option<String>` / `preview_error: Option<String>`; `schema_body(Option<&DatasetDetail>, Option<&str>)` and `preview_body(Option<&PreviewData>, bool, Option<&str>)`.
- Consumes: `FetchError`'s `Display` (`e.to_string()`, already used for `LoadStatus::Error` at `main.rs:107`); the item-3 `fetch_gen` guard already in these effects.

- [ ] **Step 1: Add the error props and render an error line (catalog.rs)**

In `src/ui/src/surfaces/catalog.rs`, add two fields to `CatalogDrawerProps` (after `preview_loading`, line 106):

```rust
    /// A non-401 fetch error for the Schema tab, surfaced in place of "Loading…".
    pub detail_error: Option<String>,
    /// A non-401 fetch error for the Preview tab, surfaced in place of "Loading…".
    pub preview_error: Option<String>,
```

Add a `.error` rule to the drawer's `css!` block (alongside the `.empty` rule at line 132):

```rust
        .error { color: var(--loom-danger); font-size: 13px; padding: 8px 0; }
```

Update the tab-body dispatch (lines 165 and 174) to pass the error through:

```rust
        "preview" => preview_body(
            props.preview.as_ref(),
            props.preview_loading,
            props.preview_error.as_deref(),
        ),
```
```rust
        _ => schema_body(props.detail.as_ref(), props.detail_error.as_deref()),
```

Give `schema_body` an `error` param and an error-first branch (replace its signature + leading `let Some(detail) …`):

```rust
fn schema_body(detail: Option<&DatasetDetail>, error: Option<&str>) -> Html {
    if let Some(e) = error {
        return html! { <p class="error">{ e.to_owned() }</p> };
    }
    let Some(detail) = detail else {
        return html! { <p class="empty">{ "Loading…" }</p> };
    };
```

(The rest of `schema_body` is unchanged.)

Give `preview_body` an `error` param and an error-first branch (replace its signature + leading `if loading …`):

```rust
fn preview_body(preview: Option<&PreviewData>, loading: bool, error: Option<&str>) -> Html {
    if let Some(e) = error {
        return html! { <p class="error">{ e.to_owned() }</p> };
    }
    if loading {
        return html! { <p class="empty">{ "Loading…" }</p> };
    }
```

(The rest of `preview_body` is unchanged.)

- [ ] **Step 2: Add error state and wire the effects (main.rs)**

In `src/ui/src/main.rs`, add two state hooks after `preview_loading` (line 76):

```rust
    let detail_error = use_state(|| Option::<String>::None);
    let preview_error = use_state(|| Option::<String>::None);
```

**Row-select effect** (the reset block ~line 132) — NOTE: this is the SAME `use_effect_with` block as the "Detail effect error arm" below (the detail fetch lives in it), so add `detail_error` to its clone-prelude exactly ONCE (used synchronously for the reset, then moved into the `async move` block for the error arm — the same pattern `detail` itself already follows). Add `detail_error`/`preview_error` to the clone-prelude (with the other `.clone()`s), and reset them alongside the other state resets:

```rust
            detail.set(None);
            preview.set(None);
            preview_loading.set(false);
            detail_error.set(None);
            preview_error.set(None);
```

**Detail effect error arm** (line 152, currently `Err(_) => {}`): clone `detail_error` into this effect, and set it (epoch-guarded):

```rust
                    Err(e) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            detail_error.set(Some(e.to_string()));
                        }
                    }
```

**Preview effect error arm** (line ~192, currently `Err(_) => { if is_current { preview_loading.set(false) } }`): clone `preview_error` into this effect, and set the error alongside clearing the loading flag (epoch-guarded):

```rust
                    Err(e) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            preview_error.set(Some(e.to_string()));
                            preview_loading.set(false);
                        }
                    }
```

**`CatalogDrawer` invocation** (line ~324): pass the two new props:

```rust
                            preview_loading={*preview_loading}
                            detail_error={(*detail_error).clone()}
                            preview_error={(*preview_error).clone()}
                            active_tab={(*catalog_tab).clone()}
```

- [ ] **Step 3: Cross-compile + clippy**

Run: `buck2 build --console none //src/ui:app`
Expected: builds clean (wasm cross-compile) — the `CatalogDrawerProps` fields, the two body signatures, and the `CatalogDrawer` call site all agree.

Run: `buck2 build --console none '//src/ui:app[clippy.txt]'` and confirm the printed clippy file is empty.

- [ ] **Step 4: Confirm the login e2e still builds/passes**

The change touches the Catalog drawer, not the login flow, but the drawer is part of the same bundle. Run: `buck2 test --console none //src/ui:logic` (pure-logic tests unaffected) and, if time permits, `buck2 build --console none //src/ui:bundle` (the full bundle builds).

- [ ] **Step 5: Commit**

```bash
git add src/ui/src/surfaces/catalog.rs src/ui/src/main.rs
git commit -m "fix(ui): surface non-401 fetch errors on Catalog Schema/Preview tabs"
```

(Commit body carries the two required trailers.)
