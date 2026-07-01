# loom UI Component Library — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the foundation of loom's web-UI design system — a CSS-variable token layer, a set of `stylist`-styled primitive components in a new `loom_ui_components` wasm library, and a dev-only `:gallery` bundle that renders them all in isolation.

**Architecture:** Pure, testable logic (token enums + `format_count`) lives in the existing lint-clean `loom_ui_core` lib with real `rust_test` coverage. The `html!`/`stylist` components live in a new `loom_ui_components` wasm `rust_library` (crate-level pedantic/restriction allow, like `:app`). A new `:gallery` `rust_binary` + `:gallery-bundle` genrule composes every primitive; it never ships in the production login bundle. A `GlobalStyles` component injects the `:root { --loom-* }` token block once at the root.

**Tech Stack:** Rust 2024, yew `0.21` (`#[function_component]`, features `["csr"]`), `stylist` `0.13` (CSS-in-Rust, `<Global>` + `css!`), wasm-bindgen `=0.2.126`, buck2, reindeer.

## Global Constraints

- **Tests are `rust_test` integration targets only** — never inline `#[cfg(test)]` (the `no-inline-tests` prek hook enforces it). Pure-logic tests live in `src/ui/tests/*.rs` wired as `rust_test` targets via `load("//src:loom_test.bzl", "rust_test")`.
- **Strict clippy** (pedantic + restriction) runs on `//src`. Any crate containing `html!` carries a crate-level `#![allow(clippy::pedantic, clippy::restriction, reason = "…")]` (`allow`, not `expect`). `loom_ui_core` stays pure and lint-clean — **no allow**.
- **Wasm platform:** every wasm rule sets `default_target_platform = "//platforms:wasm"`. `default_target_platform` does **not** propagate through a genrule dep — a genrule consuming a wasm target via `$(location …)` must set it too.
- **New dep discipline:** adding `stylist` = edit `src/ui/Cargo.toml` → refresh lock (`eval "$(./tools/env.sh)"; cargo generate-lockfile`) → `./tools/buckify.sh` → **full** `buck2 test //src/...` (the reindeer `[platform]` config is graph-global; a wasm cross-compile leak surfaces tree-wide).
- **Don't pipe `buck2 test` through `tail`** — redirect to a file and grep: `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`.
- **Component rendering is NOT unit-testable** in buck2 (no DOM). Component tasks are verified by (a) the wasm **compile gate** (`buck2 build` of the wasm targets catches type/prop errors) and (b) **eyeballing the served gallery**. This is deliberate — see `fut-ui-component-test-fixture`. Do not fake a green-but-empty render test.
- **Tokens (verbatim, from the spec):**
  ```
  --loom-bg:#0b0e14  --loom-panel:#161b22  --loom-panel-2:#1c2230  --loom-border:#232a35
  --loom-text:#e6edf3  --loom-text-mut:#8b949e  --loom-accent:#3b82f6  --loom-accent-fg:#ffffff
  --loom-ok:#3fb950  --loom-warn:#d29922  --loom-danger:#f85149
  --loom-radius:6px  --loom-radius-sm:4px
  ```
- **Serving the gallery** (wasm needs `fetch`, `file://` is blocked): `buck2 run //src/ui:gallery-serve` after `buck2 build //src/ui:gallery-bundle`.

---

### Task 1: Core token enums + `format_count` (pure, TDD)

All logic here is pure Rust in the existing `loom_ui_core` lib — no yew, no wasm, no new deps. This is the only genuinely `rust_test`-able task; do it test-first.

**Files:**
- Modify: `src/ui/src/lib.rs` (append enums + `format_count`)
- Create: `src/ui/tests/tokens.rs` (new `rust_test`)
- Modify: `src/ui/BUCK` (add the `:tokens` rust_test target)

**Interfaces:**
- Produces (consumed by Tasks 3–5 and the gallery):
  - `enum ButtonVariant { Primary, Secondary, Ghost }` — `fn modifier(self) -> &'static str` → `"primary"|"secondary"|"ghost"`
  - `enum BadgeTone { Neutral, Info, Pii, Success, Warning, Danger }` — `fn css_var(self) -> &'static str` (accent color var name, see mapping below)
  - `enum Status { Ok, Warn, Error }` — `fn css_var(self) -> &'static str` → `"--loom-ok"|"--loom-warn"|"--loom-danger"`
  - `enum Align { Start, End }` — `fn css_value(self) -> &'static str` → `"flex-start"|"flex-end"`
  - `fn format_count(n: u64) -> String`
  - All enums derive `Clone, Copy, PartialEq, Eq, Debug`.

- [ ] **Step 1: Write the failing test**

Create `src/ui/tests/tokens.rs`:

```rust
use loom_ui_core::{format_count, Align, BadgeTone, ButtonVariant, Status};

#[test]
fn format_count_scales_and_rounds() {
    assert_eq!(format_count(2_410_000), "2.41M");
    assert_eq!(format_count(18_200), "18.2K");
    assert_eq!(format_count(880_000), "880K");
    assert_eq!(format_count(9_700), "9.7K");
    assert_eq!(format_count(142_000), "142K");
    assert_eq!(format_count(999), "999");
    assert_eq!(format_count(0), "0");
    assert_eq!(format_count(1_000), "1K");
    assert_eq!(format_count(1_000_000), "1M");
}

#[test]
fn status_maps_to_token_var() {
    assert_eq!(Status::Ok.css_var(), "--loom-ok");
    assert_eq!(Status::Warn.css_var(), "--loom-warn");
    assert_eq!(Status::Error.css_var(), "--loom-danger");
}

#[test]
fn button_variant_modifier() {
    assert_eq!(ButtonVariant::Primary.modifier(), "primary");
    assert_eq!(ButtonVariant::Secondary.modifier(), "secondary");
    assert_eq!(ButtonVariant::Ghost.modifier(), "ghost");
}

#[test]
fn badge_tone_maps_to_token_var() {
    assert_eq!(BadgeTone::Neutral.css_var(), "--loom-text-mut");
    assert_eq!(BadgeTone::Info.css_var(), "--loom-accent");
    assert_eq!(BadgeTone::Pii.css_var(), "--loom-danger");
    assert_eq!(BadgeTone::Success.css_var(), "--loom-ok");
    assert_eq!(BadgeTone::Warning.css_var(), "--loom-warn");
    assert_eq!(BadgeTone::Danger.css_var(), "--loom-danger");
}

#[test]
fn align_maps_to_css_value() {
    assert_eq!(Align::Start.css_value(), "flex-start");
    assert_eq!(Align::End.css_value(), "flex-end");
}
```

- [ ] **Step 2: Add the `rust_test` target and run it — verify it fails to build**

Add to `src/ui/BUCK` (after the existing `:logic` test):

```python
rust_test(
    name = "tokens",
    crate = "tokens",
    srcs = ["tests/tokens.rs"],
    crate_root = "tests/tokens.rs",
    edition = "2024",
    deps = [":ui-core"],
)
```

Run: `buck2 test //src/ui:tokens > /tmp/t.log 2>&1; grep -E "error|FAIL|Tests finished" /tmp/t.log`
Expected: build failure — `format_count`, `Status`, etc. are unresolved in `loom_ui_core`.

- [ ] **Step 3: Implement the enums + `format_count` in `loom_ui_core`**

Append to `src/ui/src/lib.rs`:

```rust
/// Visual weight of a [`Button`](loom_ui_components::Button).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ButtonVariant {
    Primary,
    Secondary,
    Ghost,
}

impl ButtonVariant {
    /// BEM-style modifier suffix, e.g. `loom-btn--primary`.
    #[must_use]
    pub fn modifier(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
            Self::Ghost => "ghost",
        }
    }
}

/// Semantic colour of a tag [`Badge`](loom_ui_components::Badge).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BadgeTone {
    Neutral,
    Info,
    Pii,
    Success,
    Warning,
    Danger,
}

impl BadgeTone {
    /// The `--loom-*` custom property this tone draws its accent colour from.
    #[must_use]
    pub fn css_var(self) -> &'static str {
        match self {
            Self::Neutral => "--loom-text-mut",
            Self::Info => "--loom-accent",
            Self::Pii | Self::Danger => "--loom-danger",
            Self::Success => "--loom-ok",
            Self::Warning => "--loom-warn",
        }
    }
}

/// Health / build state, rendered as a coloured dot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Ok,
    Warn,
    Error,
}

impl Status {
    /// The `--loom-*` custom property for this status colour.
    #[must_use]
    pub fn css_var(self) -> &'static str {
        match self {
            Self::Ok => "--loom-ok",
            Self::Warn => "--loom-warn",
            Self::Error => "--loom-danger",
        }
    }
}

/// Horizontal cell alignment in a [`DataTable`](loom_ui_components::DataTable).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Align {
    Start,
    End,
}

impl Align {
    /// The flexbox `justify-content` value for this alignment.
    #[must_use]
    pub fn css_value(self) -> &'static str {
        match self {
            Self::Start => "flex-start",
            Self::End => "flex-end",
        }
    }
}

/// Format a row count the way the catalog table shows it: `2_410_000` → `"2.41M"`,
/// `18_200` → `"18.2K"`, `880_000` → `"880K"`. Values below 1000 are rendered as-is.
/// Scaled values show up to 3 significant figures with trailing zeros trimmed.
#[must_use]
pub fn format_count(n: u64) -> String {
    let (scaled, suffix) = if n >= 1_000_000 {
        (n as f64 / 1_000_000.0, "M")
    } else if n >= 1_000 {
        (n as f64 / 1_000.0, "K")
    } else {
        return n.to_string();
    };
    // 3 significant figures: 2.41M, 18.2K, 880K, 9.7K, 142K.
    let precision = if scaled >= 100.0 {
        0
    } else if scaled >= 10.0 {
        1
    } else {
        2
    };
    let mut s = format!("{scaled:.precision$}");
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    format!("{s}{suffix}")
}
```

Note: `n as f64` trips `clippy::cast_precision_loss` (restriction). Add `#[expect(clippy::cast_precision_loss, reason = "display-only count formatting; exactness not required")]` on `format_count` if the build flags it.

- [ ] **Step 4: Run the test — verify it passes**

Run: `buck2 test //src/ui:tokens > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: `Tests finished: Pass 5. Fail 0.`

- [ ] **Step 5: Clippy-clean the lib**

Run: `buck2 build '//src/ui:ui-core[clippy.txt]' > /tmp/c.log 2>&1; cat "$(buck2 build --show-output //src/ui:ui-core 2>/dev/null | awk '{print $2}')" 2>/dev/null; grep -c . <(buck2 build '//src/ui:ui-core[clippy.txt]' 2>/dev/null) || true`
Simpler: `tools/clippy-all.sh 2>&1 | grep -iE "ui-core|error|clean|clippy" | tail`
Expected: `:ui-core` clippy output is empty (clean). Fix any lint (likely the cast note above).

- [ ] **Step 6: Commit**

```bash
git add src/ui/src/lib.rs src/ui/tests/tokens.rs src/ui/BUCK
git commit -m "feat(ui): token enums + format_count in loom_ui_core"
```

---

### Task 2: `stylist` dep + `loom_ui_components` scaffold + `GlobalStyles` + gallery skeleton

Wires the new library and the gallery bundle end-to-end with a single component (`GlobalStyles`) so every later task just adds primitives to a working pipeline.

**Files:**
- Modify: `src/ui/Cargo.toml` (add `stylist`)
- Regenerate: `Cargo.lock`, `third-party/BUCK` (via `./tools/buckify.sh`)
- Create: `src/ui/src/components/mod.rs` (crate root for `loom_ui_components`)
- Create: `src/ui/src/components/global.rs` (`GlobalStyles`)
- Create: `src/ui/src/gallery.rs` (`:gallery` binary root)
- Create: `src/ui/gallery.html` (gallery bundle entrypoint)
- Modify: `src/ui/BUCK` (add `:ui-components`, `:gallery`, `:gallery-bundle`, `:gallery-serve`)

**Interfaces:**
- Produces: `loom_ui_components::GlobalStyles` (a `#[function_component]` with no props that renders a `stylist::yew::Global`), and the crate root re-exporting every component (`pub use button::Button;` etc. added as tasks land).

- [ ] **Step 1: Add `stylist` to the manifest**

Edit `src/ui/Cargo.toml`, under `[dependencies]`:

```toml
# CSS-in-Rust for the component library. 0.13 is the yew-0.21-compatible line.
stylist = { version = "0.13", features = ["yew"] }
```

- [ ] **Step 2: Refresh lock + regenerate buck rules**

```bash
eval "$(./tools/env.sh)"
cargo generate-lockfile
./tools/buckify.sh
```

Run: `git diff --stat Cargo.lock third-party/BUCK`
Expected: `stylist` (+ its transitive deps) appear in `third-party/BUCK`. If unrelated native crates (`zstd-sys`, `ring`, `duckdb`) moved in `Cargo.lock`, that's the silent-downgrade footgun — diff against the merge-base and stop if so.

- [ ] **Step 3: Create the `GlobalStyles` component**

Create `src/ui/src/components/global.rs`:

```rust
use stylist::yew::Global;
use yew::prelude::*;

/// Injects the loom design tokens (`:root { --loom-* }`) and base body/font rules
/// once at the app root. Render this before any other component.
#[function_component(GlobalStyles)]
pub fn global_styles() -> Html {
    html! {
        <Global css={r#"
            :root {
                --loom-bg: #0b0e14;
                --loom-panel: #161b22;
                --loom-panel-2: #1c2230;
                --loom-border: #232a35;
                --loom-text: #e6edf3;
                --loom-text-mut: #8b949e;
                --loom-accent: #3b82f6;
                --loom-accent-fg: #ffffff;
                --loom-ok: #3fb950;
                --loom-warn: #d29922;
                --loom-danger: #f85149;
                --loom-radius: 6px;
                --loom-radius-sm: 4px;
            }
            * { box-sizing: border-box; }
            body {
                margin: 0;
                background: var(--loom-bg);
                color: var(--loom-text);
                font-family: "Inter", system-ui, -apple-system, sans-serif;
                font-size: 13px;
                line-height: 1.5;
            }
        "#} />
    }
}
```

- [ ] **Step 4: Create the crate root**

Create `src/ui/src/components/mod.rs`:

```rust
//! loom UI component library — `stylist`-styled yew primitives. The pure token
//! enums and `format_count` live in the sibling `loom_ui_core` lib (lint-clean);
//! this crate holds the `html!`/`stylist` render code, which is not lint-clean
//! under loom's strict gate — hence the crate-level allow.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! + stylist css! macro expansion is not lint-clean under loom's strict gate"
)]

mod global;

pub use global::GlobalStyles;
```

- [ ] **Step 5: Create the gallery binary + HTML**

Create `src/ui/src/gallery.rs`:

```rust
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

use loom_ui_components::GlobalStyles;
use yew::prelude::*;

#[function_component(Gallery)]
fn gallery() -> Html {
    html! {
        <>
            <GlobalStyles />
            <main style="padding: 24px; max-width: 1100px; margin: 0 auto;">
                <h1>{ "loom component gallery" }</h1>
            </main>
        </>
    }
}

fn main() {
    yew::Renderer::<Gallery>::new().render();
}
```

Create `src/ui/gallery.html`:

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>loom — component gallery</title>
  </head>
  <body>
    <script type="module">
      import init from "./gallery.js";
      init();
    </script>
  </body>
</html>
```

- [ ] **Step 6: Wire the buck targets**

Add to `src/ui/BUCK` (mirror `:app`/`:bundle`/`:serve`):

```python
rust_library(
    name = "ui-components",
    crate = "loom_ui_components",
    srcs = glob(["src/components/**/*.rs"]),
    crate_root = "src/components/mod.rs",
    edition = "2024",
    default_target_platform = "//platforms:wasm",
    deps = [
        "//third-party:stylist",
        "//third-party:yew",
        ":ui-core",
    ],
    visibility = ["PUBLIC"],
)

rust_binary(
    name = "gallery",
    crate = "gallery",
    srcs = ["src/gallery.rs"],
    crate_root = "src/gallery.rs",
    edition = "2024",
    default_target_platform = "//platforms:wasm",
    deps = [
        "//third-party:wasm-bindgen",
        "//third-party:web-sys",
        "//third-party:yew",
        ":ui-components",
        ":ui-core",
    ],
    visibility = ["PUBLIC"],
)

# Dev-only gallery bundle — mirrors :bundle but has no config.js (no backend).
genrule(
    name = "gallery-bundle",
    out = "dist",
    default_target_platform = "//platforms:wasm",
    srcs = ["gallery.html"],
    cmd = """
set -euo pipefail
got=`$(exe //tools:wasm-bindgen) --version`
want="wasm-bindgen {ver}"
if [ "$got" != "$want" ]; then
  echo "wasm-bindgen CLI/version mismatch: got '$got', want '$want'" >&2
  exit 1
fi
mkdir -p $OUT
$(exe //tools:wasm-bindgen) --target web --no-typescript --out-dir $OUT --out-name gallery $(location :gallery)
cp $SRCDIR/gallery.html $OUT/index.html
""".format(ver = WASM_BINDGEN_VERSION),
    visibility = ["PUBLIC"],
)

genrule(
    name = "gallery-serve",
    out = "serve.sh",
    default_target_platform = "//platforms:wasm",
    cmd = """
cat > $OUT <<'SCRIPT'
#!/usr/bin/env bash
cd "`dirname "$0"`"
exec python3 -m http.server --directory "$(location :gallery-bundle)" "$@"
SCRIPT
chmod +x $OUT
""",
    executable = True,
    visibility = ["PUBLIC"],
)
```

- [ ] **Step 7: Build the wasm targets + full test sweep**

```bash
buck2 build //src/ui:ui-components //src/ui:gallery //src/ui:gallery-bundle > /tmp/b.log 2>&1; tail -3 /tmp/b.log
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
```
Expected: BUILD SUCCEEDED; the full sweep still green (the `stylist` `[platform]` change didn't leak). If a non-wasm crate now fails to cross-compile, revisit `reindeer.toml` `[platform]` config per `src/ui/CLAUDE.md`.

- [ ] **Step 8: Eyeball the gallery**

```bash
buck2 build //src/ui:gallery-bundle
buck2 run //src/ui:gallery-serve -- --bind 127.0.0.1 8100
```
Open `http://127.0.0.1:8100` — expect a dark near-black page, light text, the heading "loom component gallery". Confirms `GlobalStyles` tokens + `stylist` injection work end-to-end. Ctrl-C to stop.

- [ ] **Step 9: Commit**

```bash
git add src/ui/Cargo.toml Cargo.lock third-party/BUCK src/ui/src/components src/ui/src/gallery.rs src/ui/gallery.html src/ui/BUCK
git commit -m "feat(ui): loom_ui_components library + GlobalStyles + gallery bundle"
```

---

### Task 3: Leaf primitives — `Button`, `Badge`, `StatusDot`

Three simple prop-driven components. No local state.

**Files:**
- Create: `src/ui/src/components/button.rs`, `badge.rs`, `status.rs`
- Modify: `src/ui/src/components/mod.rs` (declare + re-export)
- Modify: `src/ui/src/gallery.rs` (add sections)

**Interfaces:**
- Consumes: `loom_ui_core::{ButtonVariant, BadgeTone, Status}` (Task 1).
- Produces:
  - `Button` props: `variant: ButtonVariant` (`#[prop_or(ButtonVariant::Primary)]`), `disabled: bool` (`#[prop_or_default]`), `onclick: Callback<MouseEvent>` (`#[prop_or_default]`), `children: Children`.
  - `Badge` props: `label: AttrValue`, `tone: BadgeTone` (`#[prop_or(BadgeTone::Neutral)]`).
  - `StatusDot` props: `status: Status`.

- [ ] **Step 1: `Button`**

Create `src/ui/src/components/button.rs`:

```rust
use loom_ui_core::ButtonVariant;
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ButtonProps {
    #[prop_or(ButtonVariant::Primary)]
    pub variant: ButtonVariant,
    #[prop_or_default]
    pub disabled: bool,
    #[prop_or_default]
    pub onclick: Callback<MouseEvent>,
    #[prop_or_default]
    pub children: Children,
}

#[styled_component(Button)]
pub fn button(props: &ButtonProps) -> Html {
    let base = css!(
        r#"
        font: inherit; font-size: 13px; font-weight: 500;
        padding: 6px 12px; border-radius: var(--loom-radius);
        border: 1px solid transparent; cursor: pointer;
        &:disabled { opacity: 0.5; cursor: not-allowed; }
    "#
    );
    let variant = match props.variant {
        ButtonVariant::Primary => css!(
            "background: var(--loom-accent); color: var(--loom-accent-fg);"
        ),
        ButtonVariant::Secondary => css!(
            "background: var(--loom-panel-2); color: var(--loom-text); border-color: var(--loom-border);"
        ),
        ButtonVariant::Ghost => css!(
            "background: transparent; color: var(--loom-text-mut);"
        ),
    };
    html! {
        <button class={classes!(base, variant)} disabled={props.disabled} onclick={props.onclick.clone()}>
            { for props.children.iter() }
        </button>
    }
}
```

- [ ] **Step 2: `Badge`**

Create `src/ui/src/components/badge.rs`:

```rust
use loom_ui_core::BadgeTone;
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct BadgeProps {
    pub label: AttrValue,
    #[prop_or(BadgeTone::Neutral)]
    pub tone: BadgeTone,
}

#[styled_component(Badge)]
pub fn badge(props: &BadgeProps) -> Html {
    // The tone colour comes from a css var chosen in loom_ui_core; feed it through
    // an inline custom property so the scoped rule can reference it uniformly.
    let style = format!("--badge-c: var({});", props.tone.css_var());
    let cls = css!(
        r#"
        display: inline-flex; align-items: center;
        padding: 1px 6px; border-radius: var(--loom-radius-sm);
        font-size: 11px; line-height: 1.4;
        color: var(--badge-c);
        background: color-mix(in srgb, var(--badge-c) 15%, transparent);
    "#
    );
    html! { <span class={cls} style={style}>{ &props.label }</span> }
}
```

- [ ] **Step 3: `StatusDot`**

Create `src/ui/src/components/status.rs`:

```rust
use loom_ui_core::Status;
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct StatusDotProps {
    pub status: Status,
}

#[styled_component(StatusDot)]
pub fn status_dot(props: &StatusDotProps) -> Html {
    let style = format!("--dot-c: var({});", props.status.css_var());
    let cls = css!(
        r#"
        display: inline-block; width: 8px; height: 8px;
        border-radius: 50%; background: var(--dot-c);
    "#
    );
    html! { <span class={cls} style={style} /> }
}
```

- [ ] **Step 4: Re-export**

In `src/ui/src/components/mod.rs`, add after `mod global;`:

```rust
mod badge;
mod button;
mod status;
```

and after `pub use global::GlobalStyles;`:

```rust
pub use badge::Badge;
pub use button::Button;
pub use status::StatusDot;
```

- [ ] **Step 5: Gallery sections**

In `src/ui/src/gallery.rs`, update imports and body. Replace the import line with:

```rust
use loom_ui_components::{Badge, Button, GlobalStyles, StatusDot};
use loom_ui_core::{BadgeTone, ButtonVariant, Status};
```

Replace the `<main>` contents' heading with the heading plus:

```rust
<section>
    <h2>{ "Buttons" }</h2>
    <div style="display:flex; gap:8px; align-items:center;">
        <Button variant={ButtonVariant::Primary}>{ "Open in Workbook" }</Button>
        <Button variant={ButtonVariant::Secondary}>{ "Explore" }</Button>
        <Button variant={ButtonVariant::Ghost}>{ "Cancel" }</Button>
        <Button variant={ButtonVariant::Primary} disabled=true>{ "Disabled" }</Button>
    </div>
</section>
<section>
    <h2>{ "Badges" }</h2>
    <div style="display:flex; gap:8px;">
        <Badge label="pii" tone={BadgeTone::Pii} />
        <Badge label="finance" tone={BadgeTone::Info} />
        <Badge label="certified" tone={BadgeTone::Success} />
        <Badge label="draft" tone={BadgeTone::Neutral} />
    </div>
</section>
<section>
    <h2>{ "Status" }</h2>
    <div style="display:flex; gap:16px; align-items:center;">
        <span><StatusDot status={Status::Ok} />{ " healthy" }</span>
        <span><StatusDot status={Status::Warn} />{ " stale" }</span>
        <span><StatusDot status={Status::Error} />{ " failed" }</span>
    </div>
</section>
```

- [ ] **Step 6: Build gate + eyeball**

```bash
buck2 build //src/ui:gallery-bundle > /tmp/b.log 2>&1; tail -3 /tmp/b.log
buck2 run //src/ui:gallery-serve -- --bind 127.0.0.1 8100
```
Expected: BUILD SUCCEEDED. Gallery shows three button variants + disabled, four coloured tag badges, three status dots (green/amber/red). Then `tools/clippy-all.sh 2>&1 | grep -iE "ui-components|error" | tail` — clean.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/components src/ui/src/gallery.rs
git commit -m "feat(ui): Button, Badge, StatusDot primitives + gallery sections"
```

---

### Task 4: Interactive primitives — `Input`, `Tabs`

`Input` is a controlled component (value + `oninput` in, like the login form). `Tabs` is prop-driven with the active tab controlled by the caller; the gallery drives it with `use_state`.

**Files:**
- Create: `src/ui/src/components/input.rs`, `tabs.rs`
- Modify: `src/ui/src/components/mod.rs`, `src/ui/src/gallery.rs`

**Interfaces:**
- Produces:
  - `enum InputKind { Text, Password, Search }` (in `loom_ui_components::input`, re-exported) — maps to the `type=` attr; `Search` adds a leading magnifier + a `⌘K` hint.
  - `Input` props: `value: AttrValue`, `placeholder: AttrValue` (`#[prop_or_default]`), `input_type: InputKind` (`#[prop_or(InputKind::Text)]`), `oninput: Callback<InputEvent>` (`#[prop_or_default]`), `disabled: bool` (`#[prop_or_default]`). The component **forwards** the raw `InputEvent`; callers extract the value (as `main.rs` does today).
  - `struct TabItem { id: AttrValue, label: AttrValue }` (derives `Clone, PartialEq`).
  - `Tabs` props: `tabs: Vec<TabItem>`, `active: AttrValue`, `onselect: Callback<AttrValue>` (`#[prop_or_default]`).

- [ ] **Step 1: `Input`**

Create `src/ui/src/components/input.rs`:

```rust
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Text,
    Password,
    Search,
}

impl InputKind {
    fn html_type(self) -> &'static str {
        match self {
            Self::Password => "password",
            // Search renders as a text input with adornments (native `search` adds
            // a browser clear button we don't want).
            Self::Text | Self::Search => "text",
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct InputProps {
    pub value: AttrValue,
    #[prop_or_default]
    pub placeholder: AttrValue,
    #[prop_or(InputKind::Text)]
    pub input_type: InputKind,
    #[prop_or_default]
    pub oninput: Callback<InputEvent>,
    #[prop_or_default]
    pub disabled: bool,
}

#[styled_component(Input)]
pub fn input(props: &InputProps) -> Html {
    let wrap = css!(
        r#"
        display: inline-flex; align-items: center; gap: 6px;
        padding: 5px 8px; border-radius: var(--loom-radius-sm);
        background: var(--loom-panel); border: 1px solid var(--loom-border);
        input { all: unset; flex: 1; color: var(--loom-text); font: inherit; font-size: 13px; }
        input::placeholder { color: var(--loom-text-mut); }
        .hint { color: var(--loom-text-mut); font-size: 11px; }
    "#
    );
    let is_search = matches!(props.input_type, InputKind::Search);
    html! {
        <span class={wrap}>
            if is_search { <span class="hint">{ "⌕" }</span> }
            <input
                type={props.input_type.html_type()}
                value={props.value.clone()}
                placeholder={props.placeholder.clone()}
                disabled={props.disabled}
                oninput={props.oninput.clone()}
            />
            if is_search { <span class="hint">{ "⌘K" }</span> }
        </span>
    }
}
```

- [ ] **Step 2: `Tabs`**

Create `src/ui/src/components/tabs.rs`:

```rust
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Clone, PartialEq)]
pub struct TabItem {
    pub id: AttrValue,
    pub label: AttrValue,
}

#[derive(Properties, PartialEq)]
pub struct TabsProps {
    pub tabs: Vec<TabItem>,
    pub active: AttrValue,
    #[prop_or_default]
    pub onselect: Callback<AttrValue>,
}

#[styled_component(Tabs)]
pub fn tabs(props: &TabsProps) -> Html {
    let bar = css!(
        r#"
        display: flex; gap: 16px; border-bottom: 1px solid var(--loom-border);
        button {
            all: unset; cursor: pointer; padding: 6px 2px; font-size: 13px;
            color: var(--loom-text-mut); border-bottom: 2px solid transparent;
            margin-bottom: -1px;
        }
        button.active { color: var(--loom-text); border-bottom-color: var(--loom-accent); }
    "#
    );
    html! {
        <div class={bar}>
            { for props.tabs.iter().map(|t| {
                let active = t.id == props.active;
                let onselect = props.onselect.clone();
                let id = t.id.clone();
                let onclick = Callback::from(move |_| onselect.emit(id.clone()));
                html! {
                    <button class={classes!(active.then_some("active"))} {onclick}>
                        { &t.label }
                    </button>
                }
            }) }
        </div>
    }
}
```

- [ ] **Step 3: Re-export**

Add `mod input;` / `mod tabs;` and `pub use input::{Input, InputKind};` / `pub use tabs::{TabItem, Tabs};` to `mod.rs`.

- [ ] **Step 4: Gallery sections (with live tab state)**

In `src/ui/src/gallery.rs`, extend imports:

```rust
use loom_ui_components::{Badge, Button, GlobalStyles, Input, InputKind, StatusDot, TabItem, Tabs};
```

Add inside `Gallery` before the `html!` (to hold tab + input state):

```rust
let active_tab = use_state(|| AttrValue::from("preview"));
let tabs = vec![
    TabItem { id: "preview".into(), label: "Preview".into() },
    TabItem { id: "schema".into(), label: "Schema".into() },
    TabItem { id: "lineage".into(), label: "Lineage".into() },
    TabItem { id: "history".into(), label: "History".into() },
];
let onselect = {
    let active_tab = active_tab.clone();
    Callback::from(move |id: AttrValue| active_tab.set(id))
};
```

Add sections:

```rust
<section>
    <h2>{ "Inputs" }</h2>
    <div style="display:flex; gap:8px;">
        <Input value="" placeholder="username" />
        <Input value="" placeholder="password" input_type={InputKind::Password} />
        <Input value="" placeholder="Search datasets…" input_type={InputKind::Search} />
    </div>
</section>
<section>
    <h2>{ "Tabs" }</h2>
    <Tabs tabs={tabs} active={(*active_tab).clone()} onselect={onselect} />
    <p>{ format!("active: {}", *active_tab) }</p>
</section>
```

- [ ] **Step 5: Build gate + eyeball**

```bash
buck2 build //src/ui:gallery-bundle > /tmp/b.log 2>&1; tail -3 /tmp/b.log
buck2 run //src/ui:gallery-serve -- --bind 127.0.0.1 8100
```
Expected: three inputs (text, masked password, search with ⌕ + ⌘K); a 4-tab strip where clicking a tab moves the blue underline and updates the "active: …" line. `tools/clippy-all.sh` clean.

- [ ] **Step 6: Commit**

```bash
git add src/ui/src/components src/ui/src/gallery.rs
git commit -m "feat(ui): Input + Tabs primitives + gallery sections"
```

---

### Task 5: `Panel`, generic `DataTable<R>`, `TopNav`

The container, the generic table (with the `TableRow` trait), and the app-shell bar — the pieces the real screens are built from. The gallery's `DatasetRow` demo exercises `format_count` + `StatusDot` + `Badge` together.

**Files:**
- Create: `src/ui/src/components/panel.rs`, `table.rs`, `topnav.rs`
- Modify: `src/ui/src/components/mod.rs`, `src/ui/src/gallery.rs`

**Interfaces:**
- Produces:
  - `Panel` props: `title: Option<AttrValue>` (`#[prop_or_default]`), `children: Children`.
  - `trait TableRow { fn cells(&self) -> Vec<Html>; }`
  - `struct Column { pub label: AttrValue, pub align: Align }` (derives `Clone, PartialEq`).
  - `DataTable<R>` props (`DataTableProps<R: PartialEq>`): `columns: Vec<Column>`, `rows: Vec<R>`, `selected: Option<usize>` (`#[prop_or_default]`), `onrow: Callback<usize>` (`#[prop_or_default]`). Component bound: `R: PartialEq + Clone + TableRow + 'static`.
  - `struct NavItem { label: AttrValue, active: bool }` (`Clone, PartialEq`).
  - `TopNav` props: `items: Vec<NavItem>`, `on_select: Callback<AttrValue>` (`#[prop_or_default]`), `search: Html` (`#[prop_or_default]`), `avatar: AttrValue` (`#[prop_or_default]`).

- [ ] **Step 1: `Panel`**

Create `src/ui/src/components/panel.rs`:

```rust
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct PanelProps {
    #[prop_or_default]
    pub title: Option<AttrValue>,
    #[prop_or_default]
    pub children: Children,
}

#[styled_component(Panel)]
pub fn panel(props: &PanelProps) -> Html {
    let cls = css!(
        r#"
        background: var(--loom-panel); border: 1px solid var(--loom-border);
        border-radius: var(--loom-radius); overflow: hidden;
        .title {
            padding: 8px 12px; border-bottom: 1px solid var(--loom-border);
            font-size: 12px; color: var(--loom-text-mut); text-transform: uppercase;
            letter-spacing: 0.04em;
        }
        .body { padding: 12px; }
    "#
    );
    html! {
        <div class={cls}>
            if let Some(t) = &props.title { <div class="title">{ t }</div> }
            <div class="body">{ for props.children.iter() }</div>
        </div>
    }
}
```

- [ ] **Step 2: generic `DataTable<R>` + `TableRow`**

Create `src/ui/src/components/table.rs`:

```rust
use loom_ui_core::Align;
use stylist::yew::styled_component;
use yew::prelude::*;

/// A row that knows how to render itself into table cells. Callers implement this
/// for their domain struct so `DataTable` stays generic and type-safe.
pub trait TableRow {
    fn cells(&self) -> Vec<Html>;
}

#[derive(Clone, PartialEq)]
pub struct Column {
    pub label: AttrValue,
    pub align: Align,
}

#[derive(Properties, PartialEq)]
pub struct DataTableProps<R: PartialEq> {
    pub columns: Vec<Column>,
    pub rows: Vec<R>,
    #[prop_or_default]
    pub selected: Option<usize>,
    #[prop_or_default]
    pub onrow: Callback<usize>,
}

#[styled_component(DataTable)]
pub fn data_table<R>(props: &DataTableProps<R>) -> Html
where
    R: PartialEq + Clone + TableRow + 'static,
{
    let cls = css!(
        r#"
        width: 100%; border-collapse: collapse; font-size: 13px;
        th, td { padding: 6px 10px; border-bottom: 1px solid var(--loom-border); }
        th { color: var(--loom-text-mut); font-weight: 500; text-align: left; font-size: 12px; }
        tbody tr { cursor: pointer; }
        tbody tr:hover { background: var(--loom-panel-2); }
        tbody tr.selected { background: color-mix(in srgb, var(--loom-accent) 18%, transparent); }
        td.end { text-align: right; font-variant-numeric: tabular-nums; }
    "#
    );
    html! {
        <table class={cls}>
            <thead>
                <tr>
                    { for props.columns.iter().map(|c| {
                        let end = matches!(c.align, Align::End);
                        html! { <th class={classes!(end.then_some("end"))}>{ &c.label }</th> }
                    }) }
                </tr>
            </thead>
            <tbody>
                { for props.rows.iter().enumerate().map(|(i, row)| {
                    let selected = props.selected == Some(i);
                    let onrow = props.onrow.clone();
                    let onclick = Callback::from(move |_| onrow.emit(i));
                    let cells = row.cells();
                    html! {
                        <tr class={classes!(selected.then_some("selected"))} {onclick}>
                            { for props.columns.iter().zip(cells).map(|(c, cell)| {
                                let end = matches!(c.align, Align::End);
                                html! { <td class={classes!(end.then_some("end"))}>{ cell }</td> }
                            }) }
                        </tr>
                    }
                }) }
            </tbody>
        </table>
    }
}
```

Note (yew 0.21 generics): `#[styled_component(DataTable)]` over a generic `fn data_table<R>` is the target form. If the stylist macro rejects the generic, fall back to a plain `#[function_component(DataTable)]` + a `stylist::css!`-produced class (the `css!` macro works outside `styled_component`). Verify which compiles against the vendored yew/stylist and keep the one that builds. Instantiate as `<DataTable<DatasetRow> … />`.

- [ ] **Step 3: `TopNav`**

Create `src/ui/src/components/topnav.rs`:

```rust
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Clone, PartialEq)]
pub struct NavItem {
    pub label: AttrValue,
    pub active: bool,
}

#[derive(Properties, PartialEq)]
pub struct TopNavProps {
    pub items: Vec<NavItem>,
    #[prop_or_default]
    pub on_select: Callback<AttrValue>,
    #[prop_or_default]
    pub search: Html,
    #[prop_or_default]
    pub avatar: AttrValue,
}

#[styled_component(TopNav)]
pub fn top_nav(props: &TopNavProps) -> Html {
    let cls = css!(
        r#"
        display: flex; align-items: center; gap: 16px;
        padding: 8px 16px; background: var(--loom-panel);
        border-bottom: 1px solid var(--loom-border);
        .brand { font-weight: 600; color: var(--loom-text); }
        .nav { display: flex; gap: 12px; }
        .nav button {
            all: unset; cursor: pointer; font-size: 13px; color: var(--loom-text-mut);
        }
        .nav button.active { color: var(--loom-text); }
        .spacer { flex: 1; }
        .avatar {
            width: 24px; height: 24px; border-radius: 50%;
            background: var(--loom-accent); color: var(--loom-accent-fg);
            display: inline-flex; align-items: center; justify-content: center;
            font-size: 11px;
        }
    "#
    );
    html! {
        <nav class={cls}>
            <span class="brand">{ "loom" }</span>
            <div class="nav">
                { for props.items.iter().map(|it| {
                    let on_select = props.on_select.clone();
                    let label = it.label.clone();
                    let onclick = Callback::from(move |_| on_select.emit(label.clone()));
                    html! {
                        <button class={classes!(it.active.then_some("active"))} {onclick}>
                            { &it.label }
                        </button>
                    }
                }) }
            </div>
            <div class="spacer" />
            { props.search.clone() }
            if !props.avatar.is_empty() { <span class="avatar">{ &props.avatar }</span> }
        </nav>
    }
}
```

- [ ] **Step 4: Re-export**

Add `mod panel; mod table; mod topnav;` and:

```rust
pub use panel::Panel;
pub use table::{Column, DataTable, TableRow};
pub use topnav::{NavItem, TopNav};
```

- [ ] **Step 5: Gallery — `TopNav` shell + a real `DataTable`**

In `src/ui/src/gallery.rs`, extend imports:

```rust
use loom_ui_components::{
    Badge, Button, Column, DataTable, GlobalStyles, Input, InputKind, NavItem, Panel,
    StatusDot, TabItem, Tabs, TableRow, TopNav,
};
use loom_ui_core::{format_count, Align, BadgeTone, ButtonVariant, Status};
```

Define a demo row type (above `fn gallery`):

```rust
#[derive(Clone, PartialEq)]
struct DatasetRow {
    name: &'static str,
    rows: u64,
    owner: &'static str,
    health: Status,
}

impl TableRow for DatasetRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <><input type="checkbox" />{ " " }{ self.name }</> },
            html! { { format_count(self.rows) } },
            html! { { self.owner } },
            html! { <StatusDot status={self.health} /> },
        ]
    }
}
```

Wrap the whole gallery body in a `TopNav` + move each section into a `Panel`, and add the table section. Minimal shell + table additions:

```rust
let nav = vec![
    NavItem { label: "Catalog".into(), active: true },
    NavItem { label: "Pipelines".into(), active: false },
    NavItem { label: "Ontology".into(), active: false },
];
let columns = vec![
    Column { label: "NAME".into(), align: Align::Start },
    Column { label: "ROWS".into(), align: Align::End },
    Column { label: "OWNER".into(), align: Align::Start },
    Column { label: "HEALTH".into(), align: Align::Start },
];
let rows = vec![
    DatasetRow { name: "transactions_raw", rows: 2_410_000, owner: "A. Mehta", health: Status::Ok },
    DatasetRow { name: "fx_rates_daily", rows: 18_200, owner: "J. Liu", health: Status::Warn },
    DatasetRow { name: "chargebacks", rows: 9_700, owner: "R. Park", health: Status::Error },
];
```

Add to the render (search slot uses the existing `Input`):

```rust
<TopNav items={nav}
        search={html!{ <Input value="" placeholder="Search…" input_type={InputKind::Search} /> }}
        avatar="DK" />
// … existing sections, ideally each wrapped in <Panel title="…"> … </Panel> …
<section>
    <h2>{ "DataTable" }</h2>
    <Panel title="Finance / Transactions">
        <DataTable<DatasetRow> columns={columns} rows={rows} selected={Some(0)} />
    </Panel>
</section>
```

- [ ] **Step 6: Build gate + eyeball**

```bash
buck2 build //src/ui:gallery-bundle > /tmp/b.log 2>&1; tail -3 /tmp/b.log
buck2 run //src/ui:gallery-serve -- --bind 127.0.0.1 8100
```
Expected: a top nav bar (brand + Catalog/Pipelines/Ontology + right-aligned search + "DK" avatar); a titled panel wrapping a dense table with a highlighted first row, right-aligned tabular ROWS rendered via `format_count` (`2.41M`, `18.2K`, `9.7K`), and a coloured health dot per row. `tools/clippy-all.sh` clean.

- [ ] **Step 7: Commit**

```bash
git add src/ui/src/components src/ui/src/gallery.rs
git commit -m "feat(ui): Panel, generic DataTable<R>, TopNav + gallery shell"
```

---

### Task 6: Documentation + final verification

**Files:**
- Modify: `src/ui/CLAUDE.md`
- Modify: `docs/ROADMAP.md` (or run `loom-docs-update` at the end)

- [ ] **Step 1: Document the library + gallery in `src/ui/CLAUDE.md`**

Add a section describing: the `:ui-components` library (stylist primitives, crate-level pedantic/restriction allow), the token layer via `GlobalStyles`, the pure-enums-in-`loom_ui_core` split, and the dev-only `:gallery` / `:gallery-bundle` / `:gallery-serve` targets (`buck2 build //src/ui:gallery-bundle && buck2 run //src/ui:gallery-serve`). Note explicitly that component rendering is gallery-verified (no DOM in buck2 `rust_test`), tracked by `fut-ui-component-test-fixture`.

- [ ] **Step 2: Register the completed slice**

Run the `loom-docs-update` skill (preferred) to add `road-ui-component-library` (status `done`, `spec:2026-07-01-ui-component-library-design`) and confirm `fut-ui-component-test-fixture` / `fut-ui-browser-test-fixture` are still open. If adding the roadmap line by hand, mirror the grammar of an existing item and pick an area from the controlled vocab (`devx` is the closest existing bucket; extending the vocab with `ui` is a separate register change).

Run: `bash tools/docs.sh validate`
Expected: `OK`.

- [ ] **Step 3: Full-tree green + clippy + fmt**

```bash
buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t.log
tools/clippy-all.sh 2>&1 | tail
buck2 run //tools:rustfmt -- $(git ls-files 'src/ui/*.rs')
```
Expected: full sweep green, clippy clean, rustfmt no-op (or apply + amend).

- [ ] **Step 4: Commit**

```bash
git add src/ui/CLAUDE.md docs/ROADMAP.md docs/FUTURE.md
git commit -m "docs(ui): document component library + gallery; register slice"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 = tokens/enums/`format_count`; Task 2 = stylist + library + `GlobalStyles` + gallery + bundle; Tasks 3–5 = the eight primitives incl. generic `DataTable<R>`; Task 6 = docs/register + honest testing note. All spec sections map to a task.
- **Type consistency:** enum names (`ButtonVariant`/`BadgeTone`/`Status`/`Align`) and their methods (`modifier`/`css_var`/`css_value`) are defined in Task 1 and consumed unchanged in Tasks 3/5. `Column`, `TableRow`, `DataTableProps<R>` names match between the `table.rs` definition and the gallery instantiation.
- **Version caveats to verify at build time (not guesses to trust):** (1) `stylist` `0.13` yew-0.21 compat + the `Global`/`styled_component`/`css!` import paths; (2) the generic-component form for `DataTable<R>` under yew 0.21 (`#[styled_component(DataTable)]` vs `#[function_component(DataTable)]` + `css!`); (3) `color-mix()` browser support (fine in current Chromium/Firefox — the gallery is a dev tool). Each has a stated fallback.
- **Honest limits:** only Task 1 has real `rust_test` coverage; Tasks 2–5 rely on the wasm compile gate + gallery eyeballing, as the spec and `fut-ui-component-test-fixture` record.
