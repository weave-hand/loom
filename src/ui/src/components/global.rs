use loom_ui_core::{LOOM_BG, LOOM_TEXT};
use stylist::yew::Global;
use yew::prelude::*;

/// Injects the loom design tokens (`:root { --loom-* }`) and base body/font rules
/// once at the app root. Render this before any other component.
///
/// `--loom-bg`/`--loom-text` are interpolated from `loom_ui_core::{LOOM_BG, LOOM_TEXT}`
/// so the two tokens the Monaco editor theme also needs (it can't read CSS vars) have
/// a single source of truth. The block is assembled by concatenation to keep the CSS
/// braces literal (a `format!` would require escaping every `{`/`}`).
#[function_component(GlobalStyles)]
pub fn global_styles() -> Html {
    let css: String = [
        r#"
            :root {
                --loom-bg: "#,
        LOOM_BG,
        r#";
                --loom-panel: #161b22;
                --loom-panel-2: #1c2230;
                --loom-border: #232a35;
                --loom-text: "#,
        LOOM_TEXT,
        r#";
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
        "#,
    ]
    .concat();
    html! { <Global css={css} /> }
}
