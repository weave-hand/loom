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
