use loom_ui_core::{Surface, clamp_drawer_width};
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct ShellProps {
    pub active: Surface,
    pub on_switch: Callback<Surface>,
    #[prop_or_default]
    pub search: Html,
    #[prop_or_default]
    pub avatar: AttrValue,
    #[prop_or_default]
    pub list: Html,
    #[prop_or_default]
    pub drawer: Html,
}

#[styled_component(Shell)]
pub fn shell(props: &ShellProps) -> Html {
    const DRAWER_MIN: u32 = 360;
    const DRAWER_MAX: u32 = 900;
    const DRAWER_DEFAULT: u32 = 428;

    // Persisted drawer width (localStorage), clamped on load.
    let width = use_state(|| {
        web_sys::window()
            .and_then(|w| w.local_storage().ok().flatten())
            .and_then(|s| s.get_item("loom_drawer_width").ok().flatten())
            .and_then(|v| v.parse::<i32>().ok())
            .map_or(DRAWER_DEFAULT, |px| {
                clamp_drawer_width(px, DRAWER_MIN, DRAWER_MAX)
            })
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
                let vw = win
                    .inner_width()
                    .ok()
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
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
                if let Some(store) =
                    web_sys::window().and_then(|w| w.local_storage().ok().flatten())
                {
                    let _ = store.set_item("loom_drawer_width", &width.to_string());
                }
            }
        })
    };

    // NOTE: stylist scopes these selectors as descendant rules (`.<hash> .foo`), not
    // direct-child, so a generic class name here leaks into any nested styled component
    // that reuses it. The structural regions are therefore `shell-*`-prefixed to avoid
    // colliding with e.g. `Panel`'s `.body`/`.title` rendered inside the drawer slot
    // (an unprefixed `.body { display:flex }` turned the drawer's Panel body into a row).
    let cls = css!(
        r#"
        min-height: 100vh; background: var(--loom-bg); color: var(--loom-text);
        .shell-bar {
            display: flex; align-items: center; gap: 20px; height: 48px; padding: 0 16px;
            background: var(--loom-panel); border-bottom: 1px solid var(--loom-border);
        }
        .shell-brand { display: flex; align-items: center; gap: 8px; font-weight: 700; font-size: 15px; }
        .shell-logo { width: 18px; height: 18px; border-radius: 5px;
                background: linear-gradient(135deg, #3b82f6, #1d4ed8); }
        .shell-nav { display: flex; gap: 20px; }
        .shell-nav button {
            all: unset; cursor: pointer; font-size: 13px; font-weight: 500; color: var(--loom-text-mut);
            padding-bottom: 2px; border-bottom: 2px solid transparent;
        }
        .shell-nav button.active { color: var(--loom-text); border-bottom-color: var(--loom-accent); }
        .shell-spacer { flex: 1; }
        .shell-avatar { width: 26px; height: 26px; border-radius: 50%;
                  background: var(--loom-accent); color: #fff;
                  display: inline-flex; align-items: center; justify-content: center; font-size: 11px; }
        .shell-body { display: flex; align-items: stretch; }
        .shell-list { flex: 1; min-width: 0; padding: 18px 22px; }
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
    "#
    );
    let accent_style = format!("--loom-accent: {}", props.active.accent());
    html! {
        <div class={cls} style={accent_style}>
            <nav class="shell-bar">
                <span class="shell-brand"><span class="shell-logo"></span>{ "loom" }</span>
                <div class="shell-nav">
                    { for Surface::all().into_iter().map(|s| {
                        let on_switch = props.on_switch.clone();
                        let onclick = Callback::from(move |_| on_switch.emit(s));
                        let active = s == props.active;
                        html! {
                            <button class={classes!(active.then_some("active"))} {onclick}>
                                { s.label() }
                            </button>
                        }
                    }) }
                </div>
                <div class="shell-spacer" />
                { props.search.clone() }
                if !props.avatar.is_empty() { <span class="shell-avatar">{ &props.avatar }</span> }
            </nav>
            <div class={classes!("shell-body")}
                 onmousemove={on_mouse_move} onmouseup={on_mouse_up.clone()}
                 onmouseleave={on_mouse_up.clone()}>
                <div class="shell-list">{ props.list.clone() }</div>
                if !is_empty(&props.drawer) {
                    <div class="shell-drawer" style={format!("width: {}px", *width)}>
                        <div class="shell-drawer-handle" onmousedown={on_handle_down}></div>
                        { props.drawer.clone() }
                    </div>
                }
            </div>
        </div>
    }
}

// Yew's `Html::default()` is `VNode::default()` (an empty list); treat that as "no drawer".
fn is_empty(h: &Html) -> bool {
    h == &Html::default()
}
