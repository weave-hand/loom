use loom_ui_core::Surface;
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
    let cls = css!(
        r#"
        min-height: 100vh; background: var(--loom-bg); color: var(--loom-text);
        .bar {
            display: flex; align-items: center; gap: 20px; height: 48px; padding: 0 16px;
            background: var(--loom-panel); border-bottom: 1px solid var(--loom-border);
        }
        .brand { display: flex; align-items: center; gap: 8px; font-weight: 700; font-size: 15px; }
        .logo { width: 18px; height: 18px; border-radius: 5px;
                background: linear-gradient(135deg, #3b82f6, #1d4ed8); }
        .nav { display: flex; gap: 20px; }
        .nav button {
            all: unset; cursor: pointer; font-size: 13px; font-weight: 500; color: var(--loom-text-mut);
            padding-bottom: 2px; border-bottom: 2px solid transparent;
        }
        .nav button.active { color: var(--loom-text); border-bottom-color: var(--loom-accent); }
        .spacer { flex: 1; }
        .avatar { width: 26px; height: 26px; border-radius: 50%;
                  background: var(--loom-accent); color: #fff;
                  display: inline-flex; align-items: center; justify-content: center; font-size: 11px; }
        .body { display: flex; align-items: stretch; }
        .list { flex: 1; min-width: 0; padding: 18px 22px; }
        .drawer { width: 428px; flex: none; background: var(--loom-panel);
                  border-left: 1px solid var(--loom-border); }
    "#
    );
    let accent_style = format!("--loom-accent: {}", props.active.accent());
    html! {
        <div class={cls} style={accent_style}>
            <div class="bar">
                <span class="brand"><span class="logo"></span>{ "loom" }</span>
                <div class="nav">
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
                <div class="spacer" />
                { props.search.clone() }
                if !props.avatar.is_empty() { <span class="avatar">{ &props.avatar }</span> }
            </div>
            <div class="body">
                <div class="list">{ props.list.clone() }</div>
                if !is_empty(&props.drawer) {
                    <div class="drawer">{ props.drawer.clone() }</div>
                }
            </div>
        </div>
    }
}

// Yew's `Html::default()` is `VNode::default()` (an empty list); treat that as "no drawer".
fn is_empty(h: &Html) -> bool {
    h == &Html::default()
}
