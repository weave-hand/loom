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
