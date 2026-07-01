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
