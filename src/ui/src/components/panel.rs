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
