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
