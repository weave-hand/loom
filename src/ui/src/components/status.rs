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
