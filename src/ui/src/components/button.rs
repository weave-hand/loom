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
        ButtonVariant::Primary => {
            css!("background: var(--loom-accent); color: var(--loom-accent-fg);")
        }
        ButtonVariant::Secondary => css!(
            "background: var(--loom-panel-2); color: var(--loom-text); border-color: var(--loom-border);"
        ),
        ButtonVariant::Ghost => css!("background: transparent; color: var(--loom-text-mut);"),
    };
    html! {
        <button class={classes!(base, variant)} disabled={props.disabled} onclick={props.onclick.clone()}>
            { for props.children.iter() }
        </button>
    }
}
