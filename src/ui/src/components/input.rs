use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    Text,
    Password,
    Search,
}

impl InputKind {
    fn html_type(self) -> &'static str {
        match self {
            Self::Password => "password",
            // Search renders as a text input with adornments (native `search` adds
            // a browser clear button we don't want).
            Self::Text | Self::Search => "text",
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct InputProps {
    pub value: AttrValue,
    #[prop_or_default]
    pub placeholder: AttrValue,
    #[prop_or(InputKind::Text)]
    pub input_type: InputKind,
    #[prop_or_default]
    pub oninput: Callback<InputEvent>,
    #[prop_or_default]
    pub disabled: bool,
}

#[styled_component(Input)]
pub fn input(props: &InputProps) -> Html {
    let wrap = css!(
        r#"
        display: inline-flex; align-items: center; gap: 6px;
        padding: 5px 8px; border-radius: var(--loom-radius-sm);
        background: var(--loom-panel); border: 1px solid var(--loom-border);
        input { all: unset; flex: 1; color: var(--loom-text); font: inherit; font-size: 13px; }
        input::placeholder { color: var(--loom-text-mut); }
        .hint { color: var(--loom-text-mut); font-size: 11px; }
    "#
    );
    let is_search = matches!(props.input_type, InputKind::Search);
    html! {
        <span class={wrap}>
            if is_search { <span class="hint">{ "⌕" }</span> }
            <input
                type={props.input_type.html_type()}
                value={props.value.clone()}
                placeholder={props.placeholder.clone()}
                disabled={props.disabled}
                oninput={props.oninput.clone()}
            />
            if is_search { <span class="hint">{ "⌘K" }</span> }
        </span>
    }
}
