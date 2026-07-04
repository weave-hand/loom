use loom_ui_core::Surface;
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct StubViewProps {
    pub surface: Surface,
}

#[styled_component(StubView)]
pub fn stub_view(props: &StubViewProps) -> Html {
    let cls = css!(
        r#"
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        height: 60vh; gap: 8px; color: var(--loom-text-mut); text-align: center;
        .title { color: var(--loom-text); font-size: 15px; font-weight: 600; }
        .accent { color: var(--loom-accent); }
    "#
    );
    html! {
        <div class={cls}>
            <div class="title"><span class="accent">{ props.surface.label() }</span></div>
            <div>{ "This surface isn't available on this instance yet." }</div>
        </div>
    }
}
