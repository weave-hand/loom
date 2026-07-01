#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

use loom_ui_components::GlobalStyles;
use yew::prelude::*;

#[function_component(Gallery)]
fn gallery() -> Html {
    html! {
        <>
            <GlobalStyles />
            <main style="padding: 24px; max-width: 1100px; margin: 0 auto;">
                <h1>{ "loom component gallery" }</h1>
            </main>
        </>
    }
}

fn main() {
    yew::Renderer::<Gallery>::new().render();
}
