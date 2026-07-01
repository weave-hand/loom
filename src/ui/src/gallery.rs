#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

use loom_ui_components::{Badge, Button, GlobalStyles, StatusDot};
use loom_ui_core::{BadgeTone, ButtonVariant, Status};
use yew::prelude::*;

#[function_component(Gallery)]
fn gallery() -> Html {
    html! {
        <>
            <GlobalStyles />
            <main style="padding: 24px; max-width: 1100px; margin: 0 auto;">
                <h1>{ "loom component gallery" }</h1>
                <section>
                    <h2>{ "Buttons" }</h2>
                    <div style="display:flex; gap:8px; align-items:center;">
                        <Button variant={ButtonVariant::Primary}>{ "Open in Workbook" }</Button>
                        <Button variant={ButtonVariant::Secondary}>{ "Explore" }</Button>
                        <Button variant={ButtonVariant::Ghost}>{ "Cancel" }</Button>
                        <Button variant={ButtonVariant::Primary} disabled=true>{ "Disabled" }</Button>
                    </div>
                </section>
                <section>
                    <h2>{ "Badges" }</h2>
                    <div style="display:flex; gap:8px;">
                        <Badge label="pii" tone={BadgeTone::Pii} />
                        <Badge label="finance" tone={BadgeTone::Info} />
                        <Badge label="certified" tone={BadgeTone::Success} />
                        <Badge label="draft" tone={BadgeTone::Neutral} />
                    </div>
                </section>
                <section>
                    <h2>{ "Status" }</h2>
                    <div style="display:flex; gap:16px; align-items:center;">
                        <span><StatusDot status={Status::Ok} />{ " healthy" }</span>
                        <span><StatusDot status={Status::Warn} />{ " stale" }</span>
                        <span><StatusDot status={Status::Error} />{ " failed" }</span>
                    </div>
                </section>
            </main>
        </>
    }
}

fn main() {
    yew::Renderer::<Gallery>::new().render();
}
