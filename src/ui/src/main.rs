// Strict clippy (pedantic + restriction) runs on this crate; yew's html! macro
// expansion is not lint-clean under that gate, so allow the two groups crate-wide
// here rather than weakening the global CLIPPY_ALLOWS. `allow` (not `expect`)
// because which group members fire depends on the macro expansion — an `expect`
// would trip `unfulfilled_lint_expectations` for whichever group stays clean.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

use yew::prelude::*;

#[function_component(App)]
fn app() -> Html {
    let count = use_state(|| 0_i32);
    let onclick = {
        let count = count.clone();
        Callback::from(move |_| count.set(*count + 1))
    };
    html! {
        <main>
            <h1>{ "loom UI experiment" }</h1>
            <button {onclick}>{ format!("clicked {} times", *count) }</button>
        </main>
    }
}

fn main() {
    yew::Renderer::<App>::new().render();
}
