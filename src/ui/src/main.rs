// Strict clippy (pedantic + restriction) runs on this crate; yew's html! macro is
// not lint-clean under that gate, so allow the two groups crate-wide here. The pure
// logic lives in the loom_ui_core lib, which stays lint-clean.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

mod net;
mod session;

use loom_ui_core::AuthError;
use yew::prelude::*;

#[function_component(App)]
fn app() -> Html {
    let token = use_state(session::load);
    if token.is_some() {
        let on_logout = {
            let token = token.clone();
            Callback::from(move |_| {
                let token = token.clone();
                if let Some(t) = (*token).clone() {
                    wasm_bindgen_futures::spawn_local(async move {
                        net::logout(&net::api_base(), &t).await;
                    });
                }
                session::clear();
                token.set(None);
            })
        };
        return html! {
            <main>
                <h1>{ "loom" }</h1>
                <p>{ "You are logged in." }</p>
                <button onclick={on_logout}>{ "Log out" }</button>
            </main>
        };
    }
    html! { <Login on_login={Callback::from({ let token = token.clone(); move |t: String| { session::store(&t); token.set(Some(t)); } })} /> }
}

#[derive(Properties, PartialEq)]
struct LoginProps {
    on_login: Callback<String>,
}

#[function_component(Login)]
fn login(props: &LoginProps) -> Html {
    let username = use_state(String::new);
    let password = use_state(String::new);
    let error = use_state(|| Option::<String>::None);

    let oninput_user = {
        let username = username.clone();
        Callback::from(move |e: InputEvent| username.set(input_value(&e)))
    };
    let oninput_pass = {
        let password = password.clone();
        Callback::from(move |e: InputEvent| password.set(input_value(&e)))
    };

    let onsubmit = {
        let (username, password, error, on_login) = (
            username.clone(),
            password.clone(),
            error.clone(),
            props.on_login.clone(),
        );
        Callback::from(move |e: SubmitEvent| {
            e.prevent_default();
            let (u, p) = ((*username).clone(), (*password).clone());
            let (error, on_login) = (error.clone(), on_login.clone());
            wasm_bindgen_futures::spawn_local(async move {
                match net::login(&net::api_base(), &u, &p).await {
                    Ok(token) => {
                        error.set(None);
                        on_login.emit(token);
                    }
                    Err(err) => error.set(Some(AuthError::to_string(&err))),
                }
            });
        })
    };

    html! {
        <main>
            <h1>{ "loom — sign in" }</h1>
            <form onsubmit={onsubmit}>
                <input type="text" placeholder="username" value={(*username).clone()} oninput={oninput_user} />
                <input type="password" placeholder="password" value={(*password).clone()} oninput={oninput_pass} />
                <button type="submit">{ "Sign in" }</button>
            </form>
            { for error.as_ref().map(|e| html! { <p class="error">{ e }</p> }) }
        </main>
    }
}

fn input_value(e: &InputEvent) -> String {
    use wasm_bindgen::JsCast;
    e.target()
        .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
        .map(|el| el.value())
        .unwrap_or_default()
}

fn main() {
    yew::Renderer::<App>::new().render();
}
