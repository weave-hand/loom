// Strict clippy (pedantic + restriction) runs on this crate; yew's html! macro is
// not lint-clean under that gate, so allow the two groups crate-wide here. The pure
// logic lives in the loom_ui_core lib, which stays lint-clean.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

mod explorer;
mod net;
mod session;

use explorer::Explorer;
use loom_ui_components::{Badge, GlobalStyles};
use loom_ui_core::{AuthError, BadgeTone};
use stylist::yew::styled_component;
use yew::prelude::*;

#[function_component(App)]
fn app() -> Html {
    let token = use_state(session::load);
    if token.is_some() {
        let on_logout: Callback<()> = {
            let token = token.clone();
            Callback::from(move |()| {
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
            <Explorer token={(*token).clone().unwrap_or_default()} on_logout={on_logout.clone()} />
        };
    }
    html! { <Login on_login={Callback::from({ let token = token.clone(); move |t: String| { session::store(&t); token.set(Some(t)); } })} /> }
}

#[derive(Properties, PartialEq)]
struct LoginProps {
    on_login: Callback<String>,
}

#[styled_component(Login)]
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

    // Footer line: same-origin bundles report "Self-hosted"; a detached config.js
    // that points the UI at a remote query-api surfaces that base for clarity.
    let host_line = {
        let base = net::api_base();
        if base.is_empty() {
            "Self-hosted".to_string()
        } else {
            format!("Self-hosted · {base}")
        }
    };

    let styles = css!(
        r#"
        min-height: 100vh; display: flex; align-items: center; justify-content: center;
        padding: 24px; background: var(--loom-bg);

        .card {
            display: grid; grid-template-columns: 1fr 1fr;
            width: 100%; max-width: 1120px; min-height: 620px;
            background: var(--loom-bg);
            border: 1px solid var(--loom-border); border-radius: 12px; overflow: hidden;
        }

        .left {
            padding: 44px; display: flex; flex-direction: column; justify-content: space-between;
            background: linear-gradient(160deg, var(--loom-panel) 0%, var(--loom-bg) 62%);
            border-right: 1px solid var(--loom-border);
        }
        .brand { display: flex; align-items: center; gap: 10px; }
        .logo {
            width: 26px; height: 26px; border-radius: 7px;
            background: linear-gradient(140deg, var(--loom-accent), #7aa7ff);
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.08);
        }
        .wordmark { font-size: 18px; font-weight: 600; letter-spacing: -0.01em; }
        .headline {
            font-size: 30px; line-height: 1.15; font-weight: 600;
            letter-spacing: -0.02em; margin: 0 0 14px; max-width: 15ch;
        }
        .sub { color: var(--loom-text-mut); font-size: 14px; max-width: 36ch; margin: 0 0 24px; }
        .features { list-style: none; padding: 0; margin: 0; display: flex; flex-direction: column; gap: 13px; }
        .feature { display: flex; align-items: center; gap: 10px; font-size: 13px; }
        .check {
            width: 18px; height: 18px; border-radius: 5px; flex: none;
            display: inline-flex; align-items: center; justify-content: center;
            font-size: 11px; color: var(--loom-accent);
            background: color-mix(in srgb, var(--loom-accent) 18%, transparent);
        }
        .meta { display: flex; gap: 18px; color: var(--loom-text-mut); font-size: 12px; }

        .right { padding: 44px; display: flex; align-items: center; justify-content: center; }
        .form-wrap { width: 100%; max-width: 340px; }
        .title { font-size: 22px; font-weight: 600; margin: 0 0 6px; }
        .welcome { color: var(--loom-text-mut); font-size: 13px; margin: 0 0 24px; }
        .field { margin-bottom: 16px; }
        .label-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
        .label-row label { font-size: 12px; font-weight: 500; }
        .forgot { font-size: 12px; color: var(--loom-text-mut); cursor: not-allowed; user-select: none; }
        .tf {
            all: unset; box-sizing: border-box; width: 100%; padding: 9px 11px;
            border-radius: var(--loom-radius); background: var(--loom-panel);
            border: 1px solid var(--loom-border); color: var(--loom-text);
            font: inherit; font-size: 13px;
        }
        .tf::placeholder { color: var(--loom-text-mut); }
        .tf { transition: border-color 120ms ease, box-shadow 120ms ease; }
        .tf:hover { border-color: var(--loom-text-mut); }
        .tf:focus {
            border-color: var(--loom-accent);
            box-shadow: 0 0 0 3px color-mix(in srgb, var(--loom-accent) 25%, transparent);
        }
        .signin {
            all: unset; box-sizing: border-box; width: 100%; margin-top: 6px; padding: 10px;
            text-align: center; border-radius: var(--loom-radius);
            background: var(--loom-accent); color: var(--loom-accent-fg);
            font: inherit; font-size: 13px; font-weight: 600; cursor: pointer;
            transition: background 120ms ease, box-shadow 120ms ease, transform 80ms ease;
        }
        .signin:hover {
            background: color-mix(in srgb, var(--loom-accent) 88%, white);
            box-shadow: 0 2px 12px color-mix(in srgb, var(--loom-accent) 35%, transparent);
        }
        .signin:focus-visible {
            box-shadow: 0 0 0 3px color-mix(in srgb, var(--loom-accent) 45%, transparent);
        }
        .signin:active { background: color-mix(in srgb, var(--loom-accent) 84%, black); transform: translateY(1px); }
        .divider { display: flex; align-items: center; gap: 12px; margin: 22px 0; color: var(--loom-text-mut); font-size: 12px; }
        .divider::before, .divider::after { content: ""; flex: 1; height: 1px; background: var(--loom-border); }
        .oauth { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
        .oauth button {
            all: unset; box-sizing: border-box; display: flex; align-items: center; justify-content: center; gap: 8px;
            padding: 9px; border-radius: var(--loom-radius); background: var(--loom-panel-2);
            border: 1px solid var(--loom-border); color: var(--loom-text); font: inherit; font-size: 13px;
            cursor: not-allowed; opacity: 0.55;
            transition: background 120ms ease, border-color 120ms ease;
        }
        .oauth button:not(:disabled) { cursor: pointer; opacity: 1; }
        .oauth button:not(:disabled):hover { background: var(--loom-panel); border-color: var(--loom-text-mut); }
        .oauth button:focus-visible { box-shadow: 0 0 0 3px color-mix(in srgb, var(--loom-accent) 40%, transparent); }
        .oauth svg { width: 15px; height: 15px; }
        .oauth .g { font-weight: 700; font-size: 14px; }
        .error { color: var(--loom-danger); font-size: 12px; margin: 14px 0 0; }
        .foot { text-align: center; color: var(--loom-text-mut); font-size: 12px; margin-top: 26px; }
        .foot b { color: var(--loom-text); font-weight: 500; }

        @media (max-width: 860px) {
            .card { grid-template-columns: 1fr; min-height: 0; }
            .left { display: none; }
        }
    "#
    );

    html! {
        <>
            <GlobalStyles />
            <div class={styles}>
                <div class="card">
                    <section class="left">
                        <div class="brand">
                            <span class="logo"></span>
                            <span class="wordmark">{ "loom" }</span>
                            <Badge label="open source" tone={BadgeTone::Info} />
                        </div>
                        <div>
                            <h2 class="headline">{ "The open data platform you run yourself." }</h2>
                            <p class="sub">{ "Catalog, transform, model and ship data — one self-hosted workspace for your whole team." }</p>
                            <ul class="features">
                                <li class="feature"><span class="check">{ "✓" }</span>{ "Unified catalog & column-level lineage" }</li>
                                <li class="feature"><span class="check">{ "✓" }</span>{ "SQL + Python workbooks" }</li>
                                <li class="feature"><span class="check">{ "✓" }</span>{ "Ontology objects & live dashboards" }</li>
                            </ul>
                        </div>
                        <div class="meta">
                            <span>{ "★ 18.2k" }</span>
                            <span>{ "Apache-2.0" }</span>
                            <span>{ "v1.4.2" }</span>
                        </div>
                    </section>
                    <section class="right">
                        <div class="form-wrap">
                            <h1 class="title">{ "Sign in" }</h1>
                            <p class="welcome">{ "Welcome back. Sign in to your workspace." }</p>
                            <form onsubmit={onsubmit}>
                                <div class="field">
                                    <div class="label-row"><label for="login-username">{ "Username" }</label></div>
                                    <input id="login-username" class="tf" type="text" placeholder="admin"
                                        value={(*username).clone()} oninput={oninput_user} />
                                </div>
                                <div class="field">
                                    <div class="label-row">
                                        <label for="login-password">{ "Password" }</label>
                                        <span class="forgot" title="Password reset is not configured on this instance">{ "Forgot?" }</span>
                                    </div>
                                    <input id="login-password" class="tf" type="password" placeholder="••••••••••"
                                        value={(*password).clone()} oninput={oninput_pass} />
                                </div>
                                <button class="signin" type="submit">{ "Sign in" }</button>
                            </form>
                            { for error.as_ref().map(|e| html! { <p class="error">{ e }</p> }) }
                            <div class="divider">{ "or continue with" }</div>
                            <div class="oauth">
                                <button type="button" disabled=true title="Not configured on this instance">
                                    <svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
                                        <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.01 8.01 0 0016 8c0-4.42-3.58-8-8-8z"/>
                                    </svg>
                                    { "GitHub" }
                                </button>
                                <button type="button" disabled=true title="Not configured on this instance">
                                    <span class="g">{ "G" }</span>{ "Google" }
                                </button>
                            </div>
                            <p class="foot">{ host_line }</p>
                        </div>
                    </section>
                </div>
            </div>
        </>
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
