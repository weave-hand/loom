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
mod surfaces;

use loom_ui_components::{Badge, Button, GlobalStyles, Shell, StubView};
use loom_ui_core::{
    AuthError, BadgeTone, ButtonVariant, DatasetDetail, DatasetRow, PreviewData, Surface,
    TypeDetail,
};
use net::FetchError;
use serde_json::{Map, Value};
use stylist::yew::styled_component;
use surfaces::{CatalogDrawer, CatalogList, LoadStatus, OntologyDrawer, OntologyList};
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
            <Workspace token={(*token).clone().unwrap_or_default()} on_logout={on_logout.clone()} />
        };
    }
    html! { <Login on_login={Callback::from({ let token = token.clone(); move |t: String| { session::store(&t); token.set(Some(t)); } })} /> }
}

#[derive(Properties, PartialEq)]
struct WorkspaceProps {
    token: AttrValue,
    on_logout: Callback<()>,
}

#[function_component(Workspace)]
fn workspace(props: &WorkspaceProps) -> Html {
    let surface = use_state(|| Surface::Catalog);

    // Ontology surface state (lifted verbatim from the old explorer.rs), plus the
    // drawer's type-detail + active-tab.
    let types = use_state(Vec::<String>::new);
    let selected_type = use_state(|| Option::<String>::None);
    let objs = use_state(Vec::<Map<String, Value>>::new);
    let columns = use_state(Vec::<String>::new);
    let next = use_state(|| Option::<String>::None);
    let selected_row = use_state(|| Option::<usize>::None);
    let status = use_state(|| LoadStatus::Idle);
    let load_more_error = use_state(|| Option::<String>::None);
    let loading_more = use_state(|| false);
    let type_detail = use_state(|| Option::<TypeDetail>::None);
    let active_tab = use_state(|| AttrValue::from("properties"));

    // Catalog surface state: the dataset list plus the selected dataset's detail,
    // lazily-loaded preview, and active drawer tab.
    let datasets = use_state(Vec::<DatasetRow>::new);
    let catalog_status = use_state(|| LoadStatus::Idle);
    let selected_dataset = use_state(|| Option::<usize>::None);
    let detail = use_state(|| Option::<DatasetDetail>::None);
    let preview = use_state(|| Option::<PreviewData>::None);
    let preview_loading = use_state(|| false);
    let catalog_tab = use_state(|| AttrValue::from("schema"));
    // Lazily-loaded lineage closures (upstream, downstream) for the selected dataset,
    // plus whether the Lineage tab is showing the full-canvas stub.
    #[allow(
        clippy::type_complexity,
        reason = "small local (up, down) closure pair"
    )]
    let lineage = use_state(|| Option::<(Vec<(String, String)>, Vec<(String, String)>)>::None);
    let show_full_lineage = use_state(|| false);

    // On mount: load the dataset list. Catalog is the default surface, so a
    // mount-keyed effect loads it exactly once (mirrors the ontology type list).
    {
        let datasets = datasets.clone();
        let catalog_status = catalog_status.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        use_effect_with((), move |()| {
            catalog_status.set(LoadStatus::Loading);
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_datasets(&net::api_base(), &token).await {
                    Ok(d) => {
                        datasets.set(d);
                        catalog_status.set(LoadStatus::Idle);
                    }
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => catalog_status.set(LoadStatus::Error(e.to_string())),
                }
            });
            || ()
        });
    }

    // On dataset row-select: reset the drawer to the Schema tab, clear the previous
    // detail/preview, and load the selected dataset's schema detail.
    {
        let detail = detail.clone();
        let preview = preview.clone();
        let preview_loading = preview_loading.clone();
        let catalog_tab = catalog_tab.clone();
        let lineage = lineage.clone();
        let show_full_lineage = show_full_lineage.clone();
        let datasets = datasets.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let selected_dep = *selected_dataset;
        use_effect_with(selected_dep, move |sel| {
            let Some(ds) = sel.and_then(|i| datasets.get(i).cloned()) else {
                return;
            };
            detail.set(None);
            preview.set(None);
            preview_loading.set(false);
            lineage.set(None);
            show_full_lineage.set(false);
            catalog_tab.set(AttrValue::from("schema"));
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_dataset_detail(&net::api_base(), &token, &ds.schema, &ds.name)
                    .await
                {
                    Ok(d) => detail.set(Some(d)),
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    // Best-effort: a failure leaves the Schema tab on its "Loading…"
                    // line rather than blocking the rest of the drawer.
                    Err(_) => {}
                }
            });
        });
    }

    // Lazy preview: only fetch when the Preview tab is active for the selected
    // dataset and no preview is loaded yet. Keyed on (selection, active tab); the
    // row-select effect above resets `preview` to None, so switching datasets and
    // re-opening Preview refetches.
    {
        let preview = preview.clone();
        let preview_loading = preview_loading.clone();
        let datasets = datasets.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let already_loaded = preview.is_some();
        let dep = (*selected_dataset, (*catalog_tab).clone());
        use_effect_with(dep, move |(sel, tab)| {
            if tab.as_str() != "preview" || already_loaded {
                return;
            }
            let Some(ds) = sel.and_then(|i| datasets.get(i).cloned()) else {
                return;
            };
            preview_loading.set(true);
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_preview(&net::api_base(), &token, &ds.schema, &ds.name, 50).await {
                    Ok(p) => {
                        preview.set(Some(p));
                        preview_loading.set(false);
                    }
                    Err(FetchError::Unauthorized) => {
                        preview_loading.set(false);
                        on_logout.emit(());
                    }
                    Err(_) => preview_loading.set(false),
                }
            });
        });
    }

    // Lazy lineage: only fetch the upstream + downstream closures when the Lineage
    // tab is active for the selected dataset and nothing is loaded yet. Keyed on
    // (selection, active tab); the row-select effect resets `lineage` to None, so
    // switching datasets and re-opening Lineage refetches.
    {
        let lineage = lineage.clone();
        let datasets = datasets.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let already_loaded = lineage.is_some();
        let dep = (*selected_dataset, (*catalog_tab).clone());
        use_effect_with(dep, move |(sel, tab)| {
            if tab.as_str() != "lineage" || already_loaded {
                return;
            }
            let Some(ds) = sel.and_then(|i| datasets.get(i).cloned()) else {
                return;
            };
            wasm_bindgen_futures::spawn_local(async move {
                let base = net::api_base();
                let up = net::fetch_lineage(&base, &token, &ds.schema, &ds.name, "upstream").await;
                let down =
                    net::fetch_lineage(&base, &token, &ds.schema, &ds.name, "downstream").await;
                // A 401 on either leg fails closed to logout; any other error degrades
                // to an empty closure so the mini-DAG still renders the current node.
                if up.as_ref().err() == Some(&FetchError::Unauthorized)
                    || down.as_ref().err() == Some(&FetchError::Unauthorized)
                {
                    on_logout.emit(());
                    return;
                }
                lineage.set(Some((up.unwrap_or_default(), down.unwrap_or_default())));
            });
        });
    }

    // On mount: load the ontology's type list.
    {
        let types = types.clone();
        let status = status.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        use_effect_with((), move |()| {
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_types(&net::api_base(), &token).await {
                    Ok(t) => types.set(t),
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => status.set(LoadStatus::Error(e.to_string())),
                }
            });
            || ()
        });
    }

    // On type selection: reset the table state and load page 1.
    {
        let objs = objs.clone();
        let columns = columns.clone();
        let next = next.clone();
        let selected_row = selected_row.clone();
        let status = status.clone();
        let load_more_error = load_more_error.clone();
        let loading_more = loading_more.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let selected_type_dep = (*selected_type).clone();
        use_effect_with(selected_type_dep, move |ty| {
            let Some(ty) = ty.clone() else {
                return;
            };
            objs.set(Vec::new());
            columns.set(Vec::new());
            next.set(None);
            selected_row.set(None);
            status.set(LoadStatus::Loading);
            load_more_error.set(None);
            loading_more.set(false);
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_page(&net::api_base(), &token, &ty, None, 50).await {
                    Ok(page) => {
                        columns.set(loom_ui_core::columns_from_objects(&page.rows));
                        objs.set(page.rows);
                        next.set(page.next);
                        status.set(LoadStatus::Idle);
                    }
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => status.set(LoadStatus::Error(e.to_string())),
                }
            });
        });
    }

    // On type selection: fetch the type detail that feeds the drawer's tabs.
    {
        let type_detail = type_detail.clone();
        let active_tab = active_tab.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let selected_type_dep = (*selected_type).clone();
        use_effect_with(selected_type_dep, move |ty| {
            let Some(ty) = ty.clone() else {
                return;
            };
            type_detail.set(None);
            active_tab.set(AttrValue::from("properties"));
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_type_detail(&net::api_base(), &token, &ty).await {
                    Ok(d) => type_detail.set(Some(d)),
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    // The detail is best-effort; a fetch failure leaves the tabs empty
                    // rather than blocking the object table.
                    Err(_) => {}
                }
            });
        });
    }

    let on_switch = {
        let surface = surface.clone();
        Callback::from(move |s: Surface| surface.set(s))
    };
    let on_logout = props.on_logout.clone();
    let logout_btn = html! {
        <Button variant={ButtonVariant::Ghost}
            onclick={Callback::from(move |_: MouseEvent| on_logout.emit(()))}>{ "Log out" }</Button>
    };

    let (list, drawer) = match *surface {
        Surface::Catalog => {
            let on_row = {
                let selected_dataset = selected_dataset.clone();
                Callback::from(move |i: usize| selected_dataset.set(Some(i)))
            };
            let on_tab = {
                let catalog_tab = catalog_tab.clone();
                Callback::from(move |t: AttrValue| catalog_tab.set(t))
            };
            let on_toggle_full = {
                let show_full_lineage = show_full_lineage.clone();
                Callback::from(move |()| show_full_lineage.set(!*show_full_lineage))
            };
            let list = html! {
                <CatalogList
                    datasets={(*datasets).clone()}
                    status={(*catalog_status).clone()}
                    selected={*selected_dataset}
                    on_row={on_row}
                />
            };
            // Drawer contract: only a real drawer when a row is selected; otherwise
            // Html::default() so the Shell hides the drawer region.
            let drawer = (*selected_dataset)
                .and_then(|i| datasets.get(i).cloned())
                .map(|ds| {
                    let lineage_view = (*lineage).as_ref().map(|(up, down)| {
                        loom_ui_core::lineage_dag((&ds.schema, &ds.name), up, down)
                    });
                    html! {
                        <CatalogDrawer
                            name={AttrValue::from(ds.name)}
                            detail={(*detail).clone()}
                            preview={(*preview).clone()}
                            preview_loading={*preview_loading}
                            active_tab={(*catalog_tab).clone()}
                            on_tab={on_tab}
                            lineage={lineage_view}
                            show_full={*show_full_lineage}
                            on_toggle_full={on_toggle_full}
                        />
                    }
                })
                .unwrap_or_default();
            (list, drawer)
        }
        Surface::Ontology => {
            let on_select_type = {
                let selected_type = selected_type.clone();
                Callback::from(move |ty: String| selected_type.set(Some(ty)))
            };
            let on_row = {
                let selected_row = selected_row.clone();
                Callback::from(move |i: usize| selected_row.set(Some(i)))
            };
            let on_tab = {
                let active_tab = active_tab.clone();
                Callback::from(move |t: AttrValue| active_tab.set(t))
            };
            let on_load_more = {
                let objs = objs.clone();
                let columns = columns.clone();
                let next = next.clone();
                let load_more_error = load_more_error.clone();
                let loading_more = loading_more.clone();
                let token = props.token.to_string();
                let on_logout = props.on_logout.clone();
                let selected_type = selected_type.clone();
                Callback::from(move |_: MouseEvent| {
                    if *loading_more {
                        return;
                    }
                    let Some(ty) = (*selected_type).clone() else {
                        return;
                    };
                    let cursor = (*next).clone();
                    let (objs, columns, next, load_more_error, loading_more, on_logout) = (
                        objs.clone(),
                        columns.clone(),
                        next.clone(),
                        load_more_error.clone(),
                        loading_more.clone(),
                        on_logout.clone(),
                    );
                    let token = token.clone();
                    loading_more.set(true);
                    wasm_bindgen_futures::spawn_local(async move {
                        match net::fetch_page(&net::api_base(), &token, &ty, cursor.as_deref(), 50)
                            .await
                        {
                            Ok(page) => {
                                let mut merged = (*objs).clone();
                                merged.extend(page.rows);
                                columns.set(loom_ui_core::columns_from_objects(&merged));
                                objs.set(merged);
                                next.set(page.next);
                                load_more_error.set(None);
                                loading_more.set(false);
                            }
                            Err(FetchError::Unauthorized) => {
                                loading_more.set(false);
                                on_logout.emit(());
                            }
                            Err(e) => {
                                load_more_error.set(Some(e.to_string()));
                                loading_more.set(false);
                            }
                        }
                    });
                })
            };

            let list = html! {
                <OntologyList
                    types={(*types).clone()}
                    selected_type={(*selected_type).clone()}
                    objs={(*objs).clone()}
                    columns={(*columns).clone()}
                    status={(*status).clone()}
                    has_next={next.is_some()}
                    selected_row={*selected_row}
                    loading_more={*loading_more}
                    load_more_error={(*load_more_error).clone()}
                    on_select_type={on_select_type}
                    on_row={on_row}
                    on_load_more={on_load_more}
                />
            };
            // Drawer contract: only pass a real drawer when a row is selected;
            // otherwise Html::default() so the Shell hides the drawer region.
            let drawer = (*selected_row)
                .and_then(|i| objs.get(i).cloned())
                .map(|obj| {
                    html! {
                        <OntologyDrawer
                            selected_type={(*selected_type).clone()}
                            selected_obj={obj}
                            detail={(*type_detail).clone()}
                            active_tab={(*active_tab).clone()}
                            on_tab={on_tab}
                        />
                    }
                })
                .unwrap_or_default();
            (list, drawer)
        }
        other => (html! { <StubView surface={other} /> }, Html::default()),
    };

    html! {
        <>
            <GlobalStyles />
            <Shell active={*surface} on_switch={on_switch} search={logout_btn} avatar="DK"
                   list={list} drawer={drawer} />
        </>
    }
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
