// Strict clippy (pedantic + restriction) runs on this crate; yew's html! macro is
// not lint-clean under that gate, so allow the two groups crate-wide here. The pure
// logic lives in the loom_ui_core lib, which stays lint-clean.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

mod net;
mod router;
mod session;
mod surfaces;

use loom_ui_components::{Badge, Button, GlobalStyles, Shell, StubView};
use loom_ui_core::{
    AuthError, BadgeTone, ButtonVariant, CatalogQuery, CatalogSortDir, CompletionSchema,
    DatasetDetail, DatasetRow, DatasetRunRow, DatasetSort, FetchGeneration, FieldError,
    PreviewData, Route, RunRow, Surface, TableRef, TransformDefView, TransformForm, TransformIo,
    TransformKind, TransformSummary, TypeDetail, bump_epoch, dataset_index, dataset_route_id,
    delete_action_effect, distinct_projects, form_to_body, form_to_def, project_chip_options,
    run_action_effect, schema_from_dataset_details, schema_from_types, split_dataset_id,
};
use net::FetchError;
use router::{Navigator, use_route};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use stylist::yew::styled_component;
use surfaces::{
    CatalogControls, CatalogDrawer, CatalogList, LoadStatus, OntologyDrawer, OntologyList,
    OntologyTypeRow, QueryView, TransformDrawer, TransformEditor, TransformsList,
};
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

/// Fetch the dataset list for `query` and commit it under the generation guard:
/// the response is applied only if `catalog_gen` has not advanced since this fetch
/// was spawned (so an out-of-order arrival can't stale the list). On an unfiltered
/// load (`project_is_all`) the project chip options are refreshed from the result.
#[allow(
    clippy::too_many_arguments,
    reason = "mechanical extraction of the load-effect's closure captures; each param is a distinct piece of Yew state the async body needs (#619)"
)]
fn spawn_catalog_load(
    datasets: UseStateHandle<Vec<DatasetRow>>,
    catalog_status: UseStateHandle<LoadStatus>,
    catalog_all_projects: UseStateHandle<Vec<String>>,
    catalog_gen: Rc<RefCell<FetchGeneration>>,
    token: String,
    on_logout: Callback<()>,
    query: String,
    project_is_all: bool,
) {
    let my_gen = catalog_gen.borrow_mut().bump();
    wasm_bindgen_futures::spawn_local(async move {
        match net::fetch_datasets(&net::api_base(), &token, &query).await {
            Ok(d) => {
                if catalog_gen.borrow().is_current(my_gen) {
                    if project_is_all {
                        catalog_all_projects.set(distinct_projects(&d));
                    }
                    datasets.set(d);
                    catalog_status.set(LoadStatus::Idle);
                }
            }
            Err(FetchError::Unauthorized) => on_logout.emit(()),
            Err(e) => {
                if catalog_gen.borrow().is_current(my_gen) {
                    catalog_status.set(LoadStatus::Error(e.to_string()));
                }
            }
        }
    });
}

/// The three Catalog controls-bar callbacks. Each navigates to the same route with
/// one control replaced; `Route::with_catalog` clears the selection, because
/// re-sorting or filtering changes which rows the list holds. These **replace** the
/// history entry rather than pushing: adjusting a filter is a view tweak, and
/// pushing would make every chip click cost a Back press to escape.
fn catalog_control_callbacks(
    route: &Route,
    navigate: &Navigator,
) -> (
    Callback<DatasetSort>,
    Callback<CatalogSortDir>,
    Callback<Option<String>>,
) {
    let on_sort = {
        let (route, navigate) = (route.clone(), navigate.clone());
        Callback::from(move |sort: DatasetSort| {
            let next = CatalogQuery {
                sort,
                ..route.catalog.clone()
            };
            navigate.replace(route.with_catalog(next));
        })
    };
    let on_dir = {
        let (route, navigate) = (route.clone(), navigate.clone());
        Callback::from(move |dir: CatalogSortDir| {
            let next = CatalogQuery {
                dir,
                ..route.catalog.clone()
            };
            navigate.replace(route.with_catalog(next));
        })
    };
    let on_project = {
        let (route, navigate) = (route.clone(), navigate.clone());
        Callback::from(move |project: Option<String>| {
            let next = CatalogQuery {
                project,
                ..route.catalog.clone()
            };
            navigate.replace(route.with_catalog(next));
        })
    };
    (on_sort, on_dir, on_project)
}

#[derive(Properties, PartialEq)]
struct WorkspaceProps {
    token: AttrValue,
    on_logout: Callback<()>,
}

/// Custom hook owning the Catalog History tab's run-history fetch: resets on
/// dataset change and lazily loads `/lineage/{ns}/{name}/runs` when the History tab
/// is active, under the shared fetch-generation guard. Extracted from `workspace`
/// so the tab's state + effects stay out of that (already large) component; returns
/// `(runs, loading, error)` for the drawer. The pure parse lives in `loom_ui_core`.
#[hook]
fn use_dataset_run_history(
    selected: Option<String>,
    tab: AttrValue,
    token: String,
    on_logout: Callback<()>,
    fetch_gen: Rc<RefCell<FetchGeneration>>,
) -> (Option<Vec<DatasetRunRow>>, bool, Option<String>) {
    let history_runs = use_state(|| Option::<Vec<DatasetRunRow>>::None);
    let history_loading = use_state(|| false);
    let history_error = use_state(|| Option::<String>::None);

    // Reset on selection change so re-opening History for a new dataset refetches.
    {
        let history_runs = history_runs.clone();
        let history_loading = history_loading.clone();
        let history_error = history_error.clone();
        use_effect_with(selected.clone(), move |_| {
            history_runs.set(None);
            history_loading.set(false);
            history_error.set(None);
        });
    }

    // Lazy fetch when the History tab is active for the selection and nothing loaded.
    {
        let history_runs = history_runs.clone();
        let history_loading = history_loading.clone();
        let history_error = history_error.clone();
        let already_loaded = history_runs.is_some();
        // `already_loaded` rides in the deps: the reset effect above clears the runs in
        // the same commit, so without it a Back/Forward that changes the selection while
        // History is active would bail on a stale `true` and never refetch.
        use_effect_with(
            (selected, tab, already_loaded),
            move |(sel, tab, _loaded)| {
                if tab.as_str() != "history" || already_loaded {
                    return;
                }
                let Some((schema, name)) = sel.as_deref().and_then(split_dataset_id) else {
                    return;
                };
                let (schema, name) = (schema.to_owned(), name.to_owned());
                history_loading.set(true);
                let my_gen = fetch_gen.borrow().current();
                wasm_bindgen_futures::spawn_local(async move {
                    match net::fetch_dataset_runs(&net::api_base(), &token, &schema, &name).await {
                        Ok(runs) => {
                            if fetch_gen.borrow().is_current(my_gen) {
                                history_runs.set(Some(runs));
                                history_loading.set(false);
                            }
                        }
                        Err(FetchError::Unauthorized) => {
                            history_loading.set(false);
                            on_logout.emit(());
                        }
                        Err(e) => {
                            if fetch_gen.borrow().is_current(my_gen) {
                                history_error.set(Some(e.to_string()));
                                history_loading.set(false);
                            }
                        }
                    }
                });
            },
        );
    }

    (
        (*history_runs).clone(),
        *history_loading,
        (*history_error).clone(),
    )
}

/// The app-bar "Log out" button. Extracted from `workspace` so its click closure
/// lives here rather than adding to that hotspot function's complexity.
fn logout_button(on_logout: Callback<()>) -> Html {
    html! {
        <Button variant={ButtonVariant::Ghost}
            onclick={Callback::from(move |_: MouseEvent| on_logout.emit(()))}>{ "Log out" }</Button>
    }
}

#[function_component(Workspace)]
fn workspace(props: &WorkspaceProps) -> Html {
    // The URL is the source of truth for the active surface, the selected row and
    // the drawer tab; `Workspace` derives them rather than owning them.
    let (route, navigate) = use_route();

    // Catalog location, read out of the URL. `selection_on`/`tab_on` scope the route
    // to this surface, so an Ontology type name can never be read as a dataset id by
    // the Catalog effects (which stay mounted on every surface).
    let catalog_sel: Option<String> = route.selection_on(Surface::Catalog).map(str::to_owned);
    let catalog_tab: AttrValue = AttrValue::from(
        route
            .tab_on(Surface::Catalog)
            .unwrap_or("schema")
            .to_owned(),
    );
    let catalog_query = route.catalog.clone();

    // Ontology location, read out of the URL — same surface-scoping as the Catalog.
    let onto_sel: Option<String> = route.selection_on(Surface::Ontology).map(str::to_owned);
    let onto_tab: AttrValue = AttrValue::from(
        route
            .tab_on(Surface::Ontology)
            .unwrap_or("properties")
            .to_owned(),
    );

    // Transforms location, read out of the URL.
    let tf_sel: Option<String> = route.selection_on(Surface::Transforms).map(str::to_owned);
    let tf_tab: AttrValue = AttrValue::from(
        route
            .tab_on(Surface::Transforms)
            .unwrap_or("definition")
            .to_owned(),
    );

    // Ontology surface state: the list of type names and each type's loaded
    // `TypeDetail` (schema) keyed by name. The selection and drawer tab live on the
    // route (`onto_sel`/`onto_tab`, above).
    let types = use_state(Vec::<String>::new);
    let onto_status = use_state(|| LoadStatus::Idle);
    let type_details = use_state(HashMap::<String, TypeDetail>::new);

    // Catalog surface state: the dataset list plus the selected dataset's detail
    // and lazily-loaded preview.
    let datasets = use_state(Vec::<DatasetRow>::new);
    let catalog_status = use_state(|| LoadStatus::Idle);
    let detail = use_state(|| Option::<DatasetDetail>::None);
    let preview = use_state(|| Option::<PreviewData>::None);
    let preview_loading = use_state(|| false);
    let detail_error = use_state(|| Option::<String>::None);
    let preview_error = use_state(|| Option::<String>::None);
    // The sort/filter controls themselves live on the route (`catalog_query`, above).
    // What stays local: the distinct project chip options (refreshed on unfiltered
    // loads only, so the chip set stays stable while filtered), and a generation
    // guard against out-of-order responses when the controls change quickly.
    let catalog_all_projects = use_state(Vec::<String>::new);
    let catalog_gen = use_mut_ref(FetchGeneration::default);
    // Lazily-loaded lineage closures (upstream, downstream) for the selected dataset,
    // plus whether the Lineage tab is showing the full-canvas stub.
    #[allow(
        clippy::type_complexity,
        reason = "small local (up, down) closure pair"
    )]
    let lineage = use_state(|| Option::<(Vec<(String, String)>, Vec<(String, String)>)>::None);
    let show_full_lineage = use_state(|| false);
    // Generation guard for selection-scoped drawer fetches: bumped on dataset
    // selection change so a slow in-flight fetch from a prior selection cannot
    // overwrite the current selection's drawer tab body (see FetchGeneration).
    let fetch_gen = use_mut_ref(FetchGeneration::default);

    // Transforms
    let transforms = use_state(Vec::<TransformSummary>::new);
    let tf_status = use_state(|| LoadStatus::Idle);
    let tf_forbidden = use_state(|| false);
    let tf_def = use_state(|| Option::<TransformDefView>::None);
    let tf_runs = use_state(Vec::<RunRow>::new);
    let tf_runs_status = use_state(|| LoadStatus::Idle);
    // Editor form: Some(form) when the editor is open (New or Edit), None when viewing.
    let tf_editing = use_state(|| Option::<TransformForm>::None);
    // When editing an EXISTING def, the name being redefined; None for a New transform.
    // Drives the editor title + disabled name field + the redefine target.
    let tf_edit_name = use_state(|| Option::<String>::None);
    let tf_schema = use_state(CompletionSchema::default);
    let tf_errors = use_state(Vec::<FieldError>::new);
    let tf_server_error = use_state(|| Option::<AttrValue>::None);
    let tf_dataset_options = use_state(Vec::<String>::new);
    let tf_type_options = use_state(Vec::<String>::new);
    // Per-stream epoch generation guards. One SHARED counter is wrong: two effects
    // keyed on the same selection each bump-then-capture, so the later effect's bump
    // invalidates the earlier effect's in-flight fetch. Each independent fetch stream
    // gets its own counter so a stream only cancels its own stale in-flight requests.
    let tf_def_gen = use_mut_ref(|| 0u64);
    let tf_runs_gen = use_mut_ref(|| 0u64);
    let tf_schema_gen = use_mut_ref(|| 0u64);
    // Render-participating runs-fetch epoch: bumped on a successful Run so the runs
    // effect refires even when (selection, tab) is unchanged — i.e. when the response
    // lands with the Runs tab already active. (tf_runs_gen stays the in-flight
    // staleness guard; this is the dep invalidator.)
    //
    // The `use_mut_ref` is the SOURCE OF TRUTH and the `use_state` only mirrors it for
    // the dep tuple: a UseStateHandle derefs to the value captured at the render that
    // built the callback, and the Run button is not disabled in-flight, so two clicks
    // from the same render would both compute `E + 1` from the same snapshot — the
    // second response would re-set the value the first already stored, the dep tuple
    // would not change, and the Runs tab would sit empty with no refetch. Bumping the
    // ref (see `bump_epoch`) keeps the counter authoritative.
    let tf_runs_epoch = use_state(|| 0u64);
    let tf_runs_epoch_ref = use_mut_ref(|| 0u64);

    // Load the dataset list, re-fetching whenever the sort/filter controls change.
    // Keyed on (sort, dir, project); guarded by `catalog_gen` against out-of-order
    // responses (a slow earlier fetch must not overwrite a newer control's result).
    // The project chip options are refreshed from the response only on an unfiltered
    // (project = None) load, so the chip set stays stable while filtered.
    {
        let datasets = datasets.clone();
        let catalog_status = catalog_status.clone();
        let catalog_all_projects = catalog_all_projects.clone();
        let catalog_gen = catalog_gen.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let sort = catalog_query.sort;
        let dir = catalog_query.dir;
        let project = catalog_query.project.clone();
        use_effect_with((sort, dir, project.clone()), move |(sort, dir, project)| {
            catalog_status.set(LoadStatus::Loading);
            let query = loom_ui_core::dataset_list_query(*sort, *dir, project.as_deref());
            spawn_catalog_load(
                datasets,
                catalog_status,
                catalog_all_projects,
                catalog_gen,
                token,
                on_logout,
                query,
                project.is_none(),
            );
            || ()
        });
    }

    // On dataset row-select: clear the previous detail/preview and load the selected
    // dataset's schema detail. Resetting the drawer tab is `Route::with_selection`'s
    // job, not this effect's. The `(schema, name)` come out of the route id rather
    // than an index into `datasets`, so a deep link loads before the list arrives.
    {
        let detail = detail.clone();
        let preview = preview.clone();
        let preview_loading = preview_loading.clone();
        let detail_error = detail_error.clone();
        let preview_error = preview_error.clone();
        let lineage = lineage.clone();
        let show_full_lineage = show_full_lineage.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let fetch_gen = fetch_gen.clone();
        let selected_dep = catalog_sel.clone();
        use_effect_with(selected_dep, move |sel| {
            let Some((schema, name)) = sel.as_deref().and_then(split_dataset_id) else {
                return;
            };
            let (schema, name) = (schema.to_owned(), name.to_owned());
            detail.set(None);
            preview.set(None);
            preview_loading.set(false);
            detail_error.set(None);
            preview_error.set(None);
            lineage.set(None);
            show_full_lineage.set(false);
            // Any in-flight fetch from the previous selection is now stale.
            let my_gen = fetch_gen.borrow_mut().bump();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_dataset_detail(&net::api_base(), &token, &schema, &name).await {
                    Ok(d) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            detail.set(Some(d));
                        }
                    }
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            detail_error.set(Some(e.to_string()));
                        }
                    }
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
        let preview_error = preview_error.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let fetch_gen = fetch_gen.clone();
        let already_loaded = preview.is_some();
        // `already_loaded` rides in the deps: the row-select effect clears `preview`
        // in the same commit, so without it a Back/Forward that changes the selection
        // while Preview is active would bail on a stale `true` and never refetch.
        let dep = (catalog_sel.clone(), catalog_tab.clone(), already_loaded);
        use_effect_with(dep, move |(sel, tab, _loaded)| {
            if tab.as_str() != "preview" || already_loaded {
                return;
            }
            let Some((schema, name)) = sel.as_deref().and_then(split_dataset_id) else {
                return;
            };
            let (schema, name) = (schema.to_owned(), name.to_owned());
            preview_loading.set(true);
            let my_gen = fetch_gen.borrow().current();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_preview(&net::api_base(), &token, &schema, &name, 50).await {
                    Ok(p) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            preview.set(Some(p));
                            preview_loading.set(false);
                        }
                    }
                    Err(FetchError::Unauthorized) => {
                        preview_loading.set(false);
                        on_logout.emit(());
                    }
                    Err(e) => {
                        if fetch_gen.borrow().is_current(my_gen) {
                            preview_error.set(Some(e.to_string()));
                            preview_loading.set(false);
                        }
                    }
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
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let fetch_gen = fetch_gen.clone();
        let already_loaded = lineage.is_some();
        // `already_loaded` in the deps — same staleness fix as the preview effect above.
        let dep = (catalog_sel.clone(), catalog_tab.clone(), already_loaded);
        use_effect_with(dep, move |(sel, tab, _loaded)| {
            if tab.as_str() != "lineage" || already_loaded {
                return;
            }
            let Some((schema, name)) = sel.as_deref().and_then(split_dataset_id) else {
                return;
            };
            let (schema, name) = (schema.to_owned(), name.to_owned());
            let my_gen = fetch_gen.borrow().current();
            wasm_bindgen_futures::spawn_local(async move {
                let base = net::api_base();
                let up = net::fetch_lineage(&base, &token, &schema, &name, "upstream").await;
                let down = net::fetch_lineage(&base, &token, &schema, &name, "downstream").await;
                // A 401 on either leg fails closed to logout; any other error degrades
                // to an empty closure so the mini-DAG still renders the current node.
                if up.as_ref().err() == Some(&FetchError::Unauthorized)
                    || down.as_ref().err() == Some(&FetchError::Unauthorized)
                {
                    on_logout.emit(());
                    return;
                }
                if fetch_gen.borrow().is_current(my_gen) {
                    lineage.set(Some((up.unwrap_or_default(), down.unwrap_or_default())));
                }
            });
        });
    }

    // Catalog History tab: the run-history fetch (reset on selection + lazy load when
    // the History tab is active) is encapsulated in a custom hook so the tab's state
    // and effects don't inflate `workspace`. Called here — after the row-select
    // effect's generation bump — so its fetch captures the post-bump generation, like
    // the preview/lineage effects above.
    let (history_runs, history_loading, history_error) = use_dataset_run_history(
        catalog_sel.clone(),
        catalog_tab.clone(),
        props.token.to_string(),
        props.on_logout.clone(),
        fetch_gen.clone(),
    );

    // On mount: load the ontology's type list, then eagerly load every type's detail
    // (schema) into one map. This is an N+1 over the type list (one /ontology/types +
    // one /ontology/types/{name} per type) — fine for small ontologies; a lazier
    // per-selection fetch is a future refinement. A 401 on any leg fails closed to
    // logout, matching the rest of the app. The details map is set once (not
    // per-insert) so concurrent renders never see a partially-built map.
    {
        let types = types.clone();
        let onto_status = onto_status.clone();
        let type_details = type_details.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        use_effect_with((), move |()| {
            onto_status.set(LoadStatus::Loading);
            wasm_bindgen_futures::spawn_local(async move {
                let base = net::api_base();
                let names = match net::fetch_types(&base, &token).await {
                    Ok(t) => t,
                    Err(FetchError::Unauthorized) => return on_logout.emit(()),
                    Err(e) => return onto_status.set(LoadStatus::Error(e.to_string())),
                };
                types.set(names.clone());
                onto_status.set(LoadStatus::Idle);
                let mut map = HashMap::<String, TypeDetail>::new();
                for name in names {
                    match net::fetch_type_detail(&base, &token, &name).await {
                        Ok(d) => {
                            map.insert(name, d);
                        }
                        Err(FetchError::Unauthorized) => return on_logout.emit(()),
                        // A per-type failure just leaves that type's drawer on its
                        // "Loading…" line rather than blocking the whole list.
                        Err(_) => {}
                    }
                }
                type_details.set(map);
            });
            || ()
        });
    }

    // Transforms list: load once on mount. A 403 (non-admin) flips `tf_forbidden` so
    // the list renders the admin-only empty state instead of an error.
    {
        let transforms = transforms.clone();
        let tf_status = tf_status.clone();
        let tf_forbidden = tf_forbidden.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        let on_logout = props.on_logout.clone();
        use_effect_with((), move |()| {
            tf_status.set(LoadStatus::Loading);
            wasm_bindgen_futures::spawn_local(async move {
                match net::list_transforms(&base, &token).await {
                    Ok(rows) => {
                        tf_forbidden.set(false);
                        transforms.set(rows);
                        tf_status.set(LoadStatus::Idle);
                    }
                    Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                    Err(net::FetchError::Forbidden) => {
                        tf_forbidden.set(true);
                        tf_status.set(LoadStatus::Idle);
                    }
                    Err(e) => tf_status.set(LoadStatus::Error(e.to_string())),
                }
            });
            || ()
        });
    }

    // Transforms editor options: load the dataset ("schema.name") + type name lists
    // once on mount so the editor's input checkbox list has options to offer.
    {
        let tf_dataset_options = tf_dataset_options.clone();
        let tf_type_options = tf_type_options.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        use_effect_with((), move |()| {
            wasm_bindgen_futures::spawn_local(async move {
                if let Ok(datasets) = net::fetch_datasets(&base, &token, "").await {
                    let opts: Vec<String> = datasets
                        .iter()
                        .map(|d| format!("{}.{}", d.schema, d.name))
                        .collect();
                    tf_dataset_options.set(opts);
                }
                if let Ok(types) = net::fetch_types(&base, &token).await {
                    tf_type_options.set(types);
                }
            });
            || ()
        });
    }

    // On transform row-select: clear the previous def/runs/editor, then load the
    // selected transform's definition. Resetting the drawer tab is
    // `Route::with_selection`'s job, not this effect's. Guarded by `tf_def_gen` so a
    // stale in-flight fetch never overwrites a newer selection's def.
    {
        let tf_def = tf_def.clone();
        let tf_editing = tf_editing.clone();
        let tf_edit_name = tf_edit_name.clone();
        let tf_server_error = tf_server_error.clone();
        let tf_runs = tf_runs.clone();
        let tf_def_gen = tf_def_gen.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        let on_logout = props.on_logout.clone();
        let selected = tf_sel.clone();
        use_effect_with(selected, move |selected| {
            *tf_def_gen.borrow_mut() += 1;
            let my_gen = *tf_def_gen.borrow();
            tf_def.set(None);
            tf_edit_name.set(None);
            tf_server_error.set(None); // a stale action error never leaks onto a new selection
            tf_runs.set(Vec::new()); // force the runs tab to refetch for the new selection
            // Guard AFTER the display-state clears, and spare only `tf_editing`:
            // clearing the selection must still drop the stale definition (or Cancel
            // would resurrect the previous transform's drawer with no row highlighted),
            // but must NOT slam the editor shut — `on_new` clears the selection and
            // opens the editor in the same batch.
            let Some(name) = selected.clone() else {
                return;
            };
            tf_editing.set(None);
            wasm_bindgen_futures::spawn_local(async move {
                match net::get_transform(&base, &token, &name).await {
                    Ok(def) => {
                        if *tf_def_gen.borrow() == my_gen {
                            tf_def.set(Some(def));
                        }
                    }
                    Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                    Err(_) => {
                        if *tf_def_gen.borrow() == my_gen {
                            tf_def.set(None);
                        }
                    }
                }
            });
        });
    }

    // Lazy runs fetch: only when the Runs tab is active for the selected transform and
    // no runs are loaded yet (the row-select effect clears `tf_runs`). Keyed on
    // (selection, active tab); guarded by its own `tf_runs_gen`.
    {
        let tf_runs = tf_runs.clone();
        let tf_runs_status = tf_runs_status.clone();
        let tf_runs_gen = tf_runs_gen.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        let on_logout = props.on_logout.clone();
        let already_loaded = !tf_runs.is_empty();
        // `already_loaded` rides in the deps for the same reason as the Catalog drawer
        // effects: the def effect clears `tf_runs` in the same commit, so a Back/Forward
        // that changes the selection while the Runs tab is active would otherwise bail
        // on a stale `true` and never refetch.
        let dep = (
            tf_sel.clone(),
            tf_tab.clone(),
            *tf_runs_epoch,
            already_loaded,
        );
        use_effect_with(dep, move |(sel, tab, _epoch, _loaded)| {
            if tab.as_str() != "runs" || already_loaded {
                return;
            }
            let Some(name) = sel.clone() else {
                return;
            };
            *tf_runs_gen.borrow_mut() += 1;
            let my_gen = *tf_runs_gen.borrow();
            tf_runs_status.set(LoadStatus::Loading);
            wasm_bindgen_futures::spawn_local(async move {
                match net::list_runs(&base, &token, &name).await {
                    Ok(rows) => {
                        if *tf_runs_gen.borrow() == my_gen {
                            tf_runs.set(rows);
                            tf_runs_status.set(LoadStatus::Idle);
                        }
                    }
                    Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => {
                        if *tf_runs_gen.borrow() == my_gen {
                            tf_runs_status.set(LoadStatus::Error(e.to_string()));
                        }
                    }
                }
            });
        });
    }

    // Input-scoped completion schema: while the editor is open, key on the form's
    // sorted (kind, inputs) fingerprint and rebuild the schema from those inputs'
    // dataset/type details. Guarded by `tf_schema_gen`; the SqlEditor remounts on the
    // same fingerprint `key`, so the fresh schema takes effect.
    {
        let tf_schema = tf_schema.clone();
        let tf_editing = tf_editing.clone();
        let tf_schema_gen = tf_schema_gen.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        let fp = tf_editing.as_ref().map(|f| {
            (f.kind, {
                let mut xs = f.inputs.clone();
                xs.sort();
                xs
            })
        });
        use_effect_with(fp, move |fp| {
            if let Some((kind, inputs)) = fp.clone() {
                *tf_schema_gen.borrow_mut() += 1;
                let my_gen = *tf_schema_gen.borrow();
                wasm_bindgen_futures::spawn_local(async move {
                    let schema = build_input_schema(&base, &token, kind, &inputs).await;
                    if *tf_schema_gen.borrow() == my_gen {
                        tf_schema.set(schema);
                    }
                });
            }
            || ()
        });
    }

    let on_switch = {
        let (route, navigate) = (route.clone(), navigate.clone());
        Callback::from(move |s: Surface| navigate.push(route.with_surface(s)))
    };
    let logout_btn = logout_button(props.on_logout.clone());

    let (list, drawer) = match route.surface {
        Surface::Catalog => {
            // The list highlight is derived: the route holds the stable "schema.name"
            // id, the table wants the row's position in the currently loaded page.
            let selected_idx = catalog_sel
                .as_deref()
                .and_then(|id| dataset_index(&datasets, id));
            let on_row = {
                let (route, navigate, datasets) =
                    (route.clone(), navigate.clone(), datasets.clone());
                // Opening a row IS a navigation → push, so Back closes the drawer.
                Callback::from(move |i: usize| {
                    if let Some(row) = datasets.get(i) {
                        navigate.push(route.with_selection(dataset_route_id(row)));
                    }
                })
            };
            let on_tab = {
                let (route, navigate) = (route.clone(), navigate.clone());
                Callback::from(move |t: AttrValue| navigate.replace(route.with_tab(t.as_str())))
            };
            let on_toggle_full = {
                let show_full_lineage = show_full_lineage.clone();
                Callback::from(move |()| show_full_lineage.set(!*show_full_lineage))
            };
            let (on_sort, on_dir, on_project) = catalog_control_callbacks(&route, &navigate);
            let list = html! {
                <>
                    <CatalogControls
                        projects={project_chip_options(
                            &catalog_all_projects, catalog_query.project.as_deref())}
                        active_project={catalog_query.project.clone()}
                        sort={catalog_query.sort}
                        dir={catalog_query.dir}
                        on_project={on_project}
                        on_sort={on_sort}
                        on_dir={on_dir}
                    />
                    <CatalogList
                        datasets={(*datasets).clone()}
                        status={(*catalog_status).clone()}
                        selected={selected_idx}
                        on_row={on_row}
                    />
                </>
            };
            // Drawer contract: only a real drawer when a row is selected; otherwise
            // Html::default() so the Shell hides the drawer region. Resolved straight
            // out of the route id, so a deep link renders before the list arrives.
            let drawer = catalog_sel
                .as_deref()
                .and_then(split_dataset_id)
                .map(|(schema, name)| {
                    let lineage_view = (*lineage)
                        .as_ref()
                        .map(|(up, down)| loom_ui_core::lineage_dag((schema, name), up, down));
                    html! {
                        <CatalogDrawer
                            name={AttrValue::from(name.to_owned())}
                            detail={(*detail).clone()}
                            preview={(*preview).clone()}
                            preview_loading={*preview_loading}
                            detail_error={(*detail_error).clone()}
                            preview_error={(*preview_error).clone()}
                            active_tab={catalog_tab.clone()}
                            on_tab={on_tab}
                            lineage={lineage_view}
                            show_full={*show_full_lineage}
                            on_toggle_full={on_toggle_full}
                            history_runs={history_runs.clone()}
                            history_loading={history_loading}
                            history_error={history_error.clone()}
                        />
                    }
                })
                .unwrap_or_default();
            (list, drawer)
        }
        Surface::Ontology => ontology_panes(
            &types,
            &type_details,
            &onto_status,
            onto_sel.as_deref(),
            &onto_tab,
            &route,
            &navigate,
        ),
        Surface::Transforms => {
            // The list highlight is derived: the route holds the transform name, the
            // table wants the row's position in the currently loaded list.
            let selected_idx = tf_sel
                .as_deref()
                .and_then(|name| transforms.iter().position(|r| r.name == name));
            let on_row = {
                let (route, navigate, transforms) =
                    (route.clone(), navigate.clone(), transforms.clone());
                // Opening a row IS a navigation → push, so Back closes the drawer.
                Callback::from(move |i: usize| {
                    if let Some(row) = transforms.get(i) {
                        navigate.push(route.with_selection(row.name.clone()));
                    }
                })
            };
            let on_new = {
                let editing = tf_editing.clone();
                let edit_name = tf_edit_name.clone();
                let (route, navigate) = (route.clone(), navigate.clone());
                let errors = tf_errors.clone();
                let server_error = tf_server_error.clone();
                Callback::from(move |()| {
                    // replace, not push: opening the editor is an action, and Back
                    // should not re-open the transform you were just looking at.
                    navigate.replace(route.cleared());
                    edit_name.set(None); // New, not Edit
                    errors.set(Vec::new());
                    server_error.set(None);
                    editing.set(Some(TransformForm::default()));
                })
            };
            let list = html! {
                <TransformsList rows={(*transforms).clone()} status={(*tf_status).clone()}
                    selected={selected_idx} on_row={on_row} on_new={on_new} forbidden={*tf_forbidden} />
            };
            // Reusable "refetch the list into state" async closure builder.
            let reload_list = {
                let transforms = transforms.clone();
                let tf_status = tf_status.clone();
                let on_logout = props.on_logout.clone();
                let token = props.token.to_string();
                let base = net::api_base();
                move || {
                    let (transforms, tf_status, on_logout) =
                        (transforms.clone(), tf_status.clone(), on_logout.clone());
                    let (token, base) = (token.clone(), base.clone());
                    wasm_bindgen_futures::spawn_local(async move {
                        match net::list_transforms(&base, &token).await {
                            Ok(rows) => {
                                transforms.set(rows);
                                tf_status.set(LoadStatus::Idle);
                            }
                            Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                            Err(e) => tf_status.set(LoadStatus::Error(e.to_string())),
                        }
                    });
                }
            };
            let drawer = if let Some(form) = (*tf_editing).clone() {
                transform_editor_drawer(
                    form,
                    tf_edit_name.is_some(),
                    tf_editing.clone(),
                    tf_errors.clone(),
                    tf_server_error.clone(),
                    tf_schema.clone(),
                    tf_dataset_options.clone(),
                    tf_type_options.clone(),
                    props.token.to_string(),
                    props.on_logout.clone(),
                    reload_list.clone(),
                )
            } else if let Some(def) = (*tf_def).clone() {
                let on_tab = {
                    let (route, navigate) = (route.clone(), navigate.clone());
                    Callback::from(move |id: AttrValue| {
                        navigate.replace(route.with_tab(id.as_str()));
                    })
                };
                // Edit: seed the form from the def, record the edit target name.
                let on_edit = {
                    let editing = tf_editing.clone();
                    let edit_name = tf_edit_name.clone();
                    let errors = tf_errors.clone();
                    let server_error = tf_server_error.clone();
                    let def = def.clone();
                    Callback::from(move |()| {
                        errors.set(Vec::new());
                        server_error.set(None);
                        edit_name.set(Some(def.name.clone()));
                        editing.set(Some(form_from_def(&def)));
                    })
                };
                // Run saved now → on success clear tf_runs + bump the runs epoch (so the runs
                // effect refires even if the Runs tab is already active) + open Runs tab; on a
                // non-401 failure surface the message beside the buttons and stay on Definition.
                let on_run = {
                    let name = def.name.clone();
                    let tf_runs = tf_runs.clone();
                    let tf_runs_epoch = tf_runs_epoch.clone();
                    let tf_runs_epoch_ref = tf_runs_epoch_ref.clone();
                    let navigate = navigate.clone();
                    let server_error = tf_server_error.clone();
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
                    let base = net::api_base();
                    Callback::from(move |()| {
                        server_error.set(None);
                        let (name, tf_runs, tf_runs_epoch, navigate) = (
                            name.clone(),
                            tf_runs.clone(),
                            tf_runs_epoch.clone(),
                            navigate.clone(),
                        );
                        let tf_runs_epoch_ref = tf_runs_epoch_ref.clone();
                        let (server_error, on_logout) = (server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        wasm_bindgen_futures::spawn_local(run_saved_transform(
                            base,
                            token,
                            name,
                            tf_runs,
                            tf_runs_epoch,
                            tf_runs_epoch_ref,
                            navigate,
                            server_error,
                            on_logout,
                        ));
                    })
                };
                // Delete → on success clear selection + refetch list; on a non-401 failure
                // surface the message beside the buttons (row + drawer stay put).
                let on_delete = {
                    let name = def.name.clone();
                    let tf_def = tf_def.clone();
                    let navigate = navigate.clone();
                    let server_error = tf_server_error.clone();
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
                    let base = net::api_base();
                    let reload_list = reload_list.clone();
                    Callback::from(move |()| {
                        server_error.set(None);
                        let (name, tf_def, navigate) =
                            (name.clone(), tf_def.clone(), navigate.clone());
                        let (server_error, on_logout) = (server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        let reload_list = reload_list.clone();
                        wasm_bindgen_futures::spawn_local(delete_saved_transform(
                            base,
                            token,
                            name,
                            tf_def,
                            navigate,
                            server_error,
                            on_logout,
                            reload_list,
                        ));
                    })
                };
                html! {
                    <TransformDrawer def={def} active_tab={tf_tab.clone()}
                        on_tab={on_tab}
                        runs={(*tf_runs).clone()} runs_status={(*tf_runs_status).clone()}
                        action_error={(*tf_server_error).clone()}
                        on_edit={on_edit} on_run={on_run} on_delete={on_delete} />
                }
            } else {
                Html::default()
            };
            (list, drawer)
        }
        Surface::Query => (
            html! { <QueryView token={props.token.clone()} on_logout={props.on_logout.clone()} /> },
            Html::default(),
        ),
        other => (html! { <StubView surface={other} /> }, Html::default()),
    };

    html! {
        <>
            <GlobalStyles />
            <Shell active={route.surface} on_switch={on_switch} search={logout_btn} avatar="DK"
                   list={list} drawer={drawer} />
        </>
    }
}

/// The Ontology surface's `(list, drawer)` pair. Lifted out of `workspace`'s surface
/// `match` whole: the arm reads only the ontology state plus the route, so it moves
/// as-is and takes that nesting (and its row/tab closures) off the hotspot function.
fn ontology_panes(
    types: &UseStateHandle<Vec<String>>,
    type_details: &UseStateHandle<HashMap<String, TypeDetail>>,
    onto_status: &UseStateHandle<LoadStatus>,
    onto_sel: Option<&str>,
    onto_tab: &AttrValue,
    route: &Route,
    navigate: &Navigator,
) -> (Html, Html) {
    // The list highlight is derived: the route holds the type name, the table
    // wants the row's position in the currently loaded list.
    let selected_idx = onto_sel.and_then(|name| types.iter().position(|t| t == name));
    let on_row = {
        let (route, navigate, types) = (route.clone(), navigate.clone(), types.clone());
        // Opening a row IS a navigation → push, so Back closes the drawer.
        // `Route::with_selection` resets the drawer to the surface default,
        // which is why there is no explicit tab reset here.
        Callback::from(move |i: usize| {
            if let Some(name) = types.get(i) {
                navigate.push(route.with_selection(name.clone()));
            }
        })
    };
    let on_tab = {
        let (route, navigate) = (route.clone(), navigate.clone());
        Callback::from(move |t: AttrValue| navigate.replace(route.with_tab(t.as_str())))
    };

    // Build one row per type by zipping the names with their loaded detail. A
    // type whose detail hasn't arrived yet shows an empty backing and "…" props.
    let rows: Vec<OntologyTypeRow> = types
        .iter()
        .map(|name| match type_details.get(name) {
            Some(d) => OntologyTypeRow {
                name: name.clone(),
                backing: format!("{}.{}", d.table_schema, d.table_name),
                props: d.properties.len().to_string(),
            },
            None => OntologyTypeRow {
                name: name.clone(),
                backing: String::new(),
                props: "…".to_string(),
            },
        })
        .collect();

    let list = html! {
        <OntologyList
            rows={rows}
            status={(**onto_status).clone()}
            selected={selected_idx}
            on_row={on_row}
        />
    };
    // Drawer contract: only pass a real drawer when a type is selected;
    // otherwise Html::default() so the Shell hides the drawer region.
    // Gate on the type being in the loaded list (unlike Catalog, whose drawer
    // needs only the id): ontology details are eagerly loaded with the list, so
    // a deep link resolves as soon as it arrives, and an unknown type name shows
    // no drawer rather than a permanent "Loading…".
    let drawer = selected_idx
        .and_then(|i| types.get(i).cloned())
        .map(|name| {
            let detail = type_details.get(&name).cloned();
            html! {
                <OntologyDrawer
                    name={AttrValue::from(name)}
                    detail={detail}
                    active_tab={onto_tab.clone()}
                    on_tab={on_tab}
                />
            }
        })
        .unwrap_or_default();
    (list, drawer)
}

/// The Transforms editor drawer (New / Edit). Lifted out of `workspace`'s drawer
/// `if let` chain whole — the three action callbacks (`on_submit`, `on_run_adhoc`,
/// `on_cancel`) are the bulk of it and touch only editor state, the token and the
/// list reloader.
#[allow(
    clippy::too_many_arguments,
    reason = "mechanical extraction of the drawer branch's captures; each param is a distinct piece of Yew state the editor needs (#617)"
)]
fn transform_editor_drawer(
    form: TransformForm,
    editing: bool,
    tf_editing: UseStateHandle<Option<TransformForm>>,
    tf_errors: UseStateHandle<Vec<FieldError>>,
    tf_server_error: UseStateHandle<Option<AttrValue>>,
    tf_schema: UseStateHandle<CompletionSchema>,
    tf_dataset_options: UseStateHandle<Vec<String>>,
    tf_type_options: UseStateHandle<Vec<String>>,
    token: String,
    on_logout: Callback<()>,
    reload_list: impl Fn() + Clone + 'static,
) -> Html {
    let on_change = {
        let e = tf_editing.clone();
        Callback::from(move |f| e.set(Some(f)))
    };
    // Define (or redefine): validate client-side, POST, then close + refetch list.
    let on_submit = {
        let form = form.clone();
        let editing_state = tf_editing.clone();
        let errors = tf_errors.clone();
        let server_error = tf_server_error.clone();
        let on_logout = on_logout.clone();
        let token = token.clone();
        let base = net::api_base();
        let reload_list = reload_list.clone();
        Callback::from(move |()| {
            errors.set(Vec::new());
            server_error.set(None);
            match form_to_def(&form) {
                Err(errs) => errors.set(errs),
                Ok(def_json) => {
                    let (editing_state, server_error, on_logout) = (
                        editing_state.clone(),
                        server_error.clone(),
                        on_logout.clone(),
                    );
                    let (token, base) = (token.clone(), base.clone());
                    let reload_list = reload_list.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        match net::define_transform(&base, &token, &def_json).await {
                            Ok(()) => {
                                editing_state.set(None);
                                reload_list();
                            }
                            Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                            Err(e) => server_error.set(Some(AttrValue::from(e.to_string()))),
                        }
                    });
                }
            }
        })
    };
    // Ad-hoc run: validate the body, POST, close the editor (result shows in Runs on reselect).
    let on_run_adhoc = {
        let form = form.clone();
        let editing_state = tf_editing.clone();
        let errors = tf_errors.clone();
        let server_error = tf_server_error.clone();
        let on_logout = on_logout.clone();
        let token = token.clone();
        let base = net::api_base();
        Callback::from(move |()| {
            errors.set(Vec::new());
            server_error.set(None);
            match form_to_body(&form) {
                Err(errs) => errors.set(errs),
                Ok(body_json) => {
                    let (editing_state, server_error, on_logout) = (
                        editing_state.clone(),
                        server_error.clone(),
                        on_logout.clone(),
                    );
                    let (token, base) = (token.clone(), base.clone());
                    wasm_bindgen_futures::spawn_local(async move {
                        match net::run_adhoc(&base, &token, &body_json).await {
                            Ok(_run_id) => editing_state.set(None),
                            Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                            Err(e) => server_error.set(Some(AttrValue::from(e.to_string()))),
                        }
                    });
                }
            }
        })
    };
    // Cancel must clear the editor's validation + server errors, not just close
    // the editor: `tf_server_error` is SHARED with the drawer's action-error
    // line, so a failed Save/Run-ad-hoc left in that slot would render beside
    // the drawer's Run/Delete buttons as though a Run or Delete had failed.
    // (Mirrors what on_edit/on_new already do on the way in.)
    let on_cancel = {
        let e = tf_editing.clone();
        let errors = tf_errors.clone();
        let server_error = tf_server_error.clone();
        Callback::from(move |()| {
            errors.set(Vec::new());
            server_error.set(None);
            e.set(None);
        })
    };
    html! {
        <TransformEditor form={form} schema={(*tf_schema).clone()}
            editing={editing}
            dataset_options={(*tf_dataset_options).clone()}
            type_options={(*tf_type_options).clone()}
            errors={(*tf_errors).clone()} server_error={(*tf_server_error).clone()}
            on_change={on_change} on_submit={on_submit}
            on_run_adhoc={on_run_adhoc} on_cancel={on_cancel} />
    }
}

/// The Run-saved-transform action body, lifted out of the drawer's `on_run` callback
/// (the callback keeps only the re-clone tuple + `spawn_local`). On success clear
/// `tf_runs` and bump the runs epoch, then open the Runs tab — but only if the LIVE
/// route still points at this transform.
#[allow(
    clippy::too_many_arguments,
    reason = "mechanical extraction of the on_run callback's captures; each param is a distinct piece of Yew state the action needs (#617)"
)]
async fn run_saved_transform(
    base: String,
    token: String,
    name: String,
    tf_runs: UseStateHandle<Vec<RunRow>>,
    tf_runs_epoch: UseStateHandle<u64>,
    tf_runs_epoch_ref: Rc<RefCell<u64>>,
    navigate: Navigator,
    server_error: UseStateHandle<Option<AttrValue>>,
    on_logout: Callback<()>,
) {
    match net::run_transform(&base, &token, &name).await {
        Err(net::FetchError::Unauthorized) => on_logout.emit(()),
        other => {
            let eff = run_action_effect(other.map(|_run_id| ()).map_err(|e| e.to_string()));
            server_error.set(eff.error.map(AttrValue::from));
            if eff.refetch_runs {
                tf_runs.set(Vec::new());
                // Bump the authoritative ref, then mirror it into the dep-tuple
                // state — never `*tf_runs_epoch + 1` (a stale render snapshot; see
                // the declaration in `workspace`).
                let next = bump_epoch(&mut tf_runs_epoch_ref.borrow_mut());
                tf_runs_epoch.set(next);
            }
            if eff.open_runs_tab {
                // Read the LIVE route: the user may have navigated away while the
                // run was in flight, and yanking them back would be wrong.
                let live = router::current_route();
                if live.selection_on(Surface::Transforms) == Some(name.as_str()) {
                    navigate.replace(live.with_tab("runs"));
                }
            }
        }
    }
}

/// The Delete-transform action body, lifted out of the drawer's `on_delete` callback.
/// Both the route clear and the `tf_def` clear sit behind the LIVE-route check: if the
/// user selected a different transform while the DELETE was in flight, collapsing the
/// def would blank *that* transform's drawer with nothing to refetch it. The list
/// reload is unconditional — the deleted row must leave the list either way.
#[allow(
    clippy::too_many_arguments,
    reason = "mechanical extraction of the on_delete callback's captures; each param is a distinct piece of Yew state the action needs (#617)"
)]
async fn delete_saved_transform(
    base: String,
    token: String,
    name: String,
    tf_def: UseStateHandle<Option<TransformDefView>>,
    navigate: Navigator,
    server_error: UseStateHandle<Option<AttrValue>>,
    on_logout: Callback<()>,
    reload_list: impl Fn() + 'static,
) {
    match net::delete_transform(&base, &token, &name).await {
        Err(net::FetchError::Unauthorized) => on_logout.emit(()),
        other => {
            let eff = delete_action_effect(other.map_err(|e| e.to_string()));
            server_error.set(eff.error.map(AttrValue::from));
            if eff.clear_selection {
                // Read the LIVE route, like run_saved_transform: only clear the
                // selection if it is still this transform.
                let live = router::current_route();
                if live.selection_on(Surface::Transforms) == Some(name.as_str()) {
                    navigate.replace(live.cleared());
                    // Redundant given the def effect clears it off the route change,
                    // but harmless — and it must not run for a different selection.
                    tf_def.set(None);
                }
                reload_list();
            }
        }
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

/// Invert `parse_transform_def` for the Edit path: seed a `TransformForm` from a
/// loaded definition. Physical inputs/output round-trip through `"schema.name"`.
fn form_from_def(def: &TransformDefView) -> TransformForm {
    let (kind, inputs, output) = match &def.body.io {
        TransformIo::Physical { inputs, output } => (
            TransformKind::Physical,
            inputs
                .iter()
                .map(|t| format!("{}.{}", t.schema, t.name))
                .collect(),
            format!("{}.{}", output.schema, output.name),
        ),
        TransformIo::Typed { inputs, output } => {
            (TransformKind::Typed, inputs.clone(), output.clone())
        }
    };
    TransformForm {
        kind,
        name: def.name.clone(),
        inputs,
        output,
        sql: def.body.sql.clone(),
        schedule: def.schedule.clone().unwrap_or_default(),
        on_input_commit: def.on_input_commit,
        output_mode: def.body.output_mode,
    }
}

/// Fetch each input's schema and assemble the input-scoped SQL completion schema.
/// Best-effort: an input that fails to load is simply omitted from completions.
async fn build_input_schema(
    base: &str,
    token: &str,
    kind: TransformKind,
    inputs: &[String],
) -> CompletionSchema {
    match kind {
        TransformKind::Physical => {
            let mut pairs = Vec::new();
            for input in inputs {
                let (schema, name) = input.split_once('.').unwrap_or(("", input.as_str()));
                if let Ok(detail) = net::fetch_dataset_detail(base, token, schema, name).await {
                    pairs.push((
                        TableRef {
                            schema: schema.to_string(),
                            name: name.to_string(),
                        },
                        detail,
                    ));
                }
            }
            schema_from_dataset_details(&pairs)
        }
        TransformKind::Typed => {
            let mut pairs = Vec::new();
            for ty in inputs {
                if let Ok(detail) = net::fetch_type_detail(base, token, ty).await {
                    pairs.push((ty.clone(), detail));
                }
            }
            schema_from_types(&pairs)
        }
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
