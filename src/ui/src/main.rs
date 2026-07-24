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
    AuthError, BadgeTone, ButtonVariant, CatalogSortDir, CompletionSchema, DatasetDetail,
    DatasetRow, DatasetSort, FetchGeneration, FieldError, PreviewData, RunRow, Surface, TableRef,
    TransformDefView, TransformForm, TransformIo, TransformKind, TransformSummary, TypeDetail,
    bump_epoch, delete_action_effect, distinct_projects, form_to_body, form_to_def,
    run_action_effect, schema_from_dataset_details, schema_from_types,
};
use net::FetchError;
use std::collections::HashMap;
use stylist::yew::styled_component;
use surfaces::{
    CatalogControls, CatalogDrawer, CatalogList, LoadStatus, OntologyDrawer, OntologyList,
    OntologyTypeRow, TransformDrawer, TransformEditor, TransformsList,
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

#[derive(Properties, PartialEq)]
struct WorkspaceProps {
    token: AttrValue,
    on_logout: Callback<()>,
}

#[function_component(Workspace)]
fn workspace(props: &WorkspaceProps) -> Html {
    let surface = use_state(|| Surface::Catalog);

    // Ontology surface state: the list of type names, each type's loaded `TypeDetail`
    // (schema) keyed by name, the selected type index, and the drawer's active tab.
    let types = use_state(Vec::<String>::new);
    let onto_status = use_state(|| LoadStatus::Idle);
    let type_details = use_state(HashMap::<String, TypeDetail>::new);
    let onto_selected = use_state(|| Option::<usize>::None);
    let onto_tab = use_state(|| AttrValue::from("properties"));

    // Catalog surface state: the dataset list plus the selected dataset's detail,
    // lazily-loaded preview, and active drawer tab.
    let datasets = use_state(Vec::<DatasetRow>::new);
    let catalog_status = use_state(|| LoadStatus::Idle);
    let selected_dataset = use_state(|| Option::<usize>::None);
    let detail = use_state(|| Option::<DatasetDetail>::None);
    let preview = use_state(|| Option::<PreviewData>::None);
    let preview_loading = use_state(|| false);
    let detail_error = use_state(|| Option::<String>::None);
    let preview_error = use_state(|| Option::<String>::None);
    let catalog_tab = use_state(|| AttrValue::from("schema"));
    // Server-side sort/filter controls: the active sort key + direction, the active
    // project filter (None = "All"), the distinct project chip options (refreshed on
    // unfiltered loads only, so the chip set stays stable while filtered), and a
    // generation guard against out-of-order responses when controls change quickly.
    let catalog_sort = use_state(|| DatasetSort::Name);
    let catalog_dir = use_state(|| CatalogSortDir::Asc);
    let catalog_project = use_state(|| Option::<String>::None);
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
    let tf_selected = use_state(|| Option::<usize>::None);
    let tf_def = use_state(|| Option::<TransformDefView>::None);
    let tf_drawer_tab = use_state(|| AttrValue::from("definition"));
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
        let sort = *catalog_sort;
        let dir = *catalog_dir;
        let project = (*catalog_project).clone();
        use_effect_with((sort, dir, project.clone()), move |(sort, dir, project)| {
            catalog_status.set(LoadStatus::Loading);
            let query = loom_ui_core::dataset_list_query(*sort, *dir, project.as_deref());
            let project_is_all = project.is_none();
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
            || ()
        });
    }

    // On dataset row-select: reset the drawer to the Schema tab, clear the previous
    // detail/preview, and load the selected dataset's schema detail.
    {
        let detail = detail.clone();
        let preview = preview.clone();
        let preview_loading = preview_loading.clone();
        let detail_error = detail_error.clone();
        let preview_error = preview_error.clone();
        let catalog_tab = catalog_tab.clone();
        let lineage = lineage.clone();
        let show_full_lineage = show_full_lineage.clone();
        let datasets = datasets.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let fetch_gen = fetch_gen.clone();
        let selected_dep = *selected_dataset;
        use_effect_with(selected_dep, move |sel| {
            let Some(ds) = sel.and_then(|i| datasets.get(i).cloned()) else {
                return;
            };
            detail.set(None);
            preview.set(None);
            preview_loading.set(false);
            detail_error.set(None);
            preview_error.set(None);
            lineage.set(None);
            show_full_lineage.set(false);
            catalog_tab.set(AttrValue::from("schema"));
            // Any in-flight fetch from the previous selection is now stale.
            let my_gen = fetch_gen.borrow_mut().bump();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_dataset_detail(&net::api_base(), &token, &ds.schema, &ds.name)
                    .await
                {
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
        let datasets = datasets.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let fetch_gen = fetch_gen.clone();
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
            let my_gen = fetch_gen.borrow().current();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_preview(&net::api_base(), &token, &ds.schema, &ds.name, 50).await {
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
        let datasets = datasets.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let fetch_gen = fetch_gen.clone();
        let already_loaded = lineage.is_some();
        let dep = (*selected_dataset, (*catalog_tab).clone());
        use_effect_with(dep, move |(sel, tab)| {
            if tab.as_str() != "lineage" || already_loaded {
                return;
            }
            let Some(ds) = sel.and_then(|i| datasets.get(i).cloned()) else {
                return;
            };
            let my_gen = fetch_gen.borrow().current();
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
                if fetch_gen.borrow().is_current(my_gen) {
                    lineage.set(Some((up.unwrap_or_default(), down.unwrap_or_default())));
                }
            });
        });
    }

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

    // On transform row-select: reset the drawer + clear the previous def/runs/editor,
    // then load the selected transform's definition. Guarded by `tf_def_gen` so a
    // stale in-flight fetch never overwrites a newer selection's def.
    {
        let tf_def = tf_def.clone();
        let tf_drawer_tab = tf_drawer_tab.clone();
        let tf_editing = tf_editing.clone();
        let tf_edit_name = tf_edit_name.clone();
        let tf_server_error = tf_server_error.clone();
        let tf_runs = tf_runs.clone();
        let transforms = transforms.clone();
        let tf_def_gen = tf_def_gen.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        let on_logout = props.on_logout.clone();
        let selected = *tf_selected;
        use_effect_with(selected, move |selected| {
            *tf_def_gen.borrow_mut() += 1;
            let my_gen = *tf_def_gen.borrow();
            tf_def.set(None);
            tf_editing.set(None);
            tf_edit_name.set(None);
            tf_server_error.set(None); // a stale action error never leaks onto a new selection
            tf_runs.set(Vec::new()); // force the runs tab to refetch for the new selection
            tf_drawer_tab.set(AttrValue::from("definition"));
            if let Some(idx) = *selected
                && let Some(row) = transforms.get(idx)
            {
                let name = row.name.clone();
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
            }
            || ()
        });
    }

    // Lazy runs fetch: only when the Runs tab is active for the selected transform and
    // no runs are loaded yet (the row-select effect clears `tf_runs`). Keyed on
    // (selection, active tab); guarded by its own `tf_runs_gen`.
    {
        let tf_runs = tf_runs.clone();
        let tf_runs_status = tf_runs_status.clone();
        let transforms = transforms.clone();
        let tf_runs_gen = tf_runs_gen.clone();
        let token = props.token.to_string();
        let base = net::api_base();
        let on_logout = props.on_logout.clone();
        let already_loaded = !tf_runs.is_empty();
        let dep = (*tf_selected, (*tf_drawer_tab).clone(), *tf_runs_epoch);
        use_effect_with(dep, move |(sel, tab, _epoch)| {
            if tab.as_str() != "runs" || already_loaded {
                return;
            }
            let Some(name) = sel.and_then(|i| transforms.get(i).map(|r| r.name.clone())) else {
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
            // Control-change callbacks also reset `selected_dataset`: reordering or
            // filtering the list shifts row indices, so a stale selection index could
            // point at the wrong dataset (or none) after the next fetch lands.
            let on_sort = {
                let catalog_sort = catalog_sort.clone();
                let selected_dataset = selected_dataset.clone();
                Callback::from(move |s: DatasetSort| {
                    catalog_sort.set(s);
                    selected_dataset.set(None);
                })
            };
            let on_dir = {
                let catalog_dir = catalog_dir.clone();
                let selected_dataset = selected_dataset.clone();
                Callback::from(move |d: CatalogSortDir| {
                    catalog_dir.set(d);
                    selected_dataset.set(None);
                })
            };
            let on_project = {
                let catalog_project = catalog_project.clone();
                let selected_dataset = selected_dataset.clone();
                Callback::from(move |p: Option<String>| {
                    catalog_project.set(p);
                    selected_dataset.set(None);
                })
            };
            let list = html! {
                <>
                    <CatalogControls
                        projects={(*catalog_all_projects).clone()}
                        active_project={(*catalog_project).clone()}
                        sort={*catalog_sort}
                        dir={*catalog_dir}
                        on_project={on_project}
                        on_sort={on_sort}
                        on_dir={on_dir}
                    />
                    <CatalogList
                        datasets={(*datasets).clone()}
                        status={(*catalog_status).clone()}
                        selected={*selected_dataset}
                        on_row={on_row}
                    />
                </>
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
                            detail_error={(*detail_error).clone()}
                            preview_error={(*preview_error).clone()}
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
            let on_row = {
                let onto_selected = onto_selected.clone();
                let onto_tab = onto_tab.clone();
                Callback::from(move |i: usize| {
                    onto_selected.set(Some(i));
                    onto_tab.set(AttrValue::from("properties"));
                })
            };
            let on_tab = {
                let onto_tab = onto_tab.clone();
                Callback::from(move |t: AttrValue| onto_tab.set(t))
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
                    status={(*onto_status).clone()}
                    selected={*onto_selected}
                    on_row={on_row}
                />
            };
            // Drawer contract: only pass a real drawer when a type is selected;
            // otherwise Html::default() so the Shell hides the drawer region.
            let drawer = (*onto_selected)
                .and_then(|i| types.get(i).cloned())
                .map(|name| {
                    let detail = type_details.get(&name).cloned();
                    html! {
                        <OntologyDrawer
                            name={AttrValue::from(name)}
                            detail={detail}
                            active_tab={(*onto_tab).clone()}
                            on_tab={on_tab}
                        />
                    }
                })
                .unwrap_or_default();
            (list, drawer)
        }
        Surface::Transforms => {
            let on_row = {
                let s = tf_selected.clone();
                Callback::from(move |i| s.set(Some(i)))
            };
            let on_new = {
                let editing = tf_editing.clone();
                let edit_name = tf_edit_name.clone();
                let selected = tf_selected.clone();
                let errors = tf_errors.clone();
                let server_error = tf_server_error.clone();
                Callback::from(move |()| {
                    selected.set(None);
                    edit_name.set(None); // New, not Edit
                    errors.set(Vec::new());
                    server_error.set(None);
                    editing.set(Some(TransformForm::default()));
                })
            };
            let list = html! {
                <TransformsList rows={(*transforms).clone()} status={(*tf_status).clone()}
                    selected={*tf_selected} on_row={on_row} on_new={on_new} forbidden={*tf_forbidden} />
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
                let on_change = {
                    let e = tf_editing.clone();
                    Callback::from(move |f| e.set(Some(f)))
                };
                let editing = tf_edit_name.is_some();
                // Define (or redefine): validate client-side, POST, then close + refetch list.
                let on_submit = {
                    let form = form.clone();
                    let editing_state = tf_editing.clone();
                    let errors = tf_errors.clone();
                    let server_error = tf_server_error.clone();
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
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
                                        Err(e) => {
                                            server_error.set(Some(AttrValue::from(e.to_string())))
                                        }
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
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
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
                                        Err(e) => {
                                            server_error.set(Some(AttrValue::from(e.to_string())))
                                        }
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
            } else if let Some(def) = (*tf_def).clone() {
                let on_tab = {
                    let t = tf_drawer_tab.clone();
                    Callback::from(move |id| t.set(id))
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
                    let tf_drawer_tab = tf_drawer_tab.clone();
                    let server_error = tf_server_error.clone();
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
                    let base = net::api_base();
                    Callback::from(move |()| {
                        server_error.set(None);
                        let (name, tf_runs, tf_runs_epoch, tf_drawer_tab) = (
                            name.clone(),
                            tf_runs.clone(),
                            tf_runs_epoch.clone(),
                            tf_drawer_tab.clone(),
                        );
                        let tf_runs_epoch_ref = tf_runs_epoch_ref.clone();
                        let (server_error, on_logout) = (server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        wasm_bindgen_futures::spawn_local(async move {
                            match net::run_transform(&base, &token, &name).await {
                                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                                other => {
                                    let eff = run_action_effect(
                                        other.map(|_run_id| ()).map_err(|e| e.to_string()),
                                    );
                                    server_error.set(eff.error.map(AttrValue::from));
                                    if eff.refetch_runs {
                                        tf_runs.set(Vec::new());
                                        // Bump the authoritative ref, then mirror it into the
                                        // dep-tuple state — never `*tf_runs_epoch + 1` (a stale
                                        // render snapshot; see the declaration above).
                                        let next = bump_epoch(&mut tf_runs_epoch_ref.borrow_mut());
                                        tf_runs_epoch.set(next);
                                    }
                                    if eff.open_runs_tab {
                                        tf_drawer_tab.set(AttrValue::from("runs"));
                                    }
                                }
                            }
                        });
                    })
                };
                // Delete → on success clear selection + refetch list; on a non-401 failure
                // surface the message beside the buttons (row + drawer stay put).
                let on_delete = {
                    let name = def.name.clone();
                    let tf_selected = tf_selected.clone();
                    let tf_def = tf_def.clone();
                    let server_error = tf_server_error.clone();
                    let on_logout = props.on_logout.clone();
                    let token = props.token.to_string();
                    let base = net::api_base();
                    let reload_list = reload_list.clone();
                    Callback::from(move |()| {
                        server_error.set(None);
                        let (name, tf_selected, tf_def) =
                            (name.clone(), tf_selected.clone(), tf_def.clone());
                        let (server_error, on_logout) = (server_error.clone(), on_logout.clone());
                        let (token, base) = (token.clone(), base.clone());
                        let reload_list = reload_list.clone();
                        wasm_bindgen_futures::spawn_local(async move {
                            match net::delete_transform(&base, &token, &name).await {
                                Err(net::FetchError::Unauthorized) => on_logout.emit(()),
                                other => {
                                    let eff =
                                        delete_action_effect(other.map_err(|e| e.to_string()));
                                    server_error.set(eff.error.map(AttrValue::from));
                                    if eff.clear_selection {
                                        tf_selected.set(None);
                                        tf_def.set(None);
                                        reload_list();
                                    }
                                }
                            }
                        });
                    })
                };
                html! {
                    <TransformDrawer def={def} active_tab={(*tf_drawer_tab).clone()}
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
