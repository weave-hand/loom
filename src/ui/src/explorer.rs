//! The object-explorer view: a type sidebar, a paginated object table, and a
//! detail drawer for the selected row. Rendered as the authenticated view once
//! `main.rs` has a session token.

use crate::net::{self, FetchError};
use loom_ui_components::{
    Button, Column, DataTable, GlobalStyles, NavItem, Panel, TabItem, TableRow, Tabs, TopNav,
};
use loom_ui_core::{Align, ButtonVariant, cell_to_string, columns_from_objects};
use serde_json::{Map, Value};
use yew::prelude::*;

#[derive(Clone, PartialEq)]
struct ObjectRow {
    cells: Vec<String>,
}

impl TableRow for ObjectRow {
    fn cells(&self) -> Vec<Html> {
        self.cells.iter().map(|c| html! { { c.clone() } }).collect()
    }
}

fn to_rows(objs: &[Map<String, Value>], columns: &[String]) -> Vec<ObjectRow> {
    objs.iter()
        .map(|o| ObjectRow {
            cells: columns
                .iter()
                .map(|c| o.get(c).map(cell_to_string).unwrap_or_default())
                .collect(),
        })
        .collect()
}

fn to_columns(columns: &[String]) -> Vec<Column> {
    columns
        .iter()
        .map(|name| Column {
            label: name.clone().into(),
            align: Align::Start,
        })
        .collect()
}

#[derive(Clone, PartialEq)]
enum LoadStatus {
    Idle,
    Loading,
    Error(String),
}

#[derive(Properties, PartialEq)]
pub struct ExplorerProps {
    pub token: AttrValue,
    pub on_logout: Callback<()>,
}

#[function_component(Explorer)]
pub fn explorer(props: &ExplorerProps) -> Html {
    let types = use_state(Vec::<String>::new);
    let selected_type = use_state(|| Option::<String>::None);
    let objs = use_state(Vec::<Map<String, Value>>::new);
    let columns = use_state(Vec::<String>::new);
    let next = use_state(|| Option::<String>::None);
    let selected_row = use_state(|| Option::<usize>::None);
    let status = use_state(|| LoadStatus::Idle);

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
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_page(&net::api_base(), &token, &ty, None, 50).await {
                    Ok(page) => {
                        columns.set(columns_from_objects(&page.rows));
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

    let on_select_type = {
        let selected_type = selected_type.clone();
        Callback::from(move |ty: String| selected_type.set(Some(ty)))
    };

    let on_load_more = {
        let objs = objs.clone();
        let columns = columns.clone();
        let next = next.clone();
        let status = status.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        let selected_type = selected_type.clone();
        Callback::from(move |_: MouseEvent| {
            let Some(ty) = (*selected_type).clone() else {
                return;
            };
            let cursor = (*next).clone();
            let (objs, columns, next, status, on_logout) = (
                objs.clone(),
                columns.clone(),
                next.clone(),
                status.clone(),
                on_logout.clone(),
            );
            let token = token.clone();
            wasm_bindgen_futures::spawn_local(async move {
                match net::fetch_page(&net::api_base(), &token, &ty, cursor.as_deref(), 50).await {
                    Ok(page) => {
                        let mut merged = (*objs).clone();
                        merged.extend(page.rows);
                        columns.set(columns_from_objects(&merged));
                        objs.set(merged);
                        next.set(page.next);
                    }
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => status.set(LoadStatus::Error(e.to_string())),
                }
            });
        })
    };

    let on_row = {
        let selected_row = selected_row.clone();
        Callback::from(move |i: usize| selected_row.set(Some(i)))
    };

    let on_logout_click = {
        let on_logout = props.on_logout.clone();
        Callback::from(move |_: MouseEvent| on_logout.emit(()))
    };

    let logout_button = html! {
        <Button variant={ButtonVariant::Ghost} onclick={on_logout_click}>{ "Log out" }</Button>
    };

    let table_area = if matches!(*status, LoadStatus::Loading) {
        html! { <p>{ "Loading…" }</p> }
    } else if let LoadStatus::Error(m) = &*status {
        html! { <p class="error">{ m.clone() }</p> }
    } else if selected_type.is_none() {
        html! { <p>{ "Select a type to browse its objects." }</p> }
    } else if objs.is_empty() {
        html! { <p>{ "No objects." }</p> }
    } else {
        let rows = to_rows(&objs, &columns);
        html! {
            <>
                <DataTable<ObjectRow>
                    columns={to_columns(&columns)}
                    rows={rows}
                    selected={*selected_row}
                    onrow={on_row}
                />
                if next.is_some() {
                    <Button variant={ButtonVariant::Secondary} onclick={on_load_more}>{ "Load more" }</Button>
                }
            </>
        }
    };

    let drawer = if let Some(i) = *selected_row {
        objs.get(i).map(|obj| {
            let title: AttrValue = selected_type
                .as_ref()
                .map_or_else(|| "object".to_string(), Clone::clone)
                .into();
            html! {
                <Panel title={title}>
                    <Tabs tabs={vec![TabItem { id: "object".into(), label: "Object".into() }]} active="object" />
                    <dl>
                        { for obj.iter().map(|(k, v)| html! {
                            <>
                                <dt>{ k.clone() }</dt>
                                <dd>{ cell_to_string(v) }</dd>
                            </>
                        }) }
                    </dl>
                </Panel>
            }
        })
    } else {
        None
    };

    html! {
        <>
            <GlobalStyles />
            <TopNav items={Vec::<NavItem>::new()} search={logout_button} avatar="·" />
            <div style="display: flex; gap: 16px; padding: 16px;">
                <div style="width: 200px;">
                    <Panel title="Types">
                        <ul style="list-style: none; margin: 0; padding: 0;">
                            { for types.iter().map(|ty| {
                                let active = selected_type.as_deref() == Some(ty.as_str());
                                let onclick = {
                                    let on_select_type = on_select_type.clone();
                                    let ty = ty.clone();
                                    Callback::from(move |_: MouseEvent| on_select_type.emit(ty.clone()))
                                };
                                html! {
                                    <li style={if active { "font-weight: 600; cursor: pointer;" } else { "cursor: pointer;" }} {onclick}>
                                        { ty.clone() }
                                    </li>
                                }
                            }) }
                        </ul>
                    </Panel>
                </div>
                <div style="flex: 1;">{ table_area }</div>
                if let Some(d) = drawer {
                    <div style="width: 320px;">{ d }</div>
                }
            </div>
        </>
    }
}
