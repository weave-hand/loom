//! The Ontology surface, re-homed from the old top-level `Explorer` into the
//! `Shell`. Two presentational components: `OntologyList` (type sidebar + paginated
//! objects table with Load-more) fills the Shell's `list` slot, and `OntologyDrawer`
//! (Properties · Links tabs over the selected type's `TypeDetail`) fills its `drawer`
//! slot. All interactive state and load effects live in `Workspace` (`main.rs`); these
//! components are driven entirely by props. The object-loading types below
//! (`ObjectRow`/`to_rows`/`to_columns`/`LoadStatus`) are ported verbatim from
//! `explorer.rs`.

use loom_ui_components::{Button, Column, DataTable, Panel, TabItem, TableRow, Tabs};
use loom_ui_core::{Align, ButtonVariant, TypeDetail, cell_to_string};
use serde_json::{Map, Value};
use stylist::yew::styled_component;
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

/// The load state of the objects table for the selected type.
#[derive(Clone, PartialEq)]
pub enum LoadStatus {
    Idle,
    Loading,
    Error(String),
}

#[derive(Properties, PartialEq)]
pub struct OntologyListProps {
    pub types: Vec<String>,
    pub selected_type: Option<String>,
    pub objs: Vec<Map<String, Value>>,
    pub columns: Vec<String>,
    pub status: LoadStatus,
    pub has_next: bool,
    pub selected_row: Option<usize>,
    pub loading_more: bool,
    pub load_more_error: Option<String>,
    pub on_select_type: Callback<String>,
    pub on_row: Callback<usize>,
    pub on_load_more: Callback<MouseEvent>,
}

/// The type sidebar + paginated objects table with Load-more. Rendered into the
/// `Shell`'s `list` slot. Behaviour ported verbatim from `explorer.rs`.
#[function_component(OntologyList)]
pub fn ontology_list(props: &OntologyListProps) -> Html {
    let table_area = if matches!(props.status, LoadStatus::Loading) {
        html! { <p>{ "Loading…" }</p> }
    } else if let LoadStatus::Error(m) = &props.status {
        html! { <p class="error">{ m.clone() }</p> }
    } else if props.selected_type.is_none() {
        html! { <p>{ "Select a type to browse its objects." }</p> }
    } else if props.objs.is_empty() {
        html! { <p>{ "No objects." }</p> }
    } else {
        let rows = to_rows(&props.objs, &props.columns);
        html! {
            <>
                <DataTable<ObjectRow>
                    columns={to_columns(&props.columns)}
                    rows={rows}
                    selected={props.selected_row}
                    onrow={props.on_row.clone()}
                />
                if props.has_next {
                    <div>
                        <Button variant={ButtonVariant::Secondary} disabled={props.loading_more} onclick={props.on_load_more.clone()}>{ "Load more" }</Button>
                        if let Some(m) = &props.load_more_error {
                            <p class="error">{ m.clone() }</p>
                        }
                    </div>
                }
            </>
        }
    };

    html! {
        <div style="display: flex; gap: 16px;">
            <div style="width: 200px;">
                <Panel title="Types">
                    <ul style="list-style: none; margin: 0; padding: 0;">
                        { for props.types.iter().map(|ty| {
                            let active = props.selected_type.as_deref() == Some(ty.as_str());
                            let onclick = {
                                let on_select_type = props.on_select_type.clone();
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
        </div>
    }
}

#[derive(Properties, PartialEq)]
pub struct OntologyDrawerProps {
    pub selected_type: Option<String>,
    pub selected_obj: Map<String, Value>,
    pub detail: Option<TypeDetail>,
    pub active_tab: AttrValue,
    pub on_tab: Callback<AttrValue>,
}

/// The detail drawer for the selected object: `Tabs` (Properties · Links) over the
/// selected type's `TypeDetail`. Rendered into the `Shell`'s `drawer` slot only when
/// a row is selected.
#[styled_component(OntologyDrawer)]
pub fn ontology_drawer(props: &OntologyDrawerProps) -> Html {
    let cls = css!(
        r#"
        padding: 4px 12px 12px;
        .prop, .link { display: flex; gap: 8px; align-items: baseline;
                       padding: 7px 0; border-bottom: 1px solid var(--loom-border); font-size: 13px; }
        .pname, .lname { color: var(--loom-accent); font-weight: 500; }
        .pty { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
               color: var(--loom-text-mut); font-size: 12px; }
        .req { color: var(--loom-danger); font-size: 11px; }
        .pval { margin-left: auto; color: var(--loom-text); max-width: 55%;
                overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
        .card { color: var(--loom-text-mut); font-size: 12px; }
        .section { font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em;
                   color: var(--loom-text-mut); margin: 14px 0 2px; }
        .empty { color: var(--loom-text-mut); font-size: 13px; padding: 8px 0; }
    "#
    );

    let title: AttrValue = props
        .selected_type
        .as_ref()
        .map_or_else(|| "object".to_string(), Clone::clone)
        .into();

    let tabs = vec![
        TabItem {
            id: "properties".into(),
            label: "Properties".into(),
        },
        TabItem {
            id: "links".into(),
            label: "Links".into(),
        },
    ];

    let body = if props.active_tab.as_str() == "links" {
        links_body(props.detail.as_ref())
    } else {
        properties_body(props.detail.as_ref(), &props.selected_obj)
    };

    html! {
        <Panel title={title}>
            <Tabs tabs={tabs} active={props.active_tab.clone()} onselect={props.on_tab.clone()} />
            <div class={cls}>{ body }</div>
        </Panel>
    }
}

/// The Properties tab: each `PropRow` as `name  ty  [required]  = value`, with the
/// property name in the accent colour and the type in monospace. The value column is
/// pulled from the selected object.
fn properties_body(detail: Option<&TypeDetail>, obj: &Map<String, Value>) -> Html {
    let Some(detail) = detail else {
        return html! { <p class="empty">{ "Loading…" }</p> };
    };
    if detail.properties.is_empty() {
        return html! { <p class="empty">{ "No properties." }</p> };
    }
    html! {
        { for detail.properties.iter().map(|p| {
            let value = obj.get(&p.name).map(cell_to_string).unwrap_or_default();
            html! {
                <div class="prop">
                    <span class="pname">{ p.name.clone() }</span>
                    <span class="pty">{ p.ty.clone() }</span>
                    if p.required { <span class="req">{ "required" }</span> }
                    <span class="pval">{ value }</span>
                </div>
            }
        }) }
    }
}

/// The Links tab: outbound `links` (`name → target · cardinality`) and inbound
/// `links_to` (`name ← source · cardinality`).
fn links_body(detail: Option<&TypeDetail>) -> Html {
    let Some(detail) = detail else {
        return html! { <p class="empty">{ "Loading…" }</p> };
    };
    if detail.links.is_empty() && detail.links_to.is_empty() {
        return html! { <p class="empty">{ "No links." }</p> };
    }
    html! {
        <>
            if !detail.links.is_empty() {
                <div class="section">{ "Outbound" }</div>
                { for detail.links.iter().map(|l| html! {
                    <div class="link">
                        <span class="lname">{ l.name.clone() }</span>
                        <span class="card">{ format!("→ {} · {}", l.to, l.cardinality) }</span>
                    </div>
                }) }
            }
            if !detail.links_to.is_empty() {
                <div class="section">{ "Inbound" }</div>
                { for detail.links_to.iter().map(|l| html! {
                    <div class="link">
                        <span class="lname">{ l.name.clone() }</span>
                        <span class="card">{ format!("← {} · {}", l.from, l.cardinality) }</span>
                    </div>
                }) }
            }
        </>
    }
}
