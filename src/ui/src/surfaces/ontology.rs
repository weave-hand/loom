//! The Ontology surface as a **type/model browser**. Two presentational
//! components: `OntologyList` (a `DataTable` of object *types* — name, backing
//! dataset, property count) fills the Shell's `list` slot, and `OntologyDrawer`
//! (Properties · Links tabs over the selected type's `TypeDetail` *schema*) fills
//! its `drawer` slot. This surface browses the semantic *model*, not object
//! instances — the drawer shows a type's properties and links, never row data. All
//! interactive state and load effects live in `Workspace` (`main.rs`); these
//! components are driven entirely by props.

use loom_ui_components::{Column, DataTable, Panel, TabItem, TableRow, Tabs};
use loom_ui_core::{Align, TypeDetail};
use stylist::yew::styled_component;
use yew::prelude::*;

/// The load state of the ontology type list.
#[derive(Clone, PartialEq)]
pub enum LoadStatus {
    Idle,
    Loading,
    Error(String),
}

/// One row of the Ontology type list: the type `name`, its `backing` dataset
/// (`"{schema}.{name}"`), and its property count as a string. The Instances column
/// is always rendered as `"—"` (no cheap per-type count is available yet).
#[derive(Clone, PartialEq)]
pub struct OntologyTypeRow {
    pub name: String,
    pub backing: String,
    pub props: String,
}

impl TableRow for OntologyTypeRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <span>{ &self.name }</span> },
            html! { { &self.backing } },
            html! { { &self.props } },
            html! { { "—" } },
        ]
    }
}

fn type_columns() -> Vec<Column> {
    vec![
        Column {
            label: "Type".into(),
            align: Align::Start,
        },
        Column {
            label: "Backing dataset".into(),
            align: Align::Start,
        },
        Column {
            label: "Props".into(),
            align: Align::End,
        },
        Column {
            label: "Instances".into(),
            align: Align::End,
        },
    ]
}

#[derive(Properties, PartialEq)]
pub struct OntologyListProps {
    pub rows: Vec<OntologyTypeRow>,
    pub status: LoadStatus,
    pub selected: Option<usize>,
    pub on_row: Callback<usize>,
}

/// The types table (Type · Backing dataset · Props · Instances). Rendered into the
/// `Shell`'s `list` slot; selecting a row selects that type (driving the drawer).
#[function_component(OntologyList)]
pub fn ontology_list(props: &OntologyListProps) -> Html {
    let body = if matches!(props.status, LoadStatus::Loading) {
        html! { <p>{ "Loading…" }</p> }
    } else if let LoadStatus::Error(m) = &props.status {
        html! { <p class="error">{ m.clone() }</p> }
    } else if props.rows.is_empty() {
        html! { <p>{ "No object types." }</p> }
    } else {
        html! {
            <DataTable<OntologyTypeRow>
                columns={type_columns()}
                rows={props.rows.clone()}
                selected={props.selected}
                onrow={props.on_row.clone()}
            />
        }
    };
    html! { <Panel title="Object types">{ body }</Panel> }
}

#[derive(Properties, PartialEq)]
pub struct OntologyDrawerProps {
    pub name: AttrValue,
    pub detail: Option<TypeDetail>,
    pub active_tab: AttrValue,
    pub on_tab: Callback<AttrValue>,
}

/// The detail drawer for the selected type: `Tabs` (Properties · Links) over the
/// type's `TypeDetail` schema. Rendered into the `Shell`'s `drawer` slot only when a
/// type is selected. The header sub-line names the backing dataset and, when present,
/// the identity (key) property.
#[styled_component(OntologyDrawer)]
pub fn ontology_drawer(props: &OntologyDrawerProps) -> Html {
    let cls = css!(
        r#"
        padding: 4px 12px 12px;
        .sub { color: var(--loom-text-mut); font-size: 12px; margin: 0 0 8px; }
        .prop, .link { display: flex; gap: 8px; align-items: baseline;
                       padding: 7px 0; border-bottom: 1px solid var(--loom-border); font-size: 13px; }
        .pname, .lname { color: var(--loom-accent); font-weight: 500; }
        .pty { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
               color: var(--loom-text-mut); font-size: 12px; }
        .req { color: var(--loom-danger); font-size: 11px; }
        .key { margin-left: auto; color: var(--loom-accent); font-size: 11px;
               border: 1px solid var(--loom-accent); border-radius: 4px; padding: 0 5px; }
        .card { color: var(--loom-text-mut); font-size: 12px; }
        .section { font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em;
                   color: var(--loom-text-mut); margin: 14px 0 2px; }
        .empty { color: var(--loom-text-mut); font-size: 13px; padding: 8px 0; }
    "#
    );

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

    // Header sub-line: the backing dataset, plus the key property when the type has an
    // identity. Derived from the loaded detail; empty while the detail is still loading.
    let subline = props.detail.as_ref().map(|d| {
        let mut s = format!("backing {}.{}", d.table_schema, d.table_name);
        if let Some(id) = &d.identity {
            s.push_str(&format!(" · key: {id}"));
        }
        s
    });

    let body = if props.active_tab.as_str() == "links" {
        links_body(props.detail.as_ref())
    } else {
        properties_body(props.detail.as_ref())
    };

    html! {
        <Panel title={props.name.clone()}>
            <Tabs tabs={tabs} active={props.active_tab.clone()} onselect={props.on_tab.clone()} />
            <div class={cls}>
                if let Some(sub) = subline {
                    <p class="sub">{ sub }</p>
                }
                { body }
            </div>
        </Panel>
    }
}

/// The Properties tab: each property as `name  ty  [required]  [key]`, with the name
/// in the accent colour and the type in monospace. The identity property carries a
/// `key` badge. This is the type's *schema* — there is no per-object value column.
fn properties_body(detail: Option<&TypeDetail>) -> Html {
    let Some(detail) = detail else {
        return html! { <p class="empty">{ "Loading…" }</p> };
    };
    if detail.properties.is_empty() {
        return html! { <p class="empty">{ "No properties." }</p> };
    }
    html! {
        { for detail.properties.iter().map(|p| {
            let is_key = detail.identity.as_deref() == Some(p.name.as_str());
            html! {
                <div class="prop">
                    <span class="pname">{ p.name.clone() }</span>
                    <span class="pty">{ p.ty.clone() }</span>
                    if p.required { <span class="req">{ "required" }</span> }
                    if is_key { <span class="key">{ "key" }</span> }
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
