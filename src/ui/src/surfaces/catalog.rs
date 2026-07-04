//! The Catalog surface. Two presentational components: `CatalogList` (the dataset
//! `DataTable`) fills the Shell's `list` slot, and `CatalogDrawer` (the Schema ·
//! Preview · Lineage · History tabs over the selected dataset's detail/preview)
//! fills its `drawer` slot. All interactive state and load effects live in
//! `Workspace` (`main.rs`); these components are driven entirely by props — the same
//! architecture the Ontology surface established.

use super::ontology::LoadStatus;
use loom_ui_components::{Column, DataTable, Panel, TabItem, TableRow, Tabs};
use loom_ui_core::{Align, DatasetDetail, DatasetRow, PreviewData};
use stylist::yew::styled_component;
use yew::prelude::*;

/// One row of the Catalog list. `rows` is always `"—"` for now — the list view does
/// not pay for a per-dataset row count.
#[derive(Clone, PartialEq)]
struct CatalogRow {
    name: String,
    project: String,
    rows: String,
    updated: String,
}

impl TableRow for CatalogRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <span>{ &self.name }</span> },
            html! { { &self.project } },
            html! { { &self.rows } },
            html! { { &self.updated } },
        ]
    }
}

fn catalog_columns() -> Vec<Column> {
    vec![
        Column {
            label: "Name".into(),
            align: Align::Start,
        },
        Column {
            label: "Project".into(),
            align: Align::Start,
        },
        Column {
            label: "Rows".into(),
            align: Align::End,
        },
        Column {
            label: "Updated".into(),
            align: Align::Start,
        },
    ]
}

fn to_rows(datasets: &[DatasetRow]) -> Vec<CatalogRow> {
    datasets
        .iter()
        .map(|d| CatalogRow {
            name: d.name.clone(),
            project: d.project.clone(),
            rows: "—".into(),
            updated: d.updated.clone(),
        })
        .collect()
}

#[derive(Properties, PartialEq)]
pub struct CatalogListProps {
    pub datasets: Vec<DatasetRow>,
    pub status: LoadStatus,
    pub selected: Option<usize>,
    pub on_row: Callback<usize>,
}

/// The dataset table (Name · Project · Rows · Updated). Rendered into the `Shell`'s
/// `list` slot.
#[function_component(CatalogList)]
pub fn catalog_list(props: &CatalogListProps) -> Html {
    let body = if matches!(props.status, LoadStatus::Loading) {
        html! { <p>{ "Loading…" }</p> }
    } else if let LoadStatus::Error(m) = &props.status {
        html! { <p class="error">{ m.clone() }</p> }
    } else if props.datasets.is_empty() {
        html! { <p>{ "No datasets." }</p> }
    } else {
        html! {
            <DataTable<CatalogRow>
                columns={catalog_columns()}
                rows={to_rows(&props.datasets)}
                selected={props.selected}
                onrow={props.on_row.clone()}
            />
        }
    };
    html! { <Panel title="Datasets">{ body }</Panel> }
}

#[derive(Properties, PartialEq)]
pub struct CatalogDrawerProps {
    pub name: AttrValue,
    pub detail: Option<DatasetDetail>,
    pub preview: Option<PreviewData>,
    pub preview_loading: bool,
    pub active_tab: AttrValue,
    pub on_tab: Callback<AttrValue>,
}

/// The detail drawer for the selected dataset: `Tabs` (Schema · Preview · Lineage ·
/// History). Rendered into the `Shell`'s `drawer` slot only when a row is selected.
#[styled_component(CatalogDrawer)]
pub fn catalog_drawer(props: &CatalogDrawerProps) -> Html {
    let cls = css!(
        r#"
        padding: 4px 12px 12px;
        .col { display: flex; gap: 8px; align-items: baseline;
               padding: 7px 0; border-bottom: 1px solid var(--loom-border); font-size: 13px; }
        .cname { color: var(--loom-accent); font-weight: 500; }
        .cty { font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
               color: var(--loom-text-mut); font-size: 12px; }
        .null { margin-left: auto; color: var(--loom-text-mut); font-size: 11px; }
        .updated { color: var(--loom-text-mut); font-size: 12px; margin: 8px 0 2px; }
        .empty { color: var(--loom-text-mut); font-size: 13px; padding: 8px 0; }
        .foot { color: var(--loom-text-mut); font-size: 12px; margin-top: 8px; }
        table.preview { width: 100%; border-collapse: collapse; font-size: 12px; }
        table.preview th, table.preview td {
            padding: 5px 8px; border-bottom: 1px solid var(--loom-border);
            text-align: left; white-space: nowrap; }
        table.preview th { color: var(--loom-text-mut); font-weight: 500; }
    "#
    );

    let tabs = vec![
        TabItem {
            id: "schema".into(),
            label: "Schema".into(),
        },
        TabItem {
            id: "preview".into(),
            label: "Preview".into(),
        },
        TabItem {
            id: "lineage".into(),
            label: "Lineage".into(),
        },
        TabItem {
            id: "history".into(),
            label: "History".into(),
        },
    ];

    let body = match props.active_tab.as_str() {
        "preview" => preview_body(props.preview.as_ref(), props.preview_loading),
        "lineage" => html! { <p class="empty">{ "Lineage — coming in the next step." }</p> },
        "history" => html! {
            <p class="empty">{ "Per-dataset run history isn't available on this instance yet." }</p>
        },
        _ => schema_body(props.detail.as_ref()),
    };

    html! {
        <Panel title={props.name.clone()}>
            <Tabs tabs={tabs} active={props.active_tab.clone()} onselect={props.on_tab.clone()} />
            <div class={cls}>{ body }</div>
        </Panel>
    }
}

/// The Schema tab: each `SchemaCol` as `name  ty  [nullable]`, name accented, `ty`
/// monospace. The dataset's snapshot time rides above as a small "updated" line.
fn schema_body(detail: Option<&DatasetDetail>) -> Html {
    let Some(detail) = detail else {
        return html! { <p class="empty">{ "Loading…" }</p> };
    };
    html! {
        <>
            if !detail.snapshot_time.is_empty() {
                <div class="updated">{ format!("updated {}", detail.snapshot_time) }</div>
            }
            if detail.columns.is_empty() {
                <p class="empty">{ "No columns." }</p>
            } else {
                { for detail.columns.iter().map(|c| html! {
                    <div class="col">
                        <span class="cname">{ c.name.clone() }</span>
                        <span class="cty">{ c.ty.clone() }</span>
                        if c.nullable { <span class="null">{ "nullable" }</span> }
                    </div>
                }) }
            }
        </>
    }
}

/// The Preview tab: a plain `<table>` of sampled rows with a "Showing N · sampled"
/// footnote. While the fetch is in flight, a "Loading…" line.
fn preview_body(preview: Option<&PreviewData>, loading: bool) -> Html {
    if loading {
        return html! { <p class="empty">{ "Loading…" }</p> };
    }
    let Some(p) = preview else {
        return html! { <p class="empty">{ "Loading…" }</p> };
    };
    if p.columns.is_empty() && p.rows.is_empty() {
        return html! { <p class="empty">{ "No preview." }</p> };
    }
    html! {
        <>
            <table class="preview">
                <thead>
                    <tr>
                        { for p.columns.iter().map(|c| html! { <th>{ c.clone() }</th> }) }
                    </tr>
                </thead>
                <tbody>
                    { for p.rows.iter().map(|row| html! {
                        <tr>
                            { for row.iter().map(|cell| html! { <td>{ cell.clone() }</td> }) }
                        </tr>
                    }) }
                </tbody>
            </table>
            <div class="foot">{ format!("Showing {} · sampled", p.rows.len()) }</div>
        </>
    }
}
