#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]

use loom_ui_components::{
    Badge, Button, Column, DataTable, GlobalStyles, Input, InputKind, LineageDagView, NavItem,
    Panel, Shell, SqlEditor, StatusDot, StubView, TabItem, TableRow, Tabs, TopNav,
};
use loom_ui_core::{Align, BadgeTone, ButtonVariant, Status, Surface, format_count, lineage_dag};
use yew::prelude::*;

#[derive(Clone, PartialEq)]
struct DatasetRow {
    name: &'static str,
    rows: u64,
    owner: &'static str,
    health: Status,
}

impl TableRow for DatasetRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <><input type="checkbox" />{ " " }{ self.name }</> },
            html! { { format_count(self.rows) } },
            html! { { self.owner } },
            html! { <StatusDot status={self.health} /> },
        ]
    }
}

#[function_component(Gallery)]
fn gallery() -> Html {
    let active_tab = use_state(|| AttrValue::from("preview"));
    let tabs = vec![
        TabItem {
            id: "preview".into(),
            label: "Preview".into(),
        },
        TabItem {
            id: "schema".into(),
            label: "Schema".into(),
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
    let onselect = {
        let active_tab = active_tab.clone();
        Callback::from(move |id: AttrValue| active_tab.set(id))
    };
    let sql = use_state(|| AttrValue::from("SELECT id, name\nFROM customers\nWHERE "));
    let on_sql = {
        let sql = sql.clone();
        Callback::from(move |v: String| sql.set(AttrValue::from(v)))
    };
    let nav = vec![
        NavItem {
            label: "Catalog".into(),
            active: true,
        },
        NavItem {
            label: "Pipelines".into(),
            active: false,
        },
        NavItem {
            label: "Ontology".into(),
            active: false,
        },
    ];
    let columns = vec![
        Column {
            label: "NAME".into(),
            align: Align::Start,
        },
        Column {
            label: "ROWS".into(),
            align: Align::End,
        },
        Column {
            label: "OWNER".into(),
            align: Align::Start,
        },
        Column {
            label: "HEALTH".into(),
            align: Align::Start,
        },
    ];
    let rows = vec![
        DatasetRow {
            name: "transactions_raw",
            rows: 2_410_000,
            owner: "A. Mehta",
            health: Status::Ok,
        },
        DatasetRow {
            name: "fx_rates_daily",
            rows: 18_200,
            owner: "J. Liu",
            health: Status::Warn,
        },
        DatasetRow {
            name: "chargebacks",
            rows: 9_700,
            owner: "R. Park",
            health: Status::Error,
        },
    ];
    html! {
        <>
            <GlobalStyles />
            <TopNav
                items={nav}
                search={html!{ <Input value="" placeholder="Search…" input_type={InputKind::Search} /> }}
                avatar="DK"
            />
            <main style="padding: 24px; max-width: 1100px; margin: 0 auto;">
                <h1>{ "loom component gallery" }</h1>
                <section>
                    <h2>{ "Buttons" }</h2>
                    <Panel title="Buttons">
                        <div style="display:flex; gap:8px; align-items:center;">
                            <Button variant={ButtonVariant::Primary}>{ "Open in Workbook" }</Button>
                            <Button variant={ButtonVariant::Secondary}>{ "Explore" }</Button>
                            <Button variant={ButtonVariant::Ghost}>{ "Cancel" }</Button>
                            <Button variant={ButtonVariant::Primary} disabled=true>{ "Disabled" }</Button>
                        </div>
                    </Panel>
                </section>
                <section>
                    <h2>{ "Badges" }</h2>
                    <Panel title="Badges">
                        <div style="display:flex; gap:8px;">
                            <Badge label="pii" tone={BadgeTone::Pii} />
                            <Badge label="finance" tone={BadgeTone::Info} />
                            <Badge label="certified" tone={BadgeTone::Success} />
                            <Badge label="draft" tone={BadgeTone::Neutral} />
                        </div>
                    </Panel>
                </section>
                <section>
                    <h2>{ "Status" }</h2>
                    <Panel title="Status">
                        <div style="display:flex; gap:16px; align-items:center;">
                            <span><StatusDot status={Status::Ok} />{ " healthy" }</span>
                            <span><StatusDot status={Status::Warn} />{ " stale" }</span>
                            <span><StatusDot status={Status::Error} />{ " failed" }</span>
                        </div>
                    </Panel>
                </section>
                <section>
                    <h2>{ "Inputs" }</h2>
                    <Panel title="Inputs">
                        <div style="display:flex; gap:8px;">
                            <Input value="" placeholder="username" />
                            <Input value="" placeholder="password" input_type={InputKind::Password} />
                            <Input value="" placeholder="Search datasets…" input_type={InputKind::Search} />
                        </div>
                    </Panel>
                </section>
                <section>
                    <h2>{ "Tabs" }</h2>
                    <Panel title="Tabs">
                        <Tabs tabs={tabs} active={(*active_tab).clone()} onselect={onselect} />
                        <p>{ format!("active: {}", *active_tab) }</p>
                    </Panel>
                </section>
                <section>
                    <h2>{ "SQL editor" }</h2>
                    <Panel title="SqlEditor">
                        <SqlEditor value={(*sql).clone()} on_change={on_sql} />
                        <p>{ format!("buffer: {}", *sql) }</p>
                    </Panel>
                </section>
                <section>
                    <h2>{ "DataTable" }</h2>
                    <Panel title="Finance / Transactions">
                        <DataTable<DatasetRow> columns={columns} rows={rows} selected={Some(0)} />
                    </Panel>
                </section>
                <section>
                    <h2>{ "Lineage mini-DAG" }</h2>
                    <Panel title="Lineage">
                        <LineageDagView dag={lineage_dag(
                            ("finance", "transactions"),
                            &[
                                ("raw".into(), "card_events".into()),
                                ("raw".into(), "fx_rates".into()),
                            ],
                            &[
                                ("marts".into(), "revenue_daily".into()),
                                ("marts".into(), "chargebacks".into()),
                                ("marts".into(), "ledger".into()),
                            ],
                        )} />
                    </Panel>
                </section>
                <section>
                    <h2>{ "Shell" }</h2>
                    <Panel title="Shell">
                        <Shell
                            active={Surface::Catalog}
                            on_switch={Callback::noop()}
                            avatar="DK"
                            list={html!{ <StubView surface={Surface::Pipelines} /> }}
                        />
                    </Panel>
                </section>
            </main>
        </>
    }
}

fn main() {
    yew::Renderer::<Gallery>::new().render();
}
