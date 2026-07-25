//! The Query surface: a self-contained governed SQL console. Unlike the other
//! surfaces (whose state lives in `Workspace`), the console owns its own state — the
//! SQL text, the last result, and the in-flight flag — so `Workspace` gains only a
//! one-line dispatch arm. It reuses the shared `SqlEditor` for input and the
//! `DataTable` primitive for the governed result grid, and POSTs to `/sql` via
//! `net::run_sql` (a 401 fails closed to logout, mirroring the other surfaces).

use loom_ui_components::{Button, Column, DataTable, Panel, SqlEditor, TableRow};
use loom_ui_core::{Align, ButtonVariant, QueryResult};
use stylist::yew::styled_component;
use wasm_bindgen_futures::spawn_local;
use yew::prelude::*;

use crate::net::{self, FetchError};

/// Rows the console requests per run. The server caps hard regardless; this bounds a
/// naive `SELECT *` before it leaves the browser's expectations.
const SQL_LIMIT: u32 = 1_000;

/// One governed result row, rendered as display-string cells. Runtime-columned, so
/// arbitrary result shapes render through the shared `DataTable` primitive without a
/// per-query row type.
#[derive(Clone, PartialEq)]
struct StringRow {
    cells: Vec<String>,
}

impl TableRow for StringRow {
    fn cells(&self) -> Vec<Html> {
        self.cells.iter().map(|c| html! { { c } }).collect()
    }
}

fn result_columns(cols: &[String]) -> Vec<Column> {
    cols.iter()
        .map(|c| Column {
            label: c.clone().into(),
            align: Align::Start,
        })
        .collect()
}

/// The status line under the editor: nothing before the first run, the error
/// message on failure, or the row count (+ truncation note) on success.
fn render_status(result: &Option<Result<QueryResult, String>>) -> Html {
    match result {
        None => html! {},
        Some(Err(msg)) => html! { <p class="error">{ msg.clone() }</p> },
        Some(Ok(qr)) => {
            let n = qr.rows.len();
            let note = if qr.truncated {
                format!("{n} rows (truncated at the row cap)")
            } else {
                format!("{n} rows")
            };
            html! { <p>{ note }</p> }
        }
    }
}

/// The governed result grid, or nothing when there is no successful, non-empty
/// result to render.
fn render_grid(result: &Option<Result<QueryResult, String>>) -> Html {
    match result {
        Some(Ok(qr)) if !qr.columns.is_empty() => html! {
            <div data-testid="sql-result">
                <DataTable<StringRow>
                    columns={result_columns(&qr.columns)}
                    rows={qr.rows.iter().map(|r| StringRow { cells: r.clone() }).collect::<Vec<_>>()}
                />
            </div>
        },
        _ => html! {},
    }
}

#[derive(Properties, PartialEq)]
pub struct QueryViewProps {
    pub token: AttrValue,
    pub on_logout: Callback<()>,
}

#[styled_component(QueryView)]
pub fn query_view(props: &QueryViewProps) -> Html {
    let sql = use_state(|| AttrValue::from("SELECT 1"));
    let result = use_state(|| Option::<Result<QueryResult, String>>::None);
    let running = use_state(|| false);

    let on_change = {
        let sql = sql.clone();
        Callback::from(move |v: String| sql.set(AttrValue::from(v)))
    };

    let on_run = {
        let sql = sql.clone();
        let result = result.clone();
        let running = running.clone();
        let token = props.token.to_string();
        let on_logout = props.on_logout.clone();
        Callback::from(move |_: MouseEvent| {
            let sql_text = sql.to_string();
            let result = result.clone();
            let running = running.clone();
            let token = token.clone();
            let on_logout = on_logout.clone();
            running.set(true);
            spawn_local(async move {
                let outcome = net::run_sql(&net::api_base(), &token, &sql_text, SQL_LIMIT).await;
                match outcome {
                    Ok(qr) => result.set(Some(Ok(qr))),
                    Err(FetchError::Unauthorized) => on_logout.emit(()),
                    Err(e) => result.set(Some(Err(e.to_string()))),
                }
                running.set(false);
            });
        })
    };

    let run_label = if *running { "Running…" } else { "Run" };
    html! {
        <Panel>
            <SqlEditor value={(*sql).clone()} on_change={on_change} read_only={false} />
            <div>
                <Button variant={ButtonVariant::Primary} disabled={*running} onclick={on_run}>
                    { run_label }
                </Button>
            </div>
            { render_status(&result) }
            { render_grid(&result) }
        </Panel>
    }
}
