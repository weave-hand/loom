//! The Transforms admin surface. Three presentational components: `TransformsList`
//! (the transform-definition `DataTable`) fills the Shell's `list` slot,
//! `TransformDrawer` (Definition · Runs tabs over the selected transform) fills its
//! `drawer` slot, and `TransformEditor` renders the define/ad-hoc-run form. All
//! interactive state and load effects live in `Workspace` (`main.rs`); these
//! components are driven entirely by props — the same architecture the Catalog and
//! Ontology surfaces established.

use loom_ui_components::{
    Badge, Button, Column, DataTable, Input, Panel, SqlEditor, StatusDot, TabItem, TableRow, Tabs,
};
use loom_ui_core::{
    Align, BadgeTone, ButtonVariant, CompletionSchema, FieldError, OutputMode, RunRow, Status,
    TransformDefView, TransformForm, TransformIo, TransformKind, TransformSummary,
    kind_badge_label, run_state_tone, trigger_label,
};
use yew::prelude::*;

use crate::surfaces::LoadStatus;

// `DataTable<R>` bounds `R: PartialEq + Clone + TableRow + 'static`, so the
// derives are mandatory (mirror `CatalogRow`/`OntologyTypeRow`).
#[derive(Clone, PartialEq)]
struct TransformListRow {
    name: String,
    kind: TransformKind,
    schedule: String,
    on_input_commit: bool,
}

impl TableRow for TransformListRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { { &self.name } },
            html! { <Badge label={kind_badge_label(self.kind)} tone={BadgeTone::Info} /> },
            html! { { &self.schedule } },
            html! { <StatusDot status={ if self.on_input_commit { Status::Ok } else { Status::Warn } } /> },
        ]
    }
}

fn list_columns() -> Vec<Column> {
    vec![
        Column {
            label: "Name".into(),
            align: Align::Start,
        },
        Column {
            label: "Kind".into(),
            align: Align::Start,
        },
        Column {
            label: "Schedule".into(),
            align: Align::Start,
        },
        Column {
            label: "On commit".into(),
            align: Align::End,
        },
    ]
}

#[derive(Properties, PartialEq)]
pub struct TransformsListProps {
    pub rows: Vec<TransformSummary>,
    pub status: LoadStatus,
    pub selected: Option<usize>,
    pub on_row: Callback<usize>,
    pub on_new: Callback<()>,
    pub forbidden: bool,
}

/// The transform-definitions table (Name · Kind · Schedule · On commit), plus a
/// "New transform" action. Rendered into the `Shell`'s `list` slot. When
/// `forbidden` is set (non-admin subject), renders the admin-only empty state
/// instead of the table.
#[function_component(TransformsList)]
pub fn transforms_list(props: &TransformsListProps) -> Html {
    if props.forbidden {
        return html! {
            <Panel title="Transforms">
                <p>{ "Transforms requires the admin role." }</p>
            </Panel>
        };
    }
    let on_new = props.on_new.clone();
    let new_click = Callback::from(move |_| on_new.emit(()));
    let body = match &props.status {
        LoadStatus::Loading => html! { <p>{ "Loading…" }</p> },
        LoadStatus::Error(e) => html! { <p class="error">{ e }</p> },
        LoadStatus::Idle => {
            let rows: Vec<TransformListRow> = props
                .rows
                .iter()
                .map(|r| TransformListRow {
                    name: r.name.clone(),
                    kind: r.kind,
                    schedule: r.schedule.clone().unwrap_or_else(|| "manual".to_string()),
                    on_input_commit: r.on_input_commit,
                })
                .collect();
            html! {
                <DataTable<TransformListRow>
                    columns={list_columns()} rows={rows}
                    selected={props.selected} onrow={props.on_row.clone()} />
            }
        }
    };
    html! {
        <Panel title="Transforms">
            <Button variant={ButtonVariant::Primary} onclick={new_click}>{ "＋ New transform" }</Button>
            { body }
        </Panel>
    }
}

#[derive(Clone, PartialEq)]
struct RunTableRow {
    state: String,
    trigger: String,
    started: String,
    finished: String,
    snapshot: String,
    error: String,
}

impl TableRow for RunTableRow {
    fn cells(&self) -> Vec<Html> {
        vec![
            html! { <Badge label={self.state.clone()} tone={run_state_tone(&self.state)} /> },
            html! { { trigger_label(&self.trigger) } },
            html! { { &self.started } },
            html! { { &self.finished } },
            html! { { &self.snapshot } },
            html! { { &self.error } },
        ]
    }
}

fn run_columns() -> Vec<Column> {
    vec![
        Column {
            label: "State".into(),
            align: Align::Start,
        },
        Column {
            label: "Trigger".into(),
            align: Align::Start,
        },
        Column {
            label: "Started".into(),
            align: Align::Start,
        },
        Column {
            label: "Finished".into(),
            align: Align::Start,
        },
        Column {
            label: "Snapshot".into(),
            align: Align::Start,
        },
        Column {
            label: "Error".into(),
            align: Align::Start,
        },
    ]
}

/// A stable fingerprint of a transform's inputs, used as the SqlEditor remount key.
fn io_fingerprint(io: &TransformIo) -> String {
    match io {
        TransformIo::Physical { inputs, .. } => {
            let mut parts: Vec<String> = inputs
                .iter()
                .map(|t| format!("{}.{}", t.schema, t.name))
                .collect();
            parts.sort();
            format!("physical:{}", parts.join(","))
        }
        TransformIo::Typed { inputs, .. } => {
            let mut parts = inputs.clone();
            parts.sort();
            format!("typed:{}", parts.join(","))
        }
    }
}

/// Render the definition-view metadata rows (inputs/output/mode/schedule/on-commit)
/// required by the spec's Surface layout, mirroring `catalog.rs`'s field-row style.
fn def_metadata(def: &TransformDefView) -> Html {
    let (inputs, output) = match &def.body.io {
        TransformIo::Physical { inputs, output } => (
            inputs
                .iter()
                .map(|t| format!("{}.{}", t.schema, t.name))
                .collect::<Vec<_>>()
                .join(", "),
            format!("{}.{}", output.schema, output.name),
        ),
        TransformIo::Typed { inputs, output } => (inputs.join(", "), output.clone()),
    };
    let schedule = def.schedule.clone().unwrap_or_else(|| "manual".to_string());
    let row = |label: &str, value: String| {
        html! {
            <div class="tf-meta-row"><span class="tf-meta-key">{ label }</span><span>{ value }</span></div>
        }
    };
    html! {
        <div class="tf-meta">
            { row("Kind", kind_badge_label(def.body.kind()).to_string()) }
            { row("Inputs", inputs) }
            { row("Output", output) }
            { row("Output mode", def.body.output_mode.as_str().to_string()) }
            { row("Schedule", schedule) }
            { row("On input commit", def.on_input_commit.to_string()) }
            { row("Next run", def.next_run_at.clone().unwrap_or_default()) }
        </div>
    }
}

#[derive(Properties, PartialEq)]
pub struct TransformDrawerProps {
    pub def: TransformDefView,
    pub active_tab: AttrValue,
    pub on_tab: Callback<AttrValue>,
    pub runs: Vec<RunRow>,
    pub runs_status: LoadStatus,
    /// A non-401 Run/Delete failure, rendered beside the action buttons
    /// (mirrors `TransformEditorProps.server_error`). `None` = no error.
    pub action_error: Option<AttrValue>,
    pub on_edit: Callback<()>,
    pub on_run: Callback<()>,
    pub on_delete: Callback<()>,
}

/// The detail drawer for the selected transform: `Tabs` (Definition · Runs).
/// Rendered into the `Shell`'s `drawer` slot only when a row is selected.
#[function_component(TransformDrawer)]
pub fn transform_drawer(props: &TransformDrawerProps) -> Html {
    let tabs = vec![
        TabItem {
            id: "definition".into(),
            label: "Definition".into(),
        },
        TabItem {
            id: "runs".into(),
            label: "Runs".into(),
        },
    ];
    let no_op = Callback::from(|_: String| {});
    let on_edit = props.on_edit.clone();
    let on_run = props.on_run.clone();
    let on_delete = props.on_delete.clone();
    let body = match props.active_tab.as_str() {
        "runs" => match &props.runs_status {
            LoadStatus::Loading => html! { <p>{ "Loading…" }</p> },
            LoadStatus::Error(e) => html! { <p class="error">{ e }</p> },
            LoadStatus::Idle => {
                let rows: Vec<RunTableRow> = props
                    .runs
                    .iter()
                    .map(|r| RunTableRow {
                        state: r.state.clone(),
                        trigger: r.trigger.clone(),
                        started: r.started_at.clone().unwrap_or_default(),
                        finished: r.finished_at.clone().unwrap_or_default(),
                        snapshot: r.snapshot_id.clone().unwrap_or_default(),
                        error: r.error.clone().unwrap_or_default(),
                    })
                    .collect();
                html! { <DataTable<RunTableRow> columns={run_columns()} rows={rows} /> }
            }
        },
        _ => {
            let key = io_fingerprint(&props.def.body.io);
            html! {
                <>
                    { def_metadata(&props.def) }
                    <SqlEditor key={key} value={props.def.body.sql.clone()}
                               on_change={no_op} read_only={true} />
                    if let Some(msg) = &props.action_error {
                        <p class="error">{ msg }</p>
                    }
                    <div class="shell-drawer-actions">
                        <Button variant={ButtonVariant::Secondary}
                                onclick={Callback::from(move |_| on_edit.emit(()))}>{ "Edit" }</Button>
                        <Button variant={ButtonVariant::Primary}
                                onclick={Callback::from(move |_| on_run.emit(()))}>{ "Run" }</Button>
                        <Button variant={ButtonVariant::Ghost}
                                onclick={Callback::from(move |_| on_delete.emit(()))}>{ "Delete" }</Button>
                    </div>
                </>
            }
        }
    };
    html! {
        <Panel title={props.def.name.clone()}>
            <Tabs tabs={tabs} active={props.active_tab.clone()} onselect={props.on_tab.clone()} />
            { body }
        </Panel>
    }
}

/// A stable fingerprint of the editor form's kind+inputs, used as the SqlEditor
/// remount key (mirrors `io_fingerprint` for the read-only drawer editor).
fn form_fingerprint(form: &TransformForm) -> String {
    let mut inputs = form.inputs.clone();
    inputs.sort();
    format!("{}:{}", form.kind.as_str(), inputs.join(","))
}

/// A content fingerprint of the completion schema (table + column names), used
/// so the editor remounts when the input-scoped schema arrives — the Monaco
/// SqlEditor captures `schema` only at mount, so a stable key would otherwise
/// keep stale (pre-fetch) completions after the input set changes.
fn schema_fingerprint(schema: &CompletionSchema) -> String {
    let mut parts: Vec<String> = schema
        .tables
        .iter()
        .map(|t| {
            let cols: Vec<&str> = t.columns.iter().map(|c| c.name.as_str()).collect();
            format!("{}[{}]", t.name, cols.join(","))
        })
        .collect();
    parts.sort();
    parts.join(";")
}

#[derive(Properties, PartialEq)]
pub struct TransformEditorProps {
    pub form: TransformForm,
    pub schema: CompletionSchema,
    pub editing: bool,
    pub dataset_options: Vec<String>,
    pub type_options: Vec<String>,
    pub errors: Vec<FieldError>,
    pub server_error: Option<AttrValue>,
    pub on_change: Callback<TransformForm>,
    pub on_submit: Callback<()>,
    pub on_run_adhoc: Callback<()>,
    pub on_cancel: Callback<()>,
}

/// The define/ad-hoc-run form: name, kind toggle, inputs multi-select, output,
/// SQL editor, schedule, on-commit toggle, output-mode toggle, inline errors, and
/// Define/Run ad-hoc/Cancel actions. Controlled: every field edit clones `form`,
/// mutates one field, and emits `on_change`.
#[function_component(TransformEditor)]
pub fn transform_editor(props: &TransformEditorProps) -> Html {
    let form = props.form.clone();
    let emit = props.on_change.clone();

    let on_name = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: InputEvent| {
            let value = input_value(&e);
            let mut next = form.clone();
            next.name = value;
            emit.emit(next);
        })
    };
    let on_sql = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |value: String| {
            let mut next = form.clone();
            next.sql = value;
            emit.emit(next);
        })
    };
    // Kind toggle: switching kind CLEARS inputs/output (their meaning changes:
    // "schema.name" for physical vs a type name for typed).
    let set_kind = {
        let form = form.clone();
        let emit = emit.clone();
        move |kind: TransformKind| {
            let mut next = form.clone();
            next.kind = kind;
            next.inputs.clear();
            next.output.clear();
            emit.emit(next);
        }
    };
    let on_physical = {
        let f = set_kind.clone();
        Callback::from(move |_| f(TransformKind::Physical))
    };
    let on_typed = {
        let f = set_kind.clone();
        Callback::from(move |_| f(TransformKind::Typed))
    };
    // The options offered by the checkbox list depend on the selected kind.
    let options: Vec<String> = match props.form.kind {
        TransformKind::Physical => props.dataset_options.clone(),
        TransformKind::Typed => props.type_options.clone(),
    };
    // One checkbox per option; toggling adds/removes the option string from `inputs`.
    let input_checkboxes: Html = options
        .into_iter()
        .map(|opt| {
            let checked = props.form.inputs.contains(&opt);
            let on_toggle = {
                let form = form.clone();
                let emit = emit.clone();
                let opt = opt.clone();
                Callback::from(move |_e: Event| {
                    let mut next = form.clone();
                    if next.inputs.contains(&opt) {
                        next.inputs.retain(|i| i != &opt);
                    } else {
                        next.inputs.push(opt.clone());
                    }
                    emit.emit(next);
                })
            };
            html! {
                <label class="tf-check">
                    <input type="checkbox" checked={checked} onchange={on_toggle} />
                    { opt }
                </label>
            }
        })
        .collect();
    let on_output = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: InputEvent| {
            let mut next = form.clone();
            next.output = input_value(&e);
            emit.emit(next);
        })
    };
    let on_schedule = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: InputEvent| {
            let mut next = form.clone();
            next.schedule = input_value(&e);
            emit.emit(next);
        })
    };
    let on_commit_toggle = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: Event| {
            use wasm_bindgen::JsCast;
            let checked = e
                .target()
                .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
                .map(|el| el.checked())
                .unwrap_or(false);
            let mut next = form.clone();
            next.on_input_commit = checked;
            emit.emit(next);
        })
    };
    let on_mode_toggle = {
        let form = form.clone();
        let emit = emit.clone();
        Callback::from(move |e: Event| {
            use wasm_bindgen::JsCast;
            let checked = e
                .target()
                .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
                .map(|el| el.checked())
                .unwrap_or(false);
            let mut next = form.clone();
            next.output_mode = if checked {
                OutputMode::Overwrite
            } else {
                OutputMode::Append
            };
            emit.emit(next);
        })
    };

    let title = if props.editing {
        "Edit transform"
    } else {
        "New transform"
    };
    let key = format!(
        "{}:{}",
        form_fingerprint(&props.form),
        schema_fingerprint(&props.schema)
    );
    let submit = props.on_submit.clone();
    let run_adhoc = props.on_run_adhoc.clone();
    let cancel = props.on_cancel.clone();

    html! {
        <Panel title={title}>
            <Input value={props.form.name.clone()} placeholder="name" oninput={on_name}
                   disabled={props.editing} />
            <div class="tf-kind">
                <Button variant={ if props.form.kind == TransformKind::Physical { ButtonVariant::Primary } else { ButtonVariant::Ghost } }
                        onclick={on_physical}>{ "Physical" }</Button>
                <Button variant={ if props.form.kind == TransformKind::Typed { ButtonVariant::Primary } else { ButtonVariant::Ghost } }
                        onclick={on_typed}>{ "Typed" }</Button>
            </div>
            <div class="tf-inputs">{ input_checkboxes }</div>
            <Input value={props.form.output.clone()} placeholder="output (schema.name or Type)"
                   oninput={on_output} />
            <SqlEditor key={key} value={props.form.sql.clone()} on_change={on_sql}
                       schema={props.schema.clone()} read_only={false} />
            <Input value={props.form.schedule.clone()} placeholder="cron schedule (optional)"
                   oninput={on_schedule} />
            <label class="tf-check">
                <input type="checkbox" checked={props.form.on_input_commit} onchange={on_commit_toggle} />
                { "Run on input commit" }
            </label>
            <label class="tf-check">
                <input type="checkbox"
                       checked={props.form.output_mode == OutputMode::Overwrite}
                       onchange={on_mode_toggle} />
                { "Overwrite output (else append)" }
            </label>
            { for props.errors.iter().map(|e| html! {
                <p class="error">{ format!("{}: {}", e.field, e.message) }</p> }) }
            if let Some(msg) = &props.server_error {
                <p class="error">{ msg }</p>
            }
            <div class="shell-drawer-actions">
                <Button variant={ButtonVariant::Primary}
                        onclick={Callback::from(move |_| submit.emit(()))}>{ "Define" }</Button>
                <Button variant={ButtonVariant::Secondary}
                        onclick={Callback::from(move |_| run_adhoc.emit(()))}>{ "Run ad-hoc" }</Button>
                <Button variant={ButtonVariant::Ghost}
                        onclick={Callback::from(move |_| cancel.emit(()))}>{ "Cancel" }</Button>
            </div>
        </Panel>
    }
}

/// Read an `<input>`'s value from an `InputEvent` (mirrors the login form's helper
/// in `main.rs`).
fn input_value(e: &InputEvent) -> String {
    use wasm_bindgen::JsCast;
    e.target()
        .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
        .map(|el| el.value())
        .unwrap_or_default()
}
