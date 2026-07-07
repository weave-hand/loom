//! Pure wire JSON ⇄ view/form values for the Transforms admin surface.
//! No Yew dependency; strictly lint-clean (covered by `//src/ui:transforms-*` tests).

use serde_json::Value;

use crate::str_field;

/// Whether a transform operates on physical tables or ontology types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransformKind {
    #[default]
    Physical,
    Typed,
}

impl TransformKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Physical => "physical",
            Self::Typed => "typed",
        }
    }
}

/// The output write mode. Server default is `Append`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputMode {
    #[default]
    Append,
    Overwrite,
}

impl OutputMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Overwrite => "overwrite",
        }
    }

    #[must_use]
    pub fn from_str_opt(s: &str) -> Self {
        if s == "overwrite" {
            Self::Overwrite
        } else {
            Self::Append
        }
    }
}

/// A physical `{schema, name}` table reference.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

/// Kind-specific inputs/output of a transform body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformIo {
    Physical {
        inputs: Vec<TableRef>,
        output: TableRef,
    },
    Typed {
        inputs: Vec<String>,
        output: String,
    },
}

/// A decoded `TransformBody` (the internally-tagged `body` object).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformBody {
    pub io: TransformIo,
    pub sql: String,
    pub output_mode: OutputMode,
}

impl TransformBody {
    #[must_use]
    pub fn kind(&self) -> TransformKind {
        match self.io {
            TransformIo::Physical { .. } => TransformKind::Physical,
            TransformIo::Typed { .. } => TransformKind::Typed,
        }
    }
}

/// A row in the transform list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformSummary {
    pub name: String,
    pub kind: TransformKind,
    pub schedule: Option<String>,
    pub on_input_commit: bool,
}

/// A full transform definition (drawer definition view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformDefView {
    pub name: String,
    pub body: TransformBody,
    pub schedule: Option<String>,
    pub on_input_commit: bool,
    pub next_run_at: Option<String>,
}

/// A row in the run-history table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunRow {
    pub run_id: String,
    pub trigger: String,
    pub state: String,
    pub queued_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub snapshot_id: Option<String>,
    pub error: Option<String>,
}

fn opt_str(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(ToOwned::to_owned)
}

/// The `kind` field of a body object; anything but `"typed"` is treated as physical.
fn body_kind(body: &Value) -> TransformKind {
    match body.get("kind").and_then(Value::as_str) {
        Some("typed") => TransformKind::Typed,
        _ => TransformKind::Physical,
    }
}

fn parse_table_ref(v: &Value) -> TableRef {
    TableRef {
        schema: str_field(v, "schema"),
        name: str_field(v, "name"),
    }
}

/// Decode a `TransformBody` object. Total: missing fields → defaults.
fn parse_body(body: &Value) -> TransformBody {
    let output_mode = OutputMode::from_str_opt(
        body.get("output_mode")
            .and_then(Value::as_str)
            .unwrap_or("append"),
    );
    let io = match body_kind(body) {
        TransformKind::Physical => {
            let inputs = body
                .get("inputs")
                .and_then(Value::as_array)
                .map(|arr| arr.iter().map(parse_table_ref).collect())
                .unwrap_or_default();
            let output = body.get("output").map(parse_table_ref).unwrap_or_default();
            TransformIo::Physical { inputs, output }
        }
        TransformKind::Typed => {
            let inputs = body
                .get("inputs")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_default();
            let output = str_field(body, "output");
            TransformIo::Typed { inputs, output }
        }
    };
    TransformBody {
        io,
        sql: str_field(body, "sql"),
        output_mode,
    }
}

/// Decode `GET /admin/transforms` → `{transforms:[TransformDefView]}`. Total.
#[must_use]
pub fn parse_transform_list(body: &Value) -> Vec<TransformSummary> {
    body.get("transforms")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|d| TransformSummary {
                    name: str_field(d, "name"),
                    kind: d.get("body").map(body_kind).unwrap_or_default(),
                    schedule: opt_str(d, "schedule"),
                    on_input_commit: d
                        .get("on_input_commit")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Decode `GET /admin/transforms/{name}` → one `TransformDefView`. Total.
#[must_use]
pub fn parse_transform_def(body: &Value) -> TransformDefView {
    const NULL: Value = Value::Null;
    TransformDefView {
        name: str_field(body, "name"),
        body: parse_body(body.get("body").unwrap_or(&NULL)),
        schedule: opt_str(body, "schedule"),
        on_input_commit: body
            .get("on_input_commit")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        next_run_at: opt_str(body, "next_run_at"),
    }
}

/// Decode `GET /admin/transforms/{name}/runs` → `{runs:[TransformRunView]}`. Total.
#[must_use]
pub fn parse_runs(body: &Value) -> Vec<RunRow> {
    body.get("runs")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|r| RunRow {
                    run_id: str_field(r, "run_id"),
                    trigger: str_field(r, "trigger"),
                    state: str_field(r, "state"),
                    queued_at: str_field(r, "queued_at"),
                    started_at: opt_str(r, "started_at"),
                    finished_at: opt_str(r, "finished_at"),
                    snapshot_id: opt_str(r, "snapshot_id"),
                    error: opt_str(r, "error"),
                })
                .collect()
        })
        .unwrap_or_default()
}

use serde_json::json;

/// The editor-form value. `inputs`/`output` are `"schema.name"` for physical
/// transforms and bare type names for typed transforms. Empty `schedule` = none.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TransformForm {
    pub kind: TransformKind,
    pub name: String,
    pub inputs: Vec<String>,
    pub output: String,
    pub sql: String,
    pub schedule: String,
    pub on_input_commit: bool,
    pub output_mode: OutputMode,
}

/// A client-side validation failure, keyed by form field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldError {
    pub field: String,
    pub message: String,
}

impl FieldError {
    fn new(field: &str, message: &str) -> Self {
        Self {
            field: field.to_owned(),
            message: message.to_owned(),
        }
    }
}

/// Split a `"schema.name"` ref on the first `.`; no `.` → `("", whole)`.
fn split_ref(s: &str) -> (&str, &str) {
    match s.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("", s),
    }
}

fn table_ref_json(s: &str) -> Value {
    let (schema, name) = split_ref(s);
    json!({ "schema": schema, "name": name })
}

/// Validation shared by define and ad-hoc-run (everything but name/schedule).
fn validate_body(form: &TransformForm, errors: &mut Vec<FieldError>) {
    if form.inputs.is_empty() {
        errors.push(FieldError::new("inputs", "select at least one input"));
    }
    if form.output.trim().is_empty() {
        errors.push(FieldError::new("output", "output is required"));
    }
    if form.sql.trim().is_empty() {
        errors.push(FieldError::new("sql", "SQL is required"));
    }
    if form.kind == TransformKind::Physical {
        if form.inputs.iter().any(|i| !i.contains('.')) {
            errors.push(FieldError::new(
                "inputs",
                "physical inputs must be schema.name",
            ));
        }
        if !form.output.trim().is_empty() && !form.output.contains('.') {
            errors.push(FieldError::new(
                "output",
                "physical output must be schema.name",
            ));
        }
    }
}

/// Build the `TransformBody` JSON (used by both define and ad-hoc run).
fn build_body(form: &TransformForm) -> Value {
    match form.kind {
        TransformKind::Physical => {
            let inputs: Vec<Value> = form.inputs.iter().map(|s| table_ref_json(s)).collect();
            json!({
                "kind": "physical",
                "inputs": inputs,
                "output": table_ref_json(&form.output),
                "sql": form.sql,
                "output_mode": form.output_mode.as_str(),
            })
        }
        TransformKind::Typed => json!({
            "kind": "typed",
            "inputs": form.inputs,
            "output": form.output,
            "sql": form.sql,
            "output_mode": form.output_mode.as_str(),
        }),
    }
}

/// Build the full `TransformDef` (`POST /admin/transforms`). Client-side light
/// validation; the server is the authoritative validator.
///
/// # Errors
/// Returns the accumulated [`FieldError`]s when the form is not submittable.
pub fn form_to_def(form: &TransformForm) -> Result<Value, Vec<FieldError>> {
    let mut errors = Vec::new();
    let name = form.name.trim();
    if name.is_empty() {
        errors.push(FieldError::new("name", "name is required"));
    } else if name == "run" {
        errors.push(FieldError::new("name", "\"run\" is reserved"));
    }
    validate_body(form, &mut errors);
    let schedule = form.schedule.trim();
    if !schedule.is_empty() && schedule.split_whitespace().count() != 5 {
        errors.push(FieldError::new(
            "schedule",
            "cron needs 5 space-separated fields",
        ));
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    let def = if schedule.is_empty() {
        json!({
            "name": name,
            "body": build_body(form),
            "on_input_commit": form.on_input_commit,
        })
    } else {
        json!({
            "name": name,
            "body": build_body(form),
            "on_input_commit": form.on_input_commit,
            "schedule": schedule,
        })
    };
    Ok(def)
}

/// Build the ad-hoc-run body (`POST /admin/transforms/run`) — the `TransformBody` alone.
///
/// # Errors
/// Returns the accumulated [`FieldError`]s when the body is not submittable.
pub fn form_to_body(form: &TransformForm) -> Result<Value, Vec<FieldError>> {
    let mut errors = Vec::new();
    validate_body(form, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(build_body(form))
}
