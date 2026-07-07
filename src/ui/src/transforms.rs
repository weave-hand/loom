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
