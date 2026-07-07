//! Platform-neutral logic shared by the wasm `app` binary and its native tests.
//! Pure Rust (no web-sys), so it compiles for the host and is `rust_test`-able.

use std::fmt;

mod completion;
pub use completion::{
    CompletionColumn, CompletionSchema, CompletionTable, SQL_KEYWORDS, Suggestion, SuggestionKind,
    cursor_context, sql_completions,
};

/// Design-token hex values that must be consumed *outside* the CSS layer and so
/// can't be read as `var(--loom-*)`. The `:root` custom properties in
/// `components/global.rs` and the Monaco editor theme in `components/sql_editor.rs`
/// (Monaco can't read CSS custom properties) both build from these, so the palette
/// has a single source of truth. Only the values needed off the CSS path live here;
/// the rest of the palette stays inline in `global.rs`.
pub const LOOM_BG: &str = "#0b0e14";
/// Foreground/body text color; the `--loom-text` token.
pub const LOOM_TEXT: &str = "#e6edf3";

/// Join a runtime API base with a request path. An empty base yields a relative
/// (same-origin) URL; a non-empty base is used as a prefix with at most one `/`.
#[must_use]
pub fn url(base: &str, path: &str) -> String {
    if base.is_empty() {
        return path.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

/// Why a login attempt failed, mapped to user-facing text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The server rejected the credentials (HTTP 401).
    BadCredentials,
    /// The server responded with an unexpected status.
    Server(u16),
    /// The request never completed (transport / decode failure).
    Network,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadCredentials => write!(f, "incorrect username or password"),
            Self::Server(code) => write!(f, "server error ({code})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}

/// Map a non-success HTTP status from `/auth/login` to an [`AuthError`].
#[must_use]
pub fn status_to_error(status: u16) -> AuthError {
    if status == 401 {
        AuthError::BadCredentials
    } else {
        AuthError::Server(status)
    }
}

/// Visual weight of a [`Button`](loom_ui_components::Button).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ButtonVariant {
    Primary,
    Secondary,
    Ghost,
}

/// Semantic colour of a tag [`Badge`](loom_ui_components::Badge).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BadgeTone {
    Neutral,
    Info,
    Pii,
    Success,
    Warning,
    Danger,
}

impl BadgeTone {
    /// The `--loom-*` custom property this tone draws its accent colour from.
    #[must_use]
    pub fn css_var(self) -> &'static str {
        match self {
            Self::Neutral => "--loom-text-mut",
            Self::Info => "--loom-accent",
            Self::Pii | Self::Danger => "--loom-danger",
            Self::Success => "--loom-ok",
            Self::Warning => "--loom-warn",
        }
    }
}

/// Health / build state, rendered as a coloured dot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Ok,
    Warn,
    Error,
}

impl Status {
    /// The `--loom-*` custom property for this status colour.
    #[must_use]
    pub fn css_var(self) -> &'static str {
        match self {
            Self::Ok => "--loom-ok",
            Self::Warn => "--loom-warn",
            Self::Error => "--loom-danger",
        }
    }
}

/// Horizontal cell alignment in a [`DataTable`](loom_ui_components::DataTable).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Align {
    Start,
    End,
}

/// Format a row count the way the catalog table shows it: `2_410_000` → `"2.41M"`,
/// `18_200` → `"18.2K"`, `880_000` → `"880K"`. Values below 1000 are rendered as-is.
/// Scaled values show up to 3 significant figures with trailing zeros trimmed.
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "display-only count formatting; exactness not required"
)]
pub fn format_count(n: u64) -> String {
    let (mut scaled, mut suffix) = if n >= 1_000_000 {
        (n as f64 / 1_000_000.0, "M")
    } else if n >= 1_000 {
        (n as f64 / 1_000.0, "K")
    } else {
        return n.to_string();
    };
    // 3 significant figures: 2.41M, 18.2K, 880K, 9.7K, 142K.
    let mut precision = if scaled >= 100.0 {
        0
    } else if scaled >= 10.0 {
        1
    } else {
        2
    };
    // Rounding at `precision` can push `scaled` up to (or past) 1000 within a
    // tier (e.g. 999.999K rounds to "1000K"); re-scale up one suffix tier so
    // the displayed value never overflows its own bucket.
    let multiplier = match precision {
        0 => 1.0,
        1 => 10.0,
        _ => 100.0,
    };
    let rounded = (scaled * multiplier).round() / multiplier;
    // No M->B tier: display-only formatting; loom defines no billions token/screen,
    // so values >= ~1e9 render as "NNNNM".
    if rounded >= 1000.0 && suffix == "K" {
        scaled /= 1000.0;
        suffix = "M";
        precision = if scaled >= 100.0 {
            0
        } else if scaled >= 10.0 {
            1
        } else {
            2
        };
    }
    let mut s = format!("{scaled:.precision$}");
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    format!("{s}{suffix}")
}

use serde_json::{Map, Value};

/// One page of a governed object read: the decoded rows plus the forward cursor
/// (`None` = last page). Mirrors the `{ "objects": [...], "next": ... }` wire shape.
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectsPage {
    pub rows: Vec<Map<String, Value>>,
    pub next: Option<String>,
}

/// Parse the `GET /objects/{type}` envelope. Total: missing/!array `objects` → no
/// rows; non-object members are skipped; missing/null `next` → `None`.
#[must_use]
pub fn parse_objects_page(body: &Value) -> ObjectsPage {
    let rows = body
        .get("objects")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(|v| v.as_object().cloned()).collect())
        .unwrap_or_default();
    let next = body
        .get("next")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    ObjectsPage { rows, next }
}

/// The union of object keys across `rows`, in first-seen order — stable table columns.
#[must_use]
pub fn columns_from_objects(rows: &[Map<String, Value>]) -> Vec<String> {
    let mut cols = Vec::new();
    for row in rows {
        for k in row.keys() {
            if !cols.iter().any(|c| c == k) {
                cols.push(k.clone());
            }
        }
    }
    cols
}

/// Render a JSON value for a table cell / drawer field. Total, display-only.
#[must_use]
pub fn cell_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(_) | Value::Number(_) => v.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

/// One of the app's five top-level surfaces (nav order). Backend-live surfaces are
/// Catalog and Ontology; the others render an honest "not available" stub.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Surface {
    Catalog,
    Pipelines,
    Ontology,
    Workbooks,
    Dashboards,
}

impl Surface {
    /// Nav order, left to right.
    #[must_use]
    pub fn all() -> [Surface; 5] {
        [
            Surface::Catalog,
            Surface::Pipelines,
            Surface::Ontology,
            Surface::Workbooks,
            Surface::Dashboards,
        ]
    }

    /// The nav label / list title for this surface.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Surface::Catalog => "Catalog",
            Surface::Pipelines => "Pipelines",
            Surface::Ontology => "Ontology",
            Surface::Workbooks => "Workbooks",
            Surface::Dashboards => "Dashboards",
        }
    }

    /// The per-surface accent hex (design tokens).
    #[must_use]
    pub fn accent(self) -> &'static str {
        match self {
            Surface::Catalog => "#3b82f6",
            Surface::Pipelines => "#2bb0a0",
            Surface::Ontology => "#8b5cf6",
            Surface::Workbooks => "#2da44e",
            Surface::Dashboards => "#d29922",
        }
    }

    /// Whether the backend can serve this surface (else the shell shows a stub).
    #[must_use]
    pub fn is_live(self) -> bool {
        matches!(self, Surface::Catalog | Surface::Ontology)
    }
}

/// A property row in the ontology drawer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropRow {
    pub name: String,
    pub ty: String,
    pub required: bool,
}

/// A link row (either outbound `links` or inbound `links_to`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkRow {
    pub name: String,
    pub from: String,
    pub to: String,
    pub cardinality: String,
}

/// The ontology drawer's Properties + Links data, decoded from `GET /ontology/types/{name}`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TypeDetail {
    pub properties: Vec<PropRow>,
    pub links: Vec<LinkRow>,
    pub links_to: Vec<LinkRow>,
    /// The backing dataset's schema (`table.schema`), or `""` when absent.
    pub table_schema: String,
    /// The backing dataset's table name (`table.name`), or `""` when absent.
    pub table_name: String,
    /// The identity (primary-key) property name, or `None` when the type has none.
    pub identity: Option<String>,
}

fn str_field(v: &Value, k: &str) -> String {
    v.get(k)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn parse_links(v: &Value, key: &str) -> Vec<LinkRow> {
    v.get(key)
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|l| LinkRow {
                    name: str_field(l, "name"),
                    from: str_field(l, "from"),
                    to: str_field(l, "to"),
                    cardinality: str_field(l, "cardinality"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Decode a type-detail body. Total: missing arrays → empty; missing scalars → default.
#[must_use]
pub fn parse_type_detail(body: &Value) -> TypeDetail {
    let properties = body
        .get("properties")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|p| PropRow {
                    name: str_field(p, "name"),
                    ty: str_field(p, "ty"),
                    required: p.get("required").and_then(Value::as_bool).unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default();
    let table = body.get("table");
    let table_schema = table.map(|t| str_field(t, "schema")).unwrap_or_default();
    let table_name = table.map(|t| str_field(t, "name")).unwrap_or_default();
    let identity = body
        .get("identity")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    TypeDetail {
        properties,
        links: parse_links(body, "links"),
        links_to: parse_links(body, "links_to"),
        table_schema,
        table_name,
        identity,
    }
}

/// A row in the Catalog list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetRow {
    pub schema: String,
    pub name: String,
    pub project: String,
    pub updated: String,
}

/// Decode `GET /datasets`. Missing array → empty; missing scalars → "".
#[must_use]
pub fn parse_datasets(body: &Value) -> Vec<DatasetRow> {
    body.get("datasets")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|d| DatasetRow {
                    schema: str_field(d, "schema"),
                    name: str_field(d, "name"),
                    project: str_field(d, "project"),
                    updated: str_field(d, "updated"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A schema column in the Catalog › Schema tab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaCol {
    pub name: String,
    pub ty: String,
    pub nullable: bool,
}

/// Decode `GET /datasets/{schema}/{table}`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DatasetDetail {
    pub snapshot_time: String,
    pub columns: Vec<SchemaCol>,
}

#[must_use]
pub fn parse_dataset_detail(body: &Value) -> DatasetDetail {
    let columns = body
        .get("columns")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .map(|c| SchemaCol {
                    name: str_field(c, "name"),
                    ty: str_field(c, "ty"),
                    nullable: c.get("nullable").and_then(Value::as_bool).unwrap_or(false),
                })
                .collect()
        })
        .unwrap_or_default();
    DatasetDetail {
        snapshot_time: str_field(body, "snapshot_time"),
        columns,
    }
}

/// Decode `GET /datasets/{schema}/{table}/preview`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PreviewData {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub sampled: bool,
}

#[must_use]
pub fn parse_preview(body: &Value) -> PreviewData {
    let columns = body
        .get("columns")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let rows = body
        .get("rows")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .map(|r| {
                    r.as_array()
                        .map(|cells| cells.iter().map(cell_to_string).collect())
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default();
    PreviewData {
        columns,
        rows,
        sampled: body
            .get("sampled")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

/// Where a node sits relative to the current dataset in the mini-DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Upstream,
    Current,
    Downstream,
}

/// A node in the lineage mini-DAG. `column` is 0 (upstream) / 1 (current) / 2 (downstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagNode {
    pub id: String,
    pub label: String,
    pub kind: NodeKind,
    pub column: usize,
}

/// A directed edge (producer → consumer) between two node ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagEdge {
    pub from: String,
    pub to: String,
}

/// The assembled lineage mini-DAG.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LineageDag {
    pub nodes: Vec<DagNode>,
    pub edges: Vec<DagEdge>,
}

fn dataset_id(ns: &str, name: &str) -> String {
    format!("{ns}.{name}")
}

/// Build the three-column mini-DAG for `current` from its upstream/downstream closures.
/// Datasets equal to `current` are dropped from the closures (the current node is unique);
/// edges run producer → current → consumer.
#[must_use]
pub fn lineage_dag(
    current: (&str, &str),
    upstream: &[(String, String)],
    downstream: &[(String, String)],
) -> LineageDag {
    let (cur_ns, cur_name) = current;
    let cur_id = dataset_id(cur_ns, cur_name);
    let mut nodes = vec![DagNode {
        id: cur_id.clone(),
        label: cur_name.to_string(),
        kind: NodeKind::Current,
        column: 1,
    }];
    let mut edges = Vec::new();

    for (ns, name) in upstream {
        let id = dataset_id(ns, name);
        if id == cur_id {
            continue;
        }
        nodes.push(DagNode {
            id: id.clone(),
            label: name.clone(),
            kind: NodeKind::Upstream,
            column: 0,
        });
        edges.push(DagEdge {
            from: id,
            to: cur_id.clone(),
        });
    }
    for (ns, name) in downstream {
        let id = dataset_id(ns, name);
        if id == cur_id {
            continue;
        }
        nodes.push(DagNode {
            id: id.clone(),
            label: name.clone(),
            kind: NodeKind::Downstream,
            column: 2,
        });
        edges.push(DagEdge {
            from: cur_id.clone(),
            to: id,
        });
    }
    LineageDag { nodes, edges }
}
