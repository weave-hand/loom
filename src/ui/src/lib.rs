//! Platform-neutral logic shared by the wasm `app` binary and its native tests.
//! Pure Rust (no web-sys), so it compiles for the host and is `rust_test`-able.

use std::fmt;

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
