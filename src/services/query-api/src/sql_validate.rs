//! `POST /sql/validate` — governed, execution-free SQL validation. The caller's SQL is
//! planned by the engine's DataFusion (via `EXPLAIN` through the same `execute_governed`
//! substrate the SQL console uses) and any planning fault comes back as a typed
//! diagnostic rather than an error status: a *valid request* about *invalid SQL* is a
//! `200`, and the diagnostic is the payload. This deliberately differs from `POST /sql`,
//! where a plan fault is a `400`.
//!
//! Two properties are load-bearing:
//!
//! * **The governed catalog is resolved server-side**, never from the wire. Validating
//!   outside the caller's catalog would let an unauthorized subject probe whether a
//!   table exists; an ungranted table must be indistinguishable from a nonexistent one
//!   here exactly as it is on `POST /sql`.
//! * **The plan text is discarded.** `EXPLAIN` returns the physical plan, which names
//!   file paths and storage layout. This endpoint returns diagnostics only — a
//!   deliberate refusal with its own e2e test, not an incidental omission.
//!
//! Validation is execution-free: the statement is planned, never run. The one input that
//! would break that is a caller-written `EXPLAIN ANALYZE`, which DataFusion executes to
//! collect metrics — [`analyze_refusal`] turns it into a diagnostic instead of planning
//! it.

use axum::extract::{Json, State};
use serde::{Deserialize, Serialize};
use service_runtime::Subject;

use crate::governed::resolve_governed_catalog;
use crate::handler::QueryError;
use crate::http::{AppState, query_error_response};
use crate::serving::ServingError;

/// Severity of a [`SqlDiagnostic`]. The engine only ever reports planning *errors*, so
/// this carries a single variant today; it is an enum (not a bare string) so the wire
/// vocabulary stays closed and a future warning class is an additive change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum DiagnosticSeverity {
    Error,
}

/// One editor diagnostic. Positions mirror `loom_ui_core::Diagnostic` — 1-based `line`
/// and `start_col`, **exclusive** `end_col` (Monaco's convention) — and are `null` when
/// the engine's message carried no position (every *plan* error; only *parser* errors
/// embed one). The client anchors a position-less diagnostic against the live editor
/// text via `loom_ui_core::anchor_diagnostic`.
///
/// Columns are char offsets, exact for ASCII — the SQL-identifier case — carrying the
/// same caveat as `loom_ui_core::Diagnostic`: sqlparser counts chars while Monaco's
/// `startColumn` counts UTF-16 units, so a line containing an astral-plane literal can
/// place the squiggle a unit or two off. Keep this shape compatible with that type;
/// `loom_ui_core::parse_server_diagnostics` parses this wire shape by hand, so a drift
/// would surface only in an e2e.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct SqlDiagnostic {
    pub message: String,
    pub severity: DiagnosticSeverity,
    pub line: Option<u32>,
    pub start_col: Option<u32>,
    pub end_col: Option<u32>,
}

/// The `POST /sql/validate` response: an empty list means the statement planned cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct SqlValidateResponse {
    pub diagnostics: Vec<SqlDiagnostic>,
}

/// The `POST /sql/validate` request body. `deny_unknown_fields` so a typo'd field is a
/// 400 rather than silently ignored, matching [`crate::sql_console::SqlQueryRequest`].
/// There is no `limit`: nothing is executed, so nothing needs capping.
#[derive(Debug, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SqlValidateRequest {
    pub sql: String,
}

/// The leading run of ASCII digits in `s`, as a `u32`. `None` when there are none or the
/// run overflows — the totality both callers rely on.
fn leading_u32(s: &str) -> Option<u32> {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    s.get(..end)?.parse().ok()
}

/// Extract the 1-based `(line, column)` that sqlparser embeds in a parse-error message
/// (`… at Line: 1, Column: 8` — `sqlparser::tokenizer`'s `Display`). Returns `None` for
/// every message without one, which is every *plan* error (`Schema error: No field named
/// …` carries no position at all). Total: a malformed or overflowing number is `None`,
/// never a panic.
///
/// `rsplit_once`, not `split_once`: sqlparser appends the position as a *suffix*, and the
/// message ahead of it echoes the offending token — which can itself contain the literal
/// `at Line: `. The last occurrence is the one sqlparser wrote.
#[must_use]
pub fn parse_error_position(message: &str) -> Option<(u32, u32)> {
    let (_head, tail) = message.rsplit_once("at Line: ")?;
    let (line, rest) = tail.split_once(", Column: ")?;
    Some((line.parse().ok()?, leading_u32(rest)?))
}

/// The first two identifier words of `sql`, skipping leading whitespace and SQL comments
/// (`-- …` to end of line, `/* … */`) the way a parser does. Stops after two words —
/// that is all either caller needs.
///
/// **Comment-awareness is load-bearing, not cosmetic.** A naive split on non-identifier
/// characters treats a comment body as ordinary words, so `EXPLAIN /*x*/ ANALYZE …` reads
/// as `EXPLAIN`, `x` — slipping past [`analyze_refusal`] into the engine, which then
/// *executes* it. sqlparser treats comments as whitespace, so anything this scanner
/// disagrees with the parser about is a hole in the refusal.
///
/// Unterminated comments consume the rest of the input, matching what the parser will
/// make of them.
fn leading_keywords(sql: &str) -> (Option<&str>, Option<&str>) {
    let mut rest = sql.trim_start();
    let mut out: Vec<&str> = Vec::with_capacity(2);
    while out.len() < 2 && !rest.is_empty() {
        rest = if let Some(after) = rest.strip_prefix("--") {
            after
                .find('\n')
                .map_or("", |k| after.get(k..).unwrap_or(""))
        } else if let Some(after) = rest.strip_prefix("/*") {
            after
                .find("*/")
                .map_or("", |k| after.get(k.saturating_add(2)..).unwrap_or(""))
        } else {
            let end = rest
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            if end == 0 {
                // A non-identifier character (punctuation, a quote, …) — consume just it,
                // so the scan always makes progress.
                rest.get(rest.chars().next().map_or(0, char::len_utf8)..)
                    .unwrap_or("")
            } else {
                if let Some(word) = rest.get(..end) {
                    out.push(word);
                }
                rest.get(end..).unwrap_or("")
            }
        }
        .trim_start();
    }
    (out.first().copied(), out.get(1).copied())
}

/// The wrapper this module prepends when the caller did not write their own `EXPLAIN`.
const EXPLAIN_PREFIX: &str = "EXPLAIN ";

/// Columns [`EXPLAIN_PREFIX`] adds to line 1. Derived from the prefix at compile time so
/// the two cannot drift: a hand-written number that disagreed with the prefix would skew
/// every reported line-1 column by the difference. (The cast is const-evaluated over a
/// fixed 8-byte ASCII literal; `try_from` is not available in const context, and a
/// runtime fallback here would mean silently reporting "nothing was prepended" when
/// something was.)
const EXPLAIN_COLS: u32 = EXPLAIN_PREFIX.len() as u32;

/// `Some(diagnostic)` when `sql` is an `EXPLAIN ANALYZE …` statement, which this endpoint
/// refuses rather than plans.
///
/// [`explain_wrap`] passes a caller-written `EXPLAIN` through untouched, and DataFusion's
/// `EXPLAIN ANALYZE` **executes** the statement to collect its runtime metrics — it is not
/// a planning-only node. On a debounced, per-keystroke validation endpoint that is an
/// unbounded cost the caller never asked for, and it would falsify this module's
/// execution-free contract. Governance is unaffected either way (the registered providers
/// are governed and read-only, and the engine's own budgets still apply); this is about
/// cost, and about the endpoint keeping the promise it makes.
///
/// Positions are left `null` on purpose: the message names `EXPLAIN ANALYZE` in
/// backticks, so the client's `anchor_diagnostic` resolves it against the live editor
/// text and squiggles the caller's own keywords, wherever on the buffer they sit.
#[must_use]
pub fn analyze_refusal(sql: &str) -> Option<SqlDiagnostic> {
    let (first, second) = leading_keywords(sql);
    let (first, second) = (first?, second?);
    if first.eq_ignore_ascii_case("EXPLAIN") && second.eq_ignore_ascii_case("ANALYZE") {
        return Some(SqlDiagnostic {
            message: "`EXPLAIN ANALYZE` runs the statement to measure it; validation only \
                      plans. Drop `ANALYZE` to validate this query."
                .to_owned(),
            severity: DiagnosticSeverity::Error,
            line: None,
            start_col: None,
            end_col: None,
        });
    }
    None
}

/// Prefix `sql` with `EXPLAIN ` so the engine plans it without executing it, returning
/// `(statement, line-1 column offset)`.
///
/// The offset is what makes the reported positions usable: sqlparser reports a column
/// absolute within the string it parsed, so a token the engine calls column 9 of
/// `EXPLAIN SELCT 1` is column 1 of the user's `SELCT 1`. Only line 1 grows, so only
/// line 1's columns need shifting back — see [`plan_diagnostic`].
///
/// A caller who already wrote their own `EXPLAIN` gets their statement untouched with a
/// zero offset: `EXPLAIN EXPLAIN …` is not a valid statement, and reporting that as a
/// diagnostic would be a false positive on perfectly valid SQL. (`EXPLAIN ANALYZE` is the
/// one pass-through that is refused instead — see [`analyze_refusal`].) The keyword counts
/// only as a whole word, so `explainer` and `EXPLAIN_ME` are ordinary identifiers and are
/// wrapped like anything else.
#[must_use]
pub fn explain_wrap(sql: &str) -> (String, u32) {
    // The first keyword IS `EXPLAIN` only as a whole word — and only when it is really
    // the leading keyword, not the contents of a leading comment.
    let already = leading_keywords(sql)
        .0
        .is_some_and(|w| w.eq_ignore_ascii_case("EXPLAIN"));
    if already {
        (sql.to_owned(), 0)
    } else {
        (format!("{EXPLAIN_PREFIX}{sql}"), EXPLAIN_COLS)
    }
}

/// Turn the engine's planning-fault message into a diagnostic, resolving the embedded
/// parser position when there is one and shifting it back into the caller's own
/// coordinates.
///
/// `col_offset` is [`explain_wrap`]'s second return value — the columns this module
/// added to line 1. It is subtracted from a line-1 position and from nothing else, then
/// floored at 1 (`saturating_sub` cannot underflow; the `.max(1)` is what keeps the
/// result a valid 1-based column). A message with no position (every *plan* error) yields
/// all-`null` positions, which the client anchors against the live editor text.
///
/// The message is echoed verbatim: it is the caller's own SQL vocabulary (the same
/// reasoning that makes it safe on `POST /sql`'s 400).
#[must_use]
pub fn plan_diagnostic(message: String, col_offset: u32) -> SqlDiagnostic {
    let pos = parse_error_position(&message).map(|(line, col)| {
        let shifted = if line == 1 {
            col.saturating_sub(col_offset).max(1)
        } else {
            col
        };
        (line, shifted)
    });
    SqlDiagnostic {
        line: pos.map(|(l, _c)| l),
        start_col: pos.map(|(_l, c)| c),
        end_col: pos.map(|(_l, c)| c.saturating_add(1)),
        message,
        severity: DiagnosticSeverity::Error,
    }
}

/// Rows buffered from the `EXPLAIN` result. The plan text is discarded, so the smallest
/// non-zero cap is right: it bounds the buffer without changing what is reported.
/// Row cap handed to `execute_governed`. It collects batches until the cumulative count
/// *exceeds* the cap, so any value below the first batch's size stops the stream after
/// **one batch** — which is the real bound here, not one row. The rows are discarded
/// either way; this exists so a pathological plan string cannot be buffered indefinitely.
const VALIDATE_MAX_ROWS: usize = 1;

/// Shape a diagnostic list into the `POST /sql/validate` body. An empty list is the
/// "planned cleanly" answer, so every exit from [`validate_sql`] goes through here.
fn diagnostics_response(diagnostics: Vec<SqlDiagnostic>) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    Json(SqlValidateResponse { diagnostics }).into_response()
}

/// `POST /sql/validate` — plan the authenticated subject's SQL under their governed
/// catalog and return typed diagnostics, executing nothing.
///
/// The statement is wrapped in `EXPLAIN` and run through the SQL console's own
/// `execute_governed` substrate, so DataFusion plans it fully — unknown columns, type
/// errors, and bad function signatures are all caught, which is exactly what no
/// client-side heuristic can do — while execution is only plan formatting. `EXPLAIN`
/// passes the read-only guard untouched: `SQLOptions::verify_plan` gates `Ddl`, `Dml`
/// and `Statement` nodes, and descends into `Explain`'s inner plan, so `EXPLAIN INSERT …`
/// is still rejected while `EXPLAIN SELECT …` is not.
///
/// A planning fault is a `200` carrying one diagnostic — a valid request about invalid
/// SQL is not an error. Empty SQL is likewise a `200` with no diagnostics: a
/// debounce-driven editor sends the empty buffer routinely and an empty statement is not
/// a *fault*, it is nothing to say. Every other engine fault delegates to the shared
/// [`query_error_response`], so this route's status mapping cannot drift from
/// `POST /sql`'s (and inherits future `ServingError` arms without a second match).
#[utoipa::path(
    post, path = "/sql/validate",
    request_body = SqlValidateRequest,
    responses(
        (status = 200, description = "Diagnostics for the statement (empty when it plans cleanly)", body = SqlValidateResponse),
        (status = 400, description = "Malformed request body"),
        (status = 500, description = "Serving error"),
    ),
    security(("bearer_auth" = [])),
    tag = "sql",
)]
pub async fn validate_sql(
    State(st): State<AppState>,
    subject: Subject,
    Json(req): Json<SqlValidateRequest>,
) -> axum::response::Response {
    if req.sql.trim().is_empty() {
        return diagnostics_response(Vec::new());
    }
    // `EXPLAIN ANALYZE` executes the statement to measure it, so it is refused before any
    // engine round-trip rather than passed through by `explain_wrap`. Validation is
    // plan-only; see `analyze_refusal`.
    //
    // This sits BEFORE the catalog resolve deliberately: the refusal reads no data, names
    // no table, and its output is a pure function of the caller's own body, so it leaks
    // nothing and skips no authorization check (`resolve_governed_catalog` constructs a
    // catalog, it is not a gate — it has no `Forbidden` outcome). It also avoids N+2
    // control-plane round trips for a request that is going to be refused, which matters
    // on a debounced per-keystroke endpoint. INVARIANT: if a route-level permission
    // ("may use the SQL console") is ever added, it must land ABOVE this line.
    if let Some(d) = analyze_refusal(&req.sql) {
        return diagnostics_response(vec![d]);
    }
    // Server-resolved, never from the wire: validating outside the caller's catalog
    // would turn this route into an existence oracle for tables they cannot read.
    let catalog = match resolve_governed_catalog(st.cp.ontology(), st.cp.acl(), &subject.0).await {
        Ok(c) => c,
        Err(e) => return query_error_response(e, "sql validate catalog"),
    };
    // The RAW `sql` is wrapped, deliberately not the trimmed one: trimming would delete
    // leading newlines and shift every reported line number away from the line the user
    // is actually looking at. Only the emptiness check above trims.
    let (statement, col_offset) = explain_wrap(&req.sql);
    match st
        .serving
        .execute_governed(statement, catalog, VALIDATE_MAX_ROWS)
        .await
    {
        // The plan text is deliberately dropped here — it names file paths and storage
        // layout, and the caller asked whether their SQL is valid, not how it will run.
        //
        // Treating a successful stream as "planned cleanly" is sound only because EXPLAIN
        // cannot fail after its first batch: `ExplainExec` emits precomputed plan text
        // with no upstream execution, so a fault surfaces either when opening the stream
        // or on the first item, and both map to `ServingError::Plan`. The row cap above
        // discards later batches, so for a statement that really executed, a late fault
        // would be swallowed into a false 200 — which is the other reason
        // `analyze_refusal` must keep `EXPLAIN ANALYZE` off this path.
        Ok(_plan_rows) => diagnostics_response(Vec::new()),
        Err(ServingError::Plan(m)) => diagnostics_response(vec![plan_diagnostic(m, col_offset)]),
        Err(e) => query_error_response(QueryError::Serving(e), "sql validate execute"),
    }
}
