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

use serde::{Deserialize, Serialize};

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

/// The maximal identifier-ish words of `sql`, in order — the cheapest whole-word view of
/// a statement's leading keywords that needs no indexing and no tokenizer.
fn ident_words(sql: &str) -> impl Iterator<Item = &str> {
    sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
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
    let mut words = ident_words(sql);
    let first = words.next()?;
    let second = words.next()?;
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
    // The first identifier run IS the keyword iff it is exactly `EXPLAIN` — the whole-word
    // rule without any byte/char index juggling.
    let already = ident_words(sql)
        .next()
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
