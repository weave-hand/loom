//! Unit coverage for the client half of server-side SQL diagnostics: anchoring a
//! position-less engine message to a range in the editor's own text, and parsing a
//! `POST /sql/validate` response body into `Diagnostic`s. Both are pure and DOM-free.

use loom_ui_core::{Diagnostic, DiagnosticSeverity, anchor_diagnostic, parse_server_diagnostics};

#[test]
fn anchors_to_an_identifier_the_message_names() {
    let sql = "SELECT nope FROM orders";
    let got = anchor_diagnostic(sql, "Schema error: No field named nope.");
    assert_eq!(got, Some((1, 8, 12)), "1-based, end exclusive");
}

#[test]
fn anchors_to_a_quoted_identifier() {
    let sql = "SELECT \"total\" FROM orders";
    // A double-quoted name in the message resolves against the same name in the SQL,
    // ignoring the quotes on both sides.
    let got = anchor_diagnostic(sql, "Schema error: No field named \"total\".");
    assert_eq!(got, Some((1, 9, 14)));
}

#[test]
fn anchors_to_the_last_segment_of_a_qualified_name() {
    let sql = "SELECT email FROM orders";
    let got = anchor_diagnostic(sql, "No field named \"wh\".\"orders\".\"email\".");
    assert_eq!(got, Some((1, 8, 13)));
}

#[test]
fn anchors_a_real_server_message_through_its_query_planning_failed_prefix() {
    // The wire message the engine actually returns, captured from the e2e: a
    // `query planning failed: ` prefix, the offending field, and a `Valid fields are …`
    // tail that names the table (`orders`) BEFORE the column (`email`) in quoted form.
    // The offending field name must still win, so the squiggle lands on the column the
    // user typed and not on the table name that also occurs in their SQL.
    let sql = "SELECT email FROM orders";
    let message = concat!(
        "query planning failed: Schema error: No field named email. ",
        "Valid fields are \"wh\".\"orders\".\"id\", \"wh\".\"orders\".\"customer_id\"."
    );
    assert_eq!(anchor_diagnostic(sql, message), Some((1, 8, 13)));
}

#[test]
fn anchors_on_the_correct_line_of_multi_line_sql() {
    let sql = "SELECT id,\n       nope\nFROM orders";
    let got = anchor_diagnostic(sql, "No field named nope.");
    assert_eq!(got, Some((2, 8, 12)));
}

#[test]
fn falls_back_to_the_first_non_empty_line_when_nothing_resolves() {
    let sql = "\n\n  SELECT 1";
    let got = anchor_diagnostic(sql, "Some totally unrelated engine complaint");
    assert_eq!(got, Some((3, 1, 11)), "the whole first non-empty line");
}

#[test]
fn empty_sql_anchors_to_nothing() {
    assert_eq!(anchor_diagnostic("", "No field named nope."), None);
    assert_eq!(anchor_diagnostic("   \n\n", "No field named nope."), None);
}

#[test]
fn matches_an_identifier_only_as_a_whole_word() {
    // `id` must not match inside `idempotent`.
    let sql = "SELECT idempotent, id FROM t";
    let got = anchor_diagnostic(sql, "No field named id.");
    assert_eq!(got, Some((1, 20, 22)));
}

#[test]
fn parses_a_server_diagnostic_with_typed_positions() {
    let body = serde_json::json!({
        "diagnostics": [
            { "message": "boom", "severity": "error", "line": 2, "start_col": 5, "end_col": 9 }
        ]
    });
    let got = parse_server_diagnostics(&body, "SELECT 1\nSELECT boom");
    assert_eq!(
        got,
        vec![Diagnostic {
            line: 2,
            start_col: 5,
            end_col: 9,
            message: "boom".to_owned(),
            severity: DiagnosticSeverity::Error,
        }]
    );
}

#[test]
fn parses_a_null_positioned_diagnostic_by_anchoring_it() {
    let body = serde_json::json!({
        "diagnostics": [
            { "message": "No field named nope.", "severity": "error",
              "line": null, "start_col": null, "end_col": null }
        ]
    });
    let got = parse_server_diagnostics(&body, "SELECT nope FROM orders");
    assert_eq!(got.len(), 1);
    assert_eq!((got[0].line, got[0].start_col, got[0].end_col), (1, 8, 12));
    assert_eq!(got[0].message, "No field named nope.");
}

#[test]
fn parses_an_empty_diagnostics_list() {
    let body = serde_json::json!({ "diagnostics": [] });
    assert!(parse_server_diagnostics(&body, "SELECT 1").is_empty());
}

#[test]
fn a_malformed_body_yields_no_diagnostics() {
    assert!(parse_server_diagnostics(&serde_json::json!({}), "SELECT 1").is_empty());
    assert!(parse_server_diagnostics(&serde_json::json!(7), "SELECT 1").is_empty());
}

#[test]
fn an_unanchorable_diagnostic_over_empty_sql_is_dropped() {
    let body = serde_json::json!({
        "diagnostics": [{ "message": "boom", "severity": "error",
                          "line": null, "start_col": null, "end_col": null }]
    });
    assert!(parse_server_diagnostics(&body, "").is_empty());
}

#[test]
fn anchors_via_a_quoted_span_when_no_field_name_is_named() {
    // Not every planning fault is a `No field named …`. When the message names its
    // subject only in quotes, the quoted-span candidates are the ONLY thing that can
    // resolve it — without this case the whole quoted-span path could be deleted and
    // every other test would still pass.
    let sql = "SELECT a FROM missing_tbl";
    let got = anchor_diagnostic(
        sql,
        "Error during planning: table \"missing_tbl\" not found",
    );
    assert_eq!(got, Some((1, 15, 26)));
}

#[test]
fn a_quoted_span_resolves_after_an_unmatched_field_name() {
    // The field name is tried first and fails to match; resolution must then fall
    // through to the quoted span rather than straight to the first-line fallback.
    let sql = "SELECT total FROM orders";
    let got = anchor_diagnostic(
        sql,
        "No field named absent_col. Valid fields are \"total\".",
    );
    assert_eq!(got, Some((1, 8, 13)), "resolved via the quoted `total`");
}

#[test]
fn a_trailing_comma_on_the_field_name_still_resolves() {
    // `nope,` can never match a token; stripping the punctuation is what keeps this
    // from silently degrading to a whole-first-line squiggle.
    let sql = "SELECT nope FROM orders";
    let got = anchor_diagnostic(sql, "No field named nope, valid fields are id");
    assert_eq!(got, Some((1, 8, 12)));
}

#[test]
fn anchors_multibyte_identifiers_by_character_not_byte() {
    // Columns are char offsets; a byte-based scan would report 10 here, not 8.
    let sql = "SELECT 日本語 FROM t";
    let got = anchor_diagnostic(sql, "No field named 日本語.");
    assert_eq!(got, Some((1, 8, 11)));
}
