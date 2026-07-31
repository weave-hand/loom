//! Unit coverage for the pure halves of the `POST /sql/validate` endpoint: the `EXPLAIN`
//! wrapper (which must not double-wrap SQL the caller already explained), the refusal of
//! an `EXPLAIN ANALYZE` that would execute rather than plan, and the extractor that lifts
//! sqlparser's embedded `at Line: N, Column: M` into typed position fields — shifted back
//! into the caller's own coordinates. All four are total, DOM-free, engine-free functions.

use query_api::sql_validate::{
    DiagnosticSeverity, SqlDiagnostic, analyze_refusal, explain_wrap, parse_error_position,
    plan_diagnostic,
};

#[test]
fn extracts_line_and_column_from_a_parser_message() {
    let msg = "SQL error: ParserError(\"Expected: an SQL statement, found: SELCT at Line: 1, Column: 8\")";
    assert_eq!(parse_error_position(msg), Some((1, 8)));
}

#[test]
fn extracts_multi_digit_position() {
    assert_eq!(
        parse_error_position("boom at Line: 12, Column: 345"),
        Some((12, 345))
    );
}

#[test]
fn plan_message_without_a_position_is_none() {
    let msg = "Schema error: No field named foo. Valid fields are \"wh\".\"orders\".\"id\".";
    assert_eq!(parse_error_position(msg), None);
}

#[test]
fn malformed_position_is_none() {
    assert_eq!(parse_error_position("at Line: x, Column: 3"), None);
    assert_eq!(parse_error_position("at Line: 1, Column:"), None);
    assert_eq!(parse_error_position("at Line: 1"), None);
}

#[test]
fn position_overflow_is_none_not_a_panic() {
    assert_eq!(
        parse_error_position("at Line: 99999999999999999999, Column: 1"),
        None
    );
}

#[test]
fn explain_wrap_prepends_explain_and_reports_its_offset() {
    assert_eq!(
        explain_wrap("SELECT 1"),
        ("EXPLAIN SELECT 1".to_owned(), 8),
        "`EXPLAIN ` is 8 characters, all of them on line 1"
    );
}

#[test]
fn explain_wrap_does_not_double_wrap_and_adds_no_offset() {
    // A caller who typed their own EXPLAIN gets it planned as-is; wrapping again
    // would report a spurious diagnostic on perfectly valid SQL. Nothing was
    // prepended, so nothing must be subtracted from the reported columns.
    assert_eq!(
        explain_wrap("EXPLAIN SELECT 1"),
        ("EXPLAIN SELECT 1".to_owned(), 0)
    );
    assert_eq!(
        explain_wrap("explain  select 1"),
        ("explain  select 1".to_owned(), 0)
    );
    assert_eq!(
        explain_wrap("\n  EXPLAIN SELECT 1"),
        ("\n  EXPLAIN SELECT 1".to_owned(), 0)
    );
}

#[test]
fn explain_wrap_treats_explain_prefixed_identifiers_as_ordinary_sql() {
    // `EXPLAIN` only counts as the keyword when it is a whole word. These two inputs are
    // the ones that actually exercise the whole-word guard: each one BEGINS with the
    // seven letters `explain`, and only what follows distinguishes the keyword from an
    // ordinary identifier. (A statement merely CONTAINING `explainer` never reaches the
    // guard — that is the plain-wrap path, covered above.)
    assert_eq!(
        explain_wrap("explainer"),
        ("EXPLAIN explainer".to_owned(), 8)
    );
    assert_eq!(
        explain_wrap("EXPLAIN_ME"),
        ("EXPLAIN EXPLAIN_ME".to_owned(), 8)
    );
}

#[test]
fn plan_diagnostic_subtracts_the_wrapper_offset_on_line_one() {
    // The engine parsed `EXPLAIN SELCT 1` and reported column 9; the user's editor
    // holds `SELCT 1`, where the same token is at column 1.
    let d = plan_diagnostic("… found: SELCT at Line: 1, Column: 9".to_owned(), 8);
    assert_eq!(d.line, Some(1));
    assert_eq!(d.start_col, Some(1), "9 - 8 = the user's own column");
    assert_eq!(d.end_col, Some(2), "end_col is exclusive");
}

#[test]
fn plan_diagnostic_does_not_subtract_beyond_the_first_line() {
    // Only line 1 was lengthened by the wrapper; every later line is untouched.
    let d = plan_diagnostic("bad at Line: 2, Column: 5".to_owned(), 8);
    assert_eq!(d.line, Some(2));
    assert_eq!(d.start_col, Some(5));
    assert_eq!(d.end_col, Some(6));
}

#[test]
fn plan_diagnostic_clamps_a_column_it_cannot_shift_below_one() {
    let d = plan_diagnostic("bad at Line: 1, Column: 3".to_owned(), 8);
    assert_eq!(d.start_col, Some(1), "saturating, never zero or wrapped");
    assert_eq!(d.end_col, Some(2));
}

#[test]
fn plan_diagnostic_keeps_the_message_and_severity() {
    let d = plan_diagnostic("bad at Line: 2, Column: 5".to_owned(), 0);
    assert_eq!(d.severity, DiagnosticSeverity::Error);
    assert_eq!(d.message, "bad at Line: 2, Column: 5");
}

#[test]
fn plan_diagnostic_leaves_positions_null_when_absent() {
    let d: SqlDiagnostic = plan_diagnostic("Schema error: No field named foo.".to_owned(), 8);
    assert_eq!(d.line, None);
    assert_eq!(d.start_col, None);
    assert_eq!(d.end_col, None);
}

#[test]
fn null_positions_serialize_as_json_null_not_omitted() {
    let d = plan_diagnostic("Schema error: No field named foo.".to_owned(), 8);
    let v = serde_json::to_value(&d).expect("serialize");
    for key in ["line", "start_col", "end_col"] {
        assert!(
            v.get(key)
                .unwrap_or_else(|| panic!("{key} key present"))
                .is_null(),
            "{key} must serialize as null, not be omitted: {v}"
        );
    }
}

#[test]
fn severity_serializes_as_the_lowercase_wire_word() {
    let d = plan_diagnostic("boom".to_owned(), 0);
    let v = serde_json::to_value(&d).expect("serialize");
    assert_eq!(v["severity"], serde_json::json!("error"));
}

#[test]
fn takes_the_last_position_because_the_echoed_token_can_contain_one() {
    // sqlparser appends the position as a suffix and echoes the offending token ahead of
    // it. A token that itself contains `at Line: ` must not win — this is the one case
    // that distinguishes `rsplit_once` from `split_once`.
    assert_eq!(
        parse_error_position(r#"found: "at Line: 9, Column: 9" at Line: 1, Column: 15"#),
        Some((1, 15))
    );
}

#[test]
fn the_wrapper_offset_round_trips_to_the_users_own_column() {
    // Couples the two halves: whatever `explain_wrap` prepends, `plan_diagnostic` must
    // undo. Independently-asserted constants would both stay green if the prefix changed.
    let (statement, offset) = explain_wrap("SELCT 1");
    let col = u32::try_from(statement.find("SELCT").expect("token in wrapped statement") + 1)
        .expect("column fits u32");
    let d = plan_diagnostic(format!("found: SELCT at Line: 1, Column: {col}"), offset);
    assert_eq!(
        (d.line, d.start_col),
        (Some(1), Some(1)),
        "the token is at column 1 of the caller's own text"
    );
}

#[test]
fn explain_analyze_is_refused_rather_than_planned() {
    // `EXPLAIN ANALYZE` executes the statement to measure it. `explain_wrap` would pass it
    // through untouched, so the refusal is what keeps validation execution-free.
    let d = analyze_refusal("EXPLAIN ANALYZE SELECT * FROM orders").expect("refused");
    assert_eq!(d.severity, DiagnosticSeverity::Error);
    assert!(d.message.contains("ANALYZE"), "message: {}", d.message);
    assert_eq!(
        (d.line, d.start_col, d.end_col),
        (None, None, None),
        "positions are left for the client to anchor"
    );
}

#[test]
fn explain_analyze_refusal_is_case_and_whitespace_insensitive() {
    assert!(analyze_refusal("explain   analyze select 1").is_some());
    assert!(analyze_refusal("\n  EXPLAIN\n  ANALYZE\n  SELECT 1").is_some());
}

#[test]
fn a_comment_cannot_smuggle_analyze_past_the_refusal() {
    // sqlparser treats comments as whitespace, so the engine reads every one of these as
    // a genuine `EXPLAIN ANALYZE` and EXECUTES it. A keyword scan that treats a comment
    // body as an ordinary word sees `EXPLAIN`, `x` and waves them through.
    for sql in [
        "EXPLAIN /*x*/ ANALYZE SELECT 1",
        "EXPLAIN --x\n ANALYZE SELECT 1",
        "/* lead */ EXPLAIN ANALYZE SELECT 1",
        "-- lead\nEXPLAIN ANALYZE SELECT 1",
        "EXPLAIN/*a*//*b*/ANALYZE SELECT 1",
    ] {
        assert!(
            analyze_refusal(sql).is_some(),
            "comment-smuggled ANALYZE must still be refused: {sql:?}"
        );
    }
}

#[test]
fn a_leading_comment_does_not_hide_the_callers_own_explain() {
    // The same scan drives double-wrap detection: miss the `EXPLAIN` behind a comment and
    // the statement gets wrapped again, reporting a spurious diagnostic on valid SQL.
    assert_eq!(
        explain_wrap("/* note */ EXPLAIN SELECT 1"),
        ("/* note */ EXPLAIN SELECT 1".to_owned(), 0)
    );
    assert_eq!(
        explain_wrap("-- note\nEXPLAIN SELECT 1"),
        ("-- note\nEXPLAIN SELECT 1".to_owned(), 0)
    );
}

#[test]
fn an_unterminated_comment_consumes_the_rest_and_refuses_nothing() {
    // Matches what the parser will make of it; the engine reports the real fault.
    assert!(analyze_refusal("/* unterminated EXPLAIN ANALYZE SELECT 1").is_none());
    assert_eq!(
        explain_wrap("/* unterminated"),
        ("EXPLAIN /* unterminated".to_owned(), 8)
    );
}

#[test]
fn ordinary_statements_are_not_refused() {
    for sql in [
        "SELECT 1",
        "EXPLAIN SELECT 1",
        "SELECT analyze FROM t",
        "EXPLAIN",
        "",
        "ANALYZE t",
    ] {
        assert!(analyze_refusal(sql).is_none(), "must not refuse: {sql:?}");
    }
}
