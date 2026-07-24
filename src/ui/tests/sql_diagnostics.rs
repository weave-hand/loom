use loom_ui_core::{
    CompletionColumn, CompletionSchema, CompletionTable, Diagnostic, DiagnosticSeverity,
    sql_diagnostics,
};

fn schema() -> CompletionSchema {
    CompletionSchema {
        tables: vec![
            CompletionTable {
                schema: None,
                name: "customers".to_owned(),
                columns: vec![CompletionColumn {
                    name: "id".to_owned(),
                    ty: "int64".to_owned(),
                }],
            },
            CompletionTable {
                schema: Some("public".to_owned()),
                name: "orders".to_owned(),
                columns: vec![CompletionColumn {
                    name: "id".to_owned(),
                    ty: "int64".to_owned(),
                }],
            },
        ],
    }
}

fn messages(d: &[Diagnostic]) -> Vec<String> {
    d.iter().map(|x| x.message.clone()).collect()
}

#[test]
fn known_table_is_not_flagged() {
    let d = sql_diagnostics(&schema(), "SELECT id FROM customers");
    assert!(d.is_empty(), "expected no diagnostics, got {d:?}");
}

#[test]
fn unknown_table_is_flagged_with_position() {
    // "SELECT id FROM custommers": cols S=1..T=6, ' '=7, id=8..9, ' '=10,
    // FROM=11..14, ' '=15, 'custommers'=16..25. Start col 16 (1-based).
    let d = sql_diagnostics(&schema(), "SELECT id FROM custommers");
    assert_eq!(d.len(), 1);
    let diag = d.first().expect("one diagnostic");
    assert_eq!(diag.severity, DiagnosticSeverity::Warning);
    assert_eq!(diag.line, 1);
    assert_eq!(diag.start_col, 16);
    assert_eq!(diag.end_col, 26); // exclusive: 16 + len("custommers")=10
    assert!(diag.message.contains("custommers"));
}

#[test]
fn unknown_table_case_insensitive_match_not_flagged() {
    let d = sql_diagnostics(&schema(), "select id from CUSTOMERS");
    assert!(
        d.is_empty(),
        "case-insensitive table match should pass: {d:?}"
    );
}

#[test]
fn join_table_unknown_is_flagged() {
    let d = sql_diagnostics(
        &schema(),
        "SELECT * FROM customers JOIN nope ON customers.id = nope.id",
    );
    let m = messages(&d);
    assert!(
        m.iter().any(|s| s.contains("nope")),
        "join table should be checked: {m:?}"
    );
    assert!(
        !m.iter().any(|s| s.contains("customers")),
        "known table not flagged: {m:?}"
    );
}

#[test]
fn comma_join_list_checks_each_table() {
    let d = sql_diagnostics(&schema(), "SELECT * FROM customers, nope");
    let m = messages(&d);
    assert_eq!(m.len(), 1);
    assert!(m.first().expect("one").contains("nope"));
}

#[test]
fn alias_is_not_flagged() {
    // 'c' is an alias, not a table; only 'customers' is in table position.
    let d = sql_diagnostics(&schema(), "SELECT c.id FROM customers c");
    assert!(d.is_empty(), "alias must not be flagged: {d:?}");
}

#[test]
fn as_alias_is_not_flagged() {
    let d = sql_diagnostics(&schema(), "SELECT c.id FROM customers AS c");
    assert!(d.is_empty(), "AS alias must not be flagged: {d:?}");
}

#[test]
fn qualified_table_name_is_skipped() {
    // schema-qualified reference — conservatively skipped, never flagged.
    let d = sql_diagnostics(&schema(), "SELECT * FROM main.whatever");
    assert!(d.is_empty(), "qualified name must be skipped: {d:?}");
}

#[test]
fn quoted_identifier_is_skipped() {
    let d = sql_diagnostics(&schema(), "SELECT * FROM \"Weird Name\"");
    assert!(d.is_empty(), "quoted identifier must be skipped: {d:?}");
}

#[test]
fn subquery_after_from_is_skipped() {
    let d = sql_diagnostics(&schema(), "SELECT * FROM (SELECT id FROM customers) t");
    assert!(
        d.is_empty(),
        "subquery + its alias must not be flagged: {d:?}"
    );
}

#[test]
fn cte_name_is_known() {
    let sql = "WITH recent AS (SELECT id FROM customers) SELECT * FROM recent";
    let d = sql_diagnostics(&schema(), sql);
    assert!(d.is_empty(), "CTE name must be treated as known: {d:?}");
}

#[test]
fn empty_schema_flags_nothing() {
    let empty = CompletionSchema { tables: vec![] };
    let d = sql_diagnostics(&empty, "SELECT * FROM anything JOIN nope ON a = b");
    assert!(
        d.is_empty(),
        "no table checks when schema is unloaded: {d:?}"
    );
}

#[test]
fn table_name_in_string_literal_is_ignored() {
    // 'nope' appears only inside a string literal, never in table position.
    let d = sql_diagnostics(&schema(), "SELECT 'FROM nope' FROM customers");
    assert!(
        d.is_empty(),
        "string-literal contents must be ignored: {d:?}"
    );
}

#[test]
fn table_name_in_comment_is_ignored() {
    let d = sql_diagnostics(&schema(), "SELECT id -- FROM nope\nFROM customers");
    assert!(d.is_empty(), "line-comment contents must be ignored: {d:?}");
}

#[test]
fn unclosed_paren_is_error() {
    let d = sql_diagnostics(&schema(), "SELECT count(id FROM customers");
    let err = d
        .iter()
        .find(|x| x.severity == DiagnosticSeverity::Error)
        .expect("a bracket error");
    assert!(
        err.message.contains('('),
        "message names the unclosed paren: {}",
        err.message
    );
    // The '(' is at column 13 (1-based) in "SELECT count(...".
    assert_eq!(err.line, 1);
    assert_eq!(err.start_col, 13);
    assert_eq!(err.end_col, 14);
}

#[test]
fn unexpected_close_paren_is_error() {
    let d = sql_diagnostics(&schema(), "SELECT id) FROM customers");
    let err = d
        .iter()
        .find(|x| x.severity == DiagnosticSeverity::Error)
        .expect("a bracket error");
    assert!(
        err.message.contains(')'),
        "message names the unexpected paren: {}",
        err.message
    );
    assert_eq!(err.start_col, 10); // ')' after "SELECT id" (col 10)
}

#[test]
fn balanced_parens_no_bracket_error() {
    let d = sql_diagnostics(&schema(), "SELECT count(id) FROM customers");
    assert!(
        d.iter().all(|x| x.severity != DiagnosticSeverity::Error),
        "balanced parens must not error: {d:?}"
    );
}

#[test]
fn multiline_position_is_reported() {
    // Unknown table on line 2, column 6 (1-based) in "     nope".
    let d = sql_diagnostics(&schema(), "SELECT *\nFROM nope");
    let diag = d.first().expect("one diagnostic");
    assert_eq!(diag.line, 2);
    assert_eq!(diag.start_col, 6);
}

#[test]
fn reserved_word_after_from_is_not_flagged_as_table() {
    // Incomplete query: 'FROM' with a following clause keyword must not squiggle.
    let d = sql_diagnostics(&schema(), "SELECT id FROM WHERE id > 1");
    assert!(
        d.iter().all(|x| x.severity != DiagnosticSeverity::Warning),
        "reserved word in table position must not be flagged: {d:?}"
    );
}

#[test]
fn extract_from_column_is_not_flagged() {
    // `FROM` inside EXTRACT(... FROM col) is an argument separator, not a clause
    // keyword; `created` is a column expression and must not be flagged as a table.
    let d = sql_diagnostics(
        &schema(),
        "SELECT EXTRACT(YEAR FROM created) FROM customers",
    );
    assert!(
        d.is_empty(),
        "EXTRACT(... FROM col) must not flag the column: {d:?}"
    );
}

#[test]
fn extract_from_still_flags_outer_unknown_table() {
    // The depth-0 FROM is still checked; only the inner EXTRACT `FROM` is exempt.
    let d = sql_diagnostics(&schema(), "SELECT EXTRACT(YEAR FROM created) FROM nope");
    let m = messages(&d);
    assert_eq!(m.len(), 1, "exactly the outer table is flagged: {m:?}");
    assert!(m.first().expect("one").contains("nope"));
    assert!(
        !m.iter().any(|s| s.contains("created")),
        "column must not be flagged: {m:?}"
    );
}

#[test]
fn table_valued_function_is_not_flagged() {
    // An identifier in FROM position immediately followed by `(` is a function
    // call (a table-valued function), not a table.
    let d = sql_diagnostics(&schema(), "SELECT * FROM generate_series(1, 10)");
    assert!(
        d.is_empty(),
        "table-valued function must not be flagged: {d:?}"
    );
}
