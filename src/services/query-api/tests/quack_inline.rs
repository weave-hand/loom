//! Unit tests for `inline_params`: typed scalars render to SQL literals and `?`
//! placeholders are substituted left-to-right. The escape (doubling `'`) is the
//! injection boundary for the inline path — exercised here server-free. The
//! in-process Iceberg/DataFusion engine consumes the inlined SQL.

use query_api::serving::{SqlValue, inline_params};

#[test]
fn renders_each_scalar_type() {
    assert_eq!(inline_params("?", &[SqlValue::Int(42)]), "42");
    assert_eq!(inline_params("?", &[SqlValue::Bool(true)]), "TRUE");
    assert_eq!(inline_params("?", &[SqlValue::Bool(false)]), "FALSE");
    assert_eq!(inline_params("?", &[SqlValue::Null]), "NULL");
    assert_eq!(inline_params("?", &[SqlValue::Text("hi".into())]), "'hi'");
}

#[test]
fn escapes_single_quotes_in_text() {
    assert_eq!(
        inline_params(
            "WHERE s = ?",
            &[SqlValue::Text("x'; DROP TABLE lake.t; --".into())]
        ),
        "WHERE s = 'x''; DROP TABLE lake.t; --'"
    );
}

#[test]
fn substitutes_placeholders_in_order_and_passes_other_chars_through() {
    let sql = "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?";
    let out = inline_params(
        sql,
        &[
            SqlValue::Int(1),
            SqlValue::Text("two".into()),
            SqlValue::Null,
        ],
    );
    assert_eq!(
        out,
        "SELECT * FROM t WHERE a = 1 AND b = 'two' AND c = NULL"
    );
}

#[test]
fn does_not_rescan_substituted_text() {
    let out = inline_params("? ?", &[SqlValue::Text("a?b".into()), SqlValue::Int(9)]);
    assert_eq!(out, "'a?b' 9");
}
