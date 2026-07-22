//! Property tests for the query-api SQL compiler (`compile_select`). Pins the three
//! load-bearing invariants at the injection boundary: (1) every emitted `?`
//! placeholder is backed by exactly one bind param, (2) every emitted identifier is
//! quote-wrapped, (3) no caller-supplied value ever appears verbatim in the emitted
//! SQL (values are always parameterized). Generators tag identifiers with a
//! private-use marker `\u{E001}` and text operands with `\u{E002}` so the assertions
//! are collision-free. See git history: 2026-07-02-pillar-idioms-audit-design.

use control_plane_core::{CompareOp, TableRef};
use proptest::prelude::*;
use query_api::filter::CallerPredicate;
use query_api::serving::SqlValue;
use query_api::sql::{SelectInputs, compile_select};

/// Marks the first char of every generated identifier. The compiler emits it ONLY
/// as the first char inside a quoted identifier, never in a constant/operator/value.
const ID_MARK: char = '\u{E001}';
/// Marks the first char of every generated text operand. Always parameterized, so
/// it must never surface in the emitted SQL.
const VAL_MARK: char = '\u{E002}';

/// Identifier: ID_MARK + safe ascii tail (no `?`, no `"`, no markers).
fn ident() -> impl Strategy<Value = String> {
    "[a-z0-9_]{0,6}".prop_map(|tail| format!("{ID_MARK}{tail}"))
}

/// A text operand value carrying VAL_MARK, with arbitrary (possibly SQL-dangerous)
/// suffix. Filters the two marker chars out of the suffix so the markers stay unique
/// (filter, not a regex char-class, to avoid depending on unicode-escape support).
fn marked_text() -> impl Strategy<Value = SqlValue> {
    any::<String>().prop_map(|s| {
        let suffix: String = s
            .chars()
            .filter(|c| *c != ID_MARK && *c != VAL_MARK)
            .take(16)
            .collect();
        SqlValue::Text(format!("{VAL_MARK}{suffix}"))
    })
}

/// Non-text scalar operands (Int/Bool/Double). No marker needed — the injection
/// property is asserted only on marked text; these still count toward placeholders.
fn scalar_operand() -> impl Strategy<Value = SqlValue> {
    prop_oneof![
        marked_text(),
        any::<i64>().prop_map(SqlValue::Int),
        any::<bool>().prop_map(SqlValue::Bool),
    ]
}

/// A well-formed CallerPredicate: op paired with the exact operand arity it needs,
/// so `compile_select` returns Ok and emits SQL.
fn predicate() -> impl Strategy<Value = CallerPredicate> {
    let scalar_ops = prop_oneof![
        Just(CompareOp::Eq),
        Just(CompareOp::Ne),
        Just(CompareOp::Lt),
        Just(CompareOp::Le),
        Just(CompareOp::Gt),
        Just(CompareOp::Ge),
        Just(CompareOp::Contains),
        Just(CompareOp::StartsWith),
        Just(CompareOp::EndsWith),
    ];
    let scalar_pred =
        (ident(), scalar_ops, scalar_operand()).prop_map(|(column, op, v)| CallerPredicate {
            column,
            op,
            values: vec![v],
        });
    let null_pred = (
        ident(),
        prop_oneof![Just(CompareOp::IsNull), Just(CompareOp::IsNotNull)],
    )
        .prop_map(|(column, op)| CallerPredicate {
            column,
            op,
            values: vec![],
        });
    let between_pred =
        (ident(), scalar_operand(), scalar_operand()).prop_map(|(column, a, b)| CallerPredicate {
            column,
            op: CompareOp::Between,
            values: vec![a, b],
        });
    let set_pred = (
        ident(),
        prop_oneof![Just(CompareOp::In), Just(CompareOp::NotIn)],
        prop::collection::vec(scalar_operand(), 1..4),
    )
        .prop_map(|(column, op, values)| CallerPredicate { column, op, values });
    prop_oneof![scalar_pred, null_pred, between_pred, set_pred]
}

fn table() -> impl Strategy<Value = TableRef> {
    (ident(), ident()).prop_map(|(schema, name)| TableRef { schema, name })
}

/// Count of `?` placeholders in the SQL. Safe because identifiers carry no `?` and
/// values are parameterized, so every `?` is a placeholder.
fn placeholder_count(sql: &str) -> usize {
    sql.matches('?').count()
}

proptest! {
    /// Property 1: one bind param per emitted `?` placeholder.
    #[test]
    fn placeholder_count_matches_param_count(
        table in table(),
        cols in prop::collection::vec(ident(), 1..4),
        preds in prop::collection::vec(predicate(), 0..5),
    ) {
        let inputs = SelectInputs { predicates: &preds, ..SelectInputs::default() };
        let (sql, params) = compile_select(&table, &cols, &[], &inputs, 100)
            .expect("well-formed predicates compile");
        prop_assert_eq!(placeholder_count(&sql), params.len());
    }

    /// Property 2: every emitted identifier is quote-wrapped — each ID_MARK in the
    /// SQL is immediately preceded by a `"`.
    #[test]
    fn every_identifier_is_quote_wrapped(
        table in table(),
        cols in prop::collection::vec(ident(), 1..4),
        preds in prop::collection::vec(predicate(), 0..5),
    ) {
        let inputs = SelectInputs { predicates: &preds, ..SelectInputs::default() };
        let (sql, _params) = compile_select(&table, &cols, &[], &inputs, 100)
            .expect("well-formed predicates compile");
        let chars: Vec<char> = sql.chars().collect();
        for (i, c) in chars.iter().enumerate() {
            if *c == ID_MARK {
                prop_assert!(i > 0 && chars[i - 1] == '"',
                    "identifier marker not preceded by a quote at {i} in: {sql}");
            }
        }
    }

    /// Property 3 (injection): no caller-supplied text value appears verbatim in the
    /// emitted SQL — the VAL_MARK never surfaces.
    #[test]
    fn caller_values_never_verbatim(
        table in table(),
        cols in prop::collection::vec(ident(), 1..4),
        preds in prop::collection::vec(predicate(), 0..5),
    ) {
        let inputs = SelectInputs { predicates: &preds, ..SelectInputs::default() };
        let (sql, _params) = compile_select(&table, &cols, &[], &inputs, 100)
            .expect("well-formed predicates compile");
        prop_assert!(!sql.contains(VAL_MARK),
            "a caller value leaked verbatim into: {sql}");
    }

    /// Robustness: arbitrary (possibly invariant-violating) RowFilter governance
    /// trees fed as row_filters never panic — compile_select validates up front and
    /// returns Ok or Err.
    #[test]
    fn arbitrary_row_filters_never_panic(f in arb_row_filter()) {
        let filters = [f];
        let inputs = SelectInputs { row_filters: &filters, ..SelectInputs::default() };
        let res = compile_select(
            &TableRef { schema: "s".into(), name: "t".into() },
            &["c".into()],
            &[],
            &inputs,
            10,
        );
        prop_assert!(res.is_ok() || res.is_err());
    }
}

/// Arbitrary RowFilter tree (may violate the CompareOp<->ScalarValue invariant on
/// purpose, to exercise compile_select's validate-before-emit path).
fn arb_row_filter() -> impl Strategy<Value = control_plane_core::RowFilter> {
    use control_plane_core::{CompareOp, RowFilter, ScalarValue};
    let op = prop_oneof![
        Just(CompareOp::Eq),
        Just(CompareOp::Ne),
        Just(CompareOp::Lt),
        Just(CompareOp::Le),
        Just(CompareOp::Gt),
        Just(CompareOp::Ge),
        Just(CompareOp::In),
        Just(CompareOp::NotIn),
        Just(CompareOp::IsNull),
        Just(CompareOp::IsNotNull),
    ];
    let value = prop_oneof![
        any::<String>().prop_map(ScalarValue::Text),
        any::<i64>().prop_map(ScalarValue::Int),
        any::<bool>().prop_map(ScalarValue::Bool),
        prop::collection::vec(any::<i64>().prop_map(ScalarValue::Int), 0..3)
            .prop_map(ScalarValue::List),
    ];
    let leaf = (".*", op, value).prop_map(|(property, op, value)| RowFilter::Compare {
        property,
        op,
        value,
    });
    leaf.prop_recursive(3, 16, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(RowFilter::And),
            prop::collection::vec(inner.clone(), 0..4).prop_map(RowFilter::Or),
            inner.prop_map(|f| RowFilter::Not(Box::new(f))),
        ]
    })
}
