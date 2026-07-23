use loom_ui_core::{
    CompletionColumn, CompletionSchema, CompletionTable, SuggestionKind, sql_completions,
};

fn sample() -> CompletionSchema {
    CompletionSchema {
        tables: vec![
            CompletionTable {
                schema: Some("public".to_owned()),
                name: "customers".to_owned(),
                columns: vec![
                    CompletionColumn {
                        name: "id".to_owned(),
                        ty: "int64".to_owned(),
                    },
                    CompletionColumn {
                        name: "name".to_owned(),
                        ty: "utf8".to_owned(),
                    },
                ],
            },
            CompletionTable {
                schema: Some("public".to_owned()),
                name: "orders".to_owned(),
                columns: vec![
                    CompletionColumn {
                        name: "id".to_owned(),
                        ty: "int64".to_owned(),
                    },
                    CompletionColumn {
                        name: "total".to_owned(),
                        ty: "float64".to_owned(),
                    },
                ],
            },
        ],
    }
}

fn labels(s: &[loom_ui_core::Suggestion]) -> Vec<String> {
    s.iter().map(|x| x.label.clone()).collect()
}

#[test]
fn unqualified_empty_prefix_includes_tables_columns_and_keywords() {
    let out = sql_completions(&sample(), "", None);
    let l = labels(&out);
    assert!(l.contains(&"customers".to_owned()));
    assert!(l.contains(&"orders".to_owned()));
    assert!(l.contains(&"name".to_owned()));
    assert!(l.contains(&"SELECT".to_owned()));
}

#[test]
fn unqualified_prefix_filters_case_insensitively() {
    let out = sql_completions(&sample(), "sel", None);
    let l = labels(&out);
    assert!(l.contains(&"SELECT".to_owned()));
    assert!(!l.contains(&"customers".to_owned()));
}

#[test]
fn unqualified_prefix_matches_table_name() {
    let out = sql_completions(&sample(), "cust", None);
    let l = labels(&out);
    assert_eq!(l, vec!["customers".to_owned()]);
}

#[test]
fn qualified_returns_that_tables_columns_with_type_detail() {
    let out = sql_completions(&sample(), "", Some("customers"));
    let l = labels(&out);
    assert_eq!(l, vec!["id".to_owned(), "name".to_owned()]);
    assert!(out.iter().all(|s| s.kind == SuggestionKind::Column));
    let id = out.iter().find(|s| s.label == "id").unwrap();
    assert_eq!(id.detail.as_deref(), Some("int64"));
}

#[test]
fn qualified_with_prefix_filters_columns() {
    let out = sql_completions(&sample(), "na", Some("customers"));
    assert_eq!(labels(&out), vec!["name".to_owned()]);
}

#[test]
fn qualified_unknown_table_is_empty() {
    assert!(sql_completions(&sample(), "", Some("bogus")).is_empty());
}

#[test]
fn empty_schema_unqualified_still_returns_keywords() {
    let empty = CompletionSchema { tables: vec![] };
    let out = sql_completions(&empty, "", None);
    assert!(labels(&out).contains(&"SELECT".to_owned()));
}

#[test]
fn unqualified_dedups_column_shared_across_tables() {
    // `id` exists in both `customers` and `orders`; it must appear once, not once
    // per table (the duplicate `Field` rows #622 reports).
    let out = sql_completions(&sample(), "id", None);
    let id_cols: Vec<_> = out
        .iter()
        .filter(|s| s.kind == SuggestionKind::Column && s.label == "id")
        .collect();
    assert_eq!(
        id_cols.len(),
        1,
        "shared column `id` must be deduped, got {id_cols:?}"
    );
}

#[test]
fn unqualified_dedup_keeps_distinct_columns() {
    // Every distinct column is still offered; only duplicates collapse.
    let out = sql_completions(&sample(), "", None);
    let cols: Vec<String> = out
        .iter()
        .filter(|s| s.kind == SuggestionKind::Column)
        .map(|s| s.label.clone())
        .collect();
    let mut deduped = cols.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        cols.len(),
        "no duplicate column labels expected: {cols:?}"
    );
    assert!(cols.contains(&"id".to_owned()));
    assert!(cols.contains(&"name".to_owned()));
    assert!(cols.contains(&"total".to_owned()));
}

#[test]
fn unqualified_dedup_is_case_insensitive() {
    // Two tables whose shared column differs only in case collapse to one row.
    let schema = CompletionSchema {
        tables: vec![
            CompletionTable {
                schema: None,
                name: "a".to_owned(),
                columns: vec![CompletionColumn {
                    name: "Id".to_owned(),
                    ty: "int".to_owned(),
                }],
            },
            CompletionTable {
                schema: None,
                name: "b".to_owned(),
                columns: vec![CompletionColumn {
                    name: "id".to_owned(),
                    ty: "int".to_owned(),
                }],
            },
        ],
    };
    let out = sql_completions(&schema, "", None);
    let id_cols: Vec<_> = out
        .iter()
        .filter(|s| s.kind == SuggestionKind::Column)
        .collect();
    assert_eq!(id_cols.len(), 1, "case-insensitive dedup: {id_cols:?}");
}
