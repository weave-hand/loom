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
