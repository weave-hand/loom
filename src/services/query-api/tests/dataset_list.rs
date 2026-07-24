//! Pure sort/filter for GET /datasets: SortKey/SortDir parsing (400 messages),
//! project filter, stable sort with a (schema,name) ascending tiebreak, and the
//! order-preserving default (no sort param). No axum, no control plane.

use query_api::dataset_list::{DatasetListParams, DatasetSummary, apply};

fn sum(schema: &str, name: &str, updated: &str, rows: Option<i64>) -> DatasetSummary {
    DatasetSummary {
        schema: schema.into(),
        name: name.into(),
        project: schema.into(),
        updated: updated.into(),
        rows,
        kind: "table",
        base: None,
    }
}

fn names(v: &[DatasetSummary]) -> Vec<String> {
    v.iter()
        .map(|d| format!("{}.{}", d.schema, d.name))
        .collect()
}

#[test]
fn default_params_preserve_input_order() {
    // No sort param => order untouched (existing-caller compatibility).
    let p = DatasetListParams::from_params(&[]).unwrap();
    let items = vec![
        sum("main", "zzz", "t2", Some(1)),
        sum("main", "aaa", "t1", Some(9)),
    ];
    assert_eq!(names(&apply(items, &p)), vec!["main.zzz", "main.aaa"]);
}

#[test]
fn sort_name_asc_and_desc_case_insensitive() {
    let items = || {
        vec![
            sum("main", "Zebra", "t", None),
            sum("main", "apple", "t", None),
            sum("main", "Mango", "t", None),
        ]
    };
    let asc = DatasetListParams::from_params(&[("sort".into(), "name".into())]).unwrap();
    assert_eq!(
        names(&apply(items(), &asc)),
        vec!["main.apple", "main.Mango", "main.Zebra"]
    );
    let desc = DatasetListParams::from_params(&[
        ("sort".into(), "name".into()),
        ("dir".into(), "desc".into()),
    ])
    .unwrap();
    assert_eq!(
        names(&apply(items(), &desc)),
        vec!["main.Zebra", "main.Mango", "main.apple"]
    );
}

#[test]
fn sort_rows_desc_orders_by_count_none_last() {
    let items = vec![
        sum("main", "a", "t", Some(3)),
        sum("main", "b", "t", Some(10)),
        sum("main", "c", "t", None),
    ];
    let p = DatasetListParams::from_params(&[
        ("sort".into(), "rows".into()),
        ("dir".into(), "desc".into()),
    ])
    .unwrap();
    // desc: 10, 3, then None (None is the smallest, so last in desc).
    assert_eq!(names(&apply(items, &p)), vec!["main.b", "main.a", "main.c"]);
}

#[test]
fn tiebreak_is_schema_name_ascending_regardless_of_dir() {
    // Equal primary key (same updated) => (schema,name) ascending tiebreak, even desc.
    let items = vec![
        sum("main", "b", "same", None),
        sum("main", "a", "same", None),
    ];
    let desc = DatasetListParams::from_params(&[
        ("sort".into(), "updated".into()),
        ("dir".into(), "desc".into()),
    ])
    .unwrap();
    assert_eq!(names(&apply(items, &desc)), vec!["main.a", "main.b"]);
}

#[test]
fn project_filter_keeps_exact_matches() {
    let items = vec![
        sum("main", "a", "t", None),
        sum("gov", "b", "t", None),
        sum("main", "c", "t", None),
    ];
    let p = DatasetListParams::from_params(&[("project".into(), "main".into())]).unwrap();
    assert_eq!(names(&apply(items, &p)), vec!["main.a", "main.c"]);
}

#[test]
fn empty_project_is_no_filter() {
    let p = DatasetListParams::from_params(&[("project".into(), String::new())]).unwrap();
    assert_eq!(p.project, None);
}

#[test]
fn unknown_sort_key_is_400_message() {
    let e = DatasetListParams::from_params(&[("sort".into(), "bogus".into())]).unwrap_err();
    assert!(e.contains("bogus") && e.contains("sort"), "{e}");
}

#[test]
fn unknown_dir_is_400_message() {
    let e = DatasetListParams::from_params(&[("dir".into(), "sideways".into())]).unwrap_err();
    assert!(e.contains("sideways") && e.contains("direction"), "{e}");
}

#[test]
fn last_occurrence_wins_for_repeated_sort() {
    let p = DatasetListParams::from_params(&[
        ("sort".into(), "name".into()),
        ("sort".into(), "rows".into()),
    ])
    .unwrap();
    // rows wins; assert via behavior: rows asc puts smaller count first.
    let items = vec![
        sum("m", "big", "t", Some(9)),
        sum("m", "small", "t", Some(1)),
    ];
    assert_eq!(names(&apply(items, &p)), vec!["m.small", "m.big"]);
}

#[test]
fn to_json_table_has_no_base_key_view_does() {
    let table = sum("main", "t", "2026-01-01T00:00:00Z", Some(2));
    let j = table.to_json();
    assert_eq!(j["schema"], "main");
    assert_eq!(j["kind"], "table");
    assert_eq!(j["rows"], serde_json::json!(2));
    assert!(j.get("base").is_none(), "table has no base: {j}");

    let mut view = sum("gov", "v", "t", None);
    view.kind = "view";
    view.base = Some(("main".into(), "t".into()));
    let jv = view.to_json();
    assert_eq!(jv["kind"], "view");
    assert_eq!(jv["rows"], serde_json::Value::Null);
    assert_eq!(jv["base"], serde_json::json!({"schema":"main","name":"t"}));
}
