//! Unit test for `engine::flight::all_in_live_set`: the pure membership check
//! the Flight `do_get` file-ticket branch uses to reject paths outside a
//! table's live snapshot. No fixture / no wire — pure logic.

use std::collections::HashSet;

use engine::flight::all_in_live_set;

fn live(paths: &[&str]) -> HashSet<String> {
    paths.iter().map(|p| (*p).to_string()).collect()
}

#[test]
fn subset_passes() {
    let live = live(&["file://a/1.parquet", "file://a/2.parquet"]);
    let req = vec!["file://a/1.parquet".to_string()];
    assert!(all_in_live_set(&live, &req));
}

#[test]
fn full_set_passes() {
    let live = live(&["file://a/1.parquet", "file://a/2.parquet"]);
    let req = vec![
        "file://a/1.parquet".to_string(),
        "file://a/2.parquet".to_string(),
    ];
    assert!(all_in_live_set(&live, &req));
}

#[test]
fn empty_requested_passes() {
    let live = live(&["file://a/1.parquet"]);
    assert!(all_in_live_set(&live, &[]));
}

#[test]
fn disjoint_path_fails() {
    let live = live(&["file://a/1.parquet"]);
    let req = vec!["file://b/9.parquet".to_string()];
    assert!(!all_in_live_set(&live, &req));
}

#[test]
fn superset_fails() {
    let live = live(&["file://a/1.parquet"]);
    let req = vec![
        "file://a/1.parquet".to_string(),
        "file://b/9.parquet".to_string(),
    ];
    assert!(!all_in_live_set(&live, &req));
}
