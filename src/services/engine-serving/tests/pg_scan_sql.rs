//! Pure-logic tests for the inline PG TableProvider's SQL generation
//! (`build_scan_sql`): projection, limit, the base (MVCC) predicate, and pushed
//! filter fragments via DataFusion's Unparser. No Postgres needed.

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{col, lit};
use engine_serving::provider::build_scan_sql;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ])
}

const BASE: &str = "begin_snapshot <= 42 and (end_snapshot is null or end_snapshot > 42)";

#[test]
fn all_columns_with_base_predicate() {
    let sql = build_scan_sql(
        "iceberg_mirror.inline_7",
        &schema(),
        Some(BASE),
        None,
        &[],
        None,
    );
    assert_eq!(
        sql,
        format!("SELECT \"id\", \"name\" FROM iceberg_mirror.inline_7 WHERE {BASE}")
    );
}

#[test]
fn projection_limits_select_list() {
    // Project only column 1 ("name").
    let sql = build_scan_sql(
        "iceberg_mirror.inline_7",
        &schema(),
        Some(BASE),
        Some(&vec![1]),
        &[],
        Some(5),
    );
    assert_eq!(
        sql,
        format!("SELECT \"name\" FROM iceberg_mirror.inline_7 WHERE {BASE} LIMIT 5")
    );
}

#[test]
fn pushed_filter_is_anded_after_base() {
    // id > 50  ->  DataFusion 54's unparser renders `("id" > 50)` (with parens);
    // our outer wrapper in build_scan_sql makes it `(("id" > 50))` in the SQL.
    let filters = vec![col("id").gt(lit(50_i64))];
    let sql = build_scan_sql(
        "iceberg_mirror.inline_7",
        &schema(),
        Some(BASE),
        None,
        &filters,
        None,
    );
    assert_eq!(
        sql,
        format!(
            "SELECT \"id\", \"name\" FROM iceberg_mirror.inline_7 WHERE {BASE} AND ((\"id\" > 50))"
        )
    );
}

#[test]
fn empty_projection_selects_constant() {
    // COUNT(*)-style: DataFusion may project zero columns.
    let sql = build_scan_sql(
        "iceberg_mirror.inline_7",
        &schema(),
        Some(BASE),
        Some(&vec![]),
        &[],
        None,
    );
    assert_eq!(
        sql,
        format!("SELECT 1 FROM iceberg_mirror.inline_7 WHERE {BASE}")
    );
}

#[test]
fn no_base_filter_no_where() {
    let sql = build_scan_sql("iceberg_mirror.inline_7", &schema(), None, None, &[], None);
    assert_eq!(sql, "SELECT \"id\", \"name\" FROM iceberg_mirror.inline_7");
}
