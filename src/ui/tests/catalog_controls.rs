//! Pure Catalog-controls logic: the /datasets query-string builder, the sort/dir
//! enum tokens, and the distinct-project derivation for the filter chips.

use loom_ui_core::{
    CatalogSortDir, DatasetRow, DatasetSort, dataset_list_query, distinct_projects,
    project_chip_options,
};

fn row(schema: &str, name: &str) -> DatasetRow {
    DatasetRow {
        schema: schema.into(),
        name: name.into(),
        project: schema.into(),
        updated: String::new(),
        rows: None,
    }
}

#[test]
fn query_encodes_sort_dir_and_optional_project() {
    assert_eq!(
        dataset_list_query(DatasetSort::Updated, CatalogSortDir::Desc, None),
        "?sort=updated&dir=desc"
    );
    assert_eq!(
        dataset_list_query(DatasetSort::Name, CatalogSortDir::Asc, Some("main")),
        "?sort=name&dir=asc&project=main"
    );
}

#[test]
fn empty_project_is_omitted() {
    assert_eq!(
        dataset_list_query(DatasetSort::Name, CatalogSortDir::Asc, Some("")),
        "?sort=name&dir=asc"
    );
}

#[test]
fn project_filter_cannot_inject_query_parameters() {
    // The project value now comes out of the URL fragment, so it must be encoded
    // on the way back into loom's own `GET /datasets` query string.
    let q = dataset_list_query(DatasetSort::Name, CatalogSortDir::Asc, Some("a&sort=rows"));
    assert_eq!(q, "?sort=name&dir=asc&project=a%26sort%3Drows");
    assert!(
        !q.contains("&sort=rows"),
        "raw `&` must not reach the query string"
    );
}

#[test]
fn sort_tokens_and_roundtrip() {
    for s in DatasetSort::all() {
        assert_eq!(DatasetSort::from_param(s.as_param()), Some(s));
    }
    assert_eq!(DatasetSort::from_param("nope"), None);
    assert_eq!(DatasetSort::Rows.as_param(), "rows");
}

#[test]
fn dir_toggle_and_tokens() {
    assert_eq!(CatalogSortDir::Asc.toggled(), CatalogSortDir::Desc);
    assert_eq!(CatalogSortDir::Desc.toggled(), CatalogSortDir::Asc);
    assert_eq!(CatalogSortDir::Asc.as_param(), "asc");
}

#[test]
fn distinct_projects_are_sorted_and_deduped() {
    let rows = vec![row("main", "a"), row("other", "b"), row("main", "c")];
    assert_eq!(
        distinct_projects(&rows),
        vec!["main".to_string(), "other".to_string()]
    );
}

#[test]
fn chip_options_are_the_discovered_projects_when_nothing_is_filtered() {
    let discovered = vec!["main".to_string(), "analytics".to_string()];
    assert_eq!(project_chip_options(&discovered, None), discovered);
}

#[test]
fn chip_options_include_an_active_project_the_list_has_not_discovered() {
    // A deep link carrying `?project=main` loads FILTERED, so the unfiltered
    // option set was never fetched — without this the bar would show only "All"
    // and the active filter would be invisible.
    assert_eq!(
        project_chip_options(&[], Some("main")),
        vec!["main".to_string()]
    );
}

#[test]
fn chip_options_do_not_duplicate_an_already_discovered_active_project() {
    let discovered = vec!["main".to_string(), "analytics".to_string()];
    assert_eq!(project_chip_options(&discovered, Some("main")), discovered);
}
