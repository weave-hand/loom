//! Unit tests for the hash-route codec (`loom_ui_core::route`).

use loom_ui_core::{CatalogQuery, CatalogSortDir, DatasetSort, Route, Surface};

#[test]
fn empty_hash_is_the_default_catalog_route() {
    let r = Route::parse("");
    assert_eq!(r.surface, Surface::Catalog);
    assert_eq!(r.selection, None);
    assert_eq!(r.tab.as_deref(), Some("schema"));
    assert_eq!(r.catalog, CatalogQuery::default());
    assert_eq!(r, Route::default());
}

#[test]
fn bare_surface_hash_parses() {
    let r = Route::parse("#/ontology");
    assert_eq!(r.surface, Surface::Ontology);
    assert_eq!(r.selection, None);
    assert_eq!(r.tab.as_deref(), Some("properties"));
}

#[test]
fn selection_segment_parses() {
    let r = Route::parse("#/catalog/main.txns");
    assert_eq!(r.surface, Surface::Catalog);
    assert_eq!(r.selection.as_deref(), Some("main.txns"));
    assert_eq!(
        r.tab.as_deref(),
        Some("schema"),
        "unspecified tab falls back to the default"
    );
}

#[test]
fn tab_param_parses_and_unknown_tabs_fall_back() {
    assert_eq!(
        Route::parse("#/catalog/main.txns?tab=preview")
            .tab
            .as_deref(),
        Some("preview")
    );
    assert_eq!(
        Route::parse("#/catalog/main.txns?tab=nonsense")
            .tab
            .as_deref(),
        Some("schema"),
        "a tab id the surface does not render must degrade to its default"
    );
    assert_eq!(
        Route::parse("#/catalog/main.txns?tab=properties")
            .tab
            .as_deref(),
        Some("schema"),
        "another surface's tab id is not valid here"
    );
}

#[test]
fn catalog_controls_parse() {
    let r = Route::parse("#/catalog?sort=updated&dir=desc&project=main");
    assert_eq!(r.catalog.sort, DatasetSort::Updated);
    assert_eq!(r.catalog.dir, CatalogSortDir::Desc);
    assert_eq!(r.catalog.project.as_deref(), Some("main"));
}

#[test]
fn malformed_input_degrades_instead_of_failing() {
    // Unknown surface, junk sort, junk dir, empty project, a stray param, a param
    // with no `=`, and a hash with no leading slash must all still render.
    let r = Route::parse("#nosuchsurface?sort=zzz&dir=zzz&project=&nope=1&novalue");
    assert_eq!(r.surface, Surface::Catalog);
    assert_eq!(r.catalog, CatalogQuery::default());
    assert_eq!(r.selection, None);
}

#[test]
fn extra_path_segments_are_ignored() {
    // Only reachable by hand-typing: `to_hash` percent-encodes `/` inside an id.
    assert_eq!(
        Route::parse("#/catalog/main.txns/extra")
            .selection
            .as_deref(),
        Some("main.txns")
    );
}

#[test]
fn defaults_are_omitted_from_the_serialised_hash() {
    assert_eq!(Route::default().to_hash(), "#/catalog");
    assert_eq!(Route::new(Surface::Query).to_hash(), "#/query");
}

#[test]
fn non_defaults_are_serialised_in_a_stable_order() {
    let r = Route {
        surface: Surface::Catalog,
        selection: Some("main.txns".to_string()),
        tab: Some("lineage".to_string()),
        catalog: CatalogQuery {
            sort: DatasetSort::Updated,
            dir: CatalogSortDir::Desc,
            project: Some("main".to_string()),
        },
    };
    assert_eq!(
        r.to_hash(),
        "#/catalog/main.txns?tab=lineage&sort=updated&dir=desc&project=main"
    );
}

#[test]
fn catalog_controls_are_not_serialised_off_the_catalog_surface() {
    let r = Route {
        surface: Surface::Ontology,
        selection: Some("Customer".to_string()),
        tab: Some("links".to_string()),
        catalog: CatalogQuery {
            sort: DatasetSort::Updated,
            dir: CatalogSortDir::Desc,
            project: Some("main".to_string()),
        },
    };
    assert_eq!(r.to_hash(), "#/ontology/Customer?tab=links");
}

#[test]
fn routes_round_trip_through_the_hash() {
    // Invariant: `parse(to_hash(r)) == r` for every route reachable from the
    // constructors, EXCEPT that Catalog list controls are dropped off-Catalog
    // (deliberately not serialised there — see the test above).
    let cases = [
        Route::default(),
        Route::new(Surface::Transforms),
        Route::new(Surface::Dashboards),
        Route {
            surface: Surface::Catalog,
            selection: Some("main.txns".to_string()),
            tab: Some("history".to_string()),
            catalog: CatalogQuery {
                sort: DatasetSort::Rows,
                dir: CatalogSortDir::Desc,
                project: Some("analytics".to_string()),
            },
        },
        Route {
            surface: Surface::Transforms,
            selection: Some("daily_rollup".to_string()),
            tab: Some("runs".to_string()),
            catalog: CatalogQuery::default(),
        },
    ];
    for r in cases {
        assert_eq!(Route::parse(&r.to_hash()), r, "round-trip failed for {r:?}");
    }
}

#[test]
fn structural_characters_in_ids_survive_the_round_trip() {
    // Selection ids and the project filter are user data (SQL identifiers): they
    // must never be able to inject a `/`, `?`, `&` or `=` into the route grammar.
    for id in ["a/b", "a?b", "a&b=c", "a b", "100%", "naïve", "a#b"] {
        let r = Route {
            selection: Some(id.to_string()),
            ..Route::default()
        };
        assert_eq!(
            Route::parse(&r.to_hash()).selection.as_deref(),
            Some(id),
            "id {id} did not survive"
        );
    }
    let r = Route {
        catalog: CatalogQuery {
            project: Some("a&b=c".to_string()),
            ..CatalogQuery::default()
        },
        ..Route::default()
    };
    assert_eq!(
        Route::parse(&r.to_hash()).catalog.project.as_deref(),
        Some("a&b=c")
    );
}

#[test]
fn a_leading_hash_is_optional() {
    assert_eq!(Route::parse("/ontology"), Route::parse("#/ontology"));
    assert_eq!(Route::parse("ontology"), Route::parse("#/ontology"));
}

#[test]
fn switching_surface_clears_the_selection_and_resets_the_tab() {
    let from = Route {
        surface: Surface::Catalog,
        selection: Some("main.txns".to_string()),
        tab: Some("preview".to_string()),
        catalog: CatalogQuery {
            sort: DatasetSort::Updated,
            dir: CatalogSortDir::Desc,
            project: Some("main".to_string()),
        },
    };
    let to = from.with_surface(Surface::Ontology);
    assert_eq!(to.surface, Surface::Ontology);
    assert_eq!(
        to.selection, None,
        "a surface switch is a change of location"
    );
    assert_eq!(to.tab.as_deref(), Some("properties"));
    assert_eq!(
        to.catalog, from.catalog,
        "list controls are view configuration, carried so returning to Catalog restores them"
    );
}

#[test]
fn switching_to_a_tabless_surface_leaves_no_tab() {
    assert_eq!(Route::default().with_surface(Surface::Query).tab, None);
}

#[test]
fn selecting_a_row_resets_the_drawer_to_its_default_tab() {
    let r = Route::default()
        .with_tab("lineage")
        .with_selection("main.txns");
    assert_eq!(r.selection.as_deref(), Some("main.txns"));
    assert_eq!(
        r.tab.as_deref(),
        Some("schema"),
        "a newly-opened drawer always starts on the default tab"
    );
}

#[test]
fn switching_tabs_keeps_the_selection() {
    let r = Route::default()
        .with_selection("main.txns")
        .with_tab("preview");
    assert_eq!(r.selection.as_deref(), Some("main.txns"));
    assert_eq!(r.tab.as_deref(), Some("preview"));
}

#[test]
fn changing_list_controls_clears_the_selection() {
    let r = Route::default()
        .with_selection("main.txns")
        .with_tab("preview")
        .with_catalog(CatalogQuery {
            sort: DatasetSort::Updated,
            ..CatalogQuery::default()
        });
    assert_eq!(r.catalog.sort, DatasetSort::Updated);
    assert_eq!(
        r.selection, None,
        "re-sorting/filtering changes what the list holds, so the drawer closes"
    );
    assert_eq!(r.tab.as_deref(), Some("schema"));
}

#[test]
fn cleared_closes_the_drawer_without_leaving_the_surface() {
    let r = Route::new(Surface::Transforms)
        .with_selection("daily_rollup")
        .with_tab("runs")
        .cleared();
    assert_eq!(r.surface, Surface::Transforms);
    assert_eq!(r.selection, None);
    assert_eq!(r.tab.as_deref(), Some("definition"));
}

#[test]
fn selection_and_tab_are_scoped_to_their_own_surface() {
    let r = Route::new(Surface::Ontology)
        .with_selection("Customer")
        .with_tab("links");
    assert_eq!(r.selection_on(Surface::Ontology), Some("Customer"));
    assert_eq!(r.tab_on(Surface::Ontology), Some("links"));
    // The Catalog surface must not read the Ontology selection as a dataset id.
    assert_eq!(r.selection_on(Surface::Catalog), None);
    assert_eq!(r.tab_on(Surface::Catalog), None);
    assert_eq!(r.selection_on(Surface::Transforms), None);
}

#[test]
fn scoped_accessors_return_none_when_nothing_is_selected() {
    let r = Route::default();
    assert_eq!(r.selection_on(Surface::Catalog), None);
    assert_eq!(r.tab_on(Surface::Catalog), Some("schema"));
    assert_eq!(r.tab_on(Surface::Query), None);
}
