use loom_ui_core::{CatalogSortDir, Surface};

#[test]
fn accents_match_the_design_tokens() {
    assert_eq!(Surface::Catalog.accent(), "#3b82f6");
    assert_eq!(Surface::Transforms.accent(), "#2bb0a0");
    assert_eq!(Surface::Query.accent(), "#e06c75");
    assert_eq!(Surface::Ontology.accent(), "#8b5cf6");
    assert_eq!(Surface::Workbooks.accent(), "#2da44e");
    assert_eq!(Surface::Dashboards.accent(), "#d29922");
}

#[test]
fn catalog_transforms_query_and_ontology_are_live() {
    let live: Vec<&str> = Surface::all()
        .into_iter()
        .filter(|s| s.is_live())
        .map(Surface::label)
        .collect();
    assert_eq!(live, vec!["Catalog", "Transforms", "Query", "Ontology"]);
}

#[test]
fn all_lists_six_surfaces_in_nav_order() {
    let labels: Vec<&str> = Surface::all().into_iter().map(Surface::label).collect();
    assert_eq!(
        labels,
        vec![
            "Catalog",
            "Transforms",
            "Query",
            "Ontology",
            "Workbooks",
            "Dashboards"
        ]
    );
}

#[test]
fn every_surface_has_a_unique_slug_that_round_trips() {
    let mut seen = Vec::new();
    for s in Surface::all() {
        let slug = s.slug();
        assert!(!slug.is_empty(), "{s:?} has an empty slug");
        assert!(!seen.contains(&slug), "duplicate slug {slug}");
        seen.push(slug);
        assert_eq!(Surface::from_slug(slug), Some(s), "slug {slug} must round-trip");
    }
}

#[test]
fn unknown_slug_is_none() {
    assert_eq!(Surface::from_slug("nope"), None);
    assert_eq!(Surface::from_slug(""), None);
    assert_eq!(Surface::from_slug("Catalog"), None, "slugs are lowercase");
}

#[test]
fn drawer_tabs_match_the_ids_the_surfaces_render() {
    assert_eq!(Surface::Catalog.tabs(), ["schema", "preview", "lineage", "history"]);
    assert_eq!(Surface::Ontology.tabs(), ["properties", "links"]);
    assert_eq!(Surface::Transforms.tabs(), ["definition", "runs"]);
    assert!(Surface::Query.tabs().is_empty());
    assert!(Surface::Workbooks.tabs().is_empty());
    assert!(Surface::Dashboards.tabs().is_empty());
}

#[test]
fn default_tab_is_the_first_tab_or_none() {
    assert_eq!(Surface::Catalog.default_tab(), Some("schema"));
    assert_eq!(Surface::Ontology.default_tab(), Some("properties"));
    assert_eq!(Surface::Transforms.default_tab(), Some("definition"));
    assert_eq!(Surface::Query.default_tab(), None);
}

#[test]
fn sort_dir_param_round_trips() {
    for d in [CatalogSortDir::Asc, CatalogSortDir::Desc] {
        assert_eq!(CatalogSortDir::from_param(d.as_param()), Some(d));
    }
    assert_eq!(CatalogSortDir::from_param("sideways"), None);
}
