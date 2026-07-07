use loom_ui_core::Surface;

#[test]
fn accents_match_the_design_tokens() {
    assert_eq!(Surface::Catalog.accent(), "#3b82f6");
    assert_eq!(Surface::Transforms.accent(), "#2bb0a0");
    assert_eq!(Surface::Ontology.accent(), "#8b5cf6");
    assert_eq!(Surface::Workbooks.accent(), "#2da44e");
    assert_eq!(Surface::Dashboards.accent(), "#d29922");
}

#[test]
fn catalog_transforms_and_ontology_are_live() {
    let live: Vec<&str> = Surface::all()
        .into_iter()
        .filter(|s| s.is_live())
        .map(Surface::label)
        .collect();
    assert_eq!(live, vec!["Catalog", "Transforms", "Ontology"]);
}

#[test]
fn all_lists_five_surfaces_in_nav_order() {
    let labels: Vec<&str> = Surface::all().into_iter().map(Surface::label).collect();
    assert_eq!(
        labels,
        vec![
            "Catalog",
            "Transforms",
            "Ontology",
            "Workbooks",
            "Dashboards"
        ]
    );
}
