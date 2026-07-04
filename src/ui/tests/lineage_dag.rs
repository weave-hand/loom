use loom_ui_core::{NodeKind, lineage_dag};

#[test]
fn builds_three_columns_with_edges_through_current() {
    let up = vec![("main".to_string(), "raw".to_string())];
    let down = vec![("main".to_string(), "report".to_string())];
    let dag = lineage_dag(("main", "txns"), &up, &down);

    // 3 nodes: raw (col 0), txns (col 1, current), report (col 2).
    assert_eq!(dag.nodes.len(), 3);
    let current = dag
        .nodes
        .iter()
        .find(|n| n.kind == NodeKind::Current)
        .unwrap();
    assert_eq!(current.id, "main.txns");
    assert_eq!(current.column, 1);
    assert_eq!(
        dag.nodes
            .iter()
            .find(|n| n.id == "main.raw")
            .unwrap()
            .column,
        0
    );
    assert_eq!(
        dag.nodes
            .iter()
            .find(|n| n.id == "main.report")
            .unwrap()
            .column,
        2
    );

    // Edges: raw -> txns, txns -> report.
    assert!(
        dag.edges
            .iter()
            .any(|e| e.from == "main.raw" && e.to == "main.txns")
    );
    assert!(
        dag.edges
            .iter()
            .any(|e| e.from == "main.txns" && e.to == "main.report")
    );
    assert_eq!(dag.edges.len(), 2);
}

#[test]
fn current_wins_over_a_self_reference_in_a_closure() {
    // A closure that echoes the current dataset must not create a duplicate node.
    let up = vec![("main".to_string(), "txns".to_string())];
    let dag = lineage_dag(("main", "txns"), &up, &[]);
    assert_eq!(dag.nodes.len(), 1);
    assert_eq!(dag.nodes[0].kind, NodeKind::Current);
    assert!(dag.edges.is_empty());
}

#[test]
fn empty_closures_yield_only_the_current_node() {
    let dag = lineage_dag(("w", "z"), &[], &[]);
    assert_eq!(dag.nodes.len(), 1);
    assert_eq!(dag.nodes[0].id, "w.z");
    assert!(dag.edges.is_empty());
}
