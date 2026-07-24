use loom_ui_core::{
    LineageDag, LineageLayout, LineageLayoutParams, NodeKind, lineage_dag, lineage_layout,
};

// A dataset with no lineage: one centred current node; canvas is one node + 2*pad.
#[test]
fn single_node_is_centred() {
    let dag = lineage_dag(("w", "z"), &[], &[]);
    let p = LineageLayoutParams::MINI;
    let l: LineageLayout = lineage_layout(&dag, &p);
    assert_eq!(l.nodes.len(), 1);
    assert!(l.edges.is_empty());
    // n_slots clamps to 1: width = node_w + 2*pad; height = node_h + 2*pad.
    assert_eq!(l.width, p.node_w + 2.0 * p.pad);
    assert_eq!(l.height, p.node_h + 2.0 * p.pad);
    let n = l.nodes.first().unwrap();
    assert_eq!(n.kind, NodeKind::Current);
    assert_eq!((n.x, n.y), (p.pad, p.pad));
    assert_eq!((n.w, n.h), (p.node_w, p.node_h));
}

// Three columns (1 upstream, current, 1 downstream): 3 slots, two edges, current centred.
#[test]
fn three_columns_positions_and_edges() {
    let up = vec![("main".to_string(), "raw".to_string())];
    let down = vec![("main".to_string(), "report".to_string())];
    let dag = lineage_dag(("main", "txns"), &up, &down);
    let p = LineageLayoutParams::MINI;
    let l = lineage_layout(&dag, &p);

    // 3 non-empty columns => 3 slots. max_count = 1 => content_h = node_h.
    let content_w = 2.0 * p.col_step + p.node_w;
    assert_eq!(l.width, content_w + 2.0 * p.pad);
    assert_eq!(l.height, p.node_h + 2.0 * p.pad);

    // Column lefts: pad, pad+col_step, pad+2*col_step. All rows single => y = pad.
    let by_id = |id: &str| l.nodes.iter().find(|n| n.id == id).unwrap();
    assert_eq!((by_id("main.raw").x, by_id("main.raw").y), (p.pad, p.pad));
    assert_eq!(by_id("main.txns").x, p.pad + p.col_step);
    assert_eq!(by_id("main.report").x, p.pad + 2.0 * p.col_step);

    // Two edges, both touch current; endpoints span producer-right -> consumer-left.
    assert_eq!(l.edges.len(), 2);
    assert!(l.edges.iter().all(|e| e.touches_current));
    let raw = by_id("main.raw");
    let e0 = l.edges.first().unwrap();
    assert_eq!(e0.x1, raw.x + p.node_w);
    assert_eq!(e0.y1, raw.y + p.node_h / 2.0);
    assert_eq!(e0.x2, p.pad + p.col_step); // current node left edge
}

// Only upstream present: the empty downstream column consumes NO slot (2 slots, not 3).
#[test]
fn empty_column_consumes_no_slot() {
    let up = vec![
        ("main".to_string(), "a".to_string()),
        ("main".to_string(), "b".to_string()),
    ];
    let dag = lineage_dag(("main", "cur"), &up, &[]);
    let p = LineageLayoutParams::MINI;
    let l = lineage_layout(&dag, &p);
    // 2 non-empty columns (upstream, current) => width = 1*col_step + node_w + 2*pad.
    assert_eq!(l.width, p.col_step + p.node_w + 2.0 * p.pad);
    // Upstream column has 2 nodes => it defines content height (2 rows).
    let row_h = p.node_h + p.v_gap;
    let content_h = 2.0 * row_h - p.v_gap;
    assert_eq!(l.height, content_h + 2.0 * p.pad);
    // The single current node is vertically centred against the 2-tall upstream column.
    let cur = l.nodes.iter().find(|n| n.kind == NodeKind::Current).unwrap();
    assert_eq!(cur.y, p.pad + (content_h - p.node_h) / 2.0);
}

// CANVAS params scale the geometry up relative to MINI (sanity that params flow through).
#[test]
fn canvas_params_scale_up() {
    let dag = lineage_dag(("w", "z"), &[], &[]);
    let mini = lineage_layout(&dag, &LineageLayoutParams::MINI);
    let canvas = lineage_layout(&dag, &LineageLayoutParams::CANVAS);
    assert!(canvas.width > mini.width);
    assert!(canvas.height > mini.height);
    assert_eq!(
        canvas.nodes.first().unwrap().w,
        LineageLayoutParams::CANVAS.node_w
    );
}
