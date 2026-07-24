//! Pure pixel-layout for a lineage DAG. Both the compact drawer mini-DAG
//! (`LineageDagView`, MINI params) and the standalone full-canvas view
//! (`LineageCanvasView`, CANVAS params) render from this one function, so the
//! geometry is unit-tested once and never duplicated across the two component
//! files. Kept index-free (no `[]` on slices/Vecs, no `unwrap`/`panic`) so it stays
//! clean under loom's strict `indexing_slicing` / panic-safety clippy gate — the
//! `loom_ui_core` lib carries no crate-level allow.

use std::collections::HashMap;

use crate::{DagNode, LineageDag, NodeKind};

/// Sizing for a lineage canvas layout. The mini-DAG uses the compact `MINI` values;
/// the standalone full-canvas view uses the generous `CANVAS` values. Same layout
/// algorithm, different scale.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LineageLayoutParams {
    /// Node box width, px.
    pub node_w: f64,
    /// Node box height, px.
    pub node_h: f64,
    /// Vertical gap between stacked nodes in a column, px.
    pub v_gap: f64,
    /// Horizontal pitch between column slots, px.
    pub col_step: f64,
    /// Interior padding between the content and the canvas edge, px.
    pub pad: f64,
}

impl LineageLayoutParams {
    /// Compact drawer sizing — verbatim the constants the mini-DAG shipped with, so
    /// rendering the mini-DAG from this layout is behaviour-preserving.
    pub const MINI: Self = Self {
        node_w: 140.0,
        node_h: 34.0,
        v_gap: 18.0,
        col_step: 176.0,
        pad: 14.0,
    };
    /// Generous full-page sizing for the standalone canvas.
    pub const CANVAS: Self = Self {
        node_w: 210.0,
        node_h: 46.0,
        v_gap: 30.0,
        col_step: 300.0,
        pad: 32.0,
    };
}

/// A node placed on the canvas: its top-left `(x, y)` plus its size, so the renderer
/// positions an absolutely-placed box without recomputing geometry.
#[derive(Debug, Clone, PartialEq)]
pub struct LaidOutNode {
    pub id: String,
    pub label: String,
    pub kind: NodeKind,
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
}

/// A placed edge: producer-right → consumer-left endpoints, plus whether it touches
/// the current node (rendered in the accent colour).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LaidOutEdge {
    pub x1: f64,
    pub y1: f64,
    pub x2: f64,
    pub y2: f64,
    pub touches_current: bool,
}

/// The assembled canvas layout: overall dimensions plus placed nodes and edges.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LineageLayout {
    pub width: f64,
    pub height: f64,
    pub nodes: Vec<LaidOutNode>,
    pub edges: Vec<LaidOutEdge>,
}

/// Lay a `LineageDag` out on a canvas at the given scale.
///
/// Nodes are grouped into three logical columns (0 upstream · 1 current · 2
/// downstream) in emitted order; only non-empty columns consume a horizontal slot,
/// so a dataset with no lineage renders one centred node rather than reserving two
/// empty side columns. Each column is vertically centred within the content area,
/// the whole thing inset by `pad`. Edges run from a producer's right edge to a
/// consumer's left edge. Returned nodes are in column order (upstream, current,
/// downstream); returned edges preserve `dag.edges` order.
#[must_use]
pub fn lineage_layout(dag: &LineageDag, p: &LineageLayoutParams) -> LineageLayout {
    // Column node lists (0 upstream / 1 current / 2 downstream), order preserved.
    // A fixed 3-element array literal (not runtime indexing) keeps this index-free.
    let column = |c: usize| -> Vec<&DagNode> {
        dag.nodes.iter().filter(|n| n.column.min(2) == c).collect()
    };
    let cols = [column(0), column(1), column(2)];
    let max_count = cols.iter().map(Vec::len).max().unwrap_or(0);

    // Non-empty columns take left→right slots; empty columns take none.
    let mut n_slots = 0_usize;
    let slots: Vec<Option<usize>> = cols
        .iter()
        .map(|nodes| {
            if nodes.is_empty() {
                None
            } else {
                let s = n_slots;
                n_slots += 1;
                Some(s)
            }
        })
        .collect();
    let n_slots = n_slots.max(1);

    let row_h = p.node_h + p.v_gap;
    let content_h = ((max_count as f64) * row_h - p.v_gap).max(p.node_h);
    let content_w = ((n_slots as f64) - 1.0) * p.col_step + p.node_w;
    let width = content_w + 2.0 * p.pad;
    let height = content_h + 2.0 * p.pad;

    // Top-left of every node id (for edge endpoints) plus the laid-out node list.
    let mut nodes = Vec::new();
    let mut pos: HashMap<String, (f64, f64)> = HashMap::new();
    for (col_nodes, slot) in cols.iter().zip(slots.iter()) {
        let Some(s) = *slot else {
            continue;
        };
        let col_h = ((col_nodes.len() as f64) * row_h - p.v_gap).max(0.0);
        let offset = p.pad + (content_h - col_h) / 2.0;
        let left = p.pad + (s as f64) * p.col_step;
        for (j, n) in col_nodes.iter().enumerate() {
            let top = offset + (j as f64) * row_h;
            pos.insert(n.id.clone(), (left, top));
            nodes.push(LaidOutNode {
                id: n.id.clone(),
                label: n.label.clone(),
                kind: n.kind,
                x: left,
                y: top,
                w: p.node_w,
                h: p.node_h,
            });
        }
    }

    let cur_id = dag
        .nodes
        .iter()
        .find(|n| n.kind == NodeKind::Current)
        .map(|n| n.id.clone());

    let edges = dag
        .edges
        .iter()
        .filter_map(|e| {
            let (fl, ft) = pos.get(&e.from)?;
            let (tl, tt) = pos.get(&e.to)?;
            let touches_current = cur_id.as_ref().is_some_and(|c| c == &e.from || c == &e.to);
            Some(LaidOutEdge {
                x1: fl + p.node_w,
                y1: ft + p.node_h / 2.0,
                x2: *tl,
                y2: tt + p.node_h / 2.0,
                touches_current,
            })
        })
        .collect();

    LineageLayout {
        width,
        height,
        nodes,
        edges,
    }
}
