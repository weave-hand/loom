//! `LineageDagView` — a presentational SVG mini-DAG for a dataset's lineage. Nodes
//! are laid out in three columns (0 upstream · 1 current · 2 downstream) on the
//! dotted-grid canvas; straight SVG `<line>`s connect producers → consumers, with
//! edges touching the current node drawn in the accent colour. Pure props in, no
//! fetching — the closures are loaded upstream in `Workspace` and turned into a
//! `LineageDag` by `loom_ui_core::lineage_dag`.

use std::collections::HashMap;

use loom_ui_core::{LineageDag, NodeKind};
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct LineageDagViewProps {
    pub dag: LineageDag,
}

// Fixed pixel layout so the absolutely-positioned node boxes and the SVG connector
// endpoints share one coordinate system (the canvas scrolls horizontally if wide).
const NODE_W: f64 = 140.0;
const NODE_H: f64 = 34.0;
const V_GAP: f64 = 18.0;
// Column pitch. Sized so a two-column DAG (current + one side) fits the ~378px-wide
// drawer canvas without scrolling; a full three-column DAG scrolls horizontally.
const COL_STEP: f64 = 176.0;
// Interior breathing room so nodes (and the current node's glow) float inside the
// dotted canvas rather than sitting flush against its border.
const PAD: f64 = 14.0;

#[styled_component(LineageDagView)]
pub fn lineage_dag_view(props: &LineageDagViewProps) -> Html {
    let cls = css!(
        r#"
        /* Only the horizontal axis ever scrolls (a wide three-column DAG); the
           canvas is always sized to its content vertically, so pin overflow-y to
           hidden — otherwise `overflow-x: auto` makes the browser compute
           overflow-y to auto too and a 1px rounding tips in a spurious scrollbar. */
        .scroll { overflow-x: auto; overflow-y: hidden; }
        .canvas {
            position: relative;
            /* Auto inline margins centre the canvas when it is narrower than the
               drawer, and collapse to 0 when it overflows (so it scrolls from the
               left). */
            margin: 0 auto;
            background: radial-gradient(#1b222b 1px, transparent 1px);
            background-size: 20px 20px;
            border: 1px solid var(--loom-border);
            border-radius: var(--loom-radius);
        }
        .wires { position: absolute; inset: 0; pointer-events: none; }
        .node {
            position: absolute; box-sizing: border-box;
            display: flex; align-items: center; justify-content: center;
            padding: 0 10px; border-radius: 6px;
            background: var(--loom-panel-2); border: 1px solid var(--loom-border);
            color: var(--loom-text); font-size: 12px;
        }
        .node.current {
            border-color: var(--loom-accent);
            box-shadow: 0 0 0 4px color-mix(in srgb, var(--loom-accent) 16%, transparent);
        }
        .lbl { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    "#
    );

    let dag = &props.dag;

    // Group node indices by logical column (0 upstream / 1 current / 2 downstream),
    // preserving their emitted order.
    let mut cols: [Vec<usize>; 3] = [Vec::new(), Vec::new(), Vec::new()];
    for (i, n) in dag.nodes.iter().enumerate() {
        cols[n.column.min(2)].push(i);
    }

    // Only non-empty columns consume a horizontal slot, so a dataset with no lineage
    // (just the current node) renders one centred node instead of reserving two empty
    // side columns and overflowing the drawer. Slots run left→right in column order,
    // so producer→consumer edges still point rightward.
    let mut slot_of = [0_usize; 3];
    let mut n_slots = 0_usize;
    for (c, idxs) in cols.iter().enumerate() {
        if !idxs.is_empty() {
            slot_of[c] = n_slots;
            n_slots += 1;
        }
    }
    let n_slots = n_slots.max(1);

    let max_count = cols.iter().map(Vec::len).max().unwrap_or(0);
    let row_h = NODE_H + V_GAP;
    let content_h = (max_count as f64 * row_h - V_GAP).max(NODE_H);
    let content_w = (n_slots as f64 - 1.0) * COL_STEP + NODE_W;
    let canvas_w = content_w + 2.0 * PAD;
    let canvas_h = content_h + 2.0 * PAD;

    // Top-left position of every node id, each column vertically centred within the
    // content area, the whole thing inset by `PAD` from the canvas edges.
    let mut pos: HashMap<String, (f64, f64)> = HashMap::new();
    for (c, idxs) in cols.iter().enumerate() {
        let col_h = (idxs.len() as f64 * row_h - V_GAP).max(0.0);
        let offset = PAD + (content_h - col_h) / 2.0;
        let left = PAD + slot_of[c] as f64 * COL_STEP;
        for (k, &i) in idxs.iter().enumerate() {
            pos.insert(dag.nodes[i].id.clone(), (left, offset + k as f64 * row_h));
        }
    }

    let cur_id = dag
        .nodes
        .iter()
        .find(|n| n.kind == NodeKind::Current)
        .map(|n| n.id.clone());

    // One SVG line per edge, from the producer's right edge to the consumer's left edge.
    let lines: Vec<Html> = dag
        .edges
        .iter()
        .filter_map(|e| {
            let (fl, ft) = pos.get(&e.from)?;
            let (tl, tt) = pos.get(&e.to)?;
            let x1 = (fl + NODE_W).to_string();
            let y1 = (ft + NODE_H / 2.0).to_string();
            let x2 = tl.to_string();
            let y2 = (tt + NODE_H / 2.0).to_string();
            let touches_cur = cur_id.as_ref().is_some_and(|c| c == &e.from || c == &e.to);
            let stroke = if touches_cur {
                "var(--loom-accent)"
            } else {
                "#2d3640"
            };
            Some(html! {
                <line x1={x1} y1={y1} x2={x2} y2={y2} stroke={stroke} stroke-width="1.5" />
            })
        })
        .collect();

    let node_els: Vec<Html> = dag
        .nodes
        .iter()
        .map(|n| {
            let (left, top) = pos.get(&n.id).copied().unwrap_or((0.0, 0.0));
            let style = format!("left:{left}px; top:{top}px; width:{NODE_W}px; height:{NODE_H}px;");
            let class = if n.kind == NodeKind::Current {
                "node current"
            } else {
                "node"
            };
            html! {
                <div class={class} style={style} title={n.id.clone()}>
                    <span class="lbl">{ n.label.clone() }</span>
                </div>
            }
        })
        .collect();

    html! {
        <div class={cls}>
            <div class="scroll">
                <div class="canvas" style={format!("width:{canvas_w}px; height:{canvas_h}px;")}>
                    <svg class="wires" width={canvas_w.to_string()} height={canvas_h.to_string()}>
                        { for lines }
                    </svg>
                    { for node_els }
                </div>
            </div>
        </div>
    }
}
