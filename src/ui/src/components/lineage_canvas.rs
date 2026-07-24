//! `LineageCanvasView` — the standalone full-canvas lineage graph, reached from the
//! Catalog drawer's Lineage tab "Open full view ↗" button (it replaces the former
//! `LineageFullStub` placeholder). It renders the same producer → current → consumer
//! DAG as the drawer mini-DAG, but at a generous full-page scale with a header
//! caption, so a busy closure has room to breathe. Geometry comes from the shared,
//! unit-tested `loom_ui_core::lineage_layout` (CANVAS scale) — the same function the
//! mini-DAG uses at MINI scale — so the two views never duplicate layout math.

use loom_ui_core::{LineageDag, LineageLayoutParams, NodeKind, lineage_layout};
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct LineageCanvasViewProps {
    pub dag: LineageDag,
}

#[styled_component(LineageCanvasView)]
pub fn lineage_canvas_view(props: &LineageCanvasViewProps) -> Html {
    let cls = css!(
        r#"
        display: flex; flex-direction: column; min-height: 320px; overflow: hidden;
        border: 1px solid var(--loom-border); border-radius: var(--loom-radius);
        background: var(--loom-panel);
        .head { padding: 10px 12px; border-bottom: 1px solid var(--loom-border); }
        .caption { color: var(--loom-text-mut); font-size: 12px; }
        /* Both axes scroll — a wide closure overflows horizontally, a tall column
           vertically; unlike the drawer mini-DAG both scrollbars are welcome here. */
        .scroll { flex: 1; overflow: auto; padding: 16px; }
        .canvas {
            position: relative; margin: 0 auto;
            background: radial-gradient(#1b222b 1px, transparent 1px); background-size: 24px 24px;
            border: 1px solid var(--loom-border); border-radius: var(--loom-radius);
        }
        .wires { position: absolute; inset: 0; pointer-events: none; }
        .node {
            position: absolute; box-sizing: border-box;
            display: flex; align-items: center; justify-content: center;
            padding: 0 12px; border-radius: 7px; font-size: 13px;
            background: var(--loom-panel-2); border: 1px solid var(--loom-border); color: var(--loom-text);
        }
        .node.current {
            border-color: var(--loom-accent);
            box-shadow: 0 0 0 5px color-mix(in srgb, var(--loom-accent) 18%, transparent);
        }
        .lbl { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    "#
    );

    // Position the DAG at the generous full-page scale. Same shared layout the
    // drawer mini-DAG uses at MINI scale — the geometry is unit-tested once.
    let layout = lineage_layout(&props.dag, &LineageLayoutParams::CANVAS);
    let up = props.dag.nodes.iter().filter(|n| n.column == 0).count();
    let down = props.dag.nodes.iter().filter(|n| n.column == 2).count();

    let lines: Vec<Html> = layout
        .edges
        .iter()
        .map(|e| {
            let stroke = if e.touches_current {
                "var(--loom-accent)"
            } else {
                "#2d3640"
            };
            html! {
                <line x1={e.x1.to_string()} y1={e.y1.to_string()}
                      x2={e.x2.to_string()} y2={e.y2.to_string()}
                      stroke={stroke} stroke-width="1.75" />
            }
        })
        .collect();

    let node_els: Vec<Html> = layout
        .nodes
        .iter()
        .map(|n| {
            let style = format!(
                "left:{}px; top:{}px; width:{}px; height:{}px;",
                n.x, n.y, n.w, n.h
            );
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
            <div class="head">
                <span class="caption">{ format!("{up} upstream · {down} downstream") }</span>
            </div>
            <div class="scroll">
                <div class="canvas" style={format!("width:{}px; height:{}px;", layout.width, layout.height)}>
                    <svg class="wires" width={layout.width.to_string()} height={layout.height.to_string()}>
                        { for lines }
                    </svg>
                    { for node_els }
                </div>
            </div>
        </div>
    }
}
