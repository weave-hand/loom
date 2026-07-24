//! `LineageDagView` — a presentational SVG mini-DAG for a dataset's lineage. Nodes
//! are laid out in three columns (0 upstream · 1 current · 2 downstream) on the
//! dotted-grid canvas; straight SVG `<line>`s connect producers → consumers, with
//! edges touching the current node drawn in the accent colour. Pure props in, no
//! fetching — the closures are loaded upstream in `Workspace`, turned into a
//! `LineageDag` by `loom_ui_core::lineage_dag`, and positioned by the shared pure
//! `loom_ui_core::lineage_layout` (MINI scale) which the full-canvas view also uses.

use loom_ui_core::{LineageDag, LineageLayoutParams, NodeKind, lineage_layout};
use stylist::yew::styled_component;
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct LineageDagViewProps {
    pub dag: LineageDag,
}

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

    // Position the DAG at the compact drawer scale. The layout math is shared with
    // the full-canvas view and unit-tested in `loom_ui_core::lineage_layout`.
    let layout = lineage_layout(&props.dag, &LineageLayoutParams::MINI);

    // One SVG line per edge, from the producer's right edge to the consumer's left
    // edge; edges touching the current node are drawn in the accent colour.
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
                      stroke={stroke} stroke-width="1.5" />
            }
        })
        .collect();

    let node_els: Vec<Html> = layout
        .nodes
        .iter()
        .map(|n| {
            let style = format!("left:{}px; top:{}px; width:{}px; height:{}px;", n.x, n.y, n.w, n.h);
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
