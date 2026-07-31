//! loom UI component library — `stylist`-styled yew primitives. The pure token
//! enums and `format_count` live in the sibling `loom_ui_core` lib (lint-clean);
//! this crate holds the `html!`/`stylist` render code, which is not lint-clean
//! under loom's strict gate — hence the crate-level allow.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! + stylist css! macro expansion is not lint-clean under loom's strict gate"
)]

mod badge;
mod button;
mod global;
mod input;
mod lineage;
mod lineage_canvas;
mod panel;
mod shell;
mod sql_editor;
mod status;
mod stub;
mod table;
mod tabs;
mod topnav;

pub use badge::Badge;
pub use button::Button;
pub use global::GlobalStyles;
pub use input::{Input, InputKind};
pub use lineage::{LineageDagView, LineageDagViewProps};
pub use lineage_canvas::{LineageCanvasView, LineageCanvasViewProps};
pub use panel::Panel;
pub use shell::{Shell, ShellProps};
pub use sql_editor::{SqlEditor, ValidateRequest, ValidateResponse};
pub use status::StatusDot;
pub use stub::{StubView, StubViewProps};
pub use table::{Column, DataTable, TableRow};
pub use tabs::{TabItem, Tabs};
pub use topnav::{NavItem, TopNav};
