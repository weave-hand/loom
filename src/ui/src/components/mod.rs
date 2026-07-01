//! loom UI component library — `stylist`-styled yew primitives. The pure token
//! enums and `format_count` live in the sibling `loom_ui_core` lib (lint-clean);
//! this crate holds the `html!`/`stylist` render code, which is not lint-clean
//! under loom's strict gate — hence the crate-level allow.
#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! + stylist css! macro expansion is not lint-clean under loom's strict gate"
)]

mod global;

pub use global::GlobalStyles;
