#![allow(
    clippy::pedantic,
    clippy::restriction,
    reason = "yew html! macro expansion is not lint-clean under loom's strict gate"
)]
//! Component render harness: a backend-free wasm bundle that mounts exactly ONE
//! shipped component, with props supplied as JSON in the URL, so a native
//! `rust_test` driving headless Chrome can assert what a component renders for
//! given props (see `//src/ui/e2e:components`).
//!
//! Deliberately a separate bundle from `:gallery`: the gallery is the
//! human-facing design showcase and stays free of test scaffolding.
//!
//! Props cross a JSON boundary, so each component under test gets a harness-local
//! `*Spec` struct rather than reusing its `Properties` type — `Properties` structs
//! hold `Callback`s, which are not deserializable. The harness constructs the
//! callbacks itself and records each invocation into `window.__loom_events`.

use loom_ui_components::{Badge, Column, DataTable, GlobalStyles, TabItem, TableRow, Tabs};
use loom_ui_core::{Align, BadgeTone};
use serde::Deserialize;
use wasm_bindgen::{JsCast, JsValue};
use yew::prelude::*;

// ---------------------------------------------------------------- URL params

/// The raw `?…` query string. Read via `Reflect` rather than
/// `web_sys::Window::location`, whose `Location` feature this crate's manifest
/// does not declare — it is present in the buck build only through reindeer's
/// graph-wide feature union, i.e. an undeclared dependency on what unrelated
/// crates happen to enable. `Reflect` needs no `web-sys` feature at all.
fn location_search() -> String {
    let Some(win) = web_sys::window() else {
        return String::new();
    };
    js_sys::Reflect::get(&win, &JsValue::from_str("location"))
        .ok()
        .and_then(|loc| js_sys::Reflect::get(&loc, &JsValue::from_str("search")).ok())
        .and_then(|s| s.as_string())
        .unwrap_or_default()
}

/// Percent-decoded value of `key` in a `?a=b&c=d` query string.
fn query_param(search: &str, key: &str) -> Option<String> {
    search
        .trim_start_matches('?')
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == key)
        .and_then(|(_, v)| js_sys::decode_uri_component(v).ok())
        .map(String::from)
}

// ------------------------------------------------------------- event logging

/// Create the (empty) `window.__loom_events` array. Called once before render so
/// a test can read an empty log rather than `undefined` when nothing fired.
fn init_event_log() {
    if let Some(win) = web_sys::window() {
        let _ = js_sys::Reflect::set(
            &win,
            &JsValue::from_str("__loom_events"),
            &js_sys::Array::new(),
        );
    }
}

/// Append `{event, payload}` to `window.__loom_events`.
fn record_event(event: &str, payload: &str) {
    let Some(win) = web_sys::window() else { return };
    let Ok(log) = js_sys::Reflect::get(&win, &JsValue::from_str("__loom_events")) else {
        return;
    };
    if !log.is_object() {
        return;
    }
    // `unchecked_into`, not `Array::from`: the latter copies the array, so the
    // push would not be visible on `window`.
    let log: js_sys::Array = log.unchecked_into();
    let entry = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &entry,
        &JsValue::from_str("event"),
        &JsValue::from_str(event),
    );
    let _ = js_sys::Reflect::set(
        &entry,
        &JsValue::from_str("payload"),
        &JsValue::from_str(payload),
    );
    log.push(&entry);
}

// ---------------------------------------------------------------- prop specs

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
enum AlignSpec {
    #[default]
    Start,
    End,
}

impl From<AlignSpec> for Align {
    fn from(a: AlignSpec) -> Self {
        match a {
            AlignSpec::Start => Self::Start,
            AlignSpec::End => Self::End,
        }
    }
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
enum ToneSpec {
    #[default]
    Neutral,
    Info,
    Pii,
    Success,
    Warning,
    Danger,
}

impl From<ToneSpec> for BadgeTone {
    fn from(t: ToneSpec) -> Self {
        match t {
            ToneSpec::Neutral => Self::Neutral,
            ToneSpec::Info => Self::Info,
            ToneSpec::Pii => Self::Pii,
            ToneSpec::Success => Self::Success,
            ToneSpec::Warning => Self::Warning,
            ToneSpec::Danger => Self::Danger,
        }
    }
}

#[derive(Deserialize)]
struct ColumnSpec {
    label: String,
    #[serde(default)]
    align: AlignSpec,
}

#[derive(Deserialize)]
struct DataTableSpec {
    columns: Vec<ColumnSpec>,
    rows: Vec<Vec<String>>,
}

#[derive(Deserialize)]
struct BadgeSpec {
    label: String,
    #[serde(default)]
    tone: ToneSpec,
}

#[derive(Deserialize)]
struct TabSpec {
    id: String,
    label: String,
}

#[derive(Deserialize)]
struct TabsSpec {
    tabs: Vec<TabSpec>,
    active: String,
}

/// A `DataTable` row built from plain strings. `DataTable` is generic over
/// `TableRow` so its callers can supply a domain struct; the harness supplies
/// this trivial one, which is exactly how the component is meant to be used.
#[derive(Clone, PartialEq, Eq)]
struct HarnessRow(Vec<String>);

impl TableRow for HarnessRow {
    fn cells(&self) -> Vec<Html> {
        self.0.iter().map(|c| html! { { c.clone() } }).collect()
    }
}

// ----------------------------------------------------------------- dispatch

fn render_component(component: &str, props: &str) -> Html {
    match component {
        "DataTable" => match serde_json::from_str::<DataTableSpec>(props) {
            Ok(spec) => {
                let columns = spec
                    .columns
                    .into_iter()
                    .map(|c| Column {
                        label: AttrValue::from(c.label),
                        align: c.align.into(),
                    })
                    .collect::<Vec<_>>();
                let rows = spec.rows.into_iter().map(HarnessRow).collect::<Vec<_>>();
                html! { <DataTable<HarnessRow> {columns} {rows} /> }
            }
            Err(e) => render_error(&format!("bad DataTable props: {e}")),
        },
        "Badge" => match serde_json::from_str::<BadgeSpec>(props) {
            Ok(spec) => {
                html! { <Badge label={AttrValue::from(spec.label)} tone={BadgeTone::from(spec.tone)} /> }
            }
            Err(e) => render_error(&format!("bad Badge props: {e}")),
        },
        "Tabs" => match serde_json::from_str::<TabsSpec>(props) {
            Ok(spec) => {
                let tabs = spec
                    .tabs
                    .into_iter()
                    .map(|t| TabItem {
                        id: AttrValue::from(t.id),
                        label: AttrValue::from(t.label),
                    })
                    .collect::<Vec<_>>();
                let onselect = Callback::from(|id: AttrValue| record_event("onselect", &id));
                html! { <Tabs {tabs} active={AttrValue::from(spec.active)} {onselect} /> }
            }
            Err(e) => render_error(&format!("bad Tabs props: {e}")),
        },
        other => render_error(&format!("unknown component: {other}")),
    }
}

fn render_error(msg: &str) -> Html {
    html! { <p id="harness-error">{ msg.to_string() }</p> }
}

#[function_component(Harness)]
fn harness() -> Html {
    let search = location_search();
    let component = query_param(&search, "component").unwrap_or_default();
    let props = query_param(&search, "props").unwrap_or_else(|| "{}".to_string());
    html! {
        <>
            <GlobalStyles />
            <div id="mount">{ render_component(&component, &props) }</div>
        </>
    }
}

fn main() {
    init_event_log();
    yew::Renderer::<Harness>::new().render();
}
