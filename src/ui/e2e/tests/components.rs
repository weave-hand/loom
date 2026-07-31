#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "rust_test body: assertions unwrap/expect and a failed browser start panics"
)]
//! Per-component render tests: mount ONE shipped component in headless Chrome
//! with JSON props and assert what it renders. The full-page layer is
//! `//src/ui/e2e:login`; this is the component-unit layer that pairs with it.
//!
//! Deliberately NOT a `loom_fixture_test`: nothing here needs Postgres, and the
//! fixture macro would burn one of the 8 shared boot slots per test.

use fantoccini::Locator;
use loom_ui_e2e::mount;
use serde_json::json;

/// Structure from props: the columns and rows given become the rendered header
/// cells and body rows, and `align: "end"` reaches the cell's class.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_table_renders_columns_and_rows() {
    let h = mount(
        "DataTable",
        &json!({
            "columns": [
                {"label": "NAME", "align": "start"},
                {"label": "ROWS", "align": "end"}
            ],
            "rows": [["alpha", "1"], ["beta", "2"], ["gamma", "3"]]
        }),
    )
    .await;
    let c = h.client();

    let headers = c.find_all(Locator::Css("#mount thead th")).await.unwrap();
    assert_eq!(headers.len(), 2, "one th per column");
    assert_eq!(headers[0].text().await.unwrap(), "NAME");
    assert_eq!(headers[1].text().await.unwrap(), "ROWS");

    let rows = c.find_all(Locator::Css("#mount tbody tr")).await.unwrap();
    assert_eq!(rows.len(), 3, "one tr per row");

    let cells = c
        .find_all(Locator::Css("#mount tbody tr td"))
        .await
        .unwrap();
    assert_eq!(cells.len(), 6, "one td per column per row");
    assert_eq!(cells[0].text().await.unwrap(), "alpha");
    assert_eq!(cells[5].text().await.unwrap(), "3");

    // `Align::End` is the only column-level styling hook, and today it is only
    // verified by eye in the gallery.
    let end_cells = c
        .find_all(Locator::Css("#mount tbody td.end"))
        .await
        .unwrap();
    assert_eq!(
        end_cells.len(),
        3,
        "the End-aligned column's cells carry .end"
    );
}

/// Token from an enum prop: every `BadgeTone` reaches the rendered element as
/// its `--loom-*` custom property — the enum→CSS-var map, asserted rather than
/// eyeballed in the gallery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn badge_tone_reaches_the_rendered_css_var() {
    let cases = [
        ("neutral", "--loom-text-mut"),
        ("info", "--loom-accent"),
        ("pii", "--loom-danger"),
        ("success", "--loom-ok"),
        ("warning", "--loom-warn"),
        ("danger", "--loom-danger"),
    ];
    let h = mount("Badge", &json!({"label": "pii", "tone": "neutral"})).await;

    for (tone, css_var) in cases {
        h.show("Badge", &json!({"label": tone, "tone": tone})).await;
        let el = h.client().find(Locator::Css("#mount span")).await.unwrap();
        assert_eq!(el.text().await.unwrap(), tone, "label renders");
        let style = el.attr("style").await.unwrap().unwrap_or_default();
        assert!(
            style.contains(&format!("var({css_var})")),
            "tone {tone} should render var({css_var}), got style {style:?}"
        );
    }
}

/// Callback assertion: a non-serializable `Callback` prop is supplied by the
/// harness, and its invocation is observable to the test through the event log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tabs_onselect_fires_with_the_clicked_id() {
    let h = mount(
        "Tabs",
        &json!({
            "tabs": [
                {"id": "preview", "label": "Preview"},
                {"id": "schema", "label": "Schema"}
            ],
            "active": "preview"
        }),
    )
    .await;
    let c = h.client();

    let buttons = c.find_all(Locator::Css("#mount button")).await.unwrap();
    assert_eq!(buttons.len(), 2, "one button per tab");
    let classes = buttons[0].attr("class").await.unwrap().unwrap_or_default();
    assert!(
        classes.split_whitespace().any(|c| c == "active"),
        "the active tab carries .active, got {classes:?}"
    );

    // Nothing has fired yet — the log exists and is empty.
    let before = c
        .execute("return window.__loom_events;", vec![])
        .await
        .unwrap();
    assert_eq!(before.as_array().map(Vec::len), Some(0), "log starts empty");

    buttons[1].click().await.unwrap();

    let after = c
        .execute("return window.__loom_events;", vec![])
        .await
        .unwrap();
    assert_eq!(
        after,
        json!([{"event": "onselect", "payload": "schema"}]),
        "onselect fired once, with the clicked tab's id"
    );
}
