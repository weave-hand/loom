#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "rust_test body: assertions unwrap/expect and a failed browser start panics"
)]
//! Full-page routing e2e: the URL fragment is the source of truth for the workspace
//! location — it is canonicalised on load, the nav rewrites it, a reload restores
//! it, a deep link opens the right drawer on the right tab, and Back walks it.
//! One backend + one browser (resource-light, per the shared-cluster/fresh-db model).

use fantoccini::{Client, Locator};
use loom_ui_e2e::{sign_in, start_backend, start_browser};

/// XPath for the app-bar nav button of `label` once it is the active surface.
fn active_nav_for(label: &str) -> String {
    format!("//nav//button[text()='{label}'][contains(@class,'active')]")
}

/// Wait until `label` is the active surface, then return its text as confirmation.
async fn await_surface(client: &Client, label: &str) -> String {
    let xpath = active_nav_for(label);
    client
        .wait()
        .for_element(Locator::XPath(&xpath))
        .await
        .unwrap_or_else(|e| panic!("surface {label} never became active: {e}"))
        .text()
        .await
        .expect("nav button text")
}

/// The browser's current `location.hash`.
async fn hash(client: &Client) -> String {
    client
        .execute("return window.location.hash;", vec![])
        .await
        .expect("read location.hash")
        .as_str()
        .expect("hash is a string")
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routing_flow() {
    let fx = control_plane_postgres::fixture::PgFixture::shared();
    let backend = start_backend(fx).await;
    let browser = start_browser()
        .await
        .expect("vendored browser must start (RE image / dev-box host libs)");
    let c = &browser.client;

    sign_in(c, &backend.base_url).await;

    // 1. The address bar is canonicalised on first render.
    assert_eq!(await_surface(c, "Catalog").await, "Catalog");
    assert_eq!(hash(c).await, "#/catalog");

    // 2. Clicking a nav item rewrites the hash.
    c.find(Locator::XPath("//nav//button[text()='Ontology']"))
        .await
        .expect("Ontology nav button")
        .click()
        .await
        .unwrap();
    await_surface(c, "Ontology").await;
    assert_eq!(hash(c).await, "#/ontology");

    // 3. The view survives a reload — the point of the whole exercise.
    c.refresh().await.unwrap();
    await_surface(c, "Ontology").await;
    assert_eq!(
        hash(c).await,
        "#/ontology",
        "the surface must be restored from the URL after a reload"
    );

    // 4. A deep link lands directly on its surface.
    c.goto(&format!("{}/#/transforms", backend.base_url))
        .await
        .unwrap();
    await_surface(c, "Transforms").await;

    // 5. Back walks the history the pushed navigations left.
    c.back().await.unwrap();
    await_surface(c, "Ontology").await;
    assert_eq!(
        hash(c).await,
        "#/ontology",
        "Back must return to the previous surface"
    );

    // 6. A deep link with a selection and a tab opens the drawer on that tab —
    //    without the dataset needing to exist, because the drawer resolves
    //    schema/name from the id rather than from the loaded list.
    c.goto(&format!(
        "{}/#/catalog/main.txns?tab=preview",
        backend.base_url
    ))
    .await
    .unwrap();
    await_surface(c, "Catalog").await;
    let title = c
        .wait()
        .for_element(Locator::Css(".shell-drawer .title"))
        .await
        .expect("deep link must open the drawer")
        .text()
        .await
        .unwrap();
    // The Panel title is CSS-uppercased, and WebDriver returns rendered text.
    assert_eq!(title.to_lowercase(), "txns");
    let tab = c
        .find(Locator::Css(".shell-drawer button.active"))
        .await
        .expect("an active drawer tab")
        .text()
        .await
        .unwrap();
    assert_eq!(
        tab, "Preview",
        "the deep link's ?tab= must select the drawer tab"
    );

    // 7. …and that survives a reload too.
    c.refresh().await.unwrap();
    await_surface(c, "Catalog").await;
    let tab = c
        .wait()
        .for_element(Locator::Css(".shell-drawer button.active"))
        .await
        .expect("drawer must reopen after reload")
        .text()
        .await
        .unwrap();
    assert_eq!(tab, "Preview");
    assert_eq!(hash(c).await, "#/catalog/main.txns?tab=preview");

    c.clone().close().await.unwrap();
}
