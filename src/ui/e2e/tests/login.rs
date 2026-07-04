#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "rust_test body: assertions unwrap/expect and a failed browser start panics"
)]
//! Full-page login e2e: render, sad path, happy path — one backend + one browser
//! (resource-light, per the shared-cluster/fresh-db model).

use fantoccini::Locator;
use loom_ui_e2e::{ADMIN_PASS, ADMIN_USER, start_backend, start_browser};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_flow() {
    let fx = control_plane_postgres::fixture::PgFixture::shared();
    let backend = start_backend(fx).await;

    // The browser is guaranteed everywhere — the vendored Chromium's host libs
    // come from the RE image (tools/ci/rbe-browser, pinned in platforms/defs.bzl)
    // on the RE workers and from the host locally — so a start failure is a real
    // defect, never a skip.
    let browser = start_browser()
        .await
        .expect("vendored browser must start (RE image / dev-box host libs)");
    let c = &browser.client;

    // 1. Renders: the styled login mounts.
    c.goto(&backend.base_url).await.unwrap();
    c.wait()
        .for_element(Locator::Css("#login-username"))
        .await
        .unwrap();
    assert!(
        c.find(Locator::Css("#login-password")).await.is_ok(),
        "password field should render"
    );

    // 2. Sad path: wrong password → error, no Explorer, no stored token.
    c.find(Locator::Css("#login-username"))
        .await
        .unwrap()
        .send_keys(ADMIN_USER)
        .await
        .unwrap();
    c.find(Locator::Css("#login-password"))
        .await
        .unwrap()
        .send_keys("wrong-password")
        .await
        .unwrap();
    c.find(Locator::Css(".signin"))
        .await
        .unwrap()
        .click()
        .await
        .unwrap();
    c.wait().for_element(Locator::Css("p.error")).await.unwrap();
    assert!(
        c.find(Locator::Css("#login-username")).await.is_ok(),
        "login form should still be present after a failed sign-in"
    );
    let tok = c
        .execute(
            "return window.sessionStorage.getItem('loom_token');",
            vec![],
        )
        .await
        .unwrap();
    assert!(
        tok.is_null(),
        "no session token should be stored on failure, got {tok:?}"
    );

    // 3. Happy path: correct password → Explorer shell + stored token.
    // Clear the password field and retype the correct one.
    let pw = c.find(Locator::Css("#login-password")).await.unwrap();
    pw.clear().await.unwrap();
    pw.send_keys(ADMIN_PASS).await.unwrap();
    c.find(Locator::Css(".signin"))
        .await
        .unwrap()
        .click()
        .await
        .unwrap();
    // The Shell renders a <nav>; the login page has none.
    c.wait().for_element(Locator::Css("nav")).await.unwrap();
    assert!(
        c.find(Locator::Css("#login-username")).await.is_err(),
        "login form should be gone after a successful sign-in"
    );
    let tok = c
        .execute(
            "return window.sessionStorage.getItem('loom_token');",
            vec![],
        )
        .await
        .unwrap();
    assert!(
        tok.is_string(),
        "a session token should be stored on success, got {tok:?}"
    );

    c.clone().close().await.unwrap();
}
