use loom_ui_core::{AuthError, FetchGeneration, status_to_error, url};

#[test]
fn url_empty_base_is_relative() {
    assert_eq!(url("", "/auth/login"), "/auth/login");
}

#[test]
fn url_nonempty_base_joins_without_double_slash() {
    assert_eq!(
        url("https://api.example", "/auth/login"),
        "https://api.example/auth/login"
    );
    assert_eq!(
        url("https://api.example/", "/auth/login"),
        "https://api.example/auth/login"
    );
}

#[test]
fn status_401_is_bad_credentials() {
    assert!(matches!(status_to_error(401), AuthError::BadCredentials));
}

#[test]
fn status_other_is_server() {
    assert!(matches!(status_to_error(500), AuthError::Server(500)));
}

#[test]
fn auth_error_displays_human_text() {
    assert_eq!(
        AuthError::BadCredentials.to_string(),
        "incorrect username or password"
    );
    assert_eq!(AuthError::Network.to_string(), "could not reach the server");
}

#[test]
fn fetch_generation_starts_at_zero_and_is_current() {
    // `gen` is a reserved keyword on this nightly (generator blocks); use `g`.
    let g = FetchGeneration::default();
    assert_eq!(g.current(), 0);
    assert!(g.is_current(0), "the initial generation is current");
}

#[test]
fn fetch_generation_bump_advances_and_stales_prior() {
    let mut g = FetchGeneration::default();
    let first = g.bump(); // a fetch spawned for the first selection captures this
    assert_eq!(first, 1);
    assert!(g.is_current(first));

    let second = g.bump(); // selection changed: a new fetch captures this
    assert_eq!(second, 2);
    assert!(g.is_current(second), "the latest generation is current");
    assert!(
        !g.is_current(first),
        "the prior selection's fetch is now stale and must not commit"
    );
}
