use loom_ui_core::{AuthError, status_to_error, url};

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
