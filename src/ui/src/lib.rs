//! Platform-neutral logic shared by the wasm `app` binary and its native tests.
//! Pure Rust (no web-sys), so it compiles for the host and is `rust_test`-able.

use std::fmt;

/// Join a runtime API base with a request path. An empty base yields a relative
/// (same-origin) URL; a non-empty base is used as a prefix with at most one `/`.
#[must_use]
pub fn url(base: &str, path: &str) -> String {
    if base.is_empty() {
        return path.to_string();
    }
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

/// Why a login attempt failed, mapped to user-facing text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The server rejected the credentials (HTTP 401).
    BadCredentials,
    /// The server responded with an unexpected status.
    Server(u16),
    /// The request never completed (transport / decode failure).
    Network,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadCredentials => write!(f, "incorrect username or password"),
            Self::Server(code) => write!(f, "server error ({code})"),
            Self::Network => write!(f, "could not reach the server"),
        }
    }
}

/// Map a non-success HTTP status from `/auth/login` to an [`AuthError`].
#[must_use]
pub fn status_to_error(status: u16) -> AuthError {
    if status == 401 {
        AuthError::BadCredentials
    } else {
        AuthError::Server(status)
    }
}

/// Visual weight of a [`Button`](loom_ui_components::Button).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ButtonVariant {
    Primary,
    Secondary,
    Ghost,
}

/// Semantic colour of a tag [`Badge`](loom_ui_components::Badge).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BadgeTone {
    Neutral,
    Info,
    Pii,
    Success,
    Warning,
    Danger,
}

impl BadgeTone {
    /// The `--loom-*` custom property this tone draws its accent colour from.
    #[must_use]
    pub fn css_var(self) -> &'static str {
        match self {
            Self::Neutral => "--loom-text-mut",
            Self::Info => "--loom-accent",
            Self::Pii | Self::Danger => "--loom-danger",
            Self::Success => "--loom-ok",
            Self::Warning => "--loom-warn",
        }
    }
}

/// Health / build state, rendered as a coloured dot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Status {
    Ok,
    Warn,
    Error,
}

impl Status {
    /// The `--loom-*` custom property for this status colour.
    #[must_use]
    pub fn css_var(self) -> &'static str {
        match self {
            Self::Ok => "--loom-ok",
            Self::Warn => "--loom-warn",
            Self::Error => "--loom-danger",
        }
    }
}

/// Horizontal cell alignment in a [`DataTable`](loom_ui_components::DataTable).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Align {
    Start,
    End,
}

/// Format a row count the way the catalog table shows it: `2_410_000` → `"2.41M"`,
/// `18_200` → `"18.2K"`, `880_000` → `"880K"`. Values below 1000 are rendered as-is.
/// Scaled values show up to 3 significant figures with trailing zeros trimmed.
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    reason = "display-only count formatting; exactness not required"
)]
pub fn format_count(n: u64) -> String {
    let (mut scaled, mut suffix) = if n >= 1_000_000 {
        (n as f64 / 1_000_000.0, "M")
    } else if n >= 1_000 {
        (n as f64 / 1_000.0, "K")
    } else {
        return n.to_string();
    };
    // 3 significant figures: 2.41M, 18.2K, 880K, 9.7K, 142K.
    let mut precision = if scaled >= 100.0 {
        0
    } else if scaled >= 10.0 {
        1
    } else {
        2
    };
    // Rounding at `precision` can push `scaled` up to (or past) 1000 within a
    // tier (e.g. 999.999K rounds to "1000K"); re-scale up one suffix tier so
    // the displayed value never overflows its own bucket.
    let multiplier = match precision {
        0 => 1.0,
        1 => 10.0,
        _ => 100.0,
    };
    let rounded = (scaled * multiplier).round() / multiplier;
    // No M->B tier: display-only formatting; loom defines no billions token/screen,
    // so values >= ~1e9 render as "NNNNM".
    if rounded >= 1000.0 && suffix == "K" {
        scaled /= 1000.0;
        suffix = "M";
        precision = if scaled >= 100.0 {
            0
        } else if scaled >= 10.0 {
            1
        } else {
            2
        };
    }
    let mut s = format!("{scaled:.precision$}");
    if s.contains('.') {
        s = s.trim_end_matches('0').trim_end_matches('.').to_string();
    }
    format!("{s}{suffix}")
}
