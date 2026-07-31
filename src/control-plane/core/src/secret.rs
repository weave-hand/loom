//! Secret-carrying wrapper. Redaction is a property of the *type*, not of a
//! hand-written `Debug` on each struct that happens to hold a secret today —
//! so a `#[derive(Debug)]` struct that gains a secret field tomorrow cannot
//! silently start rendering it.

use std::fmt;

/// A value that must never appear in logs or diagnostics. `Debug` renders
/// `<redacted>`; reading the inner value requires an explicit [`expose`] call,
/// which is the grep-able audit point for "where does this secret escape".
///
/// Deliberately implements neither `Display` nor `Deref` — an implicit deref
/// would defeat the point, letting formatting-adjacent code reach the inner
/// value with no audit marker. Deliberately implements neither `Serialize` nor
/// `Deserialize`: no wrapped field is a serde type today, and adding them would
/// invite exactly the leak this closes.
///
/// [`expose`]: Redacted::expose
#[derive(Clone, PartialEq, Eq)]
pub struct Redacted<T>(T);

impl<T> Redacted<T> {
    /// Wrap a secret.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Borrow the secret. Every call site is a place a secret leaves the
    /// wrapper — grep for `.expose()` to audit them.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consume the wrapper and yield the secret.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> From<T> for Redacted<T> {
    fn from(value: T) -> Self {
        Self(value)
    }
}

impl<T> fmt::Debug for Redacted<T> {
    /// Writes the literal `<redacted>` — no type name, no length, no hash
    /// prefix. A length is a real hint against a password.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}
