//! The pagination convention shared by every unbounded control-plane read.
//!
//! Read methods take a [`PageReq`] and return a [`Page<T>`]. Today every adapter
//! returns the full result set in a single page ([`Page::from_full`], `next: None`):
//! the request's `after`/`limit` are part of the stable signature but **not yet
//! enforced**. Real keyset limiting is a future adapter-only change that needs no
//! trait-signature churn — that is the whole point of fixing the convention now.

use serde::{Deserialize, Serialize};

/// An opaque keyset position. The encoding is adapter-defined and NOT part of the
/// contract — callers round-trip it verbatim (conventionally base64 of a keyset).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor(pub String);

/// A page request. `Default`/[`PageReq::unbounded`] = no limit, from the start.
///
/// `after`/`limit` are accepted but **not yet enforced** by any adapter (see the
/// module docs); a request for `limit(10)` currently still returns everything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PageReq {
    /// Resume after this cursor (exclusive). `None` = from the start.
    pub after: Option<Cursor>,
    /// Maximum items to return. `None` = unbounded.
    pub limit: Option<u32>,
}

impl PageReq {
    /// No cursor, no limit.
    pub fn unbounded() -> Self {
        Self::default()
    }
    /// Bounded by `n`, from the start.
    pub fn limit(n: u32) -> Self {
        Self {
            after: None,
            limit: Some(n),
        }
    }
    /// From `c`, unbounded.
    pub fn after(c: Cursor) -> Self {
        Self {
            after: Some(c),
            limit: None,
        }
    }
}

/// One page of results. `next == None` means there are no more.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Cursor to fetch the next page, or `None` if this is the last page.
    pub next: Option<Cursor>,
}

impl<T> Page<T> {
    /// The whole result set as a single, final page (`next: None`). What every
    /// adapter returns today.
    pub fn from_full(items: Vec<T>) -> Self {
        Self { items, next: None }
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

impl<T> IntoIterator for Page<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}
