//! The hash-route model: which surface, which row, which drawer tab, and — on
//! Catalog — the list controls, all encoded in the URL so a view is linkable and
//! survives a reload.
//!
//! **Why the fragment and not a path.** query-api's static serving does have an
//! SPA fallback, so path routes would survive a reload when it serves the bundle.
//! They would not under `buck2 run //src/ui:serve` (a bare `python3 -m
//! http.server`) nor on an arbitrary static host in the detached/CORS topology.
//! The fragment never reaches any server, so the URL grammar is independent of how
//! the bundle is served.
//!
//! Grammar: `#/{surface}[/{selection}][?tab=…&sort=…&dir=…&project=…]`. Extra path
//! segments beyond the selection are ignored (only reachable by hand-typing — the
//! serialiser percent-encodes `/` inside an id).
//!
//! Parsing is **total**: an unrecognised surface, tab or sort token degrades to the
//! default rather than erroring, so a hand-edited or stale URL still renders a
//! working app instead of a blank one.

use crate::{CatalogSortDir, DatasetSort, Surface};

/// One uppercase hex digit for a nibble. Deliberately not `format!`: appending a
/// `format!` to a `String` trips `clippy::format_push_string`, and this crate is
/// lint-clean with no crate-level allow. (Not a `const fn` — `From<u8> for char`
/// is not const, so `char::from` in a `const fn` would not compile.)
fn hex_digit(nibble: u8) -> char {
    char::from(if nibble < 10 {
        b'0' + nibble
    } else {
        b'A' + nibble - 10
    })
}

/// Percent-encode everything outside the RFC 3986 unreserved set. Applied to the
/// selection id and the project filter, which are user data and must not be able to
/// introduce a `/`, `?`, `&` or `=` into the route grammar. Encoding is per-byte, so
/// non-ASCII names round-trip through [`pct_decode`].
fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(hex_digit(b >> 4));
            out.push(hex_digit(b & 0x0f));
        }
    }
    out
}

/// Inverse of [`pct_encode`]. A malformed escape (`%` not followed by two hex
/// digits) is passed through literally rather than dropped, so decoding an
/// arbitrary string never loses characters.
fn pct_decode(s: &str) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(s.len());
    let mut rest = s;
    while let Some((head, tail)) = rest.split_once('%') {
        out.extend_from_slice(head.as_bytes());
        match tail.get(..2).and_then(|h| u8::from_str_radix(h, 16).ok()) {
            Some(byte) => {
                out.push(byte);
                rest = tail.get(2..).unwrap_or("");
            }
            None => {
                out.push(b'%');
                rest = tail;
            }
        }
    }
    out.extend_from_slice(rest.as_bytes());
    String::from_utf8_lossy(&out).into_owned()
}

/// Split a query string into decoded `(key, value)` pairs, skipping fragments with
/// no `=`.
fn query_pairs(query: &str) -> impl Iterator<Item = (&str, String)> {
    query.split('&').filter_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        Some((key, pct_decode(value)))
    })
}

/// The Catalog list controls that participate in the URL. These mirror the
/// `GET /datasets` query params, so a linked Catalog view reproduces the same
/// server-side sort and filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogQuery {
    pub sort: DatasetSort,
    pub dir: CatalogSortDir,
    /// The active project filter; `None` is the "All" chip.
    pub project: Option<String>,
}

impl Default for CatalogQuery {
    fn default() -> Self {
        CatalogQuery {
            sort: DatasetSort::Name,
            dir: CatalogSortDir::Asc,
            project: None,
        }
    }
}

/// One resolved location in the workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub surface: Surface,
    /// Stable id of the selected row: `"schema.name"` on Catalog, the type name on
    /// Ontology, the transform name on Transforms. `None` = drawer closed.
    ///
    /// Deliberately an id and not a list index: an index is meaningless in a URL
    /// (it moves when the list is re-sorted, filtered or reloaded) and cannot be
    /// resolved before the list has loaded.
    pub selection: Option<String>,
    /// Active drawer tab id — always one of `surface.tabs()` after parsing, or
    /// `None` for a surface with no drawer tabs.
    pub tab: Option<String>,
    /// Catalog list controls. Serialised only on the Catalog surface, but carried in
    /// memory across a surface switch so returning to Catalog restores them.
    pub catalog: CatalogQuery,
}

impl Default for Route {
    fn default() -> Self {
        Route::new(Surface::Catalog)
    }
}

impl Route {
    /// A fresh route onto `surface`: nothing selected, the surface's default tab,
    /// default list controls.
    #[must_use]
    pub fn new(surface: Surface) -> Route {
        Route {
            surface,
            selection: None,
            tab: surface.default_tab().map(str::to_owned),
            catalog: CatalogQuery::default(),
        }
    }

    /// Parse a `window.location.hash`. Total — see the module docs.
    #[must_use]
    pub fn parse(hash: &str) -> Route {
        let raw = hash.strip_prefix('#').unwrap_or(hash);
        let (path, query) = raw.split_once('?').unwrap_or((raw, ""));
        let mut segments = path.trim_start_matches('/').split('/');
        let surface = segments
            .next()
            .and_then(Surface::from_slug)
            .unwrap_or(Surface::Catalog);
        let selection = segments.next().map(pct_decode).filter(|s| !s.is_empty());
        let mut route = Route {
            selection,
            ..Route::new(surface)
        };
        for (key, value) in query_pairs(query) {
            match key {
                // An unknown tab id (or one belonging to a different surface) leaves
                // the default in place rather than blanking the drawer.
                "tab" if surface.tabs().contains(&value.as_str()) => route.tab = Some(value),
                "sort" => {
                    if let Some(sort) = DatasetSort::from_param(&value) {
                        route.catalog.sort = sort;
                    }
                }
                "dir" => {
                    if let Some(dir) = CatalogSortDir::from_param(&value) {
                        route.catalog.dir = dir;
                    }
                }
                "project" if !value.is_empty() => route.catalog.project = Some(value),
                _ => {}
            }
        }
        route
    }

    /// Serialise to a `#`-prefixed hash. Defaults are omitted, so the common route is
    /// just `#/catalog`; params are emitted in a fixed order so the address bar is
    /// stable across renders.
    #[must_use]
    pub fn to_hash(&self) -> String {
        let mut out = format!("#/{}", self.surface.slug());
        if let Some(selection) = &self.selection {
            out.push('/');
            out.push_str(&pct_encode(selection));
        }
        let mut params: Vec<String> = Vec::new();
        if let Some(tab) = &self.tab
            && Some(tab.as_str()) != self.surface.default_tab()
        {
            params.push(format!("tab={}", pct_encode(tab)));
        }
        if self.surface == Surface::Catalog {
            let defaults = CatalogQuery::default();
            if self.catalog.sort != defaults.sort {
                params.push(format!("sort={}", self.catalog.sort.as_param()));
            }
            if self.catalog.dir != defaults.dir {
                params.push(format!("dir={}", self.catalog.dir.as_param()));
            }
            if let Some(project) = &self.catalog.project {
                params.push(format!("project={}", pct_encode(project)));
            }
        }
        if !params.is_empty() {
            out.push('?');
            out.push_str(&params.join("&"));
        }
        out
    }

    /// Switch surfaces: nothing selected, the target's default tab. The Catalog list
    /// controls ride along, because they are view configuration rather than a
    /// location — returning to Catalog restores the same list.
    #[must_use]
    pub fn with_surface(&self, surface: Surface) -> Route {
        Route {
            surface,
            selection: None,
            tab: surface.default_tab().map(str::to_owned),
            catalog: self.catalog.clone(),
        }
    }

    /// Select a row on the current surface. The drawer resets to its default tab,
    /// matching what a freshly-opened drawer shows.
    #[must_use]
    pub fn with_selection(&self, id: impl Into<String>) -> Route {
        Route {
            selection: Some(id.into()),
            tab: self.surface.default_tab().map(str::to_owned),
            ..self.clone()
        }
    }

    /// Switch the drawer tab, keeping the selection.
    #[must_use]
    pub fn with_tab(&self, tab: impl Into<String>) -> Route {
        Route {
            tab: Some(tab.into()),
            ..self.clone()
        }
    }

    /// Replace the Catalog list controls. Re-sorting or filtering changes what the
    /// list contains, so the selection is cleared and the drawer closes.
    #[must_use]
    pub fn with_catalog(&self, catalog: CatalogQuery) -> Route {
        Route {
            catalog,
            selection: None,
            tab: self.surface.default_tab().map(str::to_owned),
            ..self.clone()
        }
    }

    /// Close the drawer without leaving the surface (list controls preserved).
    #[must_use]
    pub fn cleared(&self) -> Route {
        self.with_surface(self.surface)
    }

    /// The selection id, but only when the route is pointing at `surface`. Each
    /// surface reads its own selection through this, so an Ontology type name can
    /// never be mistaken for a Catalog dataset id by an effect that stays mounted
    /// regardless of the active surface.
    #[must_use]
    pub fn selection_on(&self, surface: Surface) -> Option<&str> {
        (self.surface == surface)
            .then_some(self.selection.as_deref())
            .flatten()
    }

    /// The active drawer tab, but only when the route is pointing at `surface`.
    #[must_use]
    pub fn tab_on(&self, surface: Surface) -> Option<&str> {
        (self.surface == surface)
            .then_some(self.tab.as_deref())
            .flatten()
    }
}
