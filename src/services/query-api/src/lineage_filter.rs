//! Least-disclosure governor for the `/lineage` reads. `core`'s `Lineage` serves
//! provenance without enforcing anything; this service-layer filter walks the
//! closure one hop at a time through the control plane's one-hop reads, ACL-gating
//! each frontier node via the `LineageNaming` bridge, and windows the fully
//! assembled *visible* set with the existing dataset keyset cursor. A denied
//! intermediate node is dropped AND not expanded (cut, not skip), so nodes reachable
//! only through it are never discovered. See
//! docs/superpowers/specs/2026-07-01-lineage-acl-filtering-design.md.

use control_plane_core::{
    Acl, Action, ControlPlaneError, DatasetRef, Decision, Lineage, LineageEvent, Page, PageReq,
    PolicyTarget, SubjectId, check_depth, decode_dataset_cursor, encode_dataset_cursor,
};
use lineage_naming::{LineageNaming, ResolvedDataset};

/// Hard bound on the number of distinct nodes the frontier BFS may examine per
/// request. A subject that can read almost nothing cannot force an unbounded ACL
/// fan-out over a huge closure; over-cap is a deterministic 422 (never a partial
/// page, which would have no valid resume point). Sits alongside `LINEAGE_MAX_DEPTH`.
pub const LINEAGE_FILTER_SCAN_CAP: usize = 10_000;

/// Which edge direction the closure walks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineageDir {
    Upstream,
    Downstream,
}

/// Failure modes distinct enough to map to different HTTP statuses at the handler.
#[derive(Debug)]
pub enum LineageVisibilityError {
    /// The closure examined more than `LINEAGE_FILTER_SCAN_CAP` nodes → 422.
    ScanCapExceeded,
    /// A control-plane / cursor fault. `Validation` → 400 (malformed cursor / bad
    /// depth); anything else is an infra fault → 500 (never disclosed as Allow).
    Cp(ControlPlaneError),
}

impl From<ControlPlaneError> for LineageVisibilityError {
    fn from(e: ControlPlaneError) -> Self {
        LineageVisibilityError::Cp(e)
    }
}

/// The service-layer lineage governor: borrows the ACL, the lineage read surface,
/// and the deployment naming bridge. Construct via `new` (production `scan_cap`) or
/// `new(..).with_scan_cap(n)` (tests that drive the cap).
pub struct LineageVisibility<'a> {
    acl: &'a (dyn Acl + Send + Sync),
    lineage: &'a (dyn Lineage + Send + Sync),
    bridge: &'a LineageNaming,
    scan_cap: usize,
}

impl<'a> LineageVisibility<'a> {
    /// The governor with the production scan cap.
    #[must_use]
    pub fn new(
        acl: &'a (dyn Acl + Send + Sync),
        lineage: &'a (dyn Lineage + Send + Sync),
        bridge: &'a LineageNaming,
    ) -> Self {
        LineageVisibility {
            acl,
            lineage,
            bridge,
            scan_cap: LINEAGE_FILTER_SCAN_CAP,
        }
    }

    /// Override the scan cap (tests only in practice — the production path uses
    /// `LINEAGE_FILTER_SCAN_CAP` via `new`).
    #[must_use]
    pub fn with_scan_cap(mut self, cap: usize) -> Self {
        self.scan_cap = cap;
        self
    }

    /// Classify a ref's readability for `subject`. `Table`/`Type` → readable iff
    /// `Acl::check(Read)` allows. `Unresolvable` (owned namespace, unparseable name) →
    /// **never readable**: it is denied *and cut*, same as an ACL Deny, so a bridge/
    /// mapping gap can only narrow, never widen, disclosure. The bridge now owns the
    /// namespace-ownership predicate (it holds `site_namespace`), so query-api no longer
    /// re-derives "loom-owned" here. `External` (a genuinely foreign datasource) →
    /// default-allow: it carries no loom-ACL'd data and is a source leaf.
    async fn is_readable(
        &self,
        subject: &SubjectId,
        r: &DatasetRef,
    ) -> Result<bool, LineageVisibilityError> {
        let target = match self.bridge.resolve(r) {
            ResolvedDataset::Table(t) => PolicyTarget::Table(t),
            ResolvedDataset::Type(ty) => PolicyTarget::Type(ty),
            ResolvedDataset::Unresolvable(_) => return Ok(false),
            ResolvedDataset::External(_) => return Ok(true),
        };
        Ok(self.acl.check(subject, Action::Read, &target).await? == Decision::Allow)
    }

    /// One hop of neighbours for `node` in `dir`, drained into a full vec. `depth==1`
    /// is the control plane's one-hop read; `PageReq::unbounded()` returns the whole
    /// (metadata-sized) neighbour set in one page (`from_keyset` with no limit ⇒ final
    /// page). The seed is excluded by the capability's contract.
    async fn one_hop(
        &self,
        node: &DatasetRef,
        dir: LineageDir,
    ) -> Result<Vec<DatasetRef>, LineageVisibilityError> {
        let page = match dir {
            LineageDir::Upstream => self.lineage.upstream(node, 1, PageReq::unbounded()).await?,
            LineageDir::Downstream => {
                self.lineage
                    .downstream(node, 1, PageReq::unbounded())
                    .await?
            }
        };
        Ok(page.items)
    }

    /// The governed transitive closure of `seed` in `dir` for `subject`, windowed by
    /// `page`. Empty page when the seed is not readable (seed gating: provenance *of* a
    /// dataset you cannot read is itself a fact about it; empty — not 403/404 — so the
    /// endpoint is not a seed-existence oracle). BFS expands ONLY through readable
    /// nodes (cut); a denied node is omitted and not expanded. `|visited|` is capped at
    /// `LINEAGE_FILTER_SCAN_CAP`. The visible set is a pure function of
    /// `(seed, depth, subject)`, so windowing it is stable and every page is full.
    pub async fn visible_closure(
        &self,
        subject: &SubjectId,
        seed: &DatasetRef,
        depth: u32,
        dir: LineageDir,
        page: &PageReq,
    ) -> Result<Page<DatasetRef>, LineageVisibilityError> {
        // query-api now owns depth validation: it drives depth=1 reads, so the
        // capability never sees the caller's depth.
        check_depth(depth)?;

        // Seed gating.
        if !self.is_readable(subject, seed).await? {
            return Ok(Page::from_full(Vec::new()));
        }

        // std collections used directly to avoid a hashing dep; DatasetRef: Ord+Clone.
        let mut visited: std::collections::BTreeSet<DatasetRef> = std::collections::BTreeSet::new();
        visited.insert(seed.clone());
        let mut frontier: Vec<DatasetRef> = vec![seed.clone()];
        let mut visible: Vec<DatasetRef> = Vec::new();

        for _hop in 0..depth {
            if frontier.is_empty() {
                break;
            }
            let mut next: Vec<DatasetRef> = Vec::new();
            for node in &frontier {
                for nb in self.one_hop(node, dir).await? {
                    if !visited.insert(nb.clone()) {
                        continue; // cycle / already-seen guard (mirrors the CTE UNION)
                    }
                    if visited.len() > self.scan_cap {
                        return Err(LineageVisibilityError::ScanCapExceeded);
                    }
                    if self.is_readable(subject, &nb).await? {
                        visible.push(nb.clone());
                        next.push(nb);
                    }
                    // else: cut — dropped and NOT pushed to `next`.
                }
            }
            frontier = next;
        }

        window(visible, page)
    }

    /// Governed upstream closure (output→input ancestry).
    pub async fn visible_upstream(
        &self,
        subject: &SubjectId,
        seed: &DatasetRef,
        depth: u32,
        page: &PageReq,
    ) -> Result<Page<DatasetRef>, LineageVisibilityError> {
        self.visible_closure(subject, seed, depth, LineageDir::Upstream, page)
            .await
    }

    /// Governed downstream closure (input→output descendancy).
    pub async fn visible_downstream(
        &self,
        subject: &SubjectId,
        seed: &DatasetRef,
        depth: u32,
        page: &PageReq,
    ) -> Result<Page<DatasetRef>, LineageVisibilityError> {
        self.visible_closure(subject, seed, depth, LineageDir::Downstream, page)
            .await
    }

    /// Redact denied refs *within* each event and gate the opaque `payload`. Any
    /// `DatasetRef` in `inputs`/`outputs` the subject cannot read is dropped, keeping
    /// the envelope (no row is dropped, so the page shape and event cursor are
    /// untouched). The `payload` is served verbatim **iff every typed ref was
    /// readable**; if any ref was redacted it is replaced with `Value::Null`, because
    /// loom's emitters embed dataset identifiers in the free-form `payload` and a
    /// blocklist scrub of arbitrary JSON is fail-open by construction. A nulled payload
    /// is indistinguishable from a stored-null one — it discloses nothing beyond what
    /// the already-redacted `inputs`/`outputs` imply.
    pub async fn redact_events(
        &self,
        subject: &SubjectId,
        page: Page<LineageEvent>,
    ) -> Result<Page<LineageEvent>, LineageVisibilityError> {
        let Page { items, next } = page;
        let mut out = Vec::with_capacity(items.len());
        for mut ev in items {
            let inputs_len = ev.inputs.len();
            let outputs_len = ev.outputs.len();
            ev.inputs = self.readable_only(subject, ev.inputs).await?;
            ev.outputs = self.readable_only(subject, ev.outputs).await?;
            if ev.inputs.len() != inputs_len || ev.outputs.len() != outputs_len {
                // Some ref was denied ⇒ the payload may name it in free-form text. Gate.
                ev.payload = serde_json::Value::Null;
            }
            out.push(ev);
        }
        Ok(Page { items: out, next })
    }

    /// Retain only the refs `subject` may read (preserving order).
    async fn readable_only(
        &self,
        subject: &SubjectId,
        refs: Vec<DatasetRef>,
    ) -> Result<Vec<DatasetRef>, LineageVisibilityError> {
        let mut kept = Vec::with_capacity(refs.len());
        for r in refs {
            if self.is_readable(subject, &r).await? {
                kept.push(r);
            }
        }
        Ok(kept)
    }
}

/// Sort the visible set by the stable `(namespace, name)` order and apply the keyset
/// window: drop everything `<= after`, keep `limit + 1` for `Page::from_keyset` to
/// truncate and derive `next`. Mirrors the adapter's own `from_keyset` windowing so
/// the cursor round-trips identically to the unfiltered read.
fn window(
    mut visible: Vec<DatasetRef>,
    page: &PageReq,
) -> Result<Page<DatasetRef>, LineageVisibilityError> {
    // The BFS `visited` set already guarantees uniqueness, so no dedup is needed —
    // only the stable `(namespace, name)` sort (DatasetRef: Ord).
    visible.sort();
    if let Some(cursor) = &page.after {
        let after = decode_dataset_cursor(cursor)?;
        visible.retain(|d| *d > after);
    }
    let taken: Vec<DatasetRef> = match page.limit {
        // `usize::try_from(..).unwrap_or(usize::MAX)` is the in-tree idiom for this
        // exact `limit + 1` (see memory/src/lineage.rs) — a lossless widen that avoids
        // an `as` cast and matches the adapter's own keyset windowing verbatim.
        Some(l) => {
            let keep = usize::try_from(l).unwrap_or(usize::MAX).saturating_add(1);
            visible.into_iter().take(keep).collect()
        }
        None => visible,
    };
    Ok(Page::from_keyset(taken, page.limit, encode_dataset_cursor))
}

/// A naming bridge over a local-disk warehouse root — the default when a caller has
/// no `ObjectStoreConfig` at hand. Logical `loom`/`loom:type` refs resolve regardless
/// of warehouse (the only thing the tests and the internally-emitted graph use); this
/// simply fixes the storage-derived namespace to a local root. Reuses
/// `service_runtime`'s `ObjectStoreConfig` re-export (already a lib dep), so callers
/// need no direct `store-config`/`lineage-naming` dep.
#[must_use]
pub fn local_naming() -> std::sync::Arc<LineageNaming> {
    std::sync::Arc::new(LineageNaming::from_object_store(
        &service_runtime::ObjectStoreConfig {
            warehouse_uri: "file:///loom".to_string(),
            backend: service_runtime::ObjectStoreBackend::Local,
        },
    ))
}
