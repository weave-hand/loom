# Lineage governance hardening — payload gating + owned-namespace fail-closed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the two remaining least-disclosure gaps in the landed `/lineage` governor — the opaque `payload` leaking denied dataset names, and malformed refs under the deployment's own storage namespace failing *open* — both by tightening, never widening, disclosure.

**Architecture:** Two independent fixes. (1) `redact_events` gains an all-or-nothing gate: an event's `payload` is served verbatim iff the subject can read *every* typed ref in that event; otherwise it is replaced with `serde_json::Value::Null`. (2) The `lineage_naming` bridge regains the spec's missing taxonomy outcome — a new `ResolvedDataset::Unresolvable(DatasetRef)` for refs whose namespace is loom-owned (logical `"loom"`/`"loom:type"` *or* this deployment's `site_namespace`) but whose name fails to parse; the filter maps it to denied-and-cut and drops its old, incomplete recovery hack, moving the ownership predicate into the one place that already knows `site_namespace`.

**Tech Stack:** Rust 2024, buck2 (`rust_test`/`loom_fixture_test` targets — never inline `#[cfg(test)]`), `control_plane_memory` + real file-backed `LineageNaming` for pure-logic unit tests, hermetic Postgres (`PgFixture`) + `//src/services/query-api:e2e-support` for e2e.

## Global Constraints

- **Spec:** `docs/superpowers/specs/2026-07-03-lineage-governance-hardening-design.md` (status: approved). Both fixes follow its posture: **least disclosure, fail closed, never widen on a gap.**
- **Tests are `rust_test` integration targets only.** Put tests in `tests/<name>.rs` wired in the crate's `BUCK`; never inline `#[cfg(test)]` (the `no-inline-tests` prek hook fails the build otherwise). All three test files this plan touches already exist and are already wired — **no BUCK edits are required.**
- **Fixture-backed tests use `loom_fixture_test`** (already the case for `lineage-acl-e2e`). Pure-logic tests (`lineage-visibility`, `naming`) are RE-eligible `rust_test`.
- **Never pipe `buck2 test` through `tail`/`head`** — redirect to a file and grep it: `buck2 test <target> > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|PASS" /tmp/t.log`.
- **Clippy is strict** (pedantic + restriction on production code). Removing a symbol means removing its now-unused `use` import, or the `clippy`/`unused_imports` gate reddens.
- **Closes** `iss-lineage-payload-redaction` (Task 1) and `iss-lineage-storage-namespace-fail-open` (Task 2), both in `docs/ISSUES.md`, both `spec:2026-07-03-lineage-governance-hardening-design`.

---

### Task 1: Payload all-or-nothing gate

Independent of the bridge change; the tree compiles throughout. Closes `iss-lineage-payload-redaction`.

**Files:**
- Modify: `src/services/query-api/src/lineage_filter.rs` (`redact_events`, ~lines 206-224; and the doc comment)
- Test: `src/services/query-api/tests/lineage_visibility.rs` (unit — flip one existing test, add two)
- Test: `src/services/query-api/tests/lineage_acl_e2e.rs` (e2e — extend one existing test, add one)

**Interfaces:**
- Consumes: `LineageVisibility::redact_events(&self, subject: &SubjectId, page: Page<LineageEvent>) -> Result<Page<LineageEvent>, LineageVisibilityError>` (unchanged signature); `readable_only` (private helper, unchanged); `LineageEvent { inputs: Vec<DatasetRef>, outputs: Vec<DatasetRef>, payload: serde_json::Value, .. }`.
- Produces: same `redact_events` signature, new behavior — `payload` becomes `Value::Null` whenever any ref in `inputs` or `outputs` was redacted.

- [ ] **Step 1: Flip the existing unit test red (payload now nulled on redaction)**

In `src/services/query-api/tests/lineage_visibility.rs`, replace the existing test `redact_events_drops_denied_refs_keeps_envelope` (currently ending with `assert_eq!(e.payload, serde_json::json!({ "k": 1 }), "payload verbatim");`) with:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn redact_events_nulls_payload_when_any_ref_denied() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["A"]).await; // reads A, not B
    let run = RunId(uuid::Uuid::new_v4());
    let ev = LineageEvent {
        run_id: run,
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ty("A"), ty("B")],
        outputs: vec![ty("B")],
        payload: serde_json::json!({ "k": 1 }),
    };
    let page = control_plane_core::Page {
        items: vec![ev],
        next: None,
    };
    let red = vis_for(&cp, &bridge)
        .redact_events(&subj, page)
        .await
        .unwrap();
    let e = &red.items[0];
    assert_eq!(e.inputs, vec![ty("A")], "denied B removed from inputs");
    assert!(e.outputs.is_empty(), "denied B removed from outputs");
    assert_eq!(e.run_id, run, "envelope intact");
    assert!(
        e.payload.is_null(),
        "any redaction nulls the payload (least disclosure), got {:?}",
        e.payload
    );
}
```

- [ ] **Step 2: Add the two new unit tests (verbatim-when-clean, stored-null-stays-null)**

Append to `src/services/query-api/tests/lineage_visibility.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn redact_events_keeps_payload_when_all_refs_readable() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["A", "B"]).await; // reads every ref
    let ev = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ty("A")],
        outputs: vec![ty("B")],
        payload: serde_json::json!({ "k": 1 }),
    };
    let page = control_plane_core::Page {
        items: vec![ev],
        next: None,
    };
    let red = vis_for(&cp, &bridge)
        .redact_events(&subj, page)
        .await
        .unwrap();
    let e = &red.items[0];
    assert_eq!(e.inputs, vec![ty("A")], "nothing redacted");
    assert_eq!(e.outputs, vec![ty("B")], "nothing redacted");
    assert_eq!(
        e.payload,
        serde_json::json!({ "k": 1 }),
        "all refs readable → payload verbatim"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn redact_events_stored_null_payload_stays_null() {
    let cp = cp();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["A", "B"]).await; // reads every ref → nothing redacted
    let ev = LineageEvent {
        run_id: RunId(uuid::Uuid::new_v4()),
        event_type: EventType::Complete,
        event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
        inputs: vec![ty("A")],
        outputs: vec![ty("B")],
        payload: serde_json::Value::Null,
    };
    let page = control_plane_core::Page {
        items: vec![ev],
        next: None,
    };
    let red = vis_for(&cp, &bridge)
        .redact_events(&subj, page)
        .await
        .unwrap();
    assert!(
        red.items[0].payload.is_null(),
        "a stored-null payload with no redaction stays null (no false 'redacted' signal)"
    );
}
```

- [ ] **Step 3: Run the unit tests to verify they fail**

Run: `buck2 test //src/services/query-api:lineage-visibility > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL|Pass|Fail" /tmp/t1.log`
Expected: FAIL — exactly one test is red: `redact_events_nulls_payload_when_any_ref_denied` (current code carries `payload` verbatim even when B is redacted, so the `is_null()` assertion fails). The other two **already pass** on current code and are guard tests, not red-first: `redact_events_keeps_payload_when_all_refs_readable` (nothing redacted ⇒ payload already verbatim) and `redact_events_stored_null_payload_stays_null` (null stays null today). Seeing one FAIL here is correct — it is not a problem.

- [ ] **Step 4: Implement the gate in `redact_events`**

In `src/services/query-api/src/lineage_filter.rs`, replace the `redact_events` method (the doc comment plus body) with:

```rust
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
```

- [ ] **Step 5: Run the unit tests to verify they pass**

Run: `buck2 test //src/services/query-api:lineage-visibility > /tmp/t1.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1.log`
Expected: PASS — all `lineage-visibility` tests green (the three payload tests plus the unchanged BFS/window/cut tests).

- [ ] **Step 6: Extend the e2e test to assert the payload is nulled**

In `src/services/query-api/tests/lineage_acl_e2e.rs`, in `events_redaction_omits_denied_refs_keeps_envelope`, add a payload assertion after the existing `assert_eq!(ev["event_type"], "complete", "envelope intact");` line:

```rust
    assert!(
        ev["payload"].is_null(),
        "SECRET denied ⇒ payload gated to null: {body}"
    );
```

- [ ] **Step 7: Add the all-granted e2e test (verbatim payload)**

Append to `src/services/query-api/tests/lineage_acl_e2e.rs`:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn events_all_granted_returns_verbatim_payload() {
    let fx = PgFixture::shared();
    let cp = fresh(fx).await;
    let run = RunId(uuid::Uuid::new_v4());
    cp.lineage()
        .emit(LineageEvent {
            run_id: run,
            event_type: EventType::Complete,
            event_time: time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            inputs: vec![ty("A")],
            outputs: vec![ty("OUT")],
            payload: serde_json::json!({ "k": 1 }),
        })
        .await
        .unwrap();
    // Grant both refs ⇒ nothing redacted ⇒ payload served verbatim.
    let (_u, role) = subject_with_role(&cp, "u").await;
    grant_types(&cp, &role, &["A", "OUT"]).await;
    let uri = format!("/lineage/runs/{}/events", run.0);
    let (status, body) = get(cp.clone(), Arc::new(NoServing), &uri, "u").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let ev = &body["events"][0];
    assert_eq!(
        ev["payload"],
        serde_json::json!({ "k": 1 }),
        "all refs granted ⇒ verbatim payload: {body}"
    );
}
```

- [ ] **Step 8: Run the e2e tests to verify they pass**

Run: `buck2 test //src/services/query-api:lineage-acl-e2e > /tmp/t1e.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t1e.log`
Expected: PASS — `events_redaction_omits_denied_refs_keeps_envelope` (now asserting null) and `events_all_granted_returns_verbatim_payload` both green, all other e2e tests unchanged.

- [ ] **Step 9: Commit**

```bash
git add src/services/query-api/src/lineage_filter.rs \
        src/services/query-api/tests/lineage_visibility.rs \
        src/services/query-api/tests/lineage_acl_e2e.rs
git commit -m "fix(lineage): gate opaque payload on denied typed refs (iss-lineage-payload-redaction)"
```

---

### Task 2: Owned-namespace fail-closed — restore `Unresolvable` in the bridge

The `ResolvedDataset` enum change and the filter's `is_readable` update **must land in the same task**: adding an enum variant makes the filter's non-wildcard `match` non-exhaustive, so `query-api` will not compile until the filter handles the new arm. Closes `iss-lineage-storage-namespace-fail-open`.

**Files:**
- Modify: `src/services/lineage-naming/src/lib.rs` (imports; `ResolvedDataset` enum; `resolve` method + doc)
- Modify: `src/services/query-api/src/lineage_filter.rs` (imports; `is_readable` method + doc)
- Test: `src/services/lineage-naming/tests/naming.rs` (flip two existing reverse tests, add one)
- Test: `src/services/query-api/tests/lineage_visibility.rs` (add the site-namespace cut test)

**Interfaces:**
- Consumes: `DatasetId::from_dataset_ref(&DatasetRef) -> Option<DatasetId>` (returns `None` when namespace != `"loom"` OR name is not a well-formed `schema.table`); `TypeId::from_dataset_ref(&DatasetRef) -> Option<TypeId>` (`None` when namespace != `"loom:type"` OR name empty); `LOOM_DATASET_NAMESPACE = "loom"`, `LOOM_TYPE_NAMESPACE = "loom:type"` (both `pub const` in `control_plane_core`); `LineageNaming.site_namespace: String` (the `s3://<bucket>` / `file://<root>` authority).
- Produces: `ResolvedDataset::Unresolvable(DatasetRef)` — a new variant returned by `resolve` for owned-but-malformed refs; the filter's `is_readable` maps it to `Ok(false)` (denied and cut).

- [ ] **Step 1: Flip the naming reverse tests red (owned-malformed now `Unresolvable`)**

In `src/services/lineage-naming/tests/naming.rs`, **replace** the test `reverse_malformed_under_loom_namespace_degrades_to_external` **and delete** `reverse_storage_namespace_with_type_shaped_name_is_external` (its case is folded in below), with:

```rust
#[test]
fn reverse_malformed_under_owned_namespace_is_unresolvable() {
    // A ref whose namespace is loom-owned (logical "loom"/"loom:type" OR this
    // deployment's site_namespace) but whose name fails the parse must fail closed —
    // Unresolvable, NOT External (which would default-allow and widen disclosure).
    let n = s3_naming(); // site_namespace = "s3://bucket"
    for bad in [
        dr("loom", "nodot"),           // logical table ns, no schema separator
        dr("loom", ".x"),              // empty schema
        dr("loom", "x."),              // empty table
        dr("loom:type", ""),           // logical type ns, empty name
        dr("s3://bucket", "a.b.c"),    // site ns, ambiguous multi-dot
        dr("s3://bucket", "Customer"), // site ns, no separator (type-shaped, tables only)
    ] {
        assert_eq!(
            n.resolve(&bad),
            ResolvedDataset::Unresolvable(bad.clone()),
            "owned-namespace malformed ref must fail closed"
        );
    }
}
```

Leave `reverse_external_datasources_are_not_rejected` (foreign namespaces `s3://other-bucket`, `postgres://h`, `kafka://broker`) **unchanged** — those stay `External`. Leave the forward tests, `reverse_logical_table_*`, `reverse_logical_type_*`, `reverse_storage_derived_table_*`, and `round_trip_table_and_type` unchanged — well-formed refs still resolve to `Table`/`Type`.

- [ ] **Step 2: Run the naming tests to verify they fail to compile**

Run: `buck2 test //src/services/lineage-naming:naming > /tmp/t2n.log 2>&1; grep -E "Tests finished|FAIL|error\[|cannot find" /tmp/t2n.log`
Expected: FAIL — compilation error, `no variant named \`Unresolvable\` found for enum \`ResolvedDataset\``. (An unbuildable test is the red state here; the variant does not exist yet.)

- [ ] **Step 3: Add the `Unresolvable` variant and rewrite `resolve`**

In `src/services/lineage-naming/src/lib.rs`:

First extend the import to bring in the type namespace constant:

```rust
use control_plane_core::{
    DatasetId, DatasetRef, LOOM_DATASET_NAMESPACE, LOOM_TYPE_NAMESPACE, TableRef, TypeId, TypeName,
};
```

Add the variant to `ResolvedDataset` (place it before `External`):

```rust
    /// The ref's namespace is **loom-owned** — logical `"loom"`/`"loom:type"` or this
    /// deployment's `site_namespace` — but its name fails the corresponding parse. The
    /// governed object it *should* name cannot be determined, so consumers must fail
    /// **closed** (deny) rather than treat it as external and default-allow. Carries the
    /// raw ref verbatim.
    Unresolvable(DatasetRef),
```

Replace the `resolve` method (doc comment + body) with a namespace-first form that can tell "wrong namespace" (→ `External`) apart from "owned namespace, bad name" (→ `Unresolvable`):

```rust
    /// Resolve any `DatasetRef` back to the governed object it names, or classify why
    /// not. Total; never errors. Keyed on the namespace first, so an owned namespace
    /// with a malformed name is distinguishable from a genuinely foreign one:
    ///   1. logical `"loom"` — well-formed `schema.table` → `Table`, else `Unresolvable`;
    ///   2. logical `"loom:type"` — non-empty name → `Type`, else `Unresolvable`;
    ///   3. this deployment's `site_namespace` — well-formed `schema.table` (tables only;
    ///      types live on the logical namespace) → `Table`, else `Unresolvable`;
    ///   4. any other namespace — a genuinely foreign datasource or a different loom
    ///      site's warehouse → `External(raw ref)`.
    pub fn resolve(&self, dr: &DatasetRef) -> ResolvedDataset {
        if dr.namespace == LOOM_DATASET_NAMESPACE {
            return match DatasetId::from_dataset_ref(dr) {
                Some(id) => ResolvedDataset::Table(id.table().clone()),
                None => ResolvedDataset::Unresolvable(dr.clone()),
            };
        }
        if dr.namespace == LOOM_TYPE_NAMESPACE {
            return match TypeId::from_dataset_ref(dr) {
                Some(ty) => ResolvedDataset::Type(ty.type_name().clone()),
                None => ResolvedDataset::Unresolvable(dr.clone()),
            };
        }
        if dr.namespace == self.site_namespace {
            // Storage namespace carries the same `schema.table` name shape as the
            // logical form; delegate to core's parse guards by re-namespacing. A
            // malformed name is owned-but-unparseable → Unresolvable (fail closed).
            let logical = DatasetRef {
                namespace: LOOM_DATASET_NAMESPACE.to_string(),
                name: dr.name.clone(),
            };
            return match DatasetId::from_dataset_ref(&logical) {
                Some(id) => ResolvedDataset::Table(id.table().clone()),
                None => ResolvedDataset::Unresolvable(dr.clone()),
            };
        }
        ResolvedDataset::External(dr.clone())
    }
```

- [ ] **Step 4: Run the naming tests to verify they pass**

Run: `buck2 test //src/services/lineage-naming:naming > /tmp/t2n.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2n.log`
Expected: PASS — `reverse_malformed_under_owned_namespace_is_unresolvable` green; the external, logical, storage-derived, and round-trip tests all still green.

- [ ] **Step 5: Add the filter cut test (red — query-api will not compile yet)**

In `src/services/query-api/tests/lineage_visibility.rs`, append:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn malformed_site_namespace_ref_is_denied_and_cut() {
    // The test bridge's site_namespace is "file:///loom" (see `naming`). A ref under
    // it whose name is not `schema.table` resolves to Unresolvable → fail closed:
    // denied AND cut, so a node reachable only through it is never discovered, and it
    // yields the empty page as a seed. A genuinely-external ref still passes.
    let cp = cp();
    // upstream(S): a malformed site-ns ref and a foreign s3 source both feed S.
    cp.lineage()
        .emit(edge(ext("file:///loom", "Customer"), ty("S")))
        .await
        .unwrap();
    cp.lineage()
        .emit(edge(ext("s3://raw", "landing.csv"), ty("S")))
        .await
        .unwrap();
    // G is upstream ONLY of the malformed (cut) node.
    cp.lineage()
        .emit(edge(ty("G"), ext("file:///loom", "Customer")))
        .await
        .unwrap();
    let bridge = naming();
    let subj = subject_reading(&cp, "u", &["S", "G"]).await;
    let vis = vis_for(&cp, &bridge);

    // As an intermediate: cut node absent, G (reachable only through it) hidden,
    // external source passes.
    let page = vis
        .visible_closure(&subj, &ty("S"), 3, LineageDir::Upstream, &PageReq::unbounded())
        .await
        .unwrap();
    assert_eq!(
        names(&page),
        vec!["landing.csv".to_string()],
        "malformed site-ns ref is cut (G hidden); external still passes"
    );

    // As a seed: fails closed → empty page (like a denied/unknown seed).
    let seeded = vis
        .visible_closure(
            &subj,
            &ext("file:///loom", "Customer"),
            2,
            LineageDir::Upstream,
            &PageReq::unbounded(),
        )
        .await
        .unwrap();
    assert!(
        seeded.items.is_empty(),
        "malformed site-ns seed fails closed → empty page"
    );
}
```

- [ ] **Step 6: Run the filter test to confirm query-api fails to compile**

Run: `buck2 test //src/services/query-api:lineage-visibility > /tmp/t2f.log 2>&1; grep -E "Tests finished|FAIL|non-exhaustive|error\[" /tmp/t2f.log`
Expected: FAIL — `query-api` fails to compile: `non-exhaustive patterns: \`Unresolvable(_)\` not covered` in `is_readable` (the enum gained a variant the filter's `match` does not handle). This confirms the cross-crate coupling; Step 7 resolves it.

- [ ] **Step 7: Map `Unresolvable → denied+cut` in the filter and delete the recovery hack**

In `src/services/query-api/src/lineage_filter.rs`, drop the two now-unused constants from the import (they moved into the bridge):

```rust
use control_plane_core::{
    Acl, Action, ControlPlaneError, DatasetRef, Decision, Lineage, LineageEvent, Page, PageReq,
    PolicyTarget, SubjectId, check_depth, decode_dataset_cursor, encode_dataset_cursor,
};
```

Replace the `is_readable` method (doc comment + body) with:

```rust
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
```

- [ ] **Step 8: Run the filter tests to verify they pass**

Run: `buck2 test //src/services/query-api:lineage-visibility > /tmp/t2f.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2f.log`
Expected: PASS — `malformed_site_namespace_ref_is_denied_and_cut` green; the existing `external_refs_are_default_allowed_unresolvable_loom_is_denied` still green (its `ext("loom", "nodot")` now routes through `Unresolvable` instead of the deleted `External`-recovery hack, with identical fail-closed behavior); all other tests unchanged.

- [ ] **Step 9: Run the full lineage suite (both crates) to confirm no regression**

Run: `buck2 test //src/services/query-api/... //src/services/lineage-naming/... > /tmp/t2all.log 2>&1; grep -E "Tests finished|FAIL" /tmp/t2all.log`
Expected: PASS — the whole `query-api` + `lineage-naming` test sweep green (spec acceptance criterion 5). This includes the e2e ACL tests and the `naming` reverse tests.

- [ ] **Step 10: Lint (clippy) — confirm the dropped imports leave no warning**

Run: `buck2 build '//src/services/query-api:query-api[clippy.txt]' '//src/services/lineage-naming:lineage-naming[clippy.txt]' > /tmp/t2clip.log 2>&1; echo "exit=$?"; cat buck-out/**/clippy.txt 2>/dev/null | head`
Expected: exit 0 and empty clippy output for both crates (no `unused_imports` from the removed `LOOM_*_NAMESPACE` uses). If non-empty, remove whatever it names.

- [ ] **Step 11: Commit**

```bash
git add src/services/lineage-naming/src/lib.rs \
        src/services/lineage-naming/tests/naming.rs \
        src/services/query-api/src/lineage_filter.rs \
        src/services/query-api/tests/lineage_visibility.rs
git commit -m "fix(lineage): fail closed on malformed owned-namespace refs via ResolvedDataset::Unresolvable (iss-lineage-storage-namespace-fail-open)"
```

---

### Task 3: Close the register items (documentation)

No tests. Done at finish via the `loom-docs-update` skill (invoked by `superpowers:finishing-a-development-branch`); recorded here for completeness so the plan covers the spec's register bookkeeping. **Do not hand-edit if `loom-docs-update` runs it** — this is the target end state.

**Files:**
- Modify: `docs/ISSUES.md` (both items → `[x]`, `status:fixed`, `pr:#<N>`, add resolution prose)
- Modify: `docs/superpowers/specs/2026-07-03-lineage-governance-hardening-design.md` (`Status: approved` → `Status: implemented`, if that convention is followed by peers)

- [ ] **Step 1: Mark both ISSUES items fixed**

In `docs/ISSUES.md`, for `iss-lineage-payload-redaction`: `- [ ]` → `- [x]`, `status:open` → `status:fixed`, `pr:-` → `pr:#<N>`, and append a one-line resolution note (payload now gated all-or-nothing on the event's typed refs; nulled when any ref is redacted). Do the same for `iss-lineage-storage-namespace-fail-open`: resolution note that owned-but-malformed refs now resolve to `ResolvedDataset::Unresolvable` and fail closed in the filter, with the recovery hack removed.

- [ ] **Step 2: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: no errors (grammar/ids/vocab/links all resolve; the `spec:` slug still resolves on disk).

- [ ] **Step 3: Commit (fold into the finish/PR step)**

```bash
git add docs/ISSUES.md docs/superpowers/specs/2026-07-03-lineage-governance-hardening-design.md
git commit -m "docs(registers): close iss-lineage-payload-redaction + iss-lineage-storage-namespace-fail-open"
```

---

## Self-Review

**1. Spec coverage** (against `2026-07-03-lineage-governance-hardening-design.md`):
- Design §1 payload all-or-nothing gate → Task 1 Steps 4 (impl), 1–2 + 6–7 (tests). Length-comparison predicate, `Value::Null`, no DTO change — all matched.
- Design §2 `Unresolvable` variant + `resolve` split + `is_readable` mapping + delete recovery block → Task 2 Steps 3 (bridge) and 7 (filter). Ownership predicate moved into the bridge; `External` default-allow retained for foreign refs.
- Acceptance criteria: (1) filter unit → T1 S1–S2; (2) e2e mixed + all-granted → T1 S6–S7; (3) bridge taxonomy → T2 S1; (4) filter fail-closed cut + seed + external-passes → T2 S5; (5) full suite green → T2 S9. All covered.
- Out of scope items (field-level scrub, storage-derived emit, external-stops-traversal, envelope-drop) → none implemented; correct.

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to". Every code step carries complete code; every run step carries the exact target and expected result.

**3. Type consistency:** `ResolvedDataset::Unresolvable(DatasetRef)` defined in T2 S3, matched in T2 S3 (`resolve`) and T2 S7 (`is_readable`) — same shape. `redact_events` signature unchanged across T1. `DatasetId::from_dataset_ref`/`TypeId::from_dataset_ref` return `Option`, consumed via `match … { Some/None }` — consistent with core's identity.rs. Test helpers (`ty`, `ext`, `edge`, `naming`, `cp`, `vis_for`, `subject_reading`, `names`, `grant_types`, `deftype`, `subject_with_role`, `get`, `NoServing`, `fresh`) all already exist in the touched files — no undefined references.
