# Lineage governance hardening — payload gating + owned-namespace fail-closed

- **Date:** 2026-07-03
- **Area:** lineage
- **Register items:** fixes [[iss-lineage-payload-redaction]] and
  [[iss-lineage-storage-namespace-fail-open]]; tightens [[road-lineage-acl-filtering]]
  (PR #321) and [[road-dataset-naming-bridge]]
- **Status:** approved

## Problem

The landed `LineageVisibility` filter
(`src/services/query-api/src/lineage_filter.rs`, spec
`2026-07-01-lineage-acl-filtering-design.md`) leaves two least-disclosure gaps, both
recorded in `docs/ISSUES.md`:

1. **`payload` leaks denied names** ([[iss-lineage-payload-redaction]]).
   `redact_events` governs the **typed** `inputs`/`outputs` of each event — denied
   `DatasetRef`s are dropped — but the opaque OpenLineage `payload` JSON is carried
   **verbatim** through `lineage_event_view` (`lineage_read.rs:112`) into the HTTP
   response. loom's own emitters embed dataset identifiers in it as free-form text
   (`http.rs:155` writes `{"schema":…, "name":…}`; `action.rs:899` the action/op), so
   a subject holding a `run_id` can read a denied dataset's name straight out of
   `payload`, defeating the redaction it just received on `inputs`/`outputs`.
2. **Malformed refs under the deployment's own storage namespace fail open**
   ([[iss-lineage-storage-namespace-fail-open]]). The spec's taxonomy had a third
   outcome — *Unresolvable → fail-closed* — but the landed bridge collapsed it:
   `LineageNaming::resolve` is total and degrades every parse failure to
   `External(raw)`. `is_readable` recovers fail-closed only for the loom-*logical*
   namespaces (`"loom"`/`"loom:type"`, `lineage_filter.rs:96-101`); a ref whose
   namespace **equals this deployment's `site_namespace`** (`s3://<bucket>` /
   `file://<root>`) but whose name fails the `schema.table` parse also lands in
   `External` — and is then **default-allowed** and expanded. A ref that *should*
   have resolved to a governed table thus widens disclosure instead of narrowing it.
   Narrow today (the internally-emitted graph uses logical refs), but
   [[fut-storage-derived-lineage-emit]] would make storage refs the canonical
   emitted form, raising the stakes.

## Design

Both fixes follow the landed spec's posture: **least disclosure, fail closed, never
widen on a gap**.

### 1. `payload`: all-or-nothing gating on the event's typed refs

`redact_events` gains one rule: an event's `payload` is served **verbatim iff the
subject can read every `DatasetRef` in that event's original `inputs` and
`outputs`**; if *any* ref was redacted, `payload` is replaced with
`serde_json::Value::Null`. Equivalently: omit it whenever any referenced dataset is
denied — the two "gate" options in the issue are the same predicate framed twice,
and it is the one we pick.

- **Why not field-level scrubbing.** There is no payload schema to scrub against
  ([[fut-openlineage-validation]] is deferred); scrubbing free-form JSON by
  string-matching denied names is a blocklist — fail-open by construction (nested
  keys, encodings, substrings, aliases). A blocklist on a governance surface is the
  wrong default; the gate is a whitelist ("serve only when provably clean").
- **Why this is the simplest correct.** `redact_events` already computes per-ref
  readability via `readable_only`; the gate is a length comparison
  (`kept.len() != original.len()` on either list ⇒ null the payload) — no new ACL
  calls, no bridge changes, no DTO change (`LineageEventView.payload` is already a
  `Value`; `Null` serializes as JSON `null`).
- **Disclosure of the gate itself.** A nulled payload is indistinguishable from an
  event stored with a null payload — consistent with the non-oracle posture (empty
  page for denied seed). It discloses nothing beyond what the already-visible
  redacted `inputs`/`outputs` imply.
- **Residual risk, accepted.** A payload can in principle name a dataset *not*
  among the event's typed refs; gating on the typed refs cannot see it. Governing
  that requires a typed payload schema — deferred with
  [[fut-openlineage-validation]] (see Out of scope).

### 2. Owned-namespace fail-closed: restore `Unresolvable` in the bridge

Add the missing taxonomy outcome to `lineage-naming` rather than widening the
filter's recovery hack:

- `ResolvedDataset` gains a variant: `Unresolvable(DatasetRef)` — the ref's
  namespace is **loom-owned** (logical `"loom"`/`"loom:type"` *or* this
  deployment's `site_namespace`) but the name fails the corresponding parse.
  `resolve` stays total (never errors); step 3 of its doc comment splits into
  "owned namespace + malformed name → `Unresolvable`" and "foreign namespace →
  `External`".
- `is_readable` maps `Unresolvable(_) → Ok(false)` (denied **and cut** — same
  treatment as an ACL Deny) and **deletes** its `LOOM_DATASET_NAMESPACE`/
  `LOOM_TYPE_NAMESPACE` recovery block: the namespace-ownership knowledge now lives
  in one place, the bridge, which already holds `site_namespace`. `External` keeps
  the documented default-allow pass-through for genuinely-foreign refs (different
  bucket, different scheme, non-loom logical namespaces).
- **Why a variant, not a `site_namespace()` accessor on the bridge.** An accessor
  would have query-api re-implement the ownership predicate the bridge already
  evaluates inside `resolve` — the exact duplication that produced this bug (the
  filter re-derived "loom-owned" and missed one case). The enum change is cheap:
  `lineage_filter.rs` is the only production consumer that matches on
  `ResolvedDataset`.
- **Behavioral delta** (deliberate, and the point): `{namespace: s3://<our-bucket>,
  name: "Customer"}` (or any non-`schema.table` name) was `External`/allowed, is
  now `Unresolvable`/denied. Genuinely-external refs (`s3://other-bucket/…`,
  `postgres://…`) are unchanged.

## Acceptance criteria (red-first)

All tests are `rust_test` integration targets (never inline `#[cfg(test)]`); the
postgres-backed ones use `loom_fixture_test` and `//src/services/query-api:e2e-support`.

1. **Payload gating — filter unit** (`tests/lineage_visibility.rs`, RE-eligible):
   `redact_events` over an event with one denied input ⇒ `payload` is `Value::Null`
   and the denied ref is gone; the same event for a subject reading every ref ⇒
   `payload` verbatim. An event with all refs readable and a stored-null payload
   stays null (no false "redaction" signal).
2. **Payload gating — e2e** (`loom_fixture_test`): seed a run whose event has mixed
   readable/denied inputs; `GET /lineage/runs/{id}/events` as the restricted
   subject returns the event with denied refs omitted **and** `"payload": null`;
   as an all-granted subject, the verbatim payload. Cursor/paging unchanged (no
   row is dropped).
3. **Bridge taxonomy** (`lineage-naming/tests/naming.rs`): `resolve` on a malformed
   name under `site_namespace` (e.g. `dr("s3://bucket", "Customer")`) ⇒
   `Unresolvable` (currently asserts `External` — flips red first); same for a
   malformed name under `"loom"`/`"loom:type"`; a foreign-namespace ref stays
   `External`; well-formed site-namespace and logical refs still resolve to
   `Table`/`Type` (existing round-trip tests stay green).
4. **Filter fail-closed** (filter unit): a malformed site-namespace ref in the
   closure is denied *and cut* (nodes reachable only through it never appear); as a
   seed it yields the empty page; a genuinely-external ref still passes traversal.
5. Full existing lineage suite stays green: `buck2 test //src/services/query-api/...
   //src/services/lineage-naming/...`.

## Out of scope

- **External pass-through threat model** (should a genuinely-external node *stop*
  traversal instead of passing it?) — stays with [[fut-lineage-filter-batch-resolve]],
  which carries that revisit alongside the batch/CTE work.
- **Field-level payload scrubbing / typed payload schema** — requires
  [[fut-openlineage-validation]]; the all-or-nothing gate is the correct floor until
  a schema exists to scrub against.
- **Storage-derived canonical emit** ([[fut-storage-derived-lineage-emit]]) — this
  spec only ensures that when those refs arrive, malformed ones fail closed.
- **Dropping fully-denied event envelopes** — redact-within is retained (the caller
  already holds the `run_id`); revisit only if `events_for` becomes reachable
  without prior `run_id` possession (landed spec's Open questions).
