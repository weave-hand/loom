# Control-Plane Adapter Tracing Pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every control-plane adapter SQL method a `#[tracing::instrument]` span on the concerns that currently lag (`catalog`, `snapshot`, `lineage`), matching the `acl`/`queue` convention, and correct the stale Step-2b section of the roadmap.

**Architecture:** Pure instrumentation — add the attribute above each trait-impl method (and the one natural span boundary in the snapshot-commit machinery). No behavior change; spans emit at `debug` and are inert without a subscriber. Then a docs-only roadmap correction marking the already-delivered 2b items done.

**Tech Stack:** Rust 2024, `tracing`, buck2, sqlx (Postgres), the in-memory fake.

**Spec:** `docs/superpowers/specs/2026-06-14-control-plane-tracing-pass-design.md`

---

## Before you start

- **Activate the dev shell:** `eval "$(./tools/env.sh)"`. Everything runs locally (no `--prefer-remote`).
- **Never pipe `buck2 test` through `tail`** — redirect and grep.
- The attribute uses the full path `#[tracing::instrument(...)]`, so no `use tracing;` is needed (`tracing` is already a dep of both adapter crates; `acl`/`queue`/`lineage::emit` already use it this way).
- Convention (copy exactly): `#[tracing::instrument(skip(self), level = "debug")]` directly above the method's `async fn` line, at the method's indentation (8 spaces inside an `impl`).
- Work on branch `feat/control-plane-tracing-pass` (already created; the spec commit is there).
- Git identity: `git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg`.

## File structure (what changes and why)

- `src/control-plane/postgres/src/catalog.rs` — instrument the 5 `&self` methods.
- `src/control-plane/postgres/src/snapshot.rs` — instrument `commit_snapshot` (the commit boundary) only; leaf helpers stay uninstrumented (too granular — they'd nest a span per file/column).
- `src/control-plane/postgres/src/lineage.rs` — instrument the 5 methods that lack it (`emit` already has one).
- `src/control-plane/memory/src/catalog.rs` — instrument the 4 trait methods.
- `src/control-plane/memory/src/lineage.rs` — instrument the 3 methods that lack it (`emit` already has one).
- `docs/superpowers/specs/2026-06-06-loom-roadmap.md` — 2b checklist + "Where we are" correction.

> **Note on `snapshot.rs` (deviation from the spec's "9 methods" count, allowed by its convention clause):** that file has **no trait methods** — it's the Tx-commit machinery (`commit_snapshot` + private leaf helpers `lock_catalog`/`read_head`/`resolve_table_id`/`table_exists`/`resolve_schema_id`/`write_table`/`resolve_column_id`/`write_data_file`). Instrument only `commit_snapshot`, the natural span boundary; instrumenting every leaf would produce a deep nested span tree per commit.

---

## Task 1: Tracing on the Postgres adapter

**Files:**
- Modify: `src/control-plane/postgres/src/catalog.rs`
- Modify: `src/control-plane/postgres/src/snapshot.rs`
- Modify: `src/control-plane/postgres/src/lineage.rs`

- [ ] **Step 1: Instrument `catalog.rs`**

Add `#[tracing::instrument(skip(self), level = "debug")]` directly above each of these 5 method signatures in `src/control-plane/postgres/src/catalog.rs` (match the existing 8-space indentation):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn files(
        &self,
        table: &TableRef,
        at: SnapshotId,
        _page: PageReq,
    ) -> Result<Page<FileRef>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn resolve_table(&self, table: &TableRef, at: SnapshotId) -> Result<i64> {
```

- [ ] **Step 2: Instrument `commit_snapshot` in `snapshot.rs`**

Add this attribute directly above the `pub(crate) async fn commit_snapshot(` signature in `src/control-plane/postgres/src/snapshot.rs` (this free fn takes a `tx` and two staged vecs — skip them and record cheap length summaries instead of dumping the batches):

```rust
#[tracing::instrument(
    skip(tx, staged_tables, staged_files),
    fields(tables = staged_tables.len(), files = staged_files.len()),
    level = "debug"
)]
pub(crate) async fn commit_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    staged_tables: &[(TableRef, Vec<ColumnSpec>)],
    staged_files: &[(TableRef, Vec<DataFile>)],
) -> Result<SnapshotId> {
```

Do NOT instrument the leaf helpers (`lock_catalog`, `read_head`, `resolve_table_id`, `table_exists`, `resolve_schema_id`, `write_table`, `resolve_column_id`, `write_data_file`).

- [ ] **Step 3: Instrument the remaining `lineage.rs` methods**

In `src/control-plane/postgres/src/lineage.rs`, `emit` is already instrumented. Add `#[tracing::instrument(skip(self), level = "debug")]` above each of these 5 (leave the `pub(crate) async fn pg_emit` free helper alone):

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, _page: PageReq) -> Result<Page<LineageEvent>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn upstream(&self, dataset: &DatasetRef, _page: PageReq) -> Result<Page<DatasetRef>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(&self, dataset: &DatasetRef, _page: PageReq) -> Result<Page<DatasetRef>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn event_datasets(&self, event_id: i64, direction: &str) -> Result<Vec<DatasetRef>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn graph_step(
```

- [ ] **Step 4: Build + clippy the Postgres adapter**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
buck2 build //src/control-plane/postgres:postgres > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log
buck2 build '//src/control-plane/postgres:postgres[clippy.txt]' > /tmp/c.log 2>&1
out=$(buck2 build --show-output '//src/control-plane/postgres:postgres[clippy.txt]' 2>/dev/null | awk '{print $2}')
[ -s "$out" ] && { echo "--- clippy findings ---"; cat "$out"; } || echo "clippy clean"
```
Expected: BUILD SUCCEEDED; clippy clean (empty `[clippy.txt]`).

- [ ] **Step 5: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/control-plane/postgres/src/catalog.rs src/control-plane/postgres/src/snapshot.rs src/control-plane/postgres/src/lineage.rs
git commit -m "feat(control-plane): tracing spans on the postgres catalog/snapshot/lineage methods"
```

---

## Task 2: Tracing on the memory adapter

**Files:**
- Modify: `src/control-plane/memory/src/catalog.rs`
- Modify: `src/control-plane/memory/src/lineage.rs`

- [ ] **Step 1: Instrument `catalog.rs`**

Add `#[tracing::instrument(skip(self), level = "debug")]` above each of these 4 trait methods in `src/control-plane/memory/src/catalog.rs`:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn current_snapshot(&self, table: &TableRef) -> Result<Snapshot> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn snapshots(&self, table: &TableRef, _page: PageReq) -> Result<Page<Snapshot>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn files(
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn schema(&self, table: &TableRef, at: SnapshotId) -> Result<TableSchema> {
```

- [ ] **Step 2: Instrument the remaining `lineage.rs` methods**

In `src/control-plane/memory/src/lineage.rs`, `emit` is already instrumented. Add `#[tracing::instrument(skip(self), level = "debug")]` above each of these 3:

```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn events_for(&self, run: &RunId, _page: PageReq) -> Result<Page<LineageEvent>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn upstream(&self, dataset: &DatasetRef, _page: PageReq) -> Result<Page<DatasetRef>> {
```
```rust
    #[tracing::instrument(skip(self), level = "debug")]
    async fn downstream(&self, dataset: &DatasetRef, _page: PageReq) -> Result<Page<DatasetRef>> {
```

- [ ] **Step 3: Build + clippy the memory adapter**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
buck2 build //src/control-plane/memory:memory > /tmp/b.log 2>&1; grep -E "BUILD SUCCEEDED|error" /tmp/b.log
out=$(buck2 build --show-output '//src/control-plane/memory:memory[clippy.txt]' 2>/dev/null | awk '{print $2}')
[ -s "$out" ] && { echo "--- clippy findings ---"; cat "$out"; } || echo "clippy clean"
```
Expected: BUILD SUCCEEDED; clippy clean.

- [ ] **Step 4: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add src/control-plane/memory/src/catalog.rs src/control-plane/memory/src/lineage.rs
git commit -m "feat(control-plane): tracing spans on the memory catalog/lineage methods"
```

---

## Task 3: Roadmap correction + full verification

**Files:**
- Modify: `docs/superpowers/specs/2026-06-06-loom-roadmap.md`

- [ ] **Step 1: Correct the Step-2b checklist**

In `docs/superpowers/specs/2026-06-06-loom-roadmap.md`, find the **### 2b — Trailing hardening** section (a bullet list of items). It currently lists items as if pending. Rewrite that list so each item shows its true status, marking the delivered ones ✅. Replace the bullet list under `### 2b — Trailing hardening` with:

```
- ✅ `SKIP LOCKED` concurrency test (N workers / M jobs, each claimed once) —
  `queue_concurrency_contract`, run by both adapters.
- ✅ Handler-panic policy in `Worker` (`catch_unwind` → `fail(.., Abandon)`) + test.
- ✅ Split the adapter monoliths per concern — `postgres/src/{queue,catalog,ontology,acl,
  lineage,snapshot,transaction}.rs` and the `memory` equivalents already mirror `core`.
- ✅ Pagination/cursor convention on list reads (PR #22).
- ✅ `tracing` spans around the adapter SQL — every concern's trait methods now carry a
  `#[tracing::instrument]` span (`acl`/`queue` from the start; `catalog`/`snapshot`/`lineage`
  via `2026-06-14-control-plane-tracing-pass-design.md`).
- ✅ `.sqlx` offline metadata so pg queries are compile-time-checked.
- ✅ proptest/arbitrary round-trips for `RowFilter` and the lineage envelope (PR #22).
- ✅ `ControlPlaneError::{Conflict, Unauthorized}` resolved: `Conflict` is **wired** (both ACL
  adapters raise it on a uniqueness race, asserted in the contract); `Unauthorized` is **kept
  as a documented reservation** for the Step-3 service auth layer.
```

- [ ] **Step 2: Update "Where we are"**

In the bottom **"Where we are"** section of the same file, append a sentence noting that **Step 2b is now closed** (this tracing pass was its last open item), so Step 2 is complete and Step 3 (services) is the sole active track — the queue-driven **Transform worker** (the one remaining unbuilt service pillar) and continuing richer reads (derived properties / multi-hop) are the candidate next slices. Match the document's voice; do not duplicate the deferral note already present.

- [ ] **Step 3: Full first-party build + test + lint**

Run:
```bash
eval "$(./tools/env.sh)" 2>/dev/null
rm -f /dev/shm/PostgreSQL.* 2>/dev/null   # clear any leaked fixture shm segments first
buck2 build //src/... > /tmp/build.log 2>&1; echo "build=$?"; grep -E "BUILD SUCCEEDED|error" /tmp/build.log | tail -2
buck2 test //src/... > /tmp/test.log 2>&1; echo "test=$?"; grep -E "Tests finished|FAIL" /tmp/test.log
./tools/clippy-all.sh > /tmp/clippy.log 2>&1; echo "clippy=$?"; grep -iE "warning|error" /tmp/clippy.log | head
buck2 run //tools:prek -- run --all-files > /tmp/prek.log 2>&1; echo "prek=$?"; tail -6 /tmp/prek.log
```
Expected: build SUCCEEDED; `Tests finished: … Fail 0.` (no behavior changed, so every existing test still passes); clippy exit 0, no warnings; prek passes. If a fixture test fails with `initdb`/`No space left on device`, re-clear `/dev/shm/PostgreSQL.*` and re-run that target — it's a leaked-segment env flake, not a code failure. If prek fixed any markdown in place, `git add` it.

- [ ] **Step 4: Commit**

```bash
git config user.email jackomayo@gmail.com; git config user.name rsJames-ttrpg
git add docs/superpowers/specs/2026-06-06-loom-roadmap.md
git commit -m "docs(roadmap): close out Step 2b (tracing pass; concurrency/panic/split already shipped)"
```

---

## Self-review notes (for the executor)

- **Spec coverage:** Task 1 + Task 2 = the tracing pass (A) across `catalog`/`snapshot`/`lineage`
  on both adapters; Task 3 = the roadmap correction (C) and records the error-variant audit (B).
  The "no new test logic / no behavior change" posture holds — the only verification is the
  existing suite + clippy staying green.
- **Deliberate deviation:** `snapshot.rs` gets one span (`commit_snapshot`), not nine — per the
  spec's "natural span boundary, not every leaf helper" convention (documented above).
- **Consistency:** every added attribute is the identical `#[tracing::instrument(skip(self),
  level = "debug")]` except `commit_snapshot` (a free fn → `skip(tx, staged_tables,
  staged_files)` + length summary fields), matching the pre-existing `acl`/`queue`/`emit` usage.
- **No push/PR step here** — the controller runs a final review, then finishes the branch
  (push + PR) per its convention.
