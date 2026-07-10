# Stream Continuous / Standing Queries (Slice 4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialized views as standing micro-batch queries: a registered `TransformBody::MicroBatch` def re-runs its SQL over the source stream's delta since the last committed per-bucket offset watermark and commits the result as its own declared log stream table — offset-framed, flush-armed, trigger-firing (composable), and structurally subscribable — with the watermark CAS-advanced in the same transaction as the output commit (exactly-once effect).

**Architecture:** Spec `docs/superpowers/specs/2026-07-09-stream-continuous-design.md`. Registration + triggering reuse the transforms concern (new body kind, data triggers / schedule / manual all work). New state: `stream.mv_watermark` keyed by the output identifier (`mv_key`) + source table id + bucket. New wire: an `MvDeltaTicket` Flight read (engine-resolved watermark, framed files∪inline delta under the flush advisory lock) and one `EngineControl::CommitMicroBatch` RPC (inline-land output with `StreamDecl::Log`, watermark CAS, `mark_run_succeeded`, one tx). New worker handler `handle_stream_mv` (kind `stream_mv`): fetch delta → derive per-bucket `(from, to)` from framing → strip framing → DataFusion SQL → IPC → commit.

**Tech Stack:** Rust, sqlx (runtime `AssertSqlSafe` for all new SQL), DataFusion, Arrow Flight + tonic (proto codegen is the `:pb-gen` buck genrule — editing the proto is sufficient), buck2 `loom_rust_test`/`loom_fixture_test`, Postgres migrations.

## Global Constraints

Carried from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`) with BUCK targets mirroring the named sibling given per task. The `no-inline-tests` prek hook fails on any inline `#[test]`.
- **No compile-time sqlx for new SQL.** All new watermark queries are runtime `sqlx::query(AssertSqlSafe(...))` / `query_scalar`, mirroring `version_for_table` (`postgres/src/ontology.rs`) — cloud sessions cannot run `tools/sqlx-prepare.sh`. Do not modify existing `query!` sites (no `.sqlx` churn anywhere in this plan).
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo` in production code; `#[expect(lint, reason = "...")]` for justified exceptions. Test code is exempt via the test macros.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (stage new files with `git add` FIRST — prek skips untracked files). Markdown: one trailing newline, no trailing whitespace.
- **The worker stays zero-pool.** No postgres dep may enter `src/services/worker/BUCK`'s lib closure; all new worker I/O crosses the wire.
- **Non-regression:** all existing callers of widened functions pass the neutral value (`None`); existing `Physical`/`Typed` wire shapes, ticket routing, and inline-append behavior stay byte-identical. Existing suites (`transform-e2e`, `data-triggers`, `stream-*`) stay green.
- **Build/test commands:** build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>`. Cloud: `-M none` on builds, scope tests to touched targets, `buck2 clean` between heavy phases. Full local suite needs `-j 8` (PG boot-slot starvation).

---

## File Structure

**Create:**
- `src/control-plane/core/src/stream_mv_job.rs` — `STREAM_MV_JOB_KIND` + `StreamMvJob`.
- `src/control-plane/core/tests/stream_mv_core.rs` — core types unit tests.
- `src/control-plane/postgres/migrations/0042_mv_watermark.sql` — `stream.mv_watermark`.
- `src/control-plane/memory/tests/mv_watermarks.rs` + `src/control-plane/postgres/tests/mv_watermarks.rs` — testkit contract runners.
- `src/control-plane/postgres/tests/stream_mv_triggers.rs` — trigger/debounce/composability/cycle fixture test.
- `src/services/engine-serving/src/mv_delta.rs` — `mv_delta_scan`.
- `src/services/engine/tests/mv_delta.rs` — delta scan over the wire.
- `src/services/engine/tests/mv_commit_wire.rs` — `CommitMicroBatch` atomicity.
- `src/services/worker/src/stream_mv.rs` — `StreamMvCtx` + `handle_stream_mv`.
- `src/services/worker/tests/stream_mv_e2e.rs` — the convergence e2e.

**Modify (production):**
- `src/control-plane/core/src/transforms.rs` — `TransformBody::MicroBatch`; `to_job` arm; `TriggerNode::resolve` arm; `validate_transform_def` checks.
- `src/control-plane/core/src/stream.rs` — `MvWatermarks` trait, `WatermarkAdvance`, `mv_key`.
- `src/control-plane/core/src/queue.rs` — `KNOWN_JOB_KINDS` gains `STREAM_MV_JOB_KIND`.
- `src/control-plane/core/src/lib.rs` — module + re-exports.
- `src/control-plane/memory/src/stream.rs` (+ `lib.rs` state field) — memory `MvWatermarks`.
- `src/control-plane/postgres/src/stream.rs` — `pg_mv_watermarks` / `pg_advance_mv_watermark` + `MvWatermarks for PgControlPlane`.
- `src/control-plane/postgres/src/iceberg_inline.rs` — `MvCommit`; `inline_append_decl` widened with `mv: Option<&MvCommit>`; `inline_append_mv`.
- `src/control-plane/testkit/src/lib.rs` — `mv_watermarks_contract`.
- `src/services/engine-wire/proto/engine_control.proto` — `CommitMicroBatch` RPC + messages.
- `src/services/engine-wire/src/flight.rs` — `MvDeltaTicket`, `EngineTicket::MvDelta`, `FlightTableClient::fetch_mv_delta`.
- `src/services/engine-wire/src/client.rs` — `GrpcQueueClient::commit_micro_batch`.
- `src/services/engine-serving/src/lib.rs` — export `mv_delta`.
- `src/services/engine/src/flight.rs` — `do_get_mv_delta` dispatch arm.
- `src/services/engine/src/service.rs` — `commit_micro_batch` handler.
- `src/services/worker/src/main.rs` — kind + dispatch + `StreamMvCtx` construction.
- `src/services/worker/src/transform.rs` — `mark_running_if_tracked` / `report_run_failure` made `pub(crate)`.
- `docs/system-capabilities/stream.md` + `transform.md` — capability documentation (Task 7).

**Modify (mechanical, compiler-guided):** every `match` over `TransformBody` gains a `MicroBatch` arm (adapters' body encode/decode, admin doc-strings); ~3 `inline_append_decl` call sites pass `None`.

---

## Task 1: Core types — `MicroBatch` body, `StreamMvJob`, watermark types, trigger/validation arms

**Files:**
- Modify: `src/control-plane/core/src/transforms.rs:40-57` (body enum), `:64-104` (`to_job`), `:261-276` (`validate_transform_def`), `:295-318` (`TriggerNode::resolve`)
- Modify: `src/control-plane/core/src/stream.rs` (append trait + types), `src/control-plane/core/src/queue.rs:31-39`, `src/control-plane/core/src/lib.rs`
- Create: `src/control-plane/core/src/stream_mv_job.rs`
- Test: `src/control-plane/core/tests/stream_mv_core.rs` (new) + `src/control-plane/core/BUCK` target

**Interfaces:**
- Consumes: `TableRef`, `NewJob`, existing enum/fn shapes cited above.
- Produces (later tasks rely on these EXACT names): `TransformBody::MicroBatch { source: TableRef, output: TableRef, buckets: i32, sql: String }` (tag `"microbatch"`); `STREAM_MV_JOB_KIND = "stream_mv"`; `StreamMvJob { source, output, buckets, sql, run_id }`; `MvWatermarks` trait; `WatermarkAdvance { bucket: i32, from: i64, to: i64 }`; `mv_key(&TableRef) -> String`; all re-exported at the core root.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/stream_mv_core.rs`:

```rust
//! Core types for the micro-batch MV slice: the MicroBatch transform body, the
//! stream_mv job payload, the watermark types, and the trigger/validation arms.
//! External rust_test (no inline #[cfg(test)]) — see CLAUDE.md.

use std::collections::HashMap;

use control_plane_core::{
    KNOWN_JOB_KINDS, STREAM_MV_JOB_KIND, StreamMvJob, TableRef, TransformBody, TransformDef,
    TransformName, TriggerNode, mv_key, validate_transform_def,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}

fn mv_body() -> TransformBody {
    TransformBody::MicroBatch {
        source: tref("s", "events"),
        output: tref("s", "events_doubled"),
        buckets: 2,
        sql: "select id, val * 2 as dbl from events".into(),
    }
}

#[test]
fn microbatch_body_serde_round_trips_with_tag() {
    let json = serde_json::to_value(mv_body()).expect("serialize");
    assert_eq!(json["kind"], "microbatch", "serde tag");
    let back: TransformBody = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, mv_body());
}

#[test]
fn to_job_emits_stream_mv_kind_with_frozen_payload() {
    let rid = uuid::Uuid::new_v4();
    let job = mv_body().to_job(rid);
    assert_eq!(job.kind, STREAM_MV_JOB_KIND);
    let payload: StreamMvJob = serde_json::from_value(job.payload).expect("payload decodes");
    assert_eq!(payload.source, tref("s", "events"));
    assert_eq!(payload.output, tref("s", "events_doubled"));
    assert_eq!(payload.buckets, 2);
    assert_eq!(payload.run_id, Some(rid));
    assert!(KNOWN_JOB_KINDS.contains(&STREAM_MV_JOB_KIND), "dispatchable kind");
}

#[test]
fn trigger_node_resolves_microbatch_io() {
    let node = TriggerNode::resolve(&TransformName("mv".into()), &mv_body(), &HashMap::new());
    assert_eq!(node.inputs, vec![tref("s", "events")]);
    assert_eq!(node.output, Some(tref("s", "events_doubled")));
}

#[test]
fn validate_rejects_degenerate_microbatch_defs() {
    let def = |body| TransformDef {
        name: TransformName("mv".into()),
        body,
        schedule: None,
        on_input_commit: true,
    };
    let bad_buckets = TransformBody::MicroBatch {
        source: tref("s", "a"), output: tref("s", "b"), buckets: 0, sql: "select 1".into(),
    };
    assert!(validate_transform_def(&def(bad_buckets)).is_err(), "buckets < 1");
    let empty_sql = TransformBody::MicroBatch {
        source: tref("s", "a"), output: tref("s", "b"), buckets: 1, sql: "  ".into(),
    };
    assert!(validate_transform_def(&def(empty_sql)).is_err(), "empty sql");
    let self_loop = TransformBody::MicroBatch {
        source: tref("s", "a"), output: tref("s", "a"), buckets: 1, sql: "select 1".into(),
    };
    assert!(validate_transform_def(&def(self_loop)).is_err(), "source == output");
    assert!(validate_transform_def(&def(mv_body())).is_ok(), "well-formed accepted");
}

#[test]
fn mv_key_is_the_qualified_output_name() {
    assert_eq!(mv_key(&tref("s", "events_doubled")), "s.events_doubled");
}
```

- [ ] **Step 2: Wire the test target**

In `src/control-plane/core/BUCK`, add a `rust_test` target `stream-mv-core` with `srcs = ["tests/stream_mv_core.rs"]`, mirroring the `version-property` target (deps `":core"`, `//third-party:serde_json`, `//third-party:uuid`).

- [ ] **Step 3: Run to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:stream-mv-core`
Expected: FAIL (compile — `MicroBatch`/`StreamMvJob`/`mv_key` missing).

- [ ] **Step 4: Implement the core types**

Create `src/control-plane/core/src/stream_mv_job.rs` (mirror `stream_consolidate_job.rs`):

```rust
//! The micro-batch MV job contract, shared by producers (the transforms concern's
//! `to_job`, data triggers) and the consumer (the zero-pool worker). Mirrors
//! `transform_job.rs`.

use crate::TableRef;

/// The queue `kind` for a micro-batch materialized-view job.
pub const STREAM_MV_JOB_KIND: &str = "stream_mv";

/// Payload of a `"stream_mv"` job: one micro-batch of a standing query — run
/// `sql` over `source`'s delta since the committed watermark and commit the
/// result to `output` (a declared log stream table with `buckets` buckets).
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct StreamMvJob {
    pub source: TableRef,
    pub output: TableRef,
    pub buckets: i32,
    pub sql: String,
    /// The `TransformRun` this job executes (and the lineage `run_id`), when
    /// tracked. Absent on direct enqueues.
    #[serde(default)]
    pub run_id: Option<uuid::Uuid>,
}
```

In `src/control-plane/core/src/transforms.rs`:

- Add the enum variant (after `Typed`):

```rust
    /// A standing micro-batch query (a materialized view): `sql` re-runs over
    /// `source`'s per-bucket offset delta on each trigger; the result appends to
    /// `output`, itself declared a log stream table with `buckets` buckets. The
    /// watermark is keyed by the OUTPUT (`mv_key`), not this def's name.
    #[serde(rename = "microbatch")]
    MicroBatch {
        source: TableRef,
        output: TableRef,
        buckets: i32,
        sql: String,
    },
```

(The enum's `rename_all = "lowercase"` would produce `"microbatch"` anyway; the explicit rename pins the wire token.)

- `to_job` gains the arm (same shape as the others):

```rust
            Self::MicroBatch { source, output, buckets, sql } => (
                crate::STREAM_MV_JOB_KIND,
                serde_json::to_value(crate::StreamMvJob {
                    source: source.clone(),
                    output: output.clone(),
                    buckets: *buckets,
                    sql: sql.clone(),
                    run_id: Some(run_id),
                }),
            ),
```

- `TriggerNode::resolve` gains:

```rust
            TransformBody::MicroBatch { source, output, .. } => {
                (vec![source.clone()], Some(output.clone()))
            }
```

- `validate_transform_def` gains, after the schedule check:

```rust
    if let TransformBody::MicroBatch { source, output, buckets, sql } = &def.body {
        if *buckets < 1 {
            return Err(ControlPlaneError::Validation(format!(
                "microbatch output bucket count must be >= 1, got {buckets}"
            )));
        }
        if sql.trim().is_empty() {
            return Err(ControlPlaneError::Validation(
                "microbatch sql must not be empty".into(),
            ));
        }
        if source == output {
            return Err(ControlPlaneError::Validation(
                "microbatch source and output must differ".into(),
            ));
        }
    }
```

In `src/control-plane/core/src/stream.rs`, append:

```rust
/// The canonical watermark key for a standing query: the qualified name of the
/// table it materializes. Keyed by the OUTPUT (not the def name) so the
/// watermark survives a def rename exactly when the output — and therefore
/// resuming — is kept, and ad-hoc (nameless) runs need no special case.
#[must_use]
pub fn mv_key(output: &crate::TableRef) -> String {
    format!("{}.{}", output.schema, output.name)
}

/// One bucket's watermark CAS: advance `bucket` from `from` to `to`. `from` is
/// the offset the delta was read at (0 = no row yet); a mismatch means a
/// concurrent run already covered the delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkAdvance {
    pub bucket: i32,
    pub from: i64,
    pub to: i64,
}

/// Per-standing-query offset watermarks: the next-unprocessed `loom_offset`
/// per `(mv, source_table_id, bucket)`. Advancing is a CAS — `Conflict` on a
/// stale `from` — and is issued inside the output-commit transaction by the
/// postgres adapter, so the watermark moves iff the output lands.
#[async_trait]
pub trait MvWatermarks {
    /// The recorded watermarks for `(mv, source_table_id)` — buckets with no
    /// row are absent (read as 0).
    async fn mv_watermarks(
        &self,
        mv: &str,
        source_table_id: i64,
    ) -> Result<std::collections::BTreeMap<i32, i64>>;
    /// CAS-advance each bucket; any stale `from` is `Conflict` (all-or-nothing
    /// is the postgres tx's job — this standalone surface applies in order and
    /// stops at the first conflict).
    async fn advance_mv_watermark(
        &self,
        mv: &str,
        source_table_id: i64,
        advances: &[WatermarkAdvance],
    ) -> Result<()>;
}
```

In `src/control-plane/core/src/queue.rs:31`, add `crate::STREAM_MV_JOB_KIND,` to `KNOWN_JOB_KINDS`. In `lib.rs`: `mod stream_mv_job;`, `pub use stream_mv_job::{STREAM_MV_JOB_KIND, StreamMvJob};`, and extend the stream re-export with `MvWatermarks, WatermarkAdvance, mv_key`.

- [ ] **Step 5: Compiler-guided sweep of `TransformBody` matches**

`buck2 build -v0 --console none //src/...` — every non-exhaustive `match` over `TransformBody` now errors (adapters' body encode/decode in `postgres/src/transforms.rs` + `memory/src/transforms.rs`, any UI/admin mapping). For serde-driven encode/decode sites no change is needed (the enum serializes itself); for explicit matches add a `MicroBatch` arm mirroring `Physical` (its io is already physical `TableRef`s — no ontology resolution). Read each site; do not blind-arm with `_ =>`.

**EXCEPTION — the define-time refuse-guard site (`define_transform`, `postgres/src/transforms.rs`, landed after this plan by #416).** The `output_table` match feeding `pg_refuse_stream_target` must NOT group `MicroBatch` with `Physical`. A MicroBatch MV legitimately owns its declared log-stream output and writes to it via the sanctioned `CommitMicroBatch` path, so it is **exempt** from the legacy-write refuse guard — give it its own arm returning `None`:

```rust
    TransformBody::Physical { output, .. } => Some(output.clone()),
    TransformBody::MicroBatch { .. } => None, // exempt: MV owns its stream output via CommitMicroBatch
    TransformBody::Typed { output, .. } => { /* resolve via pg_type_tables */ }
```

Without the exemption, re-defining a running MV (whose output is a declared stream after the first commit; the insert is `on conflict do update`) would be refused. Task 6 adds a regression test for this. (Human-confirmed decision, 2026-07-10.)

- [ ] **Step 6: Run the test + whole-tree build**

Run: `buck2 test --console none //src/control-plane/core:stream-mv-core` — PASS.
Run: `buck2 build -v0 --console none //src/...` — exit 0.

- [ ] **Step 7: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): MicroBatch transform body + stream_mv job + watermark core types

The standing-query registration surface (a third TransformBody kind riding
the existing define/trigger/run machinery), its queue job contract, and the
MvWatermarks concern types. No behavior yet — no adapter stores watermarks
and no worker handles the kind."
```

---

## Task 2: The watermark store — migration, postgres + memory adapters, testkit contract

**Files:**
- Create: `src/control-plane/postgres/migrations/0042_mv_watermark.sql`
- Modify: `src/control-plane/postgres/src/stream.rs` (helpers + impl), `src/control-plane/memory/src/stream.rs` + `src/control-plane/memory/src/lib.rs` (state field), `src/control-plane/testkit/src/lib.rs` (contract)
- Test: `src/control-plane/memory/tests/mv_watermarks.rs` + `src/control-plane/postgres/tests/mv_watermarks.rs` + BUCK targets

**Interfaces:**
- Consumes: `MvWatermarks`/`WatermarkAdvance` (T1).
- Produces: `stream.mv_watermark`; `pub async fn pg_mv_watermarks<'e, E: sqlx::PgExecutor<'e>>(ex: E, mv: &str, source_table_id: i64) -> Result<BTreeMap<i32, i64>>`; `pub async fn pg_advance_mv_watermark<'e, E: sqlx::PgExecutor<'e>>(ex: E, mv: &str, source_table_id: i64, adv: &WatermarkAdvance) -> Result<()>` (single-bucket CAS, `Conflict` on miss; `pub` — engine-serving and the commit tx call them); both adapters implement `MvWatermarks`; testkit `mv_watermarks_contract`.

- [ ] **Step 1: Write the failing contract + runners**

In `src/control-plane/testkit/src/lib.rs`, add (mirroring `stream_tables_contract`'s style):

```rust
/// Contract for the MvWatermarks concern: absent reads empty; from=0 inserts;
/// CAS advances; a stale `from` is Conflict and leaves the row untouched; keys
/// (mv, source_table_id, bucket) are independent.
pub async fn mv_watermarks_contract(cp: &(impl control_plane_core::MvWatermarks + Sync)) {
    use control_plane_core::{ControlPlaneError, WatermarkAdvance};
    // Absent: empty map.
    let wm = cp.mv_watermarks("s.out", 1).await.expect("read");
    assert!(wm.is_empty(), "no rows yet");
    // from=0 inserts.
    cp.advance_mv_watermark("s.out", 1, &[WatermarkAdvance { bucket: 0, from: 0, to: 5 }])
        .await
        .expect("bootstrap advance");
    assert_eq!(cp.mv_watermarks("s.out", 1).await.expect("read").get(&0), Some(&5));
    // CAS advance.
    cp.advance_mv_watermark("s.out", 1, &[WatermarkAdvance { bucket: 0, from: 5, to: 9 }])
        .await
        .expect("cas advance");
    // Stale from: Conflict, value untouched.
    let stale = cp
        .advance_mv_watermark("s.out", 1, &[WatermarkAdvance { bucket: 0, from: 5, to: 12 }])
        .await;
    assert!(matches!(stale, Err(ControlPlaneError::Conflict(_))), "stale CAS conflicts");
    assert_eq!(cp.mv_watermarks("s.out", 1).await.expect("read").get(&0), Some(&9));
    // A from>0 advance on an ABSENT bucket is also a Conflict (never a skip-insert).
    let absent = cp
        .advance_mv_watermark("s.out", 1, &[WatermarkAdvance { bucket: 3, from: 4, to: 6 }])
        .await;
    assert!(matches!(absent, Err(ControlPlaneError::Conflict(_))), "absent row + from>0 conflicts");
    // Independence: other mv key / source table / bucket unaffected.
    cp.advance_mv_watermark("s.other", 1, &[WatermarkAdvance { bucket: 0, from: 0, to: 2 }])
        .await
        .expect("other mv");
    cp.advance_mv_watermark("s.out", 2, &[WatermarkAdvance { bucket: 0, from: 0, to: 3 }])
        .await
        .expect("other source");
    cp.advance_mv_watermark("s.out", 1, &[WatermarkAdvance { bucket: 1, from: 0, to: 7 }])
        .await
        .expect("other bucket");
    let wm = cp.mv_watermarks("s.out", 1).await.expect("read");
    assert_eq!(wm.get(&0), Some(&9));
    assert_eq!(wm.get(&1), Some(&7));
}
```

Create `src/control-plane/memory/tests/mv_watermarks.rs` and `src/control-plane/postgres/tests/mv_watermarks.rs` mirroring the crates' existing `stream_tables.rs` contract runners (memory: plain `rust_test` over `MemoryControlPlane::default()`; postgres: `loom_fixture_test` over `PgFixture::shared().fresh_db()`), each calling `control_plane_testkit::mv_watermarks_contract(&cp).await`. Wire BUCK targets `mv-watermarks` mirroring each crate's `stream-tables` target.

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test --console none //src/control-plane/memory:mv-watermarks //src/control-plane/postgres:mv-watermarks`
Expected: FAIL (compile — no impls).

- [ ] **Step 3: Migration**

Create `src/control-plane/postgres/migrations/0042_mv_watermark.sql`:

```sql
-- Per-standing-query offset watermarks (road-stream-continuous): the next
-- unprocessed loom_offset per (mv, source_table_id, bucket). `mv` is the
-- OUTPUT's qualified name (core::mv_key) — the watermark follows the
-- materialization, not the def name. Advanced by CAS inside the micro-batch
-- output-commit transaction: the watermark moves iff the output lands.
create table stream.mv_watermark (
    mv              text   not null,
    source_table_id bigint not null,
    bucket          int    not null,
    next_offset     bigint not null check (next_offset >= 0),
    primary key (mv, source_table_id, bucket)
);
```

- [ ] **Step 4: Postgres helpers + impl**

In `src/control-plane/postgres/src/stream.rs` (all `AssertSqlSafe` — see Global Constraints):

```rust
/// The recorded watermarks for `(mv, source_table_id)`. Executor-generic so the
/// delta scan (pool) and any tx caller share it. Buckets with no row are absent.
pub async fn pg_mv_watermarks<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    mv: &str,
    source_table_id: i64,
) -> Result<std::collections::BTreeMap<i32, i64>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(
        "select bucket, next_offset from stream.mv_watermark \
         where mv = $1 and source_table_id = $2",
    ))
    .bind(mv)
    .bind(source_table_id)
    .fetch_all(ex)
    .await
    .map_err(backend)?;
    Ok(rows.into_iter().collect())
}

/// CAS-advance one bucket's watermark. `from == 0` may insert (bootstrap);
/// `from > 0` only updates an existing row at exactly `from`. Zero rows
/// affected => `Conflict` — inside a transaction the caller's rollback then
/// discards the whole output commit (the exactly-once mechanism).
pub async fn pg_advance_mv_watermark<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    mv: &str,
    source_table_id: i64,
    adv: &control_plane_core::WatermarkAdvance,
) -> Result<()> {
    let sql = if adv.from == 0 {
        "insert into stream.mv_watermark (mv, source_table_id, bucket, next_offset) \
         values ($1, $2, $3, $4) \
         on conflict (mv, source_table_id, bucket) do update set next_offset = $4 \
         where stream.mv_watermark.next_offset = 0"
    } else {
        "update stream.mv_watermark set next_offset = $4 \
         where mv = $1 and source_table_id = $2 and bucket = $3 and next_offset = $5"
    };
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(mv)
        .bind(source_table_id)
        .bind(adv.bucket)
        .bind(adv.to);
    if adv.from != 0 {
        q = q.bind(adv.from);
    }
    let done = q.execute(ex).await.map_err(backend)?;
    if done.rows_affected() == 0 {
        return Err(ControlPlaneError::Conflict(format!(
            "mv watermark advanced concurrently: {mv} source {source_table_id} \
             bucket {} expected {}",
            adv.bucket, adv.from
        )));
    }
    Ok(())
}
```

(Adjust to the crate's established `query_as`/typed-row idiom if `sqlx::query_as` needs an explicit row type; mirror how `fixture.rs`/`identity_for_table` bind runtime queries.) Then implement the trait on `PgControlPlane` (methods delegate to the helpers on `self.pool()`, advancing each element of `advances` in order — the standalone surface; the atomic path is the commit tx in Task 4).

- [ ] **Step 5: Memory adapter**

Add `pub(crate) mv_watermarks: Arc<Mutex<HashMap<(String, i64, i32), i64>>>` to `MemoryControlPlane` (mirroring `offsets` — the `Arc` wrapper is REQUIRED: `MemoryControlPlane` `#[derive(Clone)]`s and shares all state via `Arc<Mutex<…>>`, so a bare `Mutex` would give cloned handles independent watermark maps and silently break shared-state semantics; initialize with `Arc::new(Mutex::new(HashMap::new()))` in the constructor beside `offsets`), and implement `MvWatermarks` with the same CAS semantics (absent = 0 only satisfies `from == 0`; mismatch → `Conflict`).

- [ ] **Step 6: Run + commit**

Run: `buck2 test --console none //src/control-plane/memory:mv-watermarks //src/control-plane/postgres:mv-watermarks` — PASS.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): stream.mv_watermark store + MvWatermarks adapters

Migration 0042 plus executor-generic pg helpers (runtime AssertSqlSafe; the
CAS is issued inside the output-commit tx in the commit slice), the memory
fake, and the cross-adapter testkit contract."
```

---

## Task 3: The framed delta read — `MvDeltaTicket`, `mv_delta_scan`, `fetch_mv_delta`

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs` (ticket + `EngineTicket` + client method)
- Create: `src/services/engine-serving/src/mv_delta.rs` (+ export in `lib.rs`)
- Modify: `src/services/engine/src/flight.rs` (dispatch arm)
- Test: `src/services/engine/tests/mv_delta.rs` (new `loom_fixture_test`) + `src/services/engine/BUCK`

**Interfaces:**
- Consumes: `pg_mv_watermarks` (T2), `mv_key` (T1); `live_table_id`, `StreamTables::stream_meta`, `IcebergCatalog::{current_snapshot, schema, files_with_stats, inline_live_batch_full}`, `read_files_as_batches` (`iceberg_read.rs:25`), `lock_table` (`iceberg_flush.rs:414`) — the `consolidate_locked` read shape (`engine-serving/src/consolidate.rs:141`); `EngineTicket::decode` (`engine-wire/src/flight.rs:206`), the engine `do_get` (`engine/src/flight.rs:240`) and `do_get_files`' response encoding (`:167`).
- Produces: `MvDeltaTicket { mv: String, schema: String, name: String }` + `EngineTicket::MvDelta(MvDeltaTicket)`; `pub async fn mv_delta_scan(cp: &PgControlPlane, catalog: &SqlCatalog, pool: &PgPool, table: &TableRef, mv: &str) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError>`; `FlightTableClient::fetch_mv_delta(&self, mv: String, schema: String, name: String) -> Result<Vec<RecordBatch>>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine/tests/mv_delta.rs` (mirror the engine fixture-test harness used by `tests/transform_wire.rs`: `PgFixture` + `local_sql_catalog` + `spawn_engine_uds`):

```rust
//! MvDeltaTicket over the wire: seed a 2-bucket log stream table with inline +
//! flushed rows, fetch the framed delta from watermark zero, advance the
//! watermark, fetch the tail only. A non-stream source is a deterministic error.
```

Legs (one `#[tokio::test]` each or sequential in one, matching the sibling's style):

1. **Full delta, files ∪ inline:** `land` 4 rows into `s.events` with `stream_buckets = Some(2)` (inline), `flush_table(pool, catalog, &src)`, `land` 2 more (inline tail). `FlightTableClient::fetch_mv_delta("s.out".into(), "s".into(), "events".into())` returns all 6 rows; the batches CARRY `loom_change_kind`/`loom_bucket`/`loom_offset`; rows are `(bucket, offset)`-ordered; per bucket the offsets are gapless from 0.
2. **Watermarked tail:** `cp.advance_mv_watermark("s.out", tid, &[...])` to the post-flush high-water per bucket (compute from leg 1's framing); re-fetch → exactly the 2 tail rows.
3. **Independent consumer:** fetch with a different `mv` key (`"s.other"`) still returns all 6 (watermarks are per-mv).
4. **Non-stream source:** land a plain batch table, fetch → `Err` (deterministic message naming the table; the worker maps it to abandon).

Wire a `loom_fixture_test` target `mv-delta` in `src/services/engine/BUCK` mirroring the target that wires `tests/transform_wire.rs`.

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test --console none //src/services/engine:mv-delta` — FAIL (compile).

- [ ] **Step 3: The ticket + client method**

In `src/services/engine-wire/src/flight.rs`:

```rust
/// A loom-native `do_get` ticket requesting a standing query's framed source
/// delta: every `(loom_bucket, loom_offset)`-ordered event of `schema.name` at
/// or beyond `mv`'s committed watermark. INTERNAL data plane: the response
/// carries the `loom_*` framing columns (the worker derives its watermark CAS
/// bounds from them, then strips them before running user SQL). The required
/// `mv` field keeps it disjoint (`deny_unknown_fields`) from every other shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvDeltaTicket {
    pub mv: String,
    pub schema: String,
    pub name: String,
}
```

with `encode`/`decode` mirroring `VectorSearchTicket`. Add `EngineTicket::MvDelta(MvDeltaTicket)` and its decode attempt after `AsOfSql`, before `VectorSearch` (comment why: required-field disjointness). Add to `FlightTableClient`:

```rust
    /// Fetch a standing query's framed source delta (see [`MvDeltaTicket`]).
    pub async fn fetch_mv_delta(
        &self,
        mv: String,
        schema: String,
        name: String,
    ) -> Result<Vec<RecordBatch>> {
        let ticket = MvDeltaTicket { mv, schema, name };
        self.do_get_batches(ticket.encode()).await
    }
```

(factor the existing `fetch` body's ticket→batches plumbing into a shared private `do_get_batches(bytes)` if not already shaped that way).

- [ ] **Step 4: `mv_delta_scan`**

Create `src/services/engine-serving/src/mv_delta.rs`, mirroring `consolidate_locked`'s read (cite lines in doc comments):

```rust
pub async fn mv_delta_scan(
    cp: &PgControlPlane,
    catalog: &SqlCatalog,
    pool: &PgPool,
    table: &TableRef,
    mv: &str,
) -> Result<(arrow_schema::SchemaRef, Vec<RecordBatch>), EngineServingError>
```

1. `live_table_id` → `None` ⇒ `EngineServingError::Engine("mv delta: unknown source table …")`. `cp.stream_meta(tid)` must be `Some` with `kind == StreamKind::Log` ⇒ else `Engine("mv delta: {schema}.{name} is not a declared log stream table (cdc sources are deferred)")`.
2. `pg_mv_watermarks(pool, mv, tid)`.
3. `lock_table(pool, table)` (the flush/GC/consolidate advisory lock — a flush moving rows files↔inline mid-read would otherwise double-read or drop them), then exactly `consolidate_locked`'s read: `current_snapshot` (a `NotFound` = never written ⇒ release the lock and return the empty schema-only result), `schema` → `user_cols`, `files_with_stats` → `read_files_as_batches`, `inline_live_batch_full`; release the lock after collection.
4. One DataFusion pass over the union (same `register_batches` + `union_sql` shape as consolidate), with:

```rust
    let preds: Vec<String> = (0..meta.bucket_count)
        .map(|b| {
            let from = wm.get(&b).copied().unwrap_or(0);
            format!("(loom_bucket = {b} and loom_offset >= {from})")
        })
        .collect();
    let sql = format!(
        "select {col_list}, loom_change_kind, loom_bucket, loom_offset \
         from ({union_sql}) d where {} order by loom_bucket, loom_offset",
        preds.join(" or ")
    );
```

collect and return `(schema, batches)` (schema from the collected df, or built from `user_cols` + `framing_column_specs` when empty — make `framing_column_specs` `pub` in `iceberg_landing.rs:631` if needed, or reconstruct the three fields locally).

Export `pub mod mv_delta;`/`pub use` from `engine-serving/src/lib.rs`.

- [ ] **Step 5: Engine dispatch**

In `src/services/engine/src/flight.rs`, add `EngineTicket::MvDelta(t) => self.do_get_mv_delta(t).await` and a `do_get_mv_delta` that calls `engine_serving::mv_delta::mv_delta_scan(&self.cp, &self.catalog, &self.pool, &table, &t.mv)` and encodes the schema-first `FlightData` stream exactly as `do_get_files` (`engine/src/flight.rs:167`) does. Map the deterministic errors to `Status::failed_precondition` (unknown/non-stream source), the rest through the existing serving-status mapping.

- [ ] **Step 6: Run + commit**

Run: `buck2 test --console none //src/services/engine:mv-delta` — PASS. Also `buck2 build -v0 --console none //src/...`.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine): MvDeltaTicket framed delta scan for standing queries

A new internal Flight do_get plane: files-union-inline framed rows of a log
stream table at-or-beyond the mv's committed per-bucket watermark, read
under the flush advisory lock and (bucket, offset)-ordered. Client side:
FlightTableClient::fetch_mv_delta."
```

---

## Task 4: The atomic commit — `CommitMicroBatch` RPC, `inline_append_mv`, watermark CAS in-tx

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto` (RPC + messages; the `:pb-gen` genrule regenerates on build), `src/services/engine-wire/src/client.rs` (`commit_micro_batch`), `src/control-plane/postgres/src/iceberg_inline.rs` (`MvCommit`, widen `inline_append_decl`, `inline_append_mv`), `src/services/engine/src/service.rs` (handler)
- Test: `src/services/engine/tests/mv_commit_wire.rs` (new `loom_fixture_test`) + `src/services/engine/BUCK`

**Interfaces:**
- Consumes: `pg_advance_mv_watermark` (T2), `pg_mark_run_succeeded` (`postgres/src/transforms.rs:241`), `inline_append_decl` + `StreamDecl::Log` (`iceberg_inline.rs:448`), `reconcile_stream_mode` guards, the `CommitTransform` handler + client as templates (`engine/src/service.rs:276`, `client.rs:410`), the `WriteObject` IPC decode/encode conventions.
- Produces: proto `MvAdvance { bucket, from, to }`, `CommitMicroBatchRequest { mv, source_schema, source_name, schema, name, buckets, ipc, columns_json, lineage_json, advances, run_id }`, `CommitMicroBatchResponse { optional int64 snapshot_id }`, `rpc CommitMicroBatch`; `GrpcQueueClient::commit_micro_batch(req…) -> Result<Option<i64>>`; `pub struct MvCommit { pub mv: String, pub source_table_id: i64, pub advances: Vec<WatermarkAdvance>, pub run_id: Option<uuid::Uuid> }` + `pub async fn inline_append_mv(pool, table, columns, batch, lineage, flush_threshold, buckets: i32, mv: &MvCommit) -> Result<SnapshotId>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine/tests/mv_commit_wire.rs` (same harness as Task 3). Legs:

1. **Atomic happy path:** seed source `s.events` (log, 2 buckets, 4 rows); submit a tracked run (`Transforms::submit_run` with a `MicroBatch`-bodied `TransformRun`, mirroring how `transform_wire.rs` seeds runs); call `client.commit_micro_batch` with 4 result rows (IPC), `buckets = 1`, advances derived per bucket `{0: (0, n0), 1: (0, n1)}`, the run id. Assert: response snapshot id present; output `s.out` is a declared log stream table (`cp.stream_meta(out_tid)` = `Log`, bucket_count 1); its inline rows carry `+I` framing with gapless offsets 0..4; `cp.mv_watermarks("s.out", src_tid)` equals the advances' `to`s; `cp.transforms().get_run(rid)` is `Succeeded` with the snapshot id.
2. **Stale CAS ⇒ aborted, nothing commits:** repeat with `from = 0` after the watermark already moved → gRPC status `aborted` (the `Conflict` mapping); output row count unchanged; watermark unchanged; run NOT marked succeeded.
3. **Empty commit:** `ipc` empty, no advances, run id set → OK, absent snapshot id, run `Succeeded` (snapshot_id 0), no output table created.
4. **Output collision guard:** commit against a PRE-EXISTING batch table as output → `Validation`-mapped status (from `reconcile_stream_mode`'s batch→stream guard), nothing written.

Wire `loom_fixture_test` target `mv-commit-wire` mirroring `mv-delta`.

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test --console none //src/services/engine:mv-commit-wire` — FAIL.

- [ ] **Step 3: Proto + client**

Append to `engine_control.proto` (rpc after `WriteDelta`, messages after `CommitTransformResponse`) exactly the shapes in the spec's Produces list. Client method mirrors `commit_transform` (`client.rs:410`): serialize `columns_json`/`lineage_json`, pass `ipc: Vec<u8>`, `advances: Vec<pb::MvAdvance>`, return `resp.snapshot_id`.

- [ ] **Step 4: `MvCommit` + `inline_append_mv`**

In `iceberg_inline.rs`:

- Widen `inline_append_decl` with a final `mv: Option<&MvCommit>` (all existing callers — `inline_append`, `land_cdc`'s call, any other — pass `None`; compiler-guided). Immediately BEFORE the jobs/trigger/commit tail of the function (after the row-insert loop and offset stamping), execute:

```rust
    if let Some(m) = mv {
        for adv in &m.advances {
            crate::stream::pg_advance_mv_watermark(&mut *conn, &m.mv, m.source_table_id, adv)
                .await?;
        }
        if let Some(rid) = m.run_id {
            crate::transforms::pg_mark_run_succeeded(&mut *conn, rid, at.0).await?;
        }
    }
```

(a `Conflict` from the CAS propagates, the tx drops, and the whole append — rows, declare, triggers, jobs — rolls back).

- Add:

```rust
/// The micro-batch extras committed atomically with an MV's output append: the
/// per-bucket watermark CAS (Conflict rolls the whole tx back — the
/// exactly-once mechanism) and the run's success mark.
pub struct MvCommit {
    pub mv: String,
    pub source_table_id: i64,
    pub advances: Vec<control_plane_core::WatermarkAdvance>,
    pub run_id: Option<uuid::Uuid>,
}

/// Land one micro-batch result: inline-append `batch` to `table` declared (or
/// confirmed) a log stream table with `buckets` buckets — framing stamped,
/// flush byte-trigger armed, data triggers fired (composability) — plus the
/// [`MvCommit`] extras, all on ONE transaction.
pub async fn inline_append_mv(
    pool: &PgPool,
    table: &TableRef,
    columns: &[ColumnSpec],
    batch: &RecordBatch,
    lineage: LineageEvent,
    flush_threshold: Option<i64>,
    buckets: i32,
    mv: &MvCommit,
) -> Result<SnapshotId> {
    inline_append_decl(
        pool, table, columns, batch, lineage, flush_threshold,
        &crate::stream::StreamDecl::Log(buckets), &[], Some(mv),
    )
    .await
}
```

- [ ] **Step 5: Engine handler**

In `engine/src/service.rs`, `commit_micro_batch` (template: `commit_transform` for decode + status mapping, `write_object` for IPC decode):

1. Decode `columns_json`, `lineage_json` (`LineageWire`), optional `run_id`.
2. **Empty-output case** (key on `ipc.is_empty()` — NOT `ipc` AND `advances` both empty). No output rows to land, so declare/land NOTHING. Two sub-cases:
   - **`advances` empty** (an empty source delta / spurious wakeup): if `run_id` present, `finish_run(rid, RunOutcome::Succeeded { snapshot_id: 0 })`; respond `snapshot_id: None`.
   - **`advances` non-empty** (a FILTERING micro-batch — it consumed a non-empty delta but its SQL produced zero output rows): resolve the source tid (absent → `invalid_argument`) and call the new `advance_mv_watermark_only(&self.pool, &MvCommit { … })` — CAS-advance the per-bucket watermark + `pg_mark_run_succeeded(_, rid, 0)` in ONE tx (a `Conflict` → `aborted`), landing/declaring no output. This is REQUIRED: the watermark MUST advance past the consumed delta or a filtering MV reprocesses it on every trigger forever. Respond `snapshot_id: None`. (Spec-gap resolution, human-flagged 2026-07-10 — see Task 5 which naturally produces this case.)
3. Resolve the source tid: `live_table_id(&mut conn, &r.source_schema, &r.source_name)` → absent is `invalid_argument` (the delta it claims to consume cannot exist).
4. Decode the IPC batch(es), concat to one `RecordBatch` (the `write_object` convention).
5. `inline_append_mv(&self.pool, &out_table, &columns, &batch, lineage, Some(self.tuning.flush_byte_threshold…), r.buckets, &MvCommit { mv: r.mv, source_table_id, advances, run_id })` — thread the flush threshold exactly as the other inline-writing handlers do.
6. Status mapping: `Conflict` → `aborted` (the CAS supersede; also covers the declare-mismatch conflicts), `Validation` → `invalid_argument`, rest per the standard mapping.

- [ ] **Step 6: Run + commit**

Run: `buck2 test --console none //src/services/engine:mv-commit-wire` — PASS. Build tree.

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine): CommitMicroBatch — MV output commit with in-tx watermark CAS

One transaction: inline-land the result declared StreamDecl::Log (framing
stamped, flush armed, data triggers fired), CAS-advance the per-bucket
source watermark (Conflict aborts everything — exactly-once effect), mark
the run succeeded. Empty micro-batches just close their run."
```

---

## Task 5: The worker handler — `handle_stream_mv` + the convergence e2e

**Files:**
- Create: `src/services/worker/src/stream_mv.rs`; modify `src/services/worker/src/lib.rs` (module), `src/services/worker/src/main.rs:84-120` (kind + ctx + dispatch), `src/services/worker/src/transform.rs:112/:213` (`pub(crate)` the two lifecycle helpers)
- Test: `src/services/worker/tests/stream_mv_e2e.rs` (new `loom_fixture_test`) + `src/services/worker/BUCK` (mirror the `transform-e2e` target at `BUCK:185`)

**Interfaces:**
- Consumes: `StreamMvJob`/`STREAM_MV_JOB_KIND` (T1), `fetch_mv_delta` (T3), `commit_micro_batch` (T4), `mv_key` (T1), `mark_running_if_tracked`/`report_run_failure` (`worker/src/transform.rs:112`/`:213`), `datafusion_io::{register_batches, infer_columns}`, `WorkerTuning::backoff`.
- Produces: `pub struct StreamMvCtx { pub control: GrpcQueueClient, pub table: FlightTableClient, pub worker_tuning: WorkerTuning }`; `pub async fn handle_stream_mv(ctx: &StreamMvCtx, job: Job) -> Result<(), JobFailure>`.

- [ ] **Step 1: Write the failing e2e**

Create `src/services/worker/tests/stream_mv_e2e.rs`, mirroring `transform_e2e.rs`'s harness (`PgFixture` + `local_sql_catalog` + `spawn_engine_uds` + the same ctx construction) with a two-column source (`id: Long`, `val: Long`):

```rust
//! Micro-batch MV convergence e2e: an MV over a 2-bucket source log stream,
//! driven across multiple micro-batches (with a flush between them), converges
//! to the batch-equivalent result; its output is a declared log stream table
//! with gapless +I framing (structurally subscribable); a re-run on an
//! unchanged source is a no-op.
```

Legs (one flow):

1. Seed `s.events` via `land(..., stream_buckets: Some(2))` with rows `(1, 10), (2, 20), (3, 30)`.
2. Build `StreamMvJob { source: s.events, output: s.doubled, buckets: 1, sql: "select id, val * 2 as dbl from events", run_id: Some(rid1) }` (run seeded via `submit_run` with a frozen `MicroBatch` body, dequeued like `transform_e2e` drives its jobs); `handle_stream_mv(&ctx, job)` → `Ok`.
3. Assert convergence #1: `FlightSqlClient::execute("select id, dbl from \"s\".\"doubled\" order by id")` equals the batch-equivalent (each source row doubled). Run 1's record is `Succeeded`.
4. `flush_table(pool, catalog, &src)` (moves the processed rows to files), then `land` two more rows `(4, 40), (5, 50)`; run micro-batch #2 (fresh run id). Assert convergence #2: output = ALL 5 doubled rows, no duplicates — the delta scan crossed the flush boundary and the watermark excluded batch #1.
5. **Structural subscribability:** via `IcebergCatalog::inline_live_batch_full` on `s.doubled` (plus its flushed files if any), assert every output row carries `loom_change_kind = "+I"` and `loom_bucket = 0` with offsets exactly `0..5` (gapless from zero); `cp.stream_meta(out_tid)` is `Some(Log)`.
6. **Idempotent re-run:** micro-batch #3 with no new source rows → `Ok`, output still 5 rows, run `Succeeded` (the empty-delta path).
7. **Filtering micro-batch (empty output, non-empty delta):** use an MV whose SQL filters (e.g. `select id, val from events where val > 1000`) and `land` new source rows that ALL fail the predicate, then run the micro-batch. Assert: `Ok`; the output table gains NO rows (and the watermark for THIS mv ADVANCED past the consumed delta — `cp.mv_watermarks(mv_key, src_tid)` moved); run `Succeeded`. Re-running with no new source rows stays a no-op (does not reprocess the filtered delta). This exercises Task 4's `ipc.is_empty() && !advances.is_empty()` empty-output branch end-to-end — the worker (steps 4/6/8) naturally sends non-empty advances with empty IPC. (Can be a distinct MV/run within this flow or a sibling test.)
8. **Deterministic abandon:** a job whose source is a plain batch table → `Err(JobFailure { policy: Abandon, .. })`.

Wire `loom_fixture_test` target `stream-mv-e2e` mirroring `transform-e2e` (`worker/BUCK:185`), deps mirroring it plus nothing new.

- [ ] **Step 2: Run to verify failure**

Run: `buck2 test --console none //src/services/worker:stream-mv-e2e` — FAIL.

- [ ] **Step 3: Implement `handle_stream_mv`**

`src/services/worker/src/stream_mv.rs`:

```rust
//! The worker's micro-batch MV handler: fetch the source's framed offset delta
//! over Flight, derive the per-bucket watermark CAS bounds from the framing
//! (gapless per-bucket offsets make min == watermark), strip the framing, run
//! the standing query's SQL in a fresh DataFusion session, and commit the
//! result + watermark advance + run success as ONE CommitMicroBatch RPC.
//! Zero Postgres — the engine owns it.
```

Flow (error taxonomy mirrors `run_wire_transform`):

1. Parse `StreamMvJob` (bad payload ⇒ abandon); `mark_running_if_tracked`.
2. `ctx.table.fetch_mv_delta(mv_key(&job.output), source.schema, source.name)` — wire errors retry with backoff; a `failed_precondition`-mapped error (unknown / non-stream source) abandons.
3. If zero rows: `ctx.control.commit_micro_batch(empty…)` (marks the run succeeded), `Ok`.
4. `framing_bounds(&batches)`: locate `loom_bucket`/`loom_offset` columns by name (missing ⇒ abandon — a malformed delta is deterministic), fold per-bucket `(min, max)` → `Vec<WatermarkAdvance { bucket, from: min, to: max + 1 }>`.
5. `strip_framing(&batches)`: project out every `loom_`-prefixed column; register the result under `source.name` in a fresh `SessionContext` (empty registration is impossible here — step 3 handled empty).
6. `df_ctx.sql(&job.sql)` (plan error ⇒ abandon); `infer_columns` on the result schema (⇒ abandon on unsupported types); `collect`; concat and encode as one Arrow IPC stream (the `write_delta` client-side convention).
7. Build the lineage event: inputs `[DatasetRef::from(&job.source)]`, outputs `[DatasetRef::from(&job.output)]`, payload `{ "sql": job.sql, "mv": mv_key(&job.output), "offsets": { bucket: { "from", "to" } } }`, `run_id` = the job's.
8. `ctx.control.commit_micro_batch(...)`: `aborted` ⇒ `JobFailure::abandon("superseded: watermark advanced concurrently (a newer run covers this delta)")`; `invalid_argument`/validation ⇒ abandon; other wire errors ⇒ retry with backoff.
9. `report_run_failure` on the way out (same shape as `handle_transform`).

In `main.rs`: add `STREAM_MV_JOB_KIND.to_string()` to the kinds vec (`main.rs:84`), build `StreamMvCtx` from the already-constructed clients, and add the dispatch arm (`main.rs:99` match). In `transform.rs`, change `mark_running_if_tracked` and `report_run_failure` to `pub(crate)`.

- [ ] **Step 4: Run + commit**

Run: `buck2 test --console none //src/services/worker:stream-mv-e2e` — PASS. Also re-run `//src/services/worker:transform-e2e` (non-regression on the shared module).

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(worker): handle_stream_mv — offset-watermarked micro-batch MV runs

Fetch the framed delta, derive per-bucket CAS bounds from the (gapless)
framing, strip loom_* columns, run the standing query's SQL in DataFusion,
and commit output + watermark + run success in one CommitMicroBatch RPC.
Convergence e2e: multi-batch MV over a flushed source equals the batch
result; output is a framed, gapless, declared log stream table."
```

---

## Task 6: Registration → trigger wiring — data-trigger firing, debounce, composability, cycle rejection

**Files:**
- Test: `src/control-plane/postgres/tests/stream_mv_triggers.rs` (new `loom_fixture_test`) + `src/control-plane/postgres/BUCK` (mirror the `data-triggers` target)
- Modify (only if a leg fails): the `MicroBatch` arms Task 1 added to the adapters' define/trigger paths — this task is primarily proof that the reused machinery covers the new body.

**Interfaces:**
- Consumes: `Transforms::define_transform`/`data_triggered_defs`/`submit_run`/`list_runs` (`core/src/transforms.rs:398`), `pg_fire_data_triggers` seams (`iceberg_inline.rs:746`/`:1234`), `inline_append_mv` (T4), `Queue::dequeue`, `STREAM_MV_JOB_KIND`/`StreamMvJob` (T1).
- Produces: no new interfaces — green proof of the trigger contract for `MicroBatch` defs.

- [ ] **Step 1: Write the (mostly-passing-by-construction) fixture test**

Create `src/control-plane/postgres/tests/stream_mv_triggers.rs`, mirroring `tests/data_triggers.rs`'s seeding style:

1. **Fire + frozen payload:** define `TransformDef { name: "mv1", body: MicroBatch { source: s.events, output: s.mv1, buckets: 1, sql }, on_input_commit: true }`; `land` rows into `s.events` (`stream_buckets: Some(2)`, inline) → `dequeue(&[STREAM_MV_JOB_KIND.into()], "w")` yields a job whose payload decodes to the def's exact `StreamMvJob` with a fresh `run_id`, and `list_runs(Some("mv1"))` shows one `Queued` run with `trigger: DataTrigger`.
2. **Debounce:** land again WITHOUT draining → still exactly one `Queued` run (at-most-one-pending).
3. **Composability:** define `mv2` (`source: s.mv1`, `on_input_commit: true`); commit an MV1 micro-batch via `inline_append_mv(pool, &s_mv1, …, &MvCommit { mv: "s.mv1", source_table_id: events_tid, advances: [{0, 0, n}], run_id: None })` → an `stream_mv` job for `mv2` is enqueued by the SAME transaction (dequeue + payload check): an MV's output commit is a first-class data-trigger seam.
4. **Cycle rejection:** with `mv_a: s.t1 → s.t2` defined (`on_input_commit`), defining `mv_b: s.t2 → s.t1` (`on_input_commit`) is a `Validation` error naming both defs.
5. **Redefine-after-first-commit (refuse-guard exemption regression):** define `mv1: s.events → s.mv1`, commit a micro-batch to `s.mv1` (via `inline_append_mv`, so `s.mv1` is now a declared log stream), then **re-`define_transform`** `mv1` with edited `sql` (same output) → succeeds (`Ok`), NOT a `Validation` refusal. This locks in the `MicroBatch { .. } => None` exemption Task 1 added to the define-time `pg_refuse_stream_target` site (see Task 1 Step 5's EXCEPTION note). Contrast: a `Physical`/`Typed` transform re-targeting `s.mv1` is still refused (existing #416 behavior — do not regress it).

Wire `loom_fixture_test` target `stream-mv-triggers` mirroring `data-triggers` in `src/control-plane/postgres/BUCK`.

- [ ] **Step 2: Run — fix any arm the compiler-guided Task 1 sweep got wrong**

Run: `buck2 test --console none //src/control-plane/postgres:stream-mv-triggers`
Expected: PASS if Task 1's adapter arms were complete (the trigger scan, define-time cycle set, and body freeze all key off `TriggerNode::resolve` + serde). Any failure localizes to a missed `TransformBody` match arm — fix it, never special-case the trigger machinery.

- [ ] **Step 3: Non-regression sweep of the transforms/trigger suites**

Run: `buck2 test --console none //src/control-plane/postgres:data-triggers //src/control-plane/postgres:transforms //src/control-plane/memory:transforms //src/services/engine:transform-wire`
Expected: PASS, unmodified.

- [ ] **Step 4: prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(stream): MV registration rides the data-trigger machinery

Fixture proof: a MicroBatch def fires (debounced) on source commits with a
frozen stream_mv payload; an MV output commit fires downstream MV defs in
the same transaction (composability); an MV-MV trigger cycle is rejected at
define time."
```

---

## Task 7: Full verification + capability docs

**Files:**
- Modify: `docs/system-capabilities/stream.md` (new *Continuous / standing queries* section; move `#road-stream-continuous` out of Known gaps), `docs/system-capabilities/transform.md` (the third body kind + `stream_mv` job; trim the `fut-transform-followups` watermark clause).

- [ ] **Step 1: Whole-tree build + full suite**

Run: `buck2 build -v0 --console none //src/...` — exit 0.
Run: `buck2 test --console none //src/...` (locally add `-j 8`; cloud: scope to the btd-affected set instead) — `Fail 0`.

- [ ] **Step 2: Clippy over the touched crates**

Run: `tools/clippy-all.sh` (or per-target `[clippy.txt]` for core/postgres/memory/testkit/engine-wire/engine-serving/engine/worker) — clean.

- [ ] **Step 3: Capability docs**

Document the shipped capability in `docs/system-capabilities/stream.md` (registration, watermark semantics + the CAS/exactly-once story, delta-decomposability contract, composability, the superseded-run behavior, retention caveat) and cross-reference from `transform.md`. Keep the deferred items (CDC sources, retract/aggregate MVs, backfill control, Parquet spill, pruned/streaming scans, watermark-aware GC) named so the register close-out can cite them.

- [ ] **Step 4: prek + final commit + registers**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(stream): document continuous / standing queries (slice 4)"
```

Then run the **loom-docs-update** skill: close `road-stream-continuous` (and the watermark/incremental-output clause of `fut-transform-followups` if fully subsumed), record the new deferrals from the spec's Non-goals, staged on this branch. Finish with **superpowers:finishing-a-development-branch** (the user's standing choice: push + open PR).
