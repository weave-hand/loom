# Stream Subscribe on the Production Wire — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve `GET /objects/{type}/changes` on the production-wire deployment (no more 501), reading the feed at one consistent pair of pinned snapshots so a concurrent flush can never silently drop events.

**Architecture:** Two parts, in order. **Part A (Tasks 1–2)** fixes the torn read inside `changelog_feed_scan`: a new single-statement `current_snapshots_pair` pins the base and changelog snapshots against ONE Postgres MVCC snapshot, and both feed tiers are evaluated as-of those pins. **Part B (Tasks 3–5)** carries the feed over the wire: three unary `EngineControl` RPCs (`ChangelogLatest`, `ChangelogFeed`, `AwaitChangelog`), with the bounded page crossing as `page_json` — the `*_json` convention the governance-read RPCs already use — and `EngineServingClient` overriding the three `ServingEngine` feed methods that currently fall through to `Unsupported`. **Task 6** closes the register items.

**Tech Stack:** Rust 2024, buck2, sqlx compile-time macros, tonic/prost (`EngineControl`), DataFusion (engine-side only), Postgres control plane.

## Spec deviations (settled with the operator before planning — do NOT "restore" these)

Spec: `docs/superpowers/specs/2026-07-12-stream-subscribe-wire-design.md`.

1. **No new `inline_batch_at` read.** The spec's Part A step 3 asks for an as-of inline variant.
   It already exists: `IcebergCatalog::inline_live_batch_full(table, at)`
   (`iceberg_inline.rs:1610`) reads through `mvcc_live_pred(at)` (`iceberg_inline.rs:56`) =
   `begin_snapshot <= at and (end_snapshot is null or end_snapshot > at)` — byte-for-byte the
   predicate the spec specifies. The inline tier's ROW VISIBILITY is already as-of; the only bug
   is that the file tier reads its own, later `current_snapshot(&clog)` (`feed.rs:118`). A second
   identical SQL path would be dead code. **Part A is the pin plus the threading, nothing more.**
   (Precision: `inline_live_batch_impl` resolves the table id via a *live* `live_table_id` lookup
   (`:1740`) — only the row predicate is as-of. Harmless here: a flush never drops/recreates the
   base table.)
2. **No Flight ticket for the feed.** The spec's Part B item 1 asks for a `ChangelogFeedTicket`
   streaming framed Arrow batches, with the client re-decoding them into a `ChangeFeedPage`.
   **The cost is code-sharing, not dependencies.** query-api already deps `arrow` +
   `arrow-flight` and already decodes Arrow IPC off Flight (`engine_client.rs`), so it could
   handle batches fine. The problem is that the decode itself (`decode_page` + `cell_to_json`,
   `feed.rs:54-229`) lives in `engine-serving`, which query-api must NOT link (it is deliberately
   zero-DataFusion; `engine-serving/BUCK:19` deps `datafusion`). The only first-party crate the
   two libs share is `control-plane/core`. So a Flight variant forces either a copy-pasted decode
   (which trips the duplication gate) or relocating that decode into a crate both link (widening
   `core`, or a new codec crate). Instead the page crosses as JSON on a unary `EngineControl` RPC,
   exactly as `PoliciesFor` ships `Page<Policy>` as `page_json` (`engine_control.proto:204`).
   `ChangeEvent`/`ChangeFeedPage` already derive `Serialize`/`Deserialize` and are doc'd
   "transport-agnostic" (`core/src/stream.rs:91-107`); a page is bounded to `FEED_BATCH_LIMIT =
   256` events (`subscribe.rs:69`) and `ndjson_feed_stream` re-serializes it to JSON anyway, so
   Arrow here would be pure encode→decode→JSON overhead.
   **Consequence:** the spec's "Ticket disjointness" test bullet is moot — there is no new ticket;
   `EngineTicket` is untouched. Task 6 must also fix the *sibling* item's prose, which currently
   promises a "wire ticket" that will now never exist.

Everything else in the spec stands: `changelog_feed_scan`'s public signature is unchanged,
governance stays engine-side (`GovernedTableProvider` before the ordered read; the wire carries
the *resolved* policy, never the subject), and `http.rs` / `ndjson_feed_stream` / the cursor
contract are untouched — the 501 simply stops firing.

## Global Constraints

- **Strict clippy (pedantic + restriction).** No `unwrap`/`expect`/`panic`/`indexing_slicing`/`todo`/truncating `as`-casts in lib/bin code. Silence locally with `#[expect(lint, reason = "...")]` — a bare `#[allow]` without `reason` fails.
- **Tests are `rust_test` / `loom_fixture_test` targets ONLY** — never an inline `#[cfg(test)] mod tests` (the `no-inline-tests` prek hook fails the build). Anything booting Postgres MUST be `loom_fixture_test`, or it runs without the fixture env and fails to boot. (`loom_fixture_test` defaults `edition` to 2024 — no need to pass it.)
- **Test code IS exempt from the panic lints** (the `loom_rust_test` wrapper injects the allows), so `expect`/`panic!` in tests is correct and idiomatic here.
- **SQL changes require `./tools/sqlx-prepare.sh`**, and the resulting `src/control-plane/postgres/.sqlx/` change must be committed — otherwise the `query!` macro fails the build and `sqlx-cache-check` fails the suite.
- **Proto changes regenerate via the `:pb-gen` genrule** (`engine-wire/BUCK:22-27`) — never hand-edit generated stubs; there is no `build.rs`.
- **`buck2 run //tools:prek -- run --all-files` before EVERY commit** (rustfmt is a separate hook from clippy; `git add` new files FIRST — prek skips untracked files).
- Build/test with `--console none`. Full local suite needs `-j 8` (8 Postgres boot slots).

---

### Task 1: `current_snapshots_pair` — the atomic dual-snapshot read

**One SQL statement ⇒ one Postgres MVCC snapshot ⇒ the two snapshots are mutually consistent by
construction.** The flush commits the base append (with its inline end-cap) and the changelog
append in ONE Postgres transaction (`iceberg_flush.rs:288-320`), so a single statement sees that
flush wholly or not at all.

**Note on what the tests can and cannot prove.** The atomicity IS the SQL shape (one statement).
The tests below verify its *consequences* (correct values; absence → `None`; and, in Task 2, that
the feed is gapless under real concurrency). No unit test can distinguish "one statement" from
"two statements that happened not to race" — so **do not** refactor this into two sequential
`current_snapshot` calls: that would pass every test here and silently reintroduce the bug.

**Files:**
- Modify: `src/control-plane/postgres/src/iceberg_catalog.rs` — add to the **inherent** `impl IcebergCatalog` block (opens `:43`), beside `files_with_stats` (`:75`); NOT the `impl Catalog for IcebergCatalog` trait block (`:204`). Like `files_with_stats`, this deliberately stays off the portable trait.
- Create: `src/control-plane/postgres/tests/snapshot_pair.rs`
- Modify: `src/control-plane/postgres/BUCK`
- Regenerate + commit: `src/control-plane/postgres/.sqlx/`

**Interfaces:**
- Consumes: `Snapshot`, `SnapshotId`, `TableRef`, `Result`, `backend` — **all already imported** at `iceberg_catalog.rs:1-12`. No new imports.
- **Produces:** `IcebergCatalog::current_snapshots_pair(&self, base: &TableRef, clog: &TableRef) -> Result<(Option<Snapshot>, Option<Snapshot>)>` — `.0` = base, `.1` = changelog; `None` = no live snapshot (NOT an error). Task 2 consumes it.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/snapshot_pair.rs`:

```rust
//! `current_snapshots_pair` (road-stream-subscribe-wire, Part A): the atomic
//! dual-snapshot read the changelog feed pins both of its tiers against. ONE SQL
//! statement over `iceberg_mirror.snapshot` => ONE Postgres MVCC snapshot => the
//! returned pair is mutually consistent by construction. (The slice-2b flush appends
//! the changelog files AND end-caps the base's inline rows in a single Postgres
//! transaction, so this read can never observe half of a flush — which two
//! independent `current_snapshot` calls can.)

use control_plane_core::{Catalog, TableRef};
use control_plane_postgres::fixture::{IcebergWriter, PgFixture};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef {
        schema: schema.to_string(),
        name: name.to_string(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pair_read_matches_single_reads_and_maps_absence_to_none() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let cat = IcebergCatalog::new(pool.clone());

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    let writer = IcebergWriter::new(pool.clone(), dsn);
    writer.seed("main", "a", &cols, &[3]).await;
    writer.seed("main", "b", &cols, &[2]).await;

    let a = tref("main", "a");
    let b = tref("main", "b");
    let ghost = tref("main", "nope");

    // Both live: each element agrees with the single-table read and carries the FULL
    // Snapshot (id + time + schema_version), not just the id.
    let (pa, pb) = cat
        .current_snapshots_pair(&a, &b)
        .await
        .expect("pair read must not error");
    let sa = cat.current_snapshot(&a).await.expect("current_snapshot a");
    let sb = cat.current_snapshot(&b).await.expect("current_snapshot b");
    assert_eq!(pa.as_ref().map(|s| s.id), Some(sa.id), "base id");
    assert_eq!(pb.as_ref().map(|s| s.id), Some(sb.id), "clog id");
    assert_eq!(
        pa.as_ref().map(|s| s.schema_version),
        Some(sa.schema_version),
        "the pair carries the full Snapshot"
    );
    assert_eq!(pa.as_ref().map(|s| s.time), Some(sa.time), "snapshot time");

    // An absent table reads as `None`, NOT an error. This is exactly the feed's
    // "nothing has flushed yet, so no changelog table exists" case: the file tier must
    // be absent, not a failure. (A NULL decoded into a non-Option column would panic
    // here with UnexpectedNullError — this asserts the `?` overrides are right.)
    let (pa2, pghost) = cat
        .current_snapshots_pair(&a, &ghost)
        .await
        .expect("an absent table is None, not an error");
    assert_eq!(pa2.map(|s| s.id), Some(sa.id));
    assert!(pghost.is_none(), "absent table must read as None");

    // Both absent is still Ok.
    let (g1, g2) = cat
        .current_snapshots_pair(&ghost, &ghost)
        .await
        .expect("both absent is still Ok");
    assert!(g1.is_none() && g2.is_none());
}
```

Add to `src/control-plane/postgres/BUCK` (`IcebergWriter` owns its warehouse tempdir, so no
`tempfile` dep is needed):

```python
loom_fixture_test(
    name = "snapshot-pair",
    crate = "snapshot_pair",
    srcs = ["tests/snapshot_pair.rs"],
    crate_root = "tests/snapshot_pair.rs",
    deps = [
        ":postgres",
        "//src/control-plane/core:core",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:snapshot-pair`
Expected: FAIL — `no method named 'current_snapshots_pair' found for struct 'IcebergCatalog'`.

- [ ] **Step 3: Implement `current_snapshots_pair`**

In the inherent `impl IcebergCatalog` block. The SQL is `current_snapshot`'s `exists`-subquery
(`:207-214`) run once per table via a `left join lateral` over a two-row `values` list — the left
join keeps both rows present even when a table has no live snapshot, so the result is always
exactly two tagged rows.

**The `?` nullability overrides are MANDATORY, not a fallback.** sqlx does not model outer-join
nullability: `iceberg_mirror.snapshot.{snapshot_id,snapshot_time,schema_version}` are NOT NULL
base columns, so without `?` sqlx types them `i64`/`OffsetDateTime`/`i64` and (a) the `match`
below fails to compile (E0308) and (b) an absent table's SQL NULL would decode into `i64` and
panic at runtime. The committed cache proves the behavior: `files_with_stats`' `LEFT JOIN` records
`nullable: false` for its left-joined columns, which is why `iceberg_catalog.rs:84-87` must force
`as "column_name?"`.

```rust
    /// The current snapshots of `base` and `clog`, read in ONE statement — hence
    /// against ONE Postgres MVCC snapshot, so the pair is mutually consistent by
    /// construction. This is the changelog feed's pin.
    ///
    /// Why one statement: the slice-2b flush appends the changelog files AND end-caps
    /// the base's inline rows in a single Postgres transaction (`iceberg_flush.rs`),
    /// so a single-statement read sees that flush wholly or not at all. Two
    /// independent `current_snapshot` calls can see HALF of it — the base already
    /// advanced past the flush while the changelog files are still invisible — which
    /// tears the feed's inline-XOR-files invariant and SILENTLY DROPS events
    /// (`iss-stream-feed-torn-read`). Do not "simplify" this back into two reads.
    ///
    /// `None` for either element means that table has no live snapshot (e.g. the
    /// changelog table before the first flush). Absence is an ANSWER, not an error —
    /// unlike `Catalog::current_snapshot`, whose `NotFound` the feed would only have
    /// to catch and discard.
    pub async fn current_snapshots_pair(
        &self,
        base: &TableRef,
        clog: &TableRef,
    ) -> Result<(Option<Snapshot>, Option<Snapshot>)> {
        let rows = sqlx::query!(
            "select v.tag as \"tag!\", \
                    s.snapshot_id as \"snapshot_id?\", \
                    s.snapshot_time as \"snapshot_time?\", \
                    s.schema_version as \"schema_version?\" \
             from (values ('base', $1::text, $2::text), ('clog', $3::text, $4::text)) \
                  as v(tag, ns, nm) \
             left join lateral ( \
                 select sn.snapshot_id, sn.snapshot_time, sn.schema_version \
                 from iceberg_mirror.snapshot sn \
                 where exists ( \
                     select 1 from iceberg_mirror.table t \
                     where t.table_namespace = v.ns and t.table_name = v.nm \
                       and t.begin_snapshot <= sn.snapshot_id \
                       and (t.end_snapshot is null or t.end_snapshot > sn.snapshot_id)) \
                 order by sn.snapshot_id desc limit 1 \
             ) s on true",
            base.schema,
            base.name,
            clog.schema,
            clog.name,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;

        let mut base_snap = None;
        let mut clog_snap = None;
        for r in rows {
            // The three lateral columns are null together (no live snapshot) or present
            // together — the lateral yields a whole row or no row.
            let snap = match (r.snapshot_id, r.snapshot_time, r.schema_version) {
                (Some(id), Some(time), Some(schema_version)) => Some(Snapshot {
                    id: SnapshotId(id),
                    time,
                    schema_version,
                }),
                _ => None,
            };
            if r.tag == "base" {
                base_snap = snap;
            } else {
                clog_snap = snap;
            }
        }
        Ok((base_snap, clog_snap))
    }
```

- [ ] **Step 4: Refresh the sqlx cache**

Run: `./tools/sqlx-prepare.sh`
Expected: a new `src/control-plane/postgres/.sqlx/query-<hash>.json` (untracked in `git status`).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test --console none //src/control-plane/postgres:snapshot-pair //src/control-plane/postgres:sqlx-cache-check`
Expected: `Tests finished: Pass 2. Fail 0`.

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/iceberg_catalog.rs \
        src/control-plane/postgres/tests/snapshot_pair.rs \
        src/control-plane/postgres/BUCK \
        src/control-plane/postgres/.sqlx
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(catalog): current_snapshots_pair — atomic dual-snapshot read"
```

---

### Task 2: Pin both feed tiers (closes `iss-stream-feed-torn-read`)

Today `changelog_feed_scan` reads the changelog snapshot inside `build_file_tier` (`feed.rs:118`)
and the base snapshot at `feed.rs:354` — **two independent pooled queries, two DB states.**

**The bug's exact direction matters** (and determines the mutation check below). The changelog
tier is read **first**, so it can only ever be *older* than the base read — never newer. The
harmful interleave is therefore a **hole, not a duplicate**: a flush lands between the two reads
(so the file tier is stale and missing the flushed events `[0..k)`), and a follow-on inline write
lands (so the base snapshot advances). The inline tier, read at that *newer* base snapshot, no
longer shows the flushed rows — they were end-capped at the flush's snapshot — so events `[0..k)`
appear in NEITHER tier. `decode_page`'s `next.insert(bucket, offset + 1)` fold then advances past
the hole and those events are lost for that consumer, permanently. (`docs/ISSUES.md:31` says the
same: *"the dup case is impossible since the changelog is read first."*)

After this task both tiers are evaluated at ONE pinned pair. The inline tier needs no new read —
`inline_live_batch_full(base, pin)` is already as-of (see *Spec deviations*).

**Files:**
- Modify: `src/services/engine-serving/src/feed.rs`, `src/services/engine-serving/src/lib.rs`
- Modify: `src/services/query-api/tests/e2e_support.rs` (one new shared helper)
- Modify: `src/services/query-api/tests/stream_subscribe_scan.rs`, `tests/stream_subscribe_e2e.rs` (use the helper)
- Create: `src/services/query-api/tests/stream_feed_torn_read.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `current_snapshots_pair` (Task 1); `changelog_table_ref` (already imported, `feed.rs:21`); `ControlPlaneError` (already imported, `feed.rs:19`); `inline_live_batch_full`; `Catalog::schema`; `files_with_stats`; `build_inline_tier(catalog, base, SnapshotId)` (`feed.rs:148`).
- **Produces:**
  - `pub struct FeedPins { pub base: Snapshot, pub clog: Option<Snapshot> }`
  - `pub async fn changelog_feed_scan_at(catalog: &IcebergCatalog, base: &TableRef, serving_store: Option<&ServingStore>, positions: &BTreeMap<i32, i64>, limit: usize, policy: &TablePolicy, pins: &FeedPins) -> Result<ChangeFeedPage, EngineServingError>`
  - `changelog_feed_scan(..)` — **exact same signature as today**; resolves the pins, delegates.
  - `e2e_support::declare_cdc_table(cp: &PgControlPlane, pool: &sqlx::PgPool, schema: &str, name: &str, buckets: i32) -> TableRef` (Tasks 3 + 5 reuse it).

- [ ] **Step 1: Add the shared CDC-declare helper**

`stream_subscribe_scan.rs:57-65` and `stream_subscribe_e2e.rs:131-139` already carry this identical
block. Extract it ONCE into `src/services/query-api/tests/e2e_support.rs` (the shared support
library — exactly what it is for) rather than pasting a third copy:

```rust
/// Create the mirror table `schema.name` and declare it a CDC stream table with
/// `buckets` buckets, keyed by `id`, `LastRow` merge. Returns its `TableRef`.
/// With `buckets = 1` every identity lands in bucket 0, so a test's event offsets are
/// a plain 0,1,2,… sequence in write order.
pub async fn declare_cdc_table(
    cp: &PgControlPlane,
    pool: &sqlx::PgPool,
    schema: &str,
    name: &str,
    buckets: i32,
) -> TableRef {
    use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, schema, name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, buckets, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    TableRef {
        schema: schema.to_string(),
        name: name.to_string(),
    }
}
```

(`StreamTables` must be in scope for `declare_cdc` — add it to `e2e_support.rs`'s
`use control_plane_core::{...}` list if absent. No new BUCK deps: postgres/core/sqlx are all
already deps of `:e2e-support`.) Then rewrite those two call sites to use it.

- [ ] **Step 2: Write the failing tests**

The interleave is driven **deterministically** — pin, THEN mutate, THEN read at the pin. Events
come from governed actions like every other feed test (`createWidget` → `+I`; `updateWidget` →
`-U` then `+U`), and `buckets = 1` makes offsets a plain sequence.

Create `src/services/query-api/tests/stream_feed_torn_read.rs`:

```rust
//! Torn-read regression (`iss-stream-feed-torn-read`, closed by
//! `road-stream-subscribe-wire` Part A).
//!
//! The bug: the two tiers were read at two DB states. Because the changelog tier is
//! read FIRST it can only skew OLDER, so the harmful interleave is a HOLE, not a
//! duplicate — a flush (invisible to the stale file tier) plus a follow-on write
//! (which advances the base snapshot past the flush's inline end-cap) leaves the
//! flushed events in NEITHER tier, and the `next` fold then skips them forever.
//!
//! Three cases: the pinned page is self-consistent across that interleave; the inline
//! tier is genuinely as-of (the property Part A rests on); and the real, unpinned
//! public scan is gapless under ACTUAL concurrency (flush + writes racing a reader).

use std::collections::BTreeMap;

use control_plane_core::{ChangeFeedPage, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::changelog_table_ref;
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, grant_writer,
    spawn_engine_writer,
};
use engine_serving::TablePolicy;
use engine_serving::feed::{FeedPins, changelog_feed_scan, changelog_feed_scan_at};
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

/// Every (bucket, offset) in the page, in emission order.
fn coords(page: &ChangeFeedPage) -> Vec<(i32, i64)> {
    page.events.iter().map(|e| (e.bucket, e.offset)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_page_is_self_consistent_across_a_concurrent_flush_and_write() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    // ONE bucket => offsets are a plain 0,1,2,… sequence in write order.
    let table = declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // 3 events, all inline: +I(1) @0, then -U(1) @1 and +U(1) @2.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("+I(1)");
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("-U/+U(1)");

    let cat = IcebergCatalog::new(pool.clone());

    // --- PIN: exactly what the fixed scan does first, atomically. Nothing has flushed,
    // so the changelog table does not exist yet => clog pin is None. ---
    let clog = changelog_table_ref(&table);
    let (base_pin, clog_pin) = cat
        .current_snapshots_pair(&table, &clog)
        .await
        .expect("pair read");
    let pins = FeedPins {
        base: base_pin.expect("base table is live"),
        clog: clog_pin,
    };

    // --- The harmful interleave, committed AFTER the pin: a flush (one tx: appends the
    // changelog files AND end-caps the inline rows), then a follow-on inline write. ---
    connect_gov_client(&eg.sock)
        .await
        .flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");
    run_action(
        "createWidget",
        json!({ "id": "2", "name": "b", "qty": "2" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("+I(2) after the flush");

    // --- The read, at the pins captured BEFORE the flush. ---
    let page = changelog_feed_scan_at(
        &cat,
        &table,
        None,
        &BTreeMap::from([(0, 0)]),
        100,
        &TablePolicy::default(),
        &pins,
    )
    .await
    .expect("pinned scan");

    // The pinned view is exactly the 3 pre-flush events — contiguous from 0, NO HOLE.
    // The flush's changelog files are invisible (newer than the clog pin) and its
    // end-cap is invisible (end_snapshot > the base pin), so every event lives in
    // exactly ONE tier: the XOR invariant holds at the pinned pair.
    assert_eq!(
        coords(&page),
        vec![(0, 0), (0, 1), (0, 2)],
        "pinned page must be the pre-flush event set, contiguous and dup-free"
    );
    assert_eq!(
        page.next.get(&0).copied(),
        Some(3),
        "`next` advances to exactly one past the last event actually emitted"
    );

    // Resuming from `next` on a FRESH pin picks up precisely what the pinned page did
    // not carry — nothing skipped (the bug), nothing repeated.
    let rest = changelog_feed_scan(&cat, &table, None, &page.next, 100, &TablePolicy::default())
        .await
        .expect("resumed scan");
    assert_eq!(
        coords(&rest),
        vec![(0, 3)],
        "resume across the flush boundary is gapless and dup-free"
    );
}

/// The property Part A rests on: the inline tier is genuinely AS-OF, so a flush that
/// end-caps rows does NOT retract them from a reader pinned before it. (Spec's "as-of
/// inline visibility" test, expressed against the read that already implements it.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inline_tier_is_as_of_across_a_flush_end_cap() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let table = declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("+I(1)");

    let cat = IcebergCatalog::new(pool.clone());
    let before = cat
        .current_snapshot(&table)
        .await
        .expect("pre-flush snapshot");

    // Pre-flush pin: the row is live inline.
    let live = cat
        .inline_live_batch_full(&table, before.id)
        .await
        .expect("inline read")
        .expect("one live inline row before the flush");
    assert_eq!(live.2.num_rows(), 1, "the +I row is inline pre-flush");

    connect_gov_client(&eg.sock)
        .await
        .flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");

    // AT THE PRE-FLUSH PIN the row is STILL visible: the flush end-capped it at a NEWER
    // snapshot (`end_snapshot > before`), and `mvcc_live_pred` is as-of. This is what
    // makes the pinned union disjoint rather than holey.
    let still = cat
        .inline_live_batch_full(&table, before.id)
        .await
        .expect("as-of inline read")
        .expect("the end-capped row is STILL visible at the pre-flush pin");
    assert_eq!(
        still.2.num_rows(),
        1,
        "an end-cap must not retract rows from a reader pinned before it"
    );

    // At the CURRENT snapshot it is gone from inline (it lives in the files now) — the
    // XOR invariant, on a single consistent state.
    let now = cat.current_snapshot(&table).await.expect("post-flush snapshot");
    let after = cat
        .inline_live_batch_full(&table, now.id)
        .await
        .expect("inline read");
    assert!(
        after.is_none(),
        "post-flush the row is in the file tier, NOT inline (inline XOR files)"
    );
}

/// The real, unpinned, PUBLIC scan under ACTUAL concurrency: a reader pages the feed
/// while writes and a flush land underneath it. The concatenated stream must be
/// gapless and duplicate-free. This is the closest a test can get to asserting the
/// atomicity itself (rather than its consequences).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_scan_is_gapless_under_concurrent_flush_and_writes() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let table: TableRef = declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };
    let cat = IcebergCatalog::new(pool.clone());

    // Writer: 6 creates, flushing midway — the flush races the reader's paging.
    let mut expected = 0i64;
    for i in 0..6i64 {
        run_action(
            "createWidget",
            json!({ "id": i.to_string(), "name": "x", "qty": "1" })
                .as_object()
                .expect("object body"),
            &subj,
            &deps,
        )
        .await
        .expect("+I");
        expected += 1;
        if i == 2 {
            connect_gov_client(&eg.sock)
                .await
                .flush_table("main".to_string(), "widget".to_string())
                .await
                .expect("flush_table");
        }
    }

    // Page the feed 2 events at a time, resuming from `next` — the production loop.
    let mut positions: BTreeMap<i32, i64> = BTreeMap::from([(0, 0)]);
    let mut seen: Vec<(i32, i64)> = Vec::new();
    for _ in 0..10 {
        let page = changelog_feed_scan(&cat, &table, None, &positions, 2, &TablePolicy::default())
            .await
            .expect("scan");
        if page.events.is_empty() {
            break;
        }
        seen.extend(coords(&page));
        positions = page.next;
    }

    let want: Vec<(i32, i64)> = (0..expected).map(|o| (0, o)).collect();
    assert_eq!(
        seen, want,
        "every event exactly once, in order, across the flush boundary: {seen:?}"
    );
}
```

Add to `src/services/query-api/BUCK`:

```python
loom_fixture_test(
    name = "stream-feed-torn-read",
    crate = "stream_feed_torn_read",
    srcs = ["tests/stream_feed_torn_read.rs"],
    crate_root = "tests/stream_feed_torn_read.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-serving:engine-serving",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `buck2 test --console none //src/services/query-api:stream-feed-torn-read`
Expected: FAIL — `cannot find struct 'FeedPins'` / `cannot find function 'changelog_feed_scan_at'`.

- [ ] **Step 4: Thread the pins through `feed.rs`**

**(a)** Add `Snapshot` to the `control_plane_core` import at `feed.rs:19`, then add:

```rust
/// The pinned pair of snapshots ONE feed page is read at: the base table's, and the
/// changelog table's (`None` until the first flush creates it). Both come from a single
/// `current_snapshots_pair` statement, so they are mutually consistent — and THAT is
/// what keeps the two-tier union disjoint. The slice-2b XOR invariant (an event is
/// inline xor in the changelog files) holds on any single DB state, and a pinned pair
/// IS a single DB state.
#[derive(Debug, Clone)]
pub struct FeedPins {
    pub base: Snapshot,
    pub clog: Option<Snapshot>,
}
```

**(b)** Replace `build_file_tier` (`:113-142`) — it no longer reads a snapshot of its own:

```rust
/// Tier 1 of the feed union: the changelog Iceberg files, read AT the pinned changelog
/// snapshot. `clog_pin` is `None` when nothing has flushed yet (no changelog mirror row
/// at the pin), which — like a mirror row carrying zero files — means the file tier is
/// simply absent.
async fn build_file_tier(
    catalog: &IcebergCatalog,
    base: &TableRef,
    clog_pin: Option<&Snapshot>,
) -> Result<Option<IcebergMirrorTableProvider>, EngineServingError> {
    let Some(pin) = clog_pin else {
        return Ok(None);
    };
    let clog = changelog_table_ref(base);
    let cols = catalog
        .schema(&clog, pin.id)
        .await
        .map_err(to_serving)?
        .columns;
    let framed = with_feed_framing_fields(&arrow_schema_from_mirror(&cols)?);
    let files = catalog
        .files_with_stats(&clog, pin.id)
        .await
        .map_err(to_serving)?;
    if files.is_empty() {
        Ok(None)
    } else {
        Ok(Some(IcebergMirrorTableProvider::try_new_with_schema(
            files, framed,
        )))
    }
}
```

**(c)** Split `changelog_feed_scan` (`:330-397`) into the pin resolver + the pinned body. **Keep
the existing doc comment** (`:318-329`) and add the pinning note to it:

```rust
pub async fn changelog_feed_scan(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
) -> Result<ChangeFeedPage, EngineServingError> {
    // ONE statement, ONE Postgres MVCC snapshot: the base and changelog snapshots are
    // pinned together or not at all. Reading them independently is the torn read
    // (`iss-stream-feed-torn-read`) — the changelog is read first, so it can only skew
    // OLDER, and a flush plus a follow-on write between the two reads leaves the
    // flushed events in NEITHER tier. The `next` fold then advances past that hole and
    // the events are lost for that consumer.
    let clog = changelog_table_ref(base);
    let (base_pin, clog_pin) = catalog
        .current_snapshots_pair(base, &clog)
        .await
        .map_err(to_serving)?;
    // An unknown base table stays an error, as before. (`to_serving` erases the error
    // class to `Engine` — as it already did for the previous `current_snapshot(base)`
    // call — so this is not a `NotFound`-classed error and no caller may match on one.)
    let Some(base_snap) = base_pin else {
        return Err(to_serving(ControlPlaneError::NotFound(format!(
            "{}.{}",
            base.schema, base.name
        ))));
    };
    let pins = FeedPins {
        base: base_snap,
        clog: clog_pin,
    };
    changelog_feed_scan_at(catalog, base, serving_store, positions, limit, policy, &pins).await
}

/// [`changelog_feed_scan`]'s body at an explicit pinned pair. Public so a test can drive
/// the pin/flush/read interleave deterministically instead of racing threads.
pub async fn changelog_feed_scan_at(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
    pins: &FeedPins,
) -> Result<ChangeFeedPage, EngineServingError> {
    let empty = || ChangeFeedPage {
        events: vec![],
        next: positions.clone(),
    };
    if positions.is_empty() || limit == 0 {
        return Ok(empty());
    }

    let ctx = SessionContext::new();
    register_object_stores(&ctx, serving_store)?;

    // --- Tier 1: the changelog Iceberg files, AT the pinned changelog snapshot. ---
    let file_provider = build_file_tier(catalog, base, pins.clog.as_ref()).await?;

    // --- Tier 2: the base's inline tail, AT the pinned base snapshot.
    // `inline_live_batch_full` is ALREADY an as-of read (`mvcc_live_pred`):
    // `begin_snapshot <= pin and (end_snapshot is null or end_snapshot > pin)`. So a
    // flush committing after the pin changes NEITHER tier — its changelog files carry a
    // newer snapshot, and its end-cap stamps `end_snapshot` with a base snapshot newer
    // than the pin, leaving these rows visible here. ---
    let inline_provider = build_inline_tier(catalog, base, pins.base.id).await?;

    // --- Disjoint UNION ALL, both tiers projected to the SAME column order. ---
    let base_cols = catalog
        .schema(base, pins.base.id)
        .await
        .map_err(to_serving)?
        .columns;
    let select = union_select_columns(&base_cols);
    let Some(unioned) = union_tiers(&ctx, file_provider, inline_provider, &select)? else {
        return Ok(empty());
    };

    // --- Governance BEFORE the ordered read. ---
    let governed = GovernedTableProvider::new(unioned.into_view(), policy.clone())?;
    let df = ctx.read_table(Arc::new(governed)).map_err(to_serving)?;

    let Some(pred) = build_resume_predicate(positions) else {
        return Ok(empty());
    };

    let batches = df
        .filter(pred)
        .map_err(to_serving)?
        .sort(vec![
            col("loom_bucket").sort(true, false),
            col("loom_offset").sort(true, false),
        ])
        .map_err(to_serving)?
        .limit(0, Some(limit))
        .map_err(to_serving)?
        .collect()
        .await
        .map_err(to_serving)?;

    decode_page(&batches, positions)
}
```

**(d)** Update `src/services/engine-serving/src/lib.rs:19`:

```rust
pub use feed::{FeedPins, changelog_feed_scan, changelog_feed_scan_at};
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api:stream-feed-torn-read //src/services/query-api:stream-subscribe-scan //src/services/query-api:stream-subscribe-e2e //src/services/query-api:stream-subscribe-gov-e2e`
Expected: `Tests finished: Pass N. Fail 0` — the three existing feed suites inherit the pinned read unchanged.

- [ ] **Step 6: Prove the regression test has teeth (mutation check — GET THE DIRECTION RIGHT)**

A regression test that cannot fail is worthless — and a mutation in the *wrong* direction proves
nothing about *this* bug. The real tear makes the **base/inline** read run live while the
changelog tier stays stale, producing a **hole**. Reintroduce exactly that in
`changelog_feed_scan_at`:

```rust
    // TEMPORARY — the actual iss-stream-feed-torn-read direction (a HOLE). Revert after.
    let live_base = catalog.current_snapshot(base).await.map_err(to_serving)?;
    let inline_provider = build_inline_tier(catalog, base, live_base.id).await?;
```

Run: `buck2 test --console none //src/services/query-api:stream-feed-torn-read`
Expected: **FAIL** — `pinned_page_is_self_consistent_across_a_concurrent_flush_and_write` now sees
only `[(0, 3)]` (the file tier is absent at the `None` clog pin, and the live inline tier no
longer shows events 0–2 — they were end-capped at the flush), so both the `coords` and the `next`
assertions break. That is the silent event loss the issue describes.

Then **revert the mutation** and re-run to confirm green. Do NOT commit the mutation.

- [ ] **Step 7: Commit**

```bash
git add src/services/engine-serving/src/feed.rs src/services/engine-serving/src/lib.rs \
        src/services/query-api/tests/stream_feed_torn_read.rs \
        src/services/query-api/tests/e2e_support.rs \
        src/services/query-api/tests/stream_subscribe_scan.rs \
        src/services/query-api/tests/stream_subscribe_e2e.rs \
        src/services/query-api/BUCK
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "fix(stream): pin the feed's tiers to one snapshot pair; close iss-stream-feed-torn-read"
```

---

### Task 3: The three `EngineControl` RPCs (proto + server + client)

Server and client land together: neither is testable without the other. The RPC test lives in
**query-api's** test crate, not the engine's, because seeding CDC events needs governed actions
(`run_action`) and `e2e_support`, which the engine crate cannot reach.

**`EngineControlService` gains exactly ONE field.** It needs the object store (config-derived,
underivable from the pool) but NOT a catalog: `service.rs:190` already builds one inline
(`IcebergCatalog::new(self.pool.clone())`), and the feed handler does the same. One field keeps
the churn across the eight construction sites minimal.

**Files:**
- Modify: `src/services/engine-wire/proto/engine_control.proto`
- Modify: `src/services/engine-wire/src/client.rs` (`WirePolicy` + three methods on `GrpcQueueClient`, inherent `impl` at `:124`)
- Modify: `src/services/engine/src/service.rs` (one new field; three RPC impls beside `await_jobs` `:128`; new imports)
- Modify: `src/services/store-config/src/lib.rs` (`#[derive(Clone)]` on `ServingStore`)
- Modify: **all 8** `EngineControlService` construction sites (below)
- Modify: `src/services/query-api/tests/e2e_support.rs` (`spawn_engine_full`)
- Create: `src/services/query-api/tests/changelog_rpc_wire.rs`
- Modify: `src/services/query-api/BUCK`
- **Do NOT touch `src/services/engine/src/flight.rs`** — `serving_status` is ALREADY `pub` (`flight.rs:39`) and ALREADY imported in `service.rs:19`. Narrowing it to `pub(crate)` would break `engine/tests/serving_status.rs:8`.
- **Do NOT add `store-config` to `src/services/engine/BUCK`** — the engine reaches `ServingStore` via `service_runtime::ServingStore` (`flight.rs:26`), and `//src/services/runtime:runtime` is already a dep.

The **8 construction sites** (`grep -rn "EngineControlService {" --include=*.rs src/`) — every one
is a `will not compile` (E0063) until updated:

| site | new field |
|---|---|
| `src/services/engine/src/run.rs:177` (production) | `serving_store: serving_store.clone()` |
| `src/testing/flight.rs:110` | `serving_store: None` |
| `src/services/engine/tests/wire.rs:73` | `serving_store: None` |
| `src/services/engine/tests/wire.rs:265` | `serving_store: None` |
| `src/services/engine/tests/write_wire.rs:133` | `serving_store: None` |
| `src/services/engine/tests/compact_wire.rs:75` | `serving_store: None` |
| `src/services/worker/tests/e2e.rs:108` | `serving_store: None` |
| `src/services/worker/tests/flight_roundtrip.rs:84` | `serving_store: None` |

**Interfaces:**
- Consumes: `changelog_feed_scan` (Task 2); `control_plane_postgres::stream::{changelog_positions_latest, await_changelog}`; `engine_serving::TablePolicy` (`governed.rs:142-146` — `Vec<RowFilter>` + two `HashSet<String>`, derives `Default`); `status()` (`service.rs:21`); `serving_status` (already imported); `se()`/`be()` (`client.rs:71`/`:19`); `RowFilter` (`core/src/acl.rs:188`, already serde).
- **Produces:** the three RPCs; `engine_wire::client::WirePolicy`; three `GrpcQueueClient` methods (signatures in Task 4's *Consumes*); `e2e_support::spawn_engine_full(...) -> (String, EngineGuard)` (control **and** flight on one socket).

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/changelog_rpc_wire.rs`:

```rust
//! The three changelog RPCs on `EngineControl` (road-stream-subscribe-wire, Part B),
//! driven over a real engine socket: `ChangelogLatest` probes subscribability,
//! `ChangelogFeed` serves one bounded governed page as JSON, `AwaitChangelog`
//! long-polls. These three are what lift the production feed off its 501.

use std::collections::BTreeMap;
use std::time::Duration;

use control_plane_core::ChangeFeedPage;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, grant_writer,
    spawn_engine_full,
};
use engine_wire::client::WirePolicy;
use query_api::action::{ActionDeps, run_action};
use query_api::engine_action_client::EngineActionClient;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changelog_rpcs_probe_serve_and_wait() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;

    // Control + Flight on ONE socket, exactly as production serves them.
    let (sock, _eg) =
        spawn_engine_full(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let engine = EngineActionClient::connect(sock.clone())
        .await
        .expect("connect EngineActionClient");
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps {
        cp: &cp,
        action_engine: &engine,
        serving: &serving,
    };

    // 3 events: +I(1) @0, -U(1) @1, +U(1) @2.
    run_action(
        "createWidget",
        json!({ "id": "1", "name": "a", "qty": "1" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("+I(1)");
    run_action(
        "updateWidget",
        json!({ "id": "1", "qty": "9" })
            .as_object()
            .expect("object body"),
        &subj,
        &deps,
    )
    .await
    .expect("-U/+U(1)");

    let client = connect_gov_client(&sock).await;

    // 1. ChangelogLatest — a declared CDC table reports its per-bucket high-water.
    let latest = client
        .changelog_latest("main".to_string(), "widget".to_string())
        .await
        .expect("changelog_latest")
        .expect("a declared CDC table is subscribable");
    assert_eq!(
        latest,
        BTreeMap::from([(0, 3)]),
        "high-water is one past the last event"
    );

    // A table that is not a declared CDC table is Ok(None) — the "not subscribable"
    // discriminator query-api maps to its 400. NOT an error, NOT Unsupported.
    let plain = client
        .changelog_latest("main".to_string(), "nope".to_string())
        .await
        .expect("a non-CDC table is Ok(None), not an error");
    assert!(plain.is_none());

    // 2. ChangelogFeed — one bounded, ordered page with advanced positions.
    let page: ChangeFeedPage = client
        .changelog_feed(
            "main".to_string(),
            "widget".to_string(),
            &BTreeMap::from([(0, 0)]),
            100,
            &WirePolicy::default(),
        )
        .await
        .expect("changelog_feed");
    let coords: Vec<(i32, i64)> = page.events.iter().map(|e| (e.bucket, e.offset)).collect();
    assert_eq!(coords, vec![(0, 0), (0, 1), (0, 2)], "ordered page");
    assert_eq!(page.next.get(&0).copied(), Some(3), "positions advanced");
    let kinds: Vec<&str> = page.events.iter().map(|e| e.change_kind.as_str()).collect();
    assert_eq!(kinds, vec!["+I", "-U", "+U"], "full change sequence incl. -U");

    // The reserved framing columns never leak into the event fields.
    for e in &page.events {
        assert!(
            !e.fields.keys().any(|k| k.starts_with("loom_")),
            "framing column leaked into fields: {:?}",
            e.fields
        );
    }

    // Resume from `next`: no events left, positions unchanged. An empty page is a
    // legitimate answer, not an error — it is what drives the long-poll.
    let empty = client
        .changelog_feed(
            "main".to_string(),
            "widget".to_string(),
            &page.next,
            100,
            &WirePolicy::default(),
        )
        .await
        .expect("empty feed page");
    assert!(empty.events.is_empty());
    assert_eq!(
        empty.next, page.next,
        "an empty page returns the caller's positions unchanged"
    );

    // 3. AwaitChangelog — returns cleanly at its timeout when no write lands.
    client
        .await_changelog(
            "main".to_string(),
            "widget".to_string(),
            Duration::from_millis(300),
        )
        .await
        .expect("await_changelog returns Ok at timeout, never errors");
}
```

Add `spawn_engine_full` to `e2e_support.rs` (today's `spawn_engine` is `flight: false`, `:1205`):

```rust
/// Spawn an engine serving BOTH `EngineControl` and Arrow Flight on one UDS — the
/// production shape (`serve.rs` dials one socket for the control, action, and serving
/// clients alike). `spawn_engine` stays control-only for the write-path tests.
pub async fn spawn_engine_full(
    fx: &control_plane_postgres::fixture::PgFixture,
    db: &str,
    warehouse: &std::path::Path,
    inline_byte_limit: usize,
    flush_byte_threshold: i64,
) -> (String, EngineGuard) {
    let eng = loom_test_flight::spawn_engine_uds(
        fx,
        db,
        &warehouse.display().to_string(),
        loom_test_flight::EngineOpts {
            control: true,
            flight: true,
            inline_byte_limit,
            flush_byte_threshold,
        },
    )
    .await;
    (eng.sock.clone(), eng)
}
```

BUCK:

```python
loom_fixture_test(
    name = "changelog-rpc-wire",
    crate = "changelog_rpc_wire",
    srcs = ["tests/changelog_rpc_wire.rs"],
    crate_root = "tests/changelog_rpc_wire.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/services/engine-wire:engine-wire",
        "//src/testing:flight",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:changelog-rpc-wire`
Expected: FAIL — no `changelog_latest` on `GrpcQueueClient`, no `WirePolicy`, no `spawn_engine_full`.

- [ ] **Step 3: Extend the proto**

In `src/services/engine-wire/proto/engine_control.proto`, add to `service EngineControl` (after
`CommitMicroBatch`, before the governance comment at `:25`):

```protobuf
  // Changelog feed (query-api serves `GET /objects/{type}/changes` over these three).
  rpc ChangelogLatest (ChangelogLatestRequest) returns (ChangelogLatestResponse);
  rpc ChangelogFeed   (ChangelogFeedRequest)   returns (ChangelogFeedResponse);
  rpc AwaitChangelog  (AwaitChangelogRequest)  returns (AwaitChangelogResponse);  // unary long-poll
```

and the messages before the `---- Governance-read messages ----` block (`:193`):

```protobuf
// ---- Changelog feed. ----

// The per-bucket high-water offsets of a declared CDC table's changelog.
// `present = false` <=> not a declared CDC table — query-api maps that to its existing
// 400. Absence is an ANSWER, not an error.
message ChangelogLatestRequest  { string schema = 1; string name = 2; }
message ChangelogLatestResponse {
  bool present = 1;
  map<int32, int64> positions = 2;
}

// One bounded, governed, (bucket, offset)-ordered page of the changelog feed from
// per-bucket `positions`. `policy_json` is a serde_json `WirePolicy` — the
// CALLER-RESOLVED policy (row filters + denied/masked columns); the engine enforces it
// via `GovernedTableProvider` before the ordered read, so the SUBJECT never crosses the
// wire. `page_json` is a serde_json `ChangeFeedPage` (events + advanced positions). The
// engine CLAMPS `limit` (an unbounded page would be an unbounded unary message).
message ChangelogFeedRequest {
  string schema = 1;
  string name = 2;
  map<int32, int64> positions = 3;
  uint64 limit = 4;
  string policy_json = 5;
}
message ChangelogFeedResponse { string page_json = 1; }

// Block until a CDC write commits against the table, or `timeout_ms` elapses — whichever
// first. Returns Ok on timeout (never an error), like AwaitJobs.
message AwaitChangelogRequest  { string schema = 1; string name = 2; uint64 timeout_ms = 3; }
message AwaitChangelogResponse {}
```

- [ ] **Step 4: Add `WirePolicy` + the three client methods**

In `src/services/engine-wire/src/client.rs`, beside `TableFiles` (`:107-113`):

```rust
/// The caller-resolved change-feed policy as it crosses the wire: row filters plus the
/// denied/masked column names. Mirrors query-api's `ChangeFeedPolicy` (which engine-wire
/// must not depend on); the engine converts it to `engine_serving::TablePolicy` and
/// enforces it there. The wire carries the RESOLVED policy, never the subject.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WirePolicy {
    pub row_filters: Vec<control_plane_core::RowFilter>,
    pub denied: Vec<String>,
    pub masked: Vec<String>,
}
```

and in the inherent `impl GrpcQueueClient` block (beside `flush_table`, `:135`). Note `se()`
(`client.rs:71`) is the existing serialize helper — use it rather than re-rolling
`serde_json::to_string(..).map_err(be)`:

```rust
    /// The per-bucket high-water offsets of a declared CDC table's changelog, or `None`
    /// when it is not a declared CDC table (the "is this subscribable" probe — absence
    /// is not an error).
    pub async fn changelog_latest(
        &self,
        schema: String,
        name: String,
    ) -> Result<Option<std::collections::BTreeMap<i32, i64>>> {
        let resp = self
            .inner
            .clone()
            .changelog_latest(pb::ChangelogLatestRequest { schema, name })
            .await
            .map_err(be)?
            .into_inner();
        if resp.present {
            Ok(Some(resp.positions.into_iter().collect()))
        } else {
            Ok(None)
        }
    }

    /// One bounded, governed, ordered page of a CDC table's changelog feed from
    /// per-bucket `positions`. `policy` is the caller-RESOLVED governance policy; the
    /// engine enforces it before the ordered read.
    pub async fn changelog_feed(
        &self,
        schema: String,
        name: String,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: u64,
        policy: &WirePolicy,
    ) -> Result<control_plane_core::ChangeFeedPage> {
        let resp = self
            .inner
            .clone()
            .changelog_feed(pb::ChangelogFeedRequest {
                schema,
                name,
                positions: positions.iter().map(|(b, o)| (*b, *o)).collect(),
                limit,
                policy_json: se(policy)?,
            })
            .await
            .map_err(be)?
            .into_inner();
        de(&resp.page_json)
    }

    /// Block until a CDC write commits against the table, or `timeout` elapses. Ok on
    /// timeout (never an error).
    pub async fn await_changelog(
        &self,
        schema: String,
        name: String,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let mut req = tonic::Request::new(pb::AwaitChangelogRequest {
            schema,
            name,
            timeout_ms: u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        });
        // The client deadline must EXCEED the server's long-poll timeout, or we get a
        // spurious DeadlineExceeded before the server returns normally. Same +2s as
        // `await_jobs` (`:688-698`).
        req.set_timeout(timeout + std::time::Duration::from_secs(2));
        self.inner.clone().await_changelog(req).await.map_err(be)?;
        Ok(())
    }
```

(`de` is the existing JSON-decode helper used by the `gov_rpc!` macro; if its signature does not
fit here, `serde_json::from_str(&resp.page_json).map_err(be)` is equivalent.)

- [ ] **Step 5: `ServingStore: Clone` + the new field**

`ServingStore` (`src/services/store-config/src/lib.rs:170-173`) has **no derive**, but `run.rs:186`
*moves* it into `FlightDataService` — so the control service cannot also hold it. Add the derive
(both fields are already `Clone`):

```rust
/// Bucket name + object store handle returned by [`build_serving_object_store`].
#[derive(Clone)]
pub struct ServingStore {
    pub bucket: String,
    pub store: Arc<dyn ObjectStore>,
}
```

In `src/services/engine/src/service.rs`, add `use service_runtime::ServingStore;` (NOT
`store_config` — that is not an engine dep) and the one field to `EngineControlService`
(`:68-80`):

```rust
    /// `Some(ServingStore { bucket, store })` for an S3 warehouse; `None` => local FS.
    /// The changelog feed's object store (the catalog is built inline from `pool`, as
    /// `list_files` already does at `:190`).
    pub serving_store: Option<ServingStore>,
```

Then update **all 8 construction sites** per the table above. In `run.rs`, hoist the store so both
services can take it:

```rust
    let serving_store = service_runtime::build_serving_object_store(&cfg.object_store)?;
    let control = EngineControlService {
        cp: cp.clone(),
        catalog: catalog.clone(),
        pool: pool.clone(),
        retention: cfg.gc_retention,
        writer,
        flush_byte_threshold: tuning.flush_byte_threshold,
        serving_store: serving_store.clone(),
    };
    let flight = FlightDataService {
        catalog,
        serving_catalog: IcebergCatalog::new(pool.clone()),
        serving_store,
        pool,
        cp,
        // …unchanged
    };
```

If the worker test targets cannot name `IcebergCatalog`/`ServingStore`, they only need
`serving_store: None` — no new imports beyond what `None` requires (none).

- [ ] **Step 6: Implement the three RPCs**

In `src/services/engine/src/service.rs`, in `impl pb::engine_control_server::EngineControl for
EngineControlService`, beside `await_jobs` (`:128`). `status`, `serving_status`, and `TableRef`
are already in scope. Add a clamp constant near the top of the file:

```rust
/// Server-side ceiling on one changelog-feed page. query-api already clamps to
/// `FEED_BATCH_LIMIT` (256) before calling, but this is a public engine RPC — an
/// unbounded `limit` would mean an unbounded `page_json` in a unary message.
const MAX_FEED_LIMIT: usize = 1024;
```

```rust
    async fn changelog_latest(
        &self,
        req: Request<pb::ChangelogLatestRequest>,
    ) -> std::result::Result<Response<pb::ChangelogLatestResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let latest = control_plane_postgres::stream::changelog_positions_latest(&self.pool, &table)
            .await
            .map_err(status)?;
        // `None` = not a declared CDC table: a legitimate answer, not an error.
        Ok(Response::new(pb::ChangelogLatestResponse {
            present: latest.is_some(),
            positions: latest.unwrap_or_default().into_iter().collect(),
        }))
    }

    async fn changelog_feed(
        &self,
        req: Request<pb::ChangelogFeedRequest>,
    ) -> std::result::Result<Response<pb::ChangelogFeedResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        let wire: engine_wire::client::WirePolicy = serde_json::from_str(&r.policy_json)
            .map_err(|e| Status::invalid_argument(format!("bad policy_json: {e}")))?;
        // Governance is enforced HERE, engine-side, before the ordered read. The wire
        // carried the resolved policy; the subject never crossed it.
        let policy = engine_serving::TablePolicy {
            row_filters: wire.row_filters,
            denied: wire.denied.into_iter().collect(),
            masked: wire.masked.into_iter().collect(),
        };
        let positions: std::collections::BTreeMap<i32, i64> = r.positions.into_iter().collect();
        let limit = usize::try_from(r.limit)
            .unwrap_or(MAX_FEED_LIMIT)
            .min(MAX_FEED_LIMIT);
        let ice = control_plane_postgres::iceberg_catalog::IcebergCatalog::new(self.pool.clone());
        let page = engine_serving::changelog_feed_scan(
            &ice,
            &table,
            self.serving_store.as_ref(),
            &positions,
            limit,
            &policy,
        )
        .await
        .map_err(serving_status)?;
        let page_json = serde_json::to_string(&page)
            .map_err(|e| Status::internal(format!("page encode: {e}")))?;
        Ok(Response::new(pb::ChangelogFeedResponse { page_json }))
    }

    async fn await_changelog(
        &self,
        req: Request<pb::AwaitChangelogRequest>,
    ) -> std::result::Result<Response<pb::AwaitChangelogResponse>, Status> {
        let r = req.into_inner();
        let table = TableRef {
            schema: r.schema,
            name: r.name,
        };
        control_plane_postgres::stream::await_changelog(
            &self.pool,
            &table,
            std::time::Duration::from_millis(r.timeout_ms),
        )
        .await
        .map_err(status)?;
        Ok(Response::new(pb::AwaitChangelogResponse {}))
    }
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:changelog-rpc-wire`
Expected: `Tests finished: Pass 1. Fail 0`.

Then confirm the 8 sites all build:
Run: `buck2 build -v0 --console none //src/services/engine/... //src/services/worker/... //src/testing/...`
Expected: silent success (exit 0).

- [ ] **Step 8: Commit**

```bash
git add src/services/engine-wire/proto/engine_control.proto \
        src/services/engine-wire/src/client.rs \
        src/services/store-config/src/lib.rs \
        src/services/engine/src/service.rs src/services/engine/src/run.rs \
        src/services/engine/tests/wire.rs src/services/engine/tests/write_wire.rs \
        src/services/engine/tests/compact_wire.rs \
        src/services/worker/tests/e2e.rs src/services/worker/tests/flight_roundtrip.rs \
        src/testing/flight.rs \
        src/services/query-api/tests/changelog_rpc_wire.rs \
        src/services/query-api/tests/e2e_support.rs \
        src/services/query-api/BUCK
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(engine): ChangelogLatest/ChangelogFeed/AwaitChangelog control RPCs"
```

---

### Task 4: `EngineServingClient` overrides the three feed methods (kills the 501)

`EngineServingClient` (`engine_client.rs:33-96`) overrides only `fetch_rows` / `vector_search` /
`dialect`, so the three feed methods fall through to the trait's `Err(ServingError::Unsupported)`
defaults (`serving.rs:331-361`), which `http.rs:643-649` maps to **501**.

**Files:**
- Modify: `src/services/query-api/src/engine_client.rs`
- Modify: `src/services/query-api/tests/engine_wire_serving_e2e.rs`
- Modify: `src/services/query-api/BUCK` (the `engine-wire-serving-e2e` target has **no** `//src/control-plane/core:core` dep — the new test needs it)

**Interfaces:**
- Consumes: the three `GrpcQueueClient` methods + `WirePolicy` (Task 3); `ChangeFeedPolicy` (`serving.rs:107-112`); `ServingError`.
- **Produces:** an `EngineServingClient` whose three feed methods work over the wire. Task 5 drives it end-to-end.

- [ ] **Step 1: Write the failing test**

Add to `src/services/query-api/tests/engine_wire_serving_e2e.rs`. That file has **three**
`spawn_flight_uds` call sites (`:38`, `:87`, `:114`) — **leave them alone**: `uds_channel`
(`engine-wire/src/lib.rs:20-32`) only requires the UDS to *accept*, so the new `GrpcQueueClient`
inside `EngineServingClient::connect` connects fine against a Flight-only server (an unregistered
control RPC only fails if *called*, which those three tests never do). Only the NEW test needs the
control service. Widen the import at `:8`:

```rust
use loom_test_flight::{EngineOpts, spawn_engine_uds, spawn_flight_uds};
```

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_methods_are_no_longer_unsupported_over_the_wire() {
    use query_api::serving::ServingEngine;

    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let dsn = fx.pg_dsn(&db);
    let wh = tempfile::tempdir().expect("warehouse");

    let cols = vec![("id".to_string(), "long".to_string(), false)];
    IcebergWriter::new(pool.clone(), dsn)
        .seed("sales", "orders", &cols, &[3])
        .await;

    let eng = spawn_engine_uds(
        fx,
        &db,
        &wh.path().display().to_string(),
        EngineOpts {
            control: true,
            flight: true,
            ..EngineOpts::default()
        },
    )
    .await;
    let client = EngineServingClient::connect(&eng.sock)
        .await
        .expect("connect");

    // A plain (non-CDC) table probes as Ok(None) — "not subscribable" — NOT
    // Err(Unsupported), which is what the wire client returned before this hop and what
    // `http.rs` turns into a 501.
    let table = control_plane_core::TableRef {
        schema: "sales".into(),
        name: "orders".into(),
    };
    match client.changelog_latest(&table).await {
        Ok(None) => {}
        Ok(Some(p)) => panic!("a batch table is not subscribable, got {p:?}"),
        Err(e) => panic!("must not be Unsupported over the wire: {e}"),
    }
}
```

Add `"//src/control-plane/core:core"` to the `engine-wire-serving-e2e` deps in
`src/services/query-api/BUCK:584-597`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:engine-wire-serving-e2e`
Expected: FAIL — `must not be Unsupported over the wire: unsupported by this engine: changelog feed`.

- [ ] **Step 3: Add a control channel + the three overrides**

In `src/services/query-api/src/engine_client.rs`, extend the import at `:9` to bring
`ChangeFeedPolicy` into scope (it is NOT there today):

```rust
use crate::serving::{ChangeFeedPolicy, Rows, ServingError, SqlValue, inline_params};
```

Extend the struct (`:13-30`) — the engine serves `EngineControl` and Flight on the SAME socket
(`serve.rs:30-39` already dials that one path for three clients):

```rust
pub struct EngineServingClient {
    sql: FlightSqlClient,
    table: FlightTableClient,
    /// The changelog feed rides the control plane (three unary RPCs), not Flight: a page
    /// is bounded (<= FEED_BATCH_LIMIT events) and `ndjson_feed_stream` re-serializes it
    /// to JSON anyway.
    control: engine_wire::client::GrpcQueueClient,
}

impl EngineServingClient {
    /// Connect to the engine's Flight + control services at `socket` (a UDS path).
    pub async fn connect(socket: impl Into<String>) -> Result<Self, ServingError> {
        let socket = socket.into();
        let sql = FlightSqlClient::connect(socket.clone())
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let table = FlightTableClient::connect(socket.clone())
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        let control = engine_wire::client::GrpcQueueClient::connect(socket)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))?;
        Ok(Self {
            sql,
            table,
            control,
        })
    }
}
```

In `impl ServingEngine for EngineServingClient`, beside `vector_search`, add the three overrides —
reproducing `InProcessServingEngine`'s wiring (`e2e_support.rs:200-239`) over the wire:

```rust
    async fn changelog_latest(
        &self,
        table: &control_plane_core::TableRef,
    ) -> Result<Option<std::collections::BTreeMap<i32, i64>>, ServingError> {
        self.control
            .changelog_latest(table.schema.clone(), table.name.clone())
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn changelog_feed(
        &self,
        table: &control_plane_core::TableRef,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: usize,
        policy: &ChangeFeedPolicy,
    ) -> Result<control_plane_core::ChangeFeedPage, ServingError> {
        let wire = engine_wire::client::WirePolicy {
            row_filters: policy.row_filters.clone(),
            denied: policy.denied.clone(),
            masked: policy.masked.clone(),
        };
        // No `as` cast: usize -> u64 must not silently truncate (clippy restriction).
        let limit = u64::try_from(limit).unwrap_or(u64::MAX);
        self.control
            .changelog_feed(
                table.schema.clone(),
                table.name.clone(),
                positions,
                limit,
                &wire,
            )
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn await_changelog(
        &self,
        table: &control_plane_core::TableRef,
        timeout: std::time::Duration,
    ) -> Result<(), ServingError> {
        self.control
            .await_changelog(table.schema.clone(), table.name.clone(), timeout)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }
```

(Error-class note: `be` flattens the engine's gRPC status class, so every feed fault surfaces as
`ServingError::Engine` → HTTP 500. That matches `InProcessServingEngine`, which also maps every
fault to `Engine` — the feed has no class-carrying error contract. If a reviewer asks, this is
deliberate, not an oversight.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:engine-wire-serving-e2e`
Expected: `Tests finished: Pass N. Fail 0` (all cases, including the three pre-existing ones).

- [ ] **Step 5: Commit**

```bash
git add src/services/query-api/src/engine_client.rs \
        src/services/query-api/tests/engine_wire_serving_e2e.rs \
        src/services/query-api/BUCK
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "feat(query-api): serve the changelog feed over the production wire"
```

---

### Task 5: The wire e2e — the feed streams, governed, over a socket

Every existing subscribe e2e reads through `InProcessServingEngine`; **the feed has never crossed a
socket in any test.** This is the acceptance test.

**The governance case is the load-bearing one:** it is the ONLY test that proves the
caller-resolved `WirePolicy` actually crosses the wire and is enforced engine-side by
`GovernedTableProvider`. Every other case here would still pass with governance completely broken.
It is therefore written out in full, and it covers all three policy channels — **masked**,
**denied**, and **row-filters** (the last is the newest serde path: `RowFilter` is a recursive core
enum now JSON-round-tripped through `policy_json`).

**Files:**
- Create: `src/services/query-api/tests/stream_subscribe_wire_e2e.rs`
- Modify: `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `spawn_engine_full` (Task 3); `EngineServingClient` (Task 4); `declare_cdc_table` (Task 2); `get_ndjson(Arc<PgControlPlane>, Arc<dyn ServingEngine>, &str, &str, usize, Duration) -> (StatusCode, Vec<Value>)` (`e2e_support.rs:332`); `grant_writer_role(cp, &TypeName) -> (SubjectId, RoleId)` (`e2e_support.rs:1079` — NOT `grant_writer`, which discards the role); `subject_with_role` (`:241`); `grant_read` (`:250`); `grant_read_columns(cp, role, type, deny, mask)` (`:777`); `grant_read_filtered` (`:753`); `define_widget` (`:983`); `run_action` / `ActionDeps`.

- [ ] **Step 1: Write the test**

Create `src/services/query-api/tests/stream_subscribe_wire_e2e.rs`:

```rust
//! Subscribe over the PRODUCTION WIRE (road-stream-subscribe-wire): the same
//! `GET /objects/{type}/changes` surface every other subscribe e2e drives, but read
//! through `EngineServingClient` against a real engine socket instead of the in-process
//! engine. Before this item the route answered 501 on this path.
//!
//!   1. the feed streams (200, not 501), ordered, gapless, framing-free;
//!   2. a cursor resumes gaplessly and dup-free;
//!   3. governance holds over the wire — masked, denied, AND row-filtered (the
//!      caller-resolved WirePolicy crosses as JSON and is enforced engine-side);
//!   4. the long-poll WAKES on a write (not just times out);
//!   5. a non-CDC type is 400, not 501.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use control_plane_core::{CompareOp, ObjectType, RoleId, RowFilter, ScalarValue, SubjectId};
use control_plane_postgres::PgControlPlane;
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{
    InProcessServingEngine, connect_gov_client, declare_cdc_table, define_widget, get_ndjson,
    grant_read, grant_read_columns, grant_read_filtered, grant_writer_role, spawn_engine_full,
    subject_with_role,
};
use query_api::action::{ActionDeps, run_action};
use query_api::engine_action_client::EngineActionClient;
use query_api::engine_client::EngineServingClient;
use serde_json::json;

fn assert_no_loom_keys(lines: &[serde_json::Value]) {
    for l in lines {
        let fields = l["fields"].as_object().expect("fields object");
        assert!(
            !fields.keys().any(|k| k.starts_with("loom_")),
            "framing leaked into fields: {l}"
        );
    }
}

fn offsets(lines: &[serde_json::Value]) -> Vec<i64> {
    lines
        .iter()
        .map(|l| l["offset"].as_i64().expect("offset"))
        .collect()
}

fn ids(lines: &[serde_json::Value]) -> Vec<i64> {
    lines
        .iter()
        .map(|l| l["fields"]["id"].as_i64().expect("fields.id"))
        .collect()
}

struct Wire {
    cp: PgControlPlane,
    cp_arc: Arc<PgControlPlane>,
    read_eng: Arc<dyn query_api::serving::ServingEngine>,
    role: RoleId,
    sock: String,
    engine: EngineActionClient,
    serving: InProcessServingEngine,
    subj: SubjectId,
    _guard: loom_test_flight::EngineGuard,
    _warehouse: tempfile::TempDir,
}

impl Wire {
    /// Run a governed action as the harness's writer subject.
    async fn write(&self, action: &str, body: serde_json::Value) {
        let deps = ActionDeps {
            cp: &self.cp,
            action_engine: &self.engine,
            serving: &self.serving,
        };
        run_action(
            action,
            body.as_object().expect("object body"),
            &self.subj,
            &deps,
        )
        .await
        .unwrap_or_else(|e| panic!("{action}: {e:?}"));
    }
}

/// `main.widget` as a 1-bucket CDC table with 4 events across a flush boundary:
/// `+I(1)` @0, `-U/+U(1)` @1,2 — flush — `+I(2)` @3. ALSO defines `Gadget`, a type bound
/// to a plain (NON-CDC) mirror table, so the 400 case is genuine rather than a 404.
/// Reads go over the WIRE.
async fn setup(fx: &PgFixture) -> Wire {
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    declare_cdc_table(&cp, &pool, "main", "widget", 1).await;
    let widget = define_widget(&cp).await;
    // `grant_writer_role` (not `grant_writer`) — the governance cases need the RoleId.
    let (subj, role) = grant_writer_role(&cp, &widget).await;

    // A plain, NON-CDC mirror table + a type bound to it. `declare_cdc` is deliberately
    // NOT called, so `changelog_positions_latest` returns None => `present: false` =>
    // query-api's 400. Without this, `/objects/Gadget/changes` would 404 at type
    // resolution and never reach the probe at all.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    ensure_table(&mut tx, "main", "gadget", at0)
        .await
        .expect("ensure_table gadget");
    tx.commit().await.expect("commit");
    cp.ontology()
        .define_type(
            ObjectType::build("Gadget", ("main", "gadget"))
                .prop_req("id", "Long")
                .identity("id")
                .done(),
        )
        .await
        .expect("define Gadget");
    grant_read(&cp, &role, "Gadget").await;

    let (sock, guard) =
        spawn_engine_full(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX).await;
    let engine = EngineActionClient::connect(sock.clone())
        .await
        .expect("connect EngineActionClient");
    // Writes still go through the action path (which needs a serving engine for
    // current-state reads); the FEED is what this file reads over the wire.
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));

    let w = Wire {
        cp_arc: Arc::new(cp.clone()),
        cp,
        // THE substitution: the read engine is the production wire client.
        read_eng: Arc::new(
            EngineServingClient::connect(sock.clone())
                .await
                .expect("connect EngineServingClient"),
        ),
        role,
        sock: sock.clone(),
        engine,
        serving,
        subj,
        _guard: guard,
        _warehouse: warehouse,
    };

    w.write("createWidget", json!({ "id": "1", "name": "a", "qty": "1" }))
        .await;
    w.write("updateWidget", json!({ "id": "1", "qty": "9" })).await;
    connect_gov_client(&w.sock)
        .await
        .flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");
    w.write("createWidget", json!({ "id": "2", "name": "b", "qty": "2" }))
        .await;

    w
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_streams_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=4",
        "writer",
        5,
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "no more 501 on the wire: {lines:?}");
    assert_eq!(lines.len(), 4, "4 events across the flush boundary: {lines:?}");
    for l in &lines {
        for k in ["bucket", "offset", "change_kind", "fields", "cursor"] {
            assert!(l.get(k).is_some(), "line missing `{k}`: {l}");
        }
    }
    assert_no_loom_keys(&lines);
    assert_eq!(offsets(&lines), vec![0, 1, 2, 3], "ordered and gapless");

    let kinds: Vec<&str> = lines
        .iter()
        .map(|l| l["change_kind"].as_str().expect("change_kind"))
        .collect();
    assert_eq!(
        kinds,
        vec!["+I", "-U", "+U", "+I"],
        "the full change sequence, incl. the -U before-image, crossed the wire"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_resume_is_gapless_and_dup_free_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    let (status, first) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=2",
        "writer",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(offsets(&first), vec![0, 1]);

    let cursor = first[1]["cursor"].as_str().expect("cursor").to_string();
    let (status, rest) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        &format!("/objects/Widget/changes?max_events=2&cursor={cursor}"),
        "writer",
        3,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(offsets(&rest), vec![2, 3], "resume: no gap, no duplicate");
}

/// THE load-bearing governance test: the caller-resolved policy crosses the wire as JSON
/// and is enforced ENGINE-side. Covers all three channels — masked, denied, row-filter.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn governance_holds_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // --- Masked + denied: `qty` masked, `name` denied. ---
    let (_m, mrole) = subject_with_role(&h.cp, "masked").await;
    grant_read_columns(
        &h.cp,
        &mrole,
        "Widget",
        vec!["name".into()],
        vec!["qty".into()],
    )
    .await;

    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=4",
        "masked",
        5,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(lines.len(), 4, "all events, none dropped: {lines:?}");
    assert!(
        lines.iter().all(|l| l["fields"]["qty"] == json!("***")),
        "masked column is '***' on EVERY event, over the wire: {lines:?}"
    );
    assert!(
        lines.iter().all(|l| l["fields"].get("name").is_none()),
        "denied column is ABSENT from every event, over the wire: {lines:?}"
    );
    assert!(
        lines.iter().all(|l| l["fields"]["id"].is_i64()),
        "un-restricted columns are untouched: {lines:?}"
    );

    // --- Row filter: `id = 1` hides every event of identity 2. This is the newest serde
    // path — RowFilter is a recursive core enum, JSON-round-tripped through policy_json.
    let (_r, rrole) = subject_with_role(&h.cp, "restricted").await;
    grant_read_filtered(
        &h.cp,
        &rrole,
        "Widget",
        RowFilter::Compare {
            property: "id".into(),
            op: CompareOp::Eq,
            value: ScalarValue::Int(1),
        },
    )
    .await;

    // Sanity: the unfiltered feed DOES contain an id=2 event, so the filter below is a
    // genuine discrimination and not an accident of ordering.
    let (_s, all) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=4",
        "writer",
        5,
        Duration::from_secs(10),
    )
    .await;
    assert!(ids(&all).contains(&2), "sanity: id=2 is in the raw feed");

    let (status, filtered) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Widget/changes?max_events=3",
        "restricted",
        4,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{filtered:?}");
    assert_eq!(filtered.len(), 3, "exactly id=1's 3 events: {filtered:?}");
    assert!(
        ids(&filtered).iter().all(|id| *id == 1),
        "the row filter crossed the wire: NO id=2 event ever appears: {filtered:?}"
    );
}

/// The long-poll WAKES on a write — not merely times out. Mirrors
/// `stream_subscribe_gov_e2e::unflushed_write_is_fresh`, but the wait crosses the wire
/// (`AwaitChangelog`, whose client deadline is the server timeout + 2s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_wakes_on_a_write_over_the_wire() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // A reader blocked at the tail BEFORE the new write lands.
    let cp_arc = h.cp_arc.clone();
    let read_eng = h.read_eng.clone();
    let reader = tokio::spawn(async move {
        get_ndjson(
            cp_arc,
            read_eng,
            "/objects/Widget/changes?cursor=latest&max_events=1",
            "writer",
            1,
            Duration::from_secs(10),
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    // ONE inline write, no flush — bounded by FEED_POLL_INTERVAL (1s) even if the notify
    // is missed.
    h.write("updateWidget", json!({ "id": "1", "qty": "42" }))
        .await;

    let (status, lines) = reader.await.expect("reader task joined");
    assert_eq!(status, StatusCode::OK, "{lines:?}");
    assert_eq!(
        lines.len(),
        1,
        "the blocked reader woke on the write, over the wire: {lines:?}"
    );
    assert_eq!(lines[0]["change_kind"], json!("-U"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_cdc_type_is_400_not_501() {
    let fx = PgFixture::shared();
    let h = setup(fx).await;

    // `Gadget` is a defined, readable type bound to a plain (non-CDC) table, so the
    // request reaches the probe: the engine answers `present: false` and query-api maps
    // it to 400 — NOT the 501 the wire client used to return for EVERY type.
    let (status, lines) = get_ndjson(
        h.cp_arc.clone(),
        h.read_eng.clone(),
        "/objects/Gadget/changes",
        "writer",
        1,
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a defined-but-non-CDC type is exactly 400 (the 501 path is gone): {lines:?}"
    );
}
```

BUCK:

```python
loom_fixture_test(
    name = "stream-subscribe-wire-e2e",
    crate = "stream_subscribe_wire_e2e",
    srcs = ["tests/stream_subscribe_wire_e2e.rs"],
    crate_root = "tests/stream_subscribe_wire_e2e.rs",
    deps = [
        ":query-api",
        ":e2e-support",
        "//src/control-plane/core:core",
        "//src/control-plane/postgres:postgres",
        "//src/testing:flight",
        "//third-party:axum",
        "//third-party:serde_json",
        "//third-party:tempfile",
        "//third-party:tokio",
    ],
)
```

- [ ] **Step 2: Run it**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-wire-e2e`
Expected: `Tests finished: Pass 5. Fail 0`.

If a case fails on **behavior** (not compilation), STOP and use `superpowers:systematic-debugging` —
a real failure here means the wire hop changed feed semantics, which is exactly what this test
exists to catch. Do NOT "fix" it by weakening an assertion.

- [ ] **Step 3: Full-suite regression**

Run: `buck2 test --console none -j 8 //src/...`
Expected: `Tests finished: Pass N. Fail 0`. Part A touched a read path shared by every feed test,
and Task 3 changed `EngineControlService`'s shape across the engine, worker, and testing crates.
(`subscribe_http.rs`'s 501 assertion still passes — its stub engine keeps the trait defaults, which
nothing here changes.)

- [ ] **Step 4: Commit**

```bash
git add src/services/query-api/tests/stream_subscribe_wire_e2e.rs \
        src/services/query-api/BUCK
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "test(stream): subscribe feed e2e over the production wire"
```

---

### Task 6: Close the register items + fold the capability into the docs

The registers carry **open work only**: a closing PR REMOVES the entry and documents the landed
capability under `docs/system-capabilities/`.

**Files:**
- Modify: `docs/ROADMAP.md` — remove `road-stream-subscribe-wire` (`:20-21`) **and fix `:23`** (see below)
- Modify: `docs/ISSUES.md` — remove `iss-stream-feed-torn-read` (`:30-31`)
- Modify: `docs/system-capabilities/stream.md` (`:325-328`, `:581-585`)
- Modify: `docs/superpowers/specs/2026-07-12-stream-log-table-subscribe-design.md` (the sibling spec's ticket assumption)

- [ ] **Step 1: Run the docs skill**

Use the `loom-docs-update` skill — it closes the two resolved items and stages the edits alongside
the work. Both items go together: `iss-stream-feed-torn-read` is cross-linked from the roadmap item.

- [ ] **Step 2: Fix the DANGLING LINK and the stale "wire ticket" promise**

`docs/ROADMAP.md:23` (`road-stream-log-table-subscribe`) contains `[[road-stream-subscribe-wire]]`.
Removing the wire item makes that link dangle, and `tools/docs.sh validate` (`:161-166`) **fails on
any unresolved `[[link]]`** — so Step 4 below will fail unless this is fixed.

That same sentence also says *"the **wire ticket** from `[[road-stream-subscribe-wire]]` serves log
tables transparently"*. Under spec deviation 2 **there is no wire ticket**. Rewrite it to: the
kind dispatch lives in the `ChangelogFeed` unary RPC (engine-side), which is kind-agnostic, so log
tables slot in without a wire change — and drop the `[[…]]` cross-link (the item is closed; name
the PR instead).

Apply the same correction to the sibling spec
(`docs/superpowers/specs/2026-07-12-stream-log-table-subscribe-design.md`), which was written
against the ticket assumption — otherwise the next agent to claim `road-stream-log-table-subscribe`
will plan against a Flight ticket that does not exist.

- [ ] **Step 3: Rewrite the stale capability prose**

`docs/system-capabilities/stream.md:325-328` currently says the feed is "served today by the
**in-process engine only** … the route answers `501` on the production wire deployment until the
engine-wire hop lands (`#fut-stream-subscribe-wire`)". That is now false. Replace it with the
shipped shape:

- the feed is served on the production wire by three `EngineControl` RPCs (`ChangelogLatest` /
  `ChangelogFeed` / `AwaitChangelog`); the bounded page crosses as `page_json` (serde
  `ChangeFeedPage`), the same `*_json` convention the governance reads use, and the engine clamps
  `limit`;
- governance is enforced **engine-side** from the caller-resolved `WirePolicy` — the subject never
  crosses the wire;
- **the consistency contract** (the subtle part a future reader needs): one feed page is read at
  one pinned pair of snapshots (`current_snapshots_pair` — one SQL statement, one Postgres MVCC
  snapshot), so a flush committing mid-read can change neither tier and the inline-XOR-files
  invariant holds. The inline tier is as-of by construction (`mvcc_live_pred`); the file tier is
  pinned explicitly. **Do not refactor the pair read back into two `current_snapshot` calls** —
  that is exactly `iss-stream-feed-torn-read`, and it drops events silently.

Also delete the `#fut-stream-subscribe-wire` bullet at `:581-585`.

- [ ] **Step 4: Validate the registers**

Run: `bash tools/docs.sh validate`
Expected: no errors (no dangling `[[…]]` links; every `spec:` slug still resolves).

- [ ] **Step 5: Commit**

```bash
git add docs/
buck2 run //tools:prek -- run --all-files
git add -A && git commit -m "docs(registers): close road-stream-subscribe-wire + iss-stream-feed-torn-read"
```

---

## Final review (before the PR)

Per `loom-work-checkout`, the whole-implementation review MUST include the **metric gate**:

- [ ] `loom-complexity diff` — flag any NEW hotspot over the census thresholds (cc > 15, cognitive > 15, MI < 20, SLOC > 100).
- [ ] `loom-duplication diff` — flag any NEW cross-file duplication pair ≥ 20 lines. Watch: the per-file `setup` in the new e2e (the reason Task 2 extracted `declare_cdc_table` into `e2e_support` rather than pasting a third copy), and the three `ServingEngine` feed overrides, which necessarily resemble `InProcessServingEngine`'s (same shape, different transport).
- [ ] Findings are advisory, not auto-blocking — fix each, or justify it explicitly in the PR description.

The PR body MUST record the two **spec deviations** (no `inline_batch_at`; JSON-over-`EngineControl`
instead of a Flight ticket) with their reasons, since a reviewer diffing the PR against the spec
will otherwise read them as omissions.

## Acceptance (from the spec)

1. `GET /objects/{type}/changes` serves a live NDJSON feed on the production-wire deployment (no 501), with cursors, long-poll, and governance intact. → **Task 5** (all five cases: stream, resume, governance incl. row-filters, long-poll wake, 400-not-501).
2. The torn-read interleave test proves no event loss/duplication under a concurrent flush + write. → **Task 2** (pinned-page test + as-of inline test + a real-concurrency paging test, with a direction-correct mutation check proving they have teeth).
3. `iss-stream-feed-torn-read` closes with this item. → **Task 6**.
4. Existing suites green (`buck2 test //src/...`). → **Task 5, Step 3**.
