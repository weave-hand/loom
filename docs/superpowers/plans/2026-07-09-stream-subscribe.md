# Stream Subscribe / Tail Feed Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A governed, offset-resumable, ordered change-event feed over a declared CDC table's durable changelog: `GET /objects/{type}/changes` streams newline-delimited JSON change events (`+I/−U/+U/−D`, row image) ordered by `(bucket, offset)`, resumable from a client-held opaque cursor, driven to sub-second freshness by a `pg_notify('loom_changelog:{tid}')` wakeup fired inside the CDC inline-write commit transaction, with a poll-fallback timer bounding a missed notify.

**Architecture:** The feed is the plain disjoint `UNION ALL` of the **changelog Iceberg files** and the **base table's live inline tail** — slice-2b flush appends to the changelog and end-caps the same inline rows in one transaction (`flush_locked_cdc`, `iceberg_flush.rs:193`), so an event is inline **XOR** in files at any instant: no dedup, no flush watermark. One new substrate primitive is invented (`changelog_feed_scan` in engine-serving); everything else is reuse — `resolve_governed` gates the route, `GovernedTableProvider` enforces per-batch, `inline_live_batch_full` reads the tail, the `IcebergMirrorTableProvider` reads the files, and the queue's `pg_notify`-in-commit + `PgListener`+`timeout` shape drives freshness. The transport-agnostic logical contract (`ChangeEvent` + per-bucket positions) lives in `control-plane/core`; NDJSON and the opaque cursor are query-api HTTP framing on top of it.

**Tech Stack:** Rust, axum (chunked `Body::from_stream` NDJSON), sqlx (`PgListener`, runtime `AssertSqlSafe` — no compile-time SQL changes), DataFusion (union + sort + governed provider), Iceberg mirror, base64 (URL-safe cursor codec, new third-party dep via reindeer), buck2 `loom_fixture_test`/`rust_test`.

## Global Constraints

Carried from the spec (`docs/superpowers/specs/2026-07-08-stream-subscribe-design.md`); every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl:32`), not a bare `rust_test`, or the fixture env (PG binaries, boot-slot dir) is missing. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No `query!`/`query_scalar!` SQL changes.** Every new statement (the `pg_notify`, the feed's readbacks) is a runtime `sqlx::query(AssertSqlSafe(...))`, exactly as the queue's enqueue and the whole inline tier already do — so `tools/sqlx-prepare.sh` / `.sqlx` regeneration is NOT needed anywhere in this plan. If an implementer reaches for a compile-time macro anyway, they must run `tools/sqlx-prepare.sh` and commit `.sqlx/` (`sqlx-cache-check` enforces freshness).
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (rustfmt is a separate hook; clippy-clean ≠ lint-clean; `git add` new files first — prek skips untracked files). Markdown files end with exactly one trailing newline, no trailing whitespace.
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code. Use `#[expect(lint, reason = "...")]` for a justified local exception (precedent: `GovernedStatementQuery::encode`, `engine-wire/src/flight.rs:100`). Test code is exempted from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **The notify is fire-and-forget inside the commit tx.** `pg_notify` inside a transaction is buffered until commit, so a rolled-back write is silent; mirror `queue.rs:10`'s pg-notify-inside-the-write-tx shape exactly. It must not add a code path that can fail the write beyond what the queue's enqueue already accepts (a failed statement fails the tx — same semantics as `pg_insert`).
- **Framing stays hidden from logical reads.** The feed's emitted event carries user columns only inside `fields`; `loom_*` framing surfaces ONLY as the event envelope's `bucket`/`offset`/`change_kind` (and never as a `fields` key). `?fields=` and governance intersect on user columns only. Existing logical reads (`GET /objects/{type}`) stay byte-identical.
- **Non-regression:** the change is almost entirely additive (a new route, handler, read primitive, notify channel; one fire-and-forget statement in the two CDC inline commit paths). Existing CDC suites (`stream-cdc-e2e`, `stream-cdc-consolidate`, `stream-cdc-read-mid-window`, `stream-cdc-emission`, `stream-cdc-dual-flush`, `stream-merge-*`) must stay green and unchanged.
- **Build/test commands** (from CLAUDE.md): build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: add `-M none` to builds, scope tests; full suite locally needs `-j 8` to avoid starving the 8 PG boot-slots).

### Architecture placement (spec drift corrected — read before implementing)

The spec's Interfaces section places `ChangeEvent` and `changelog_feed_scan -> (Vec<ChangeEvent>, SubscribeCursor)` in engine-serving and `SubscribeCursor` in query-api. That is **not implementable as written**: production query-api is a **zero-DataFusion wire client** (its `rust_library` deps in `src/services/query-api/BUCK:4-33` include neither `engine-serving` nor `postgres`; only the `:e2e-support` test library links engine-serving), and engine-serving cannot depend on query-api. The plan corrects the placement while keeping every behavior the spec specifies:

1. **`ChangeEvent` + `ChangeFeedPage` live in `control_plane_core::stream`** (Task 1) — the transport-agnostic logical contract, in the one crate both sides already depend on (core already depends on serde + serde_json). `changelog_feed_scan` returns `ChangeFeedPage { events, next: BTreeMap<i32, i64> }` — the *positions map*, not a `SubscribeCursor`: the opaque cursor (type binding + base64url codec) is HTTP framing and stays in query-api (Task 2), which rebuilds it from the positions.
2. **The handler reaches the scan through the `ServingEngine` seam** (Task 5) — three new default-error trait methods (`changelog_latest`, `changelog_feed`, `await_changelog`), the exact pattern `vector_search`/`write_delta`/`current_inline_version` already use (`query-api/src/serving.rs:299-429`). The in-process engine (`e2e_support::InProcessServingEngine`, Task 6) implements them over `engine_serving::changelog_feed_scan` + the new postgres waiters — this is what both specs' Testing sections drive ("e2e over `e2e-support`"). The production wire client (`EngineServingClient`) does **not** implement them in this slice: the route answers `501 Not Implemented` on the wire engine, and the engine-wire hop (a Flight ticket mirroring `VectorSearchTicket` + a unary long-poll RPC mirroring `AwaitJobs`) is registered as a deferred follow-up in Task 8. This is consistent with the spec's non-goals (the gRPC transport work is explicitly deferred) and keeps the slice additive.
3. **Each NDJSON line carries an opaque `"cursor"` field** (the resume cursor positioned *after* that event). The spec demands both an opaque cursor AND "reconnect with a cursor built from its last-seen offsets" — impossible unless the server hands the client an encoded cursor per event. The event's logical record stays `{bucket, offset, change_kind, fields}`; `cursor` is added by the HTTP framing layer only.
4. **`?max_events=N`** is added to the route: the stream closes after emitting N events (unset = endless tail). This is the bounded-fetch knob the e2e tests need to terminate deterministically, and a legitimate API surface (a catch-up batch read).
5. **Ordering contract:** emission is ordered by `(bucket, offset)` *per scan pass* — per-bucket order is total and gapless (the contract); cross-bucket interleaving is not chronological. Resume positions are per-bucket, so no gap/dup regardless of interleaving.
6. **Stat-pruning through governance is deferred.** The resume predicate is applied via DataFrame filter above `GovernedTableProvider`, whose `scan` deliberately inner-scans full-table (`engine-serving/src/governed.rs:226-238`) — correctness over the pushdown optimization, exactly as governed reads already behave. Noted in Task 8's register update.
7. **Corrected line references** (the spec's had drifted): `resolve_governed` `query-api/src/governed.rs:59` ✓; router `http.rs:94` ✓; `GovernedTableProvider` `engine-serving/src/governed.rs:174`, its `scan` `:226` (spec said 211), `policy_for` `:152`, `TablePolicy` `:143`, `execute_governed_sql_stream` `:289` ✓; `build_serving_provider` `serving.rs:74` ✓, `build_merge_view` `:325` (spec said 288), `with_cdc_framing_fields` `:244`; `changelog_table_ref` `iceberg_landing.rs:675` (spec said 667); `inline_live_batch_full` `iceberg_inline.rs:1413` (spec said 1391); `flush_locked_cdc` `iceberg_flush.rs:193` ✓; `BucketOffsets::allocate_offset`/`peek_offset` `core/src/stream.rs:92`/`:95` (spec said 33); `StreamMeta.changelog_table_id` `core/src/stream.rs:79` (spec said 30); `pg_notify`-in-commit `queue.rs:10`, `await_jobs` `queue.rs:152` ✓; `serve_with_shutdown` `runtime/src/lib.rs:528` (spec said 518).

---

## File Structure

**Create:**
- `src/control-plane/core/tests/change_event.rs` — `ChangeEvent`/`ChangeFeedPage` unit tests (+ `rust_test` target in `core/BUCK` mirroring `:page`).
- `src/services/query-api/src/subscribe.rs` — `SubscribeCursor`, `CursorSpec`, opaque base64url codec, NDJSON feed stream builder.
- `src/services/query-api/tests/subscribe_cursor.rs` — codec unit tests (`rust_test` `subscribe-cursor`).
- `src/services/query-api/tests/subscribe_http.rs` — route gate/error tests over the memory control plane (`rust_test` `subscribe-http`, mirroring `http-smoke`).
- `src/control-plane/postgres/tests/stream_changelog_notify.rs` — notify/waiter/latest-positions fixture test (`loom_fixture_test` `stream-changelog-notify`, mirroring `stream-cdc-emission`).
- `src/services/engine-serving/src/feed.rs` — `changelog_feed_scan` (the invented primitive).
- `src/services/query-api/tests/stream_subscribe_scan.rs` — primitive-level fixture e2e (`loom_fixture_test` `stream-subscribe-scan`).
- `src/services/query-api/tests/stream_subscribe_e2e.rs` — HTTP feed e2e: flush boundary, resume, latest, multi-consumer (`loom_fixture_test` `stream-subscribe-e2e`).
- `src/services/query-api/tests/stream_subscribe_gov_e2e.rs` — governance + freshness e2e (`loom_fixture_test` `stream-subscribe-gov-e2e`).

**Modify (production):**
- `src/control-plane/core/src/stream.rs` — `ChangeEvent`, `ChangeFeedPage`; `src/control-plane/core/src/lib.rs` — re-exports.
- `src/control-plane/postgres/src/stream.rs` — `pg_notify_changelog` (crate-internal), `await_changelog`, `changelog_positions_latest` (pub).
- `src/control-plane/postgres/src/iceberg_inline.rs` — fire the notify in the two CDC inline commit paths (`inline_append_decl`'s bucketed branch, `write_inline_delta`'s CDC branch).
- `src/control-plane/postgres/src/iceberg_landing.rs:675` — `changelog_table_ref` `pub(crate)` → `pub`.
- `src/services/engine-serving/src/serving.rs` — `arrow_schema_from_mirror` (`:620`) made `pub(crate)`; `src/services/engine-serving/src/lib.rs` — `pub mod feed;` + re-export.
- `src/services/query-api/src/serving.rs` — `ServingError::Unsupported`; `ChangeFeedPolicy`; three default-error `ServingEngine` methods.
- `src/services/query-api/src/http.rs` — the `/objects/:type_name/changes` route + `get_changes` handler.
- `src/services/query-api/src/lib.rs` — `pub mod subscribe;`.
- `src/services/query-api/src/openapi.rs` — register `get_changes` in `paths(...)` (`:195-212`).
- `src/services/query-api/tests/openapi.rs` — add the route to `expected()`.
- `src/services/query-api/Cargo.toml` + `Cargo.lock` + `third-party/BUCK` — the `base64` dep (reindeer).
- `src/services/query-api/BUCK` — `base64` + `futures` on `:query-api` (futures is already a dep — verify), new test targets.
- `src/services/query-api/tests/e2e_support.rs` — `InProcessServingEngine` feed overrides + `get_ndjson` streaming reader helper.
- `docs/system-capabilities/stream.md`, `docs/ROADMAP.md`, `docs/FUTURE.md` — Task 8.

---

## Task 1: Core contract types — `ChangeEvent` + `ChangeFeedPage`

**Files:**
- Modify: `src/control-plane/core/src/stream.rs` (after `StreamMeta`, line 84)
- Modify: `src/control-plane/core/src/lib.rs` — extend the existing `pub use stream::{BucketOffsets, MergeEngine, StreamKind, StreamMeta, StreamTables};`
- Test: `src/control-plane/core/tests/change_event.rs` (new) + `rust_test` target in `src/control-plane/core/BUCK` mirroring `:page`

**Interfaces:**
- Consumes: nothing (foundation).
- Produces (later tasks rely on these EXACT names/types):
  - `pub struct ChangeEvent { pub bucket: i32, pub offset: i64, pub change_kind: String, pub fields: serde_json::Map<String, serde_json::Value> }` in `control_plane_core::stream`, re-exported as `control_plane_core::ChangeEvent`. Derives `Debug, Clone, PartialEq, Serialize, Deserialize`.
  - `pub struct ChangeFeedPage { pub events: Vec<ChangeEvent>, pub next: std::collections::BTreeMap<i32, i64> }`, re-exported as `control_plane_core::ChangeFeedPage`. Same derives.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/change_event.rs`:

```rust
//! Unit tests for the transport-agnostic changelog feed contract types.
//! External rust_test (no inline #[cfg(test)]) — see CLAUDE.md.

use std::collections::BTreeMap;

use control_plane_core::{ChangeEvent, ChangeFeedPage};

#[test]
fn change_event_serializes_to_the_wire_shape() {
    let mut fields = serde_json::Map::new();
    fields.insert("id".into(), serde_json::json!(1));
    fields.insert("qty".into(), serde_json::json!(9));
    let ev = ChangeEvent {
        bucket: 1,
        offset: 42,
        change_kind: "-U".into(),
        fields,
    };
    let json = serde_json::to_value(&ev).expect("serialize");
    assert_eq!(
        json,
        serde_json::json!({
            "bucket": 1, "offset": 42, "change_kind": "-U",
            "fields": { "id": 1, "qty": 9 }
        }),
        "the NDJSON line body is exactly the struct's serde shape"
    );
    let back: ChangeEvent = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, ev, "round-trips");
}

#[test]
fn change_feed_page_round_trips_positions() {
    let page = ChangeFeedPage {
        events: vec![],
        next: BTreeMap::from([(0, 3), (1, 0)]),
    };
    let json = serde_json::to_value(&page).expect("serialize");
    let back: ChangeFeedPage = serde_json::from_value(json).expect("deserialize");
    assert_eq!(back, page, "per-bucket next positions survive serde");
}
```

- [ ] **Step 2: Wire the test target**

In `src/control-plane/core/BUCK`, mirror the `page` `rust_test` with a `change-event` target: `srcs = ["tests/change_event.rs"]`, `crate = "change_event"`, `crate_root = "tests/change_event.rs"`, deps `[":core", "//third-party:serde_json"]`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:change-event`
Expected: FAIL — `ChangeEvent` not found.

- [ ] **Step 4: Implement**

In `src/control-plane/core/src/stream.rs`, after `StreamMeta` (line 84), add:

```rust
/// One logical change event off a CDC table's changelog feed — the
/// transport-agnostic record (`road-stream-subscribe`): NDJSON-serialized on the
/// HTTP path today, protobuf-ready for a future gRPC duplex transport. `fields`
/// holds the governed USER columns only (masked columns arrive as the mask
/// marker); the `loom_*` framing surfaces only as this envelope's
/// `bucket`/`offset`/`change_kind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub bucket: i32,
    pub offset: i64,
    /// `+I` | `-U` | `+U` | `-D` (the persisted `loom_change_kind` token).
    pub change_kind: String,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// One bounded page of the changelog feed scan: the events (ordered by
/// `(bucket, offset)`) plus the per-bucket resume positions AFTER them —
/// `next[bucket]` is the next offset a resumed scan should start from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeFeedPage {
    pub events: Vec<ChangeEvent>,
    pub next: std::collections::BTreeMap<i32, i64>,
}
```

In `src/control-plane/core/src/lib.rs`, extend the stream re-export:

```rust
pub use stream::{
    BucketOffsets, ChangeEvent, ChangeFeedPage, MergeEngine, StreamKind, StreamMeta, StreamTables,
};
```

(Core already depends on serde + serde_json — `NewJob.payload` is a `serde_json::Value`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/control-plane/core:change-event`
Expected: PASS. Then `buck2 build -v0 --console none //src/control-plane/core:core`.

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): ChangeEvent + ChangeFeedPage feed contract types

The transport-agnostic changelog-feed record and page (road-stream-subscribe),
in core so both engine-serving (producer) and query-api (HTTP framing) share
one definition. No behavior change — nothing constructs them yet."
```

---

## Task 2: `SubscribeCursor` — opaque base64url codec in query-api

**Files:**
- Create: `src/services/query-api/src/subscribe.rs`; add `pub mod subscribe;` to `src/services/query-api/src/lib.rs`
- Modify: `src/services/query-api/Cargo.toml` (add `base64 = "0.22"`), regenerate lock + `third-party/BUCK`, add `"//third-party:base64"` to the `:query-api` deps in `src/services/query-api/BUCK`
- Test: `src/services/query-api/tests/subscribe_cursor.rs` (new) + `rust_test` target `subscribe-cursor`

**Interfaces:**
- Consumes: nothing new.
- Produces (later tasks rely on these EXACT names/types):
  - `pub struct SubscribeCursor { pub t: String, pub b: std::collections::BTreeMap<i32, i64> }` in `query_api::subscribe`, with `pub fn to_opaque(&self) -> String` and `pub fn from_opaque(s: &str) -> Result<Self, String>` (unsigned base64url, `URL_SAFE_NO_PAD`, of the serde_json bytes; the `Err` string is a client-safe 400 message).
  - `pub enum CursorSpec { Earliest, Latest, Resume(SubscribeCursor) }` with `pub fn parse_cursor(raw: Option<&str>) -> Result<CursorSpec, String>` (`None`/`"earliest"` ⇒ `Earliest`, `"latest"` ⇒ `Latest`, else `from_opaque`).

- [ ] **Step 1: Add the `base64` dependency (reindeer workflow)**

`base64 0.22.1` is already in the lock graph transitively (`third-party/BUCK` has the `base64-0.22.1.crate` archive) but has no first-party alias. Follow CLAUDE.md's *Third-party Rust deps* workflow:

1. Add `base64 = "0.22"` to `src/services/query-api/Cargo.toml` `[dependencies]`.
2. `eval "$(./tools/env.sh)"` then `cargo generate-lockfile`. **Guard:** diff `Cargo.lock` against the merge-base for native/`links` crates (`zstd-sys`, `ring`) — a whole-graph re-resolve can silently move them.
3. `./tools/buckify.sh`; verify `grep -n 'name = "base64"' third-party/BUCK` now shows the alias.
4. Add `"//third-party:base64"` to the `:query-api` `rust_library` deps in `src/services/query-api/BUCK`.

- [ ] **Step 2: Write the failing test**

Create `src/services/query-api/tests/subscribe_cursor.rs`:

```rust
//! SubscribeCursor opaque-codec unit tests. External rust_test — see CLAUDE.md.

use std::collections::BTreeMap;

use query_api::subscribe::{CursorSpec, SubscribeCursor, parse_cursor};

#[test]
fn cursor_round_trips_through_the_opaque_form() {
    let c = SubscribeCursor {
        t: "Widget".into(),
        b: BTreeMap::from([(0, 3), (1, 17)]),
    };
    let opaque = c.to_opaque();
    // URL-safe: no '+', '/', '=' — usable raw in a query param.
    assert!(
        opaque.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_'),
        "opaque form is base64url without padding: {opaque}"
    );
    let back = SubscribeCursor::from_opaque(&opaque).expect("decodes");
    assert_eq!(back, c, "round-trips");
}

#[test]
fn malformed_cursors_are_client_errors() {
    assert!(SubscribeCursor::from_opaque("!!not-base64!!").is_err(), "bad alphabet");
    // Valid base64url of bytes that are not the cursor JSON.
    assert!(SubscribeCursor::from_opaque("aGVsbG8").is_err(), "not cursor JSON");
}

#[test]
fn sentinels_parse_and_opaque_dispatches_to_resume() {
    assert!(matches!(parse_cursor(None), Ok(CursorSpec::Earliest)));
    assert!(matches!(parse_cursor(Some("earliest")), Ok(CursorSpec::Earliest)));
    assert!(matches!(parse_cursor(Some("latest")), Ok(CursorSpec::Latest)));
    let c = SubscribeCursor { t: "W".into(), b: BTreeMap::from([(0, 1)]) };
    match parse_cursor(Some(&c.to_opaque())) {
        Ok(CursorSpec::Resume(got)) => assert_eq!(got, c),
        other => panic!("expected Resume, got {other:?}"),
    }
    assert!(parse_cursor(Some("???")).is_err(), "garbage is a 400, not a sentinel");
}
```

- [ ] **Step 3: Wire the test target**

In `src/services/query-api/BUCK`, mirror the `http-smoke` `rust_test` (its `name` line is `:59`) with a `subscribe-cursor` target: `srcs = ["tests/subscribe_cursor.rs"]`, deps `[":query-api"]` (add `"//third-party:serde_json"` only if the final test text needs it).

- [ ] **Step 4: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:subscribe-cursor`
Expected: FAIL — `query_api::subscribe` not found.

- [ ] **Step 5: Implement `src/services/query-api/src/subscribe.rs`**

```rust
//! The subscribe feed's HTTP framing: the client-held opaque resume cursor and
//! (Task 5) the NDJSON stream assembly. The cursor is UNSIGNED by design — a
//! tampered cursor only corrupts the consumer's own resume position; it carries
//! no authority (every connect re-runs `resolve_governed`).

use std::collections::BTreeMap;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// The client-held resume cursor: the type it was minted for (`t`, rejected on
/// mismatch so a cursor cannot be replayed across types) and the per-bucket
/// next-offset map (`b[bucket]` = first offset NOT yet consumed).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SubscribeCursor {
    pub t: String,
    pub b: BTreeMap<i32, i64>,
}

impl SubscribeCursor {
    /// Unsigned base64url (no padding) of the serde_json bytes.
    #[must_use]
    pub fn to_opaque(&self) -> String {
        #[expect(
            clippy::expect_used,
            reason = "serde_json of an owned serializable type is infallible; matches GovernedStatementQuery::encode"
        )]
        let bytes = serde_json::to_vec(self).expect("SubscribeCursor is always serializable");
        URL_SAFE_NO_PAD.encode(bytes)
    }

    /// Decode an opaque cursor. The error string is client-safe (a 400 body).
    pub fn from_opaque(s: &str) -> Result<Self, String> {
        let bytes = URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_e| "malformed cursor (not base64url)".to_string())?;
        serde_json::from_slice(&bytes).map_err(|_e| "malformed cursor".to_string())
    }
}

/// The parsed `?cursor=` parameter. `earliest` boots from offset 0 across all
/// buckets; `latest` boots from the current high-water (join-the-tail).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorSpec {
    Earliest,
    Latest,
    Resume(SubscribeCursor),
}

/// Parse `?cursor=`: absent/`earliest`/`latest` sentinels, else the opaque form.
pub fn parse_cursor(raw: Option<&str>) -> Result<CursorSpec, String> {
    match raw {
        None | Some("earliest") => Ok(CursorSpec::Earliest),
        Some("latest") => Ok(CursorSpec::Latest),
        Some(op) => SubscribeCursor::from_opaque(op).map(CursorSpec::Resume),
    }
}
```

Add `pub mod subscribe;` to `src/services/query-api/src/lib.rs` (alphabetical among the existing `pub mod` lines).

- [ ] **Step 6: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:subscribe-cursor`
Expected: PASS. Also `buck2 build -v0 --console none //src/services/query-api:query-api` (the reindeer `reindeer-check` prek hook will verify `third-party/BUCK` is in sync on commit).

- [ ] **Step 7: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query-api): opaque SubscribeCursor + base64url codec

The client-held resume cursor for the changelog feed (road-stream-subscribe):
type-bound, unsigned base64url over serde_json, with earliest/latest
sentinels. Adds the base64 third-party dep via reindeer."
```

---

## Task 3: Postgres substrate — `pg_notify` in the CDC commit tx, the waiter, and latest positions

**Files:**
- Modify: `src/control-plane/postgres/src/stream.rs` — `pg_notify_changelog`, `await_changelog`, `changelog_positions_latest`
- Modify: `src/control-plane/postgres/src/iceberg_inline.rs` — fire the notify in `inline_append_decl` (after the bucketed insert loop, ~line 675) and `write_inline_delta` (after the CDC emit branch, ~line 1166)
- Test: `src/control-plane/postgres/tests/stream_changelog_notify.rs` (new) + `loom_fixture_test` target `stream-changelog-notify` mirroring `stream-cdc-emission` (`postgres/BUCK:1321`)

**Interfaces:**
- Consumes: `pg_stream_meta` (crate-internal, `postgres/src/stream.rs:367`) / `pg_peek_offset` (`postgres/src/stream.rs:280`), `live_table_id` (`iceberg_mirror.rs:264`), the queue's waiter shape (`queue.rs:152`).
- Produces (later tasks rely on these EXACT names/types):
  - Channel name contract: `loom_changelog:{table_id}` where `table_id` is the **base** table's mirror tid, notified on every committed CDC inline write (`+I` appends and `−U/+U/−D` mutations).
  - `pub async fn await_changelog(pool: &sqlx::PgPool, table: &control_plane_core::TableRef, timeout: std::time::Duration) -> Result<()>` in `control_plane_postgres::stream` — resolves on a notify or the timeout, whichever first (never errors on timeout).
  - `pub async fn changelog_positions_latest(pool: &sqlx::PgPool, table: &control_plane_core::TableRef) -> Result<Option<std::collections::BTreeMap<i32, i64>>>` — the per-bucket high-water (`peek`) map for a declared CDC table; `Ok(None)` when `table` is not a declared CDC table (no live mirror row, no stream row, or `kind != 'cdc'`).

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/postgres/tests/stream_changelog_notify.rs`, mirroring `stream_cdc_emission.rs`'s harness (its `table`/`id_spec`/`val_spec`/`full_row_batch`/`id_batch`/`lin` helpers verbatim — same imports):

```rust
//! The changelog wakeup + resume substrate (road-stream-subscribe):
//!   * `changelog_positions_latest` reports the per-bucket high-water for a CDC
//!     table (matching `peek_offset`), and None for a non-CDC table;
//!   * a blocked `await_changelog` resolves PROMPTLY (well under its timeout)
//!     when a CDC inline write commits — proving the pg_notify fires inside the
//!     commit, not that the poll timer expired;
//!   * with no write, `await_changelog` returns Ok at its timeout (poll fallback).
//! loom_fixture_test (Postgres).

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use control_plane_core::{
    BucketOffsets, ColumnSpec, EventType, LineageEvent, RunId, StreamTables, TableRef,
};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_inline;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use control_plane_postgres::stream::{await_changelog, changelog_positions_latest};

// ... copy stream_cdc_emission.rs's table()/id_spec()/val_spec()/full_row_batch()/lin() ...

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notify_fires_in_commit_and_latest_positions_track_peek() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let table = table();
    let cols = vec![id_spec(), val_spec()];

    // ensure_table + declare_cdc(2 buckets, "id") — the stream_cdc_emission shape.
    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, &table.schema, &table.name, at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");

    // (a) Not-yet-written CDC table: positions exist and are all 0.
    let empty = changelog_positions_latest(&pool, &table)
        .await
        .expect("latest")
        .expect("declared cdc table has positions");
    assert_eq!(empty, std::collections::BTreeMap::from([(0, 0), (1, 0)]));

    // (b) A blocked waiter resolves promptly when a +I commits. The waiter's
    // timeout is 10s; the write lands ~immediately; anything under 5s proves the
    // NOTIFY path woke it (a missed notify would sleep the full 10s).
    let waiter = {
        let pool = pool.clone();
        let table = table.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            await_changelog(&pool, &table, Duration::from_secs(10))
                .await
                .expect("await_changelog");
            started.elapsed()
        })
    };
    // Give the listener time to subscribe before the write commits.
    tokio::time::sleep(Duration::from_millis(300)).await;
    iceberg_inline::inline_append(&pool, &table, &cols, &full_row_batch(1, 100), lin(), None, None)
        .await
        .expect("+I append");
    let woke_after = waiter.await.expect("join");
    assert!(
        woke_after < Duration::from_secs(5),
        "notify (not the 10s poll fallback) woke the waiter: {woke_after:?}"
    );

    // (c) Positions advanced to peek: id=1's bucket now peeks 1, the other 0.
    let after = changelog_positions_latest(&pool, &table)
        .await
        .expect("latest")
        .expect("positions");
    let mut want = std::collections::BTreeMap::new();
    for b in 0..2 {
        want.insert(b, cp.peek_offset(tid, b).await.expect("peek"));
    }
    assert_eq!(after, want, "latest positions == BucketOffsets::peek per bucket");
    assert_eq!(after.values().sum::<i64>(), 1, "exactly one event allocated");

    // (d) A mutation (write_inline_delta -U/+U) also notifies. Same waiter shape.
    let waiter = {
        let pool = pool.clone();
        let table = table.clone();
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            await_changelog(&pool, &table, Duration::from_secs(10))
                .await
                .expect("await_changelog");
            started.elapsed()
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    let v = iceberg_inline::current_inline_version(&pool, &table, &[id_spec()], "id", &id_batch(1))
        .await
        .expect("version");
    iceberg_inline::write_inline_delta(
        &pool, &table, &cols, "id", false, &full_row_batch(1, 200),
        Some((&cols, &full_row_batch(1, 100))), lin(), v, None,
    )
    .await
    .expect("update");
    let woke_after = waiter.await.expect("join");
    assert!(woke_after < Duration::from_secs(5), "mutation notify: {woke_after:?}");

    // (e) No write: the waiter returns Ok at its (short) poll-fallback timeout.
    let started = std::time::Instant::now();
    await_changelog(&pool, &table, Duration::from_millis(400))
        .await
        .expect("timeout is Ok, not an error");
    assert!(started.elapsed() >= Duration::from_millis(300), "waited out the timeout");

    // (f) Non-CDC table reads None.
    let plain = TableRef { schema: "sales".into(), name: "plain".into() };
    let mut tx = pool.begin().await.expect("begin");
    let at = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    ensure_table(&mut tx, &plain.schema, &plain.name, at)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    assert!(
        changelog_positions_latest(&pool, &plain)
            .await
            .expect("latest")
            .is_none(),
        "a non-stream table has no feed positions"
    );
}
```

(Adjust `write_inline_delta`'s argument list to the real signature at `iceberg_inline.rs:1036` — `(pool, table, columns, id_column, tombstone, batch, before, lineage, expected_version, consolidate_threshold)` — and `current_inline_version`'s to how `stream_cdc_emission.rs` calls it. Copy those call shapes from that file verbatim.)

Wire a `loom_fixture_test` target `stream-changelog-notify` in `src/control-plane/postgres/BUCK` mirroring `stream-cdc-emission` (line 1321): same deps (`:postgres`, `//src/control-plane/core:core`, arrow-array, arrow-schema, serde_json, sqlx, time, tokio, uuid).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/postgres:stream-changelog-notify`
Expected: FAIL — `await_changelog`/`changelog_positions_latest` not found (compile error).

- [ ] **Step 3: Implement the three helpers in `postgres/src/stream.rs`**

Append (using the module's existing `backend` error mapper and imports; add `use sqlx::AssertSqlSafe;`-style imports as the module already does for runtime queries — check the top of the file):

```rust
/// Fire the changelog wakeup for a CDC base table's inline write. MUST be called
/// INSIDE the write's transaction: `pg_notify` in a tx is buffered until commit,
/// so a rolled-back write is silent — the same fire-and-forget-in-commit shape as
/// the queue's enqueue (`queue::pg_insert`). Channel: `loom_changelog:{table_id}`.
pub(crate) async fn pg_notify_changelog<'e, E: sqlx::PgExecutor<'e>>(
    ex: E,
    table_id: i64,
) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(
        "select pg_notify('loom_changelog:' || $1::text, '')",
    ))
    .bind(table_id)
    .execute(ex)
    .await
    .map_err(backend)?;
    Ok(())
}

/// Block until a CDC inline write commits against `table`, or `timeout` elapses —
/// whichever first (the poll-fallback bound for a missed notify). Never errors on
/// timeout. Mirrors the queue's `await_jobs` waiter (`queue.rs:152`). Note the
/// listen-after-scan race: an event committed between the caller's empty scan and
/// this LISTEN is missed and picked up on the next poll — bounded by `timeout`.
pub async fn await_changelog(
    pool: &sqlx::PgPool,
    table: &TableRef,
    timeout: std::time::Duration,
) -> Result<()> {
    let tid = {
        let mut conn = pool.acquire().await.map_err(backend)?;
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name)
            .await?
            .ok_or_else(|| {
                ControlPlaneError::NotFound(format!(
                    "no mirror table for {}.{}",
                    table.schema, table.name
                ))
            })?
    };
    let mut listener = sqlx::postgres::PgListener::connect_with(pool)
        .await
        .map_err(backend)?;
    listener
        .listen(&format!("loom_changelog:{tid}"))
        .await
        .map_err(backend)?;
    // A notification, or the polling-fallback timeout — whichever first.
    drop(tokio::time::timeout(timeout, listener.recv()).await);
    Ok(())
}

/// The per-bucket high-water offsets (`BucketOffsets::peek_offset` per bucket)
/// for a declared CDC table — the `?cursor=latest` join-the-tail positions, and
/// the feed handler's "is this subscribable + how many buckets" probe. `None`
/// when `table` has no live mirror row, no stream declaration, or is not CDC.
pub async fn changelog_positions_latest(
    pool: &sqlx::PgPool,
    table: &TableRef,
) -> Result<Option<std::collections::BTreeMap<i32, i64>>> {
    let mut conn = pool.acquire().await.map_err(backend)?;
    let Some(tid) =
        crate::iceberg_mirror::live_table_id(&mut conn, &table.schema, &table.name).await?
    else {
        return Ok(None);
    };
    let Some(meta) = pg_stream_meta(&mut *conn, tid).await? else {
        return Ok(None);
    };
    if meta.kind != control_plane_core::StreamKind::Cdc {
        return Ok(None);
    }
    let mut out = std::collections::BTreeMap::new();
    for bucket in 0..meta.bucket_count {
        out.insert(bucket, pg_peek_offset(&mut *conn, tid, bucket).await?);
    }
    Ok(Some(out))
}
```

(Match the module's existing import style — `TableRef`/`ControlPlaneError` may need adding to its `use control_plane_core::…` line. `pg_stream_meta`/`pg_peek_offset`/`live_table_id` are crate-internal and already visible here.)

- [ ] **Step 4: Fire the notify from the two CDC inline commit paths**

In `src/control-plane/postgres/src/iceberg_inline.rs`:

**(a) `inline_append_decl`** — inside the `if let Some(bc) = effective` branch, immediately after the per-row insert loop ends (after line ~675, before the closing brace of the branch), gated on the CDC kind (`meta` is in scope from line 576):

```rust
        // Subscribe wakeup (road-stream-subscribe): one fire-and-forget notify
        // per committed CDC write batch, buffered until this tx commits. Log
        // tables don't notify (no changelog feed in this slice).
        if matches!(&meta, Some(m) if m.kind == control_plane_core::StreamKind::Cdc) {
            crate::stream::pg_notify_changelog(&mut *conn, tid).await?;
        }
```

**(b) `write_inline_delta`** — the emit logic is a 3-arm `if cdc {…} else if tombstone {…} else {…}` chain: the `if cdc {` arm opens at ~`:1109` and closes at ~`:1166`, and the whole chain closes at ~`:1208`. Insert the notify **after the entire chain closes (~`:1208`), before `tx.commit()` at ~`:1240`** — NOT at `:1166` (that is only the first arm's close; a block there is a syntax error). Variables in scope: `cdc` (`:1099`), `tid` (`:1053`), `tx` (`:1050`). Guard on `cdc` (log tables don't feed):

```rust
    // Subscribe wakeup: the -U/+U/-D events just written become visible on
    // commit; the notify is buffered with them (mirrors queue.rs's
    // pg_notify-in-commit).
    if cdc {
        crate::stream::pg_notify_changelog(&mut *tx, tid).await?;
    }
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/control-plane/postgres:stream-changelog-notify`
Expected: PASS.

Non-regression: `buck2 test --console none //src/control-plane/postgres:stream-cdc-emission //src/control-plane/postgres:stream-cdc-bucket //src/control-plane/postgres:stream-cdc-dual-flush //src/control-plane/postgres:stream-inline`
Expected: PASS (the notify is additive; no framing/row change).

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): pg_notify('loom_changelog:{tid}') in the CDC commit tx + waiter

Fire a fire-and-forget changelog wakeup inside both CDC inline-write commit
paths (append +I batch; -U/+U/-D mutations), buffered until commit like the
queue's enqueue notify. Add await_changelog (PgListener + timeout poll
fallback, mirroring await_jobs) and changelog_positions_latest (per-bucket
peek — the cursor=latest join-the-tail map). All runtime AssertSqlSafe; no
.sqlx change."
```

---

## Task 4: `changelog_feed_scan` — the ordered disjoint-union primitive in engine-serving

**Files:**
- Create: `src/services/engine-serving/src/feed.rs`; register `pub mod feed;` + `pub use feed::changelog_feed_scan;` in `src/services/engine-serving/src/lib.rs`
- Modify: `src/control-plane/postgres/src/iceberg_landing.rs:675` — `changelog_table_ref` `pub(crate) fn` → `pub fn`
- Modify: `src/services/engine-serving/src/serving.rs:620` — `fn arrow_schema_from_mirror` → `pub(crate) fn`
- Test: `src/services/query-api/tests/stream_subscribe_scan.rs` (new) + `loom_fixture_test` target `stream-subscribe-scan` in `src/services/query-api/BUCK`

**Interfaces:**
- Consumes: `ChangeEvent`/`ChangeFeedPage` (T1); `IcebergCatalog` (`.pool`, `.current_snapshot`, `.schema`, `.files_with_stats`, `.inline_live_batch_full` — `iceberg_inline.rs:1413`); `changelog_table_ref` (`iceberg_landing.rs:675`, made pub); `IcebergMirrorTableProvider::try_new_with_schema` (`serving.rs:161` call shape); `GovernedTableProvider`/`TablePolicy` (`governed.rs:174`/`:143`); `ServingStore` (`store-config/src/lib.rs:170`).
- Produces (later tasks rely on this EXACT name/signature):
  - `pub async fn changelog_feed_scan(catalog: &IcebergCatalog, base: &TableRef, serving_store: Option<&ServingStore>, positions: &BTreeMap<i32, i64>, limit: usize, policy: &TablePolicy) -> Result<ChangeFeedPage, EngineServingError>` in `engine_serving::feed` — the governed, `(bucket, offset)`-ordered, `limit`-bounded disjoint union of (changelog Iceberg files) ∪ (base live inline tail), from the per-bucket resume `positions`. `−U` events are included.

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/stream_subscribe_scan.rs`, reusing the `stream_cdc_read_mid_window.rs` harness shape (`PgFixture` → `ensure_table` → `declare_cdc(tid, 2, "id", MergeEngine::LastRow)` → `define_widget` → `grant_writer` → `spawn_engine_writer` with `flush_byte_threshold = i64::MAX` → `ActionDeps` → `run_action` writes → `connect_gov_client` + `gov.flush_table`). The test drives the PRIMITIVE directly (no HTTP):

```rust
//! changelog_feed_scan primitive e2e (road-stream-subscribe): the governed,
//! (bucket, offset)-ordered disjoint union of changelog files ∪ live inline
//! tail, resumable by per-bucket positions.
//!   1. 5 events written via governed actions (+I, -U, +U for id=1; +I, -D for
//!      id=2), all inline: scan from earliest returns all 5, ordered, -U incl.
//!   2. flush_table (events move to the changelog files, inline end-capped):
//!      the SAME scan returns the SAME 5 events — inline XOR files, no dup.
//!   3. two more events after the flush (update id=1): a scan spans the
//!      boundary (files ∪ inline) and returns all 7.
//!   4. resume: scan(limit=3) then scan(from next) concatenate to exactly the
//!      full sequence — no gap, no overlap.
//!   5. governance: a masked column reads '***' on every event; fields never
//!      carry a loom_* key.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).

use std::collections::BTreeMap;
use std::sync::Arc;

use control_plane_core::{ChangeEvent, StreamTables, TableRef};
use control_plane_postgres::fixture::PgFixture;
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_mirror::{ensure_table, next_snapshot};
use e2e_support::{InProcessServingEngine, connect_gov_client, define_widget, grant_writer};
use engine_serving::TablePolicy;
use engine_serving::feed::changelog_feed_scan;
use query_api::action::{ActionDeps, run_action};
use serde_json::json;

fn keys(evs: &[ChangeEvent]) -> Vec<(i32, i64, String)> {
    evs.iter()
        .map(|e| (e.bucket, e.offset, e.change_kind.clone()))
        .collect()
}

/// Per-bucket offsets are gapless from the given start positions, and the whole
/// sequence is sorted by (bucket, offset).
fn assert_ordered_gapless(evs: &[ChangeEvent], start: &BTreeMap<i32, i64>) {
    let k = keys(evs);
    let mut sorted = k.clone();
    sorted.sort();
    assert_eq!(k, sorted, "events ordered by (bucket, offset): {k:?}");
    let mut cursor = start.clone();
    for e in evs {
        let at = cursor.get(&e.bucket).copied().unwrap_or(0);
        assert_eq!(e.offset, at, "gapless per-bucket offsets: {k:?}");
        cursor.insert(e.bucket, at + 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_scan_unions_files_and_inline_ordered_and_resumable() {
    let fx = PgFixture::shared();
    let (cp, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;
    let warehouse = tempfile::tempdir().expect("warehouse");

    let mut tx = pool.begin().await.expect("begin");
    let at0 = next_snapshot(&mut tx, None).await.expect("next_snapshot");
    let tid = ensure_table(&mut tx, "main", "widget", at0)
        .await
        .expect("ensure_table");
    tx.commit().await.expect("commit");
    cp.declare_cdc(tid, 2, "id", control_plane_core::MergeEngine::LastRow)
        .await
        .expect("declare_cdc");
    let widget = define_widget(&cp).await;
    let subj = grant_writer(&cp, &widget).await;
    let (engine, eg) =
        e2e_support::spawn_engine_writer(fx, &db, warehouse.path(), 16 * 1024 * 1024, i64::MAX)
            .await;
    let serving = InProcessServingEngine::new(IcebergCatalog::new(pool.clone()));
    let deps = ActionDeps { cp: &cp, action_engine: &engine, serving: &serving };

    // 5 events: +I(1), -U(1), +U(1), +I(2), -D(2).
    for (action, body) in [
        ("createWidget", json!({ "id": "1", "name": "a", "qty": "1" })),
        ("updateWidget", json!({ "id": "1", "qty": "9" })),
        ("createWidget", json!({ "id": "2", "name": "b", "qty": "2" })),
        ("deleteWidget", json!({ "id": "2" })),
    ] {
        run_action(action, body.as_object().unwrap(), &subj, &deps)
            .await
            .unwrap_or_else(|e| panic!("{action}: {e:?}"));
    }

    let catalog = IcebergCatalog::new(pool.clone());
    let table = TableRef { schema: "main".into(), name: "widget".into() };
    let earliest: BTreeMap<i32, i64> = BTreeMap::from([(0, 0), (1, 0)]);
    let open = TablePolicy::default();

    // 1. All-inline scan.
    let page1 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &open)
        .await
        .expect("scan inline");
    assert_eq!(page1.events.len(), 5, "+I,-U,+U,+I,-D: {:?}", keys(&page1.events));
    assert_ordered_gapless(&page1.events, &earliest);
    let kinds: Vec<&str> = page1.events.iter().map(|e| e.change_kind.as_str()).collect();
    assert_eq!(kinds.iter().filter(|k| **k == "-U").count(), 1, "-U emitted: {kinds:?}");
    for e in &page1.events {
        assert!(
            e.fields.keys().all(|k| !k.starts_with("loom_")),
            "no framing key in fields: {:?}",
            e.fields
        );
    }
    // The -U carries the before-image (qty=1).
    let minus_u = page1.events.iter().find(|e| e.change_kind == "-U").expect("-U");
    assert_eq!(minus_u.fields.get("qty"), Some(&json!(1)), "-U before-image");

    // 2. Flush: same events, now from the changelog files. Identical keys, no dup.
    let gov = connect_gov_client(&eg.sock).await;
    gov.flush_table("main".to_string(), "widget".to_string())
        .await
        .expect("flush_table");
    let page2 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &open)
        .await
        .expect("scan files");
    assert_eq!(keys(&page2.events), keys(&page1.events), "inline XOR files: no dup, no gap");

    // 3. Two more events post-flush: the scan spans the flush boundary.
    run_action("updateWidget", json!({ "id": "1", "qty": "11" }).as_object().unwrap(), &subj, &deps)
        .await
        .expect("post-flush update");
    let page3 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &open)
        .await
        .expect("scan union");
    assert_eq!(page3.events.len(), 7, "files ∪ inline: {:?}", keys(&page3.events));
    assert_ordered_gapless(&page3.events, &earliest);

    // 4. Resume: limit 3, then from `next` — exact concatenation.
    let head = changelog_feed_scan(&catalog, &table, None, &earliest, 3, &open)
        .await
        .expect("scan head");
    assert_eq!(head.events.len(), 3);
    let tail = changelog_feed_scan(&catalog, &table, None, &head.next, 100, &open)
        .await
        .expect("scan tail");
    let mut joined = keys(&head.events);
    joined.extend(keys(&tail.events));
    assert_eq!(joined, keys(&page3.events), "resume is gapless and dup-free");

    // 5. Masked column: qty reads back as the mask marker on every event.
    let masked = TablePolicy {
        row_filters: vec![],
        denied: std::collections::HashSet::new(),
        masked: std::collections::HashSet::from(["qty".to_string()]),
    };
    let page5 = changelog_feed_scan(&catalog, &table, None, &earliest, 100, &masked)
        .await
        .expect("scan masked");
    assert!(
        page5.events.iter().all(|e| e.fields.get("qty") == Some(&json!("***"))),
        "masked column is '***' on every event"
    );

    drop(eg);
    drop(warehouse);
}
```

Wire a `loom_fixture_test` target `stream-subscribe-scan` in `src/services/query-api/BUCK`, mirroring `stream-cdc-e2e` (line 1950) and adding `"//src/services/engine-serving:engine-serving"` and `"//third-party:tempfile"` to its deps (deps: `:query-api`, `:e2e-support`, core, postgres, engine-serving, axum, serde_json, sqlx, tempfile, tokio).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-scan`
Expected: FAIL — `engine_serving::feed` not found.

- [ ] **Step 3: Implement `src/services/engine-serving/src/feed.rs`**

First the two visibility changes: `iceberg_landing.rs:675` `pub(crate) fn changelog_table_ref` → `pub fn` (keep the doc comment; it is the single place the `__changelog` naming lives — engine-serving must not duplicate it); `serving.rs:620` `fn arrow_schema_from_mirror` → `pub(crate) fn`.

Then the module (concrete sketch — adjust helper names to what `serving.rs` actually exposes; `to_serving` is `pub(crate)` at `serving.rs:65`):

```rust
//! The changelog feed scan (road-stream-subscribe): the ONE invented substrate
//! primitive. For a CDC base table, the ordered union of its durable changelog
//! files and its live inline tail, from per-bucket resume positions — a plain
//! disjoint UNION ALL (slice-2b flush appends to the changelog and end-caps the
//! same inline rows in ONE tx, so an event is inline XOR files; no dedup, no
//! flush watermark). Governance is enforced by wrapping the union in
//! `GovernedTableProvider` BEFORE the ordered read — a subject never sees an
//! event, or a column, it may not read.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use control_plane_core::{ChangeEvent, ChangeFeedPage, TableRef};
use control_plane_postgres::iceberg_catalog::IcebergCatalog;
use control_plane_postgres::iceberg_landing::changelog_table_ref;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use store_config::ServingStore;

use crate::governed::{GovernedTableProvider, TablePolicy};
use crate::serving::{
    EngineServingError, IcebergMirrorTableProvider, arrow_schema_from_mirror, to_serving,
};

/// The three reserved framing fields a feed tier presents AFTER the user
/// columns: `loom_change_kind`, `loom_bucket`, `loom_offset`. Sibling of
/// `with_cdc_framing_fields` (`serving.rs:244`), plus the bucket column the
/// feed orders/resumes on.
fn with_feed_framing_fields(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("loom_change_kind", DataType::Utf8, false));
    fields.push(Field::new("loom_bucket", DataType::Int32, true));
    fields.push(Field::new("loom_offset", DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

/// One bounded, governed, ordered page of `base`'s changelog feed from
/// per-bucket `positions` (bucket -> first offset not yet consumed; the caller
/// supplies EVERY bucket). Returns the events ordered by `(bucket, offset)`
/// (cross-bucket interleaving is positional, not chronological) plus the
/// advanced positions. `-U` before-images are included — the changelog contract
/// is full events.
pub async fn changelog_feed_scan(
    catalog: &IcebergCatalog,
    base: &TableRef,
    serving_store: Option<&ServingStore>,
    positions: &BTreeMap<i32, i64>,
    limit: usize,
    policy: &TablePolicy,
) -> Result<ChangeFeedPage, EngineServingError> {
    use control_plane_core::Catalog;

    let empty = || ChangeFeedPage { events: vec![], next: positions.clone() };
    if positions.is_empty() || limit == 0 {
        return Ok(empty());
    }

    let ctx = SessionContext::new();
    // Object stores: mirror build_serving_provider (serving.rs:85-93) — local FS
    // always; the S3 store under s3://{bucket} when configured.
    /* ...same two register_object_store calls... */

    // --- Tier 1: the changelog Iceberg files (absent until the first flush). ---
    let clog = changelog_table_ref(base);
    let file_provider = match catalog.current_snapshot(&clog).await {
        Ok(snap) => {
            let cols = catalog.schema(&clog, snap.id).await.map_err(to_serving)?.columns;
            let framed = with_feed_framing_fields(&arrow_schema_from_mirror(&cols)?);
            let files = catalog
                .files_with_stats(&clog, snap.id)
                .await
                .map_err(to_serving)?;
            if files.is_empty() {
                None
            } else {
                Some(IcebergMirrorTableProvider::try_new_with_schema(files, framed))
            }
        }
        // No changelog mirror row yet (nothing flushed): file tier absent.
        Err(control_plane_core::ControlPlaneError::NotFound(_)) => None,
        Err(e) => return Err(to_serving(e)),
    };

    // --- Tier 2: the base table's LIVE inline tail (full — keeps -U), whose
    // physical columns already carry the framing (iceberg_inline.rs:1437-1443). ---
    let base_snap = catalog.current_snapshot(base).await.map_err(to_serving)?;
    let inline_provider = match catalog
        .inline_live_batch_full(base, base_snap.id)
        .await
        .map_err(to_serving)?
    {
        Some((_tid, _row_ids, batch)) => {
            let schema = batch.schema();
            Some(MemTable::try_new(schema, vec![vec![batch]]).map_err(to_serving)?)
        }
        None => None,
    };

    // --- Disjoint UNION ALL, both tiers projected to the SAME column order:
    // [user_cols..., loom_change_kind, loom_bucket, loom_offset]. ---
    let base_cols = catalog.schema(base, base_snap.id).await.map_err(to_serving)?.columns;
    let mut names: Vec<String> = base_cols.iter().map(|c| c.name.clone()).collect();
    names.extend(["loom_change_kind".into(), "loom_bucket".into(), "loom_offset".into()]);
    let select: Vec<datafusion::prelude::Expr> = names
        .iter()
        .map(|n| datafusion::prelude::Expr::Column(datafusion::common::Column::new_unqualified(n)))
        .collect();
    let mut tiers = Vec::new();
    if let Some(f) = file_provider {
        tiers.push(ctx.read_table(Arc::new(f)).map_err(to_serving)?
            .select(select.clone()).map_err(to_serving)?);
    }
    if let Some(i) = inline_provider {
        tiers.push(ctx.read_table(Arc::new(i)).map_err(to_serving)?
            .select(select.clone()).map_err(to_serving)?);
    }
    let Some(unioned) = tiers.into_iter().reduce(|a, b| {
        // union of same-schema projections; fold errors below via try pattern
        a.union(b).unwrap_or_else(|_| unreachable!())
    }) else {
        return Ok(empty());
    };
    // (Implementation note: write the fold with explicit `?` instead of the
    // unreachable! sketch above — e.g. a small loop unioning into an Option.)

    // --- Governance BEFORE the ordered read: wrap the union view. Framing
    // columns are never denied/masked (reserved names), so ordering survives;
    // row filters and column masks apply to the user columns. ---
    let governed = GovernedTableProvider::new(unioned.into_view(), policy.clone())?;
    let df = ctx.read_table(Arc::new(governed)).map_err(to_serving)?;

    // --- Resume predicate: OR over buckets of (bucket = b AND offset >= next_b). ---
    let mut pred: Option<datafusion::prelude::Expr> = None;
    for (b, off) in positions {
        let leaf = col("loom_bucket").eq(lit(*b)).and(col("loom_offset").gt_eq(lit(*off)));
        pred = Some(match pred { None => leaf, Some(p) => p.or(leaf) });
    }
    let Some(pred) = pred else { return Ok(empty()) };

    // --- Order + bound. ---
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

    // --- Decode: framing -> envelope; every other (governed) column -> fields. ---
    let mut events = Vec::new();
    let mut next = positions.clone();
    for batch in &batches {
        /* for each row:
             - bucket  = Int32Array column "loom_bucket" value (schema.index_of)
             - offset  = Int64Array column "loom_offset" value
             - kind    = StringArray column "loom_change_kind" value
             - fields  = every non-loom_* column, cell -> serde_json::Value via a
               small `cell_to_json(array, row)` match over DataType (Utf8, Int32,
               Int64, Float64, Boolean, Date32 -> ISO date string, Timestamp
               (Microsecond) -> ISO string, null -> Value::Null). REIMPLEMENT this
               helper INSIDE `feed.rs` mirroring the value mapping in query-api's
               `serving_datafusion::batches_to_rows` (`serving_datafusion.rs:18`) —
               do NOT import it: engine-serving cannot depend on query-api.
           Then: next.insert(bucket, offset + 1); events.push(ChangeEvent { .. });
           All array downcasts via `.as_any().downcast_ref::<..>()` +
           `ok_or_else(EngineServingError::Engine)` — no indexing/unwrap. */
    }
    Ok(ChangeFeedPage { events, next })
}
```

Register in `lib.rs`: `pub mod feed;` and `pub use feed::changelog_feed_scan;`. Add any missing third-party deps to `src/services/engine-serving/BUCK` (arrow, datafusion, store-config are already there — verify `//src/services/store-config:store-config` is in the deps list at `engine-serving/BUCK:10-26`; it is, via `store-config`).

- [ ] **Step 4: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-scan`
Expected: PASS.

Non-regression: `buck2 test --console none //src/services/engine-serving:pg-scan-sql //src/services/query-api:stream-cdc-read-mid-window //src/services/query-api:stream-cdc-consolidate`
Expected: PASS (the scan is a new module; `with_cdc_framing_fields`/`build_serving_provider` untouched).

- [ ] **Step 5: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine-serving): changelog_feed_scan — governed ordered feed union

The one invented subscribe primitive: changelog Iceberg files UNION ALL the
base table's live inline tail (inline XOR files by flush atomicity — no
dedup), filtered by per-bucket resume positions, wrapped in
GovernedTableProvider, ordered by (bucket, offset), LIMIT-bounded, decoded to
ChangeEvent pages. changelog_table_ref goes pub (single home of the
__changelog naming)."
```

---

## Task 5: The governed route — `ServingEngine` seam, `GET /objects/{type}/changes`, NDJSON handler, OpenAPI

**Files:**
- Modify: `src/services/query-api/src/serving.rs` — `ServingError::Unsupported`, `ChangeFeedPolicy`, three default trait methods
- Modify: `src/services/query-api/src/subscribe.rs` — `ChangesQuery` parse helpers + `ndjson_feed_stream`
- Modify: `src/services/query-api/src/http.rs` — route (`:96` block) + `get_changes` handler
- Modify: `src/services/query-api/src/openapi.rs:195-212` — `paths(...)` registration
- Modify: `src/services/query-api/tests/openapi.rs` — `expected()` gains `("get", "/objects/{type_name}/changes")`
- Test: `src/services/query-api/tests/subscribe_http.rs` (new) + `rust_test` target `subscribe-http` mirroring `http-smoke` (`query-api/BUCK:59`)

**Interfaces:**
- Consumes: `resolve_governed`/`GovernedType`/`OnMissing` (`governed.rs:59`/`:20`/`:43`); `query_error_response` (`http.rs:751`); `Subject` (subject id at `.0`, per `handler.rs:248`); `SubscribeCursor`/`CursorSpec`/`parse_cursor` (T2); `ChangeEvent`/`ChangeFeedPage` (T1).
- Produces (later tasks rely on these EXACT names/types):
  - `ServingError::Unsupported(String)` (query-api) → handler maps to `501 Not Implemented`.
  - `pub struct ChangeFeedPolicy { pub row_filters: Vec<control_plane_core::RowFilter>, pub denied: Vec<String>, pub masked: Vec<String> }` in `query_api::serving`.
  - On `trait ServingEngine`, all defaulting to `Err(ServingError::Unsupported(...))` (the `vector_search` default-method pattern):
    - `async fn changelog_latest(&self, table: &control_plane_core::TableRef) -> Result<Option<std::collections::BTreeMap<i32, i64>>, ServingError>;`
    - `async fn changelog_feed(&self, table: &control_plane_core::TableRef, positions: &std::collections::BTreeMap<i32, i64>, limit: usize, policy: &ChangeFeedPolicy) -> Result<control_plane_core::ChangeFeedPage, ServingError>;`
    - `async fn await_changelog(&self, table: &control_plane_core::TableRef, timeout: std::time::Duration) -> Result<(), ServingError>;`
  - Route `GET /objects/{type_name}/changes?cursor=&fields=&max_events=` → chunked `application/x-ndjson`; each line `{"bucket":…,"offset":…,"change_kind":"…","fields":{…},"cursor":"<opaque>"}` where `cursor` resumes AFTER that line's event.
  - Constants in `subscribe.rs`: `pub const FEED_BATCH_LIMIT: usize = 256;` `pub const FEED_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);`

- [ ] **Step 1: Write the failing test**

Create `src/services/query-api/tests/subscribe_http.rs`, reusing `http_smoke.rs`'s memory-control-plane router harness (copy its app-construction + auth shape; it drives `protect(router(AppState{…}))` with the memory adapter and a stub serving engine):

```rust
//! /objects/{type}/changes gate + error surface, over the memory control plane
//! and a default (no-feed) ServingEngine — the pre-stream failure paths that
//! need no Postgres:
//!   * 403 before existence for an ungranted subject (deny-before-existence);
//!   * 404 for a granted-but-unknown type;
//!   * 400 for a malformed cursor and for a cursor minted for another type;
//!   * 400 for ?fields= naming an unknown/denied column;
//!   * 501 when the engine does not serve the feed (the production wire client
//!     until the engine-wire hop lands — see FUTURE).
//! rust_test (no fixture).

use query_api::subscribe::SubscribeCursor;
// ... http_smoke.rs's imports/harness: memory ControlPlane, seeded type
// "Widget" + a Read grant for subject "reader", a ServingEngine stub that only
// implements fetch_rows (feed methods keep their defaults), and a `get(uri,
// subject) -> (StatusCode, body)` driver ...

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_route_gates_and_maps_errors() {
    // 403: no grant, existence not revealed.
    let (status, _) = get("/objects/Widget/changes", "stranger").await;
    assert_eq!(status, axum::http::StatusCode::FORBIDDEN);

    // 404: granted subject, unknown type.
    let (status, _) = get("/objects/Nope/changes", "reader_all").await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);

    // 400: malformed cursor (fails before any engine call).
    let (status, _) = get("/objects/Widget/changes?cursor=%21%21", "reader").await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);

    // 400: cursor minted for another type.
    let alien = SubscribeCursor { t: "Other".into(), b: Default::default() }.to_opaque();
    let (status, _) = get(&format!("/objects/Widget/changes?cursor={alien}"), "reader").await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);

    // 400: unknown ?fields= column.
    let (status, _) = get("/objects/Widget/changes?fields=nope", "reader").await;
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);

    // 501: the default engine serves no feed (cursor omitted => earliest, which
    // probes changelog_latest first).
    let (status, _) = get("/objects/Widget/changes", "reader").await;
    assert_eq!(status, axum::http::StatusCode::NOT_IMPLEMENTED);
}
```

(Adapt subject seeding to the http_smoke harness's grant helpers; the ORDER the handler checks things — resolve_governed → cursor syntax/type-binding → fields → engine probe — is part of the contract this test pins.)

Wire `subscribe-http` in `src/services/query-api/BUCK` mirroring `http-smoke` (same deps: `:query-api`, core, memory, runtime, async-trait, axum, http-body-util, serde_json, tokio, tower).

- [ ] **Step 2: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/query-api:subscribe-http`
Expected: FAIL — route not found (404 where 403/400/501 expected) / compile error on `ServingError::Unsupported`.

- [ ] **Step 3: Extend `serving.rs` — the seam**

Add to `ServingError` (`serving.rs:75-97`):

```rust
    /// The engine does not implement this capability (e.g. the changelog feed on
    /// the wire client before the engine-wire hop lands) → 501.
    #[error("unsupported by this engine: {0}")]
    Unsupported(String),
```

Add after `StepWrite`:

```rust
/// The resolved per-connect governance the feed seam ships to the engine: the
/// subject's folded row filters + denied/masked columns (from `GovernedType`),
/// engine-side re-applied per batch via `GovernedTableProvider`. Vec (not
/// HashSet) so the seam type is order-stable and wire-encodable later.
#[derive(Debug, Clone, Default)]
pub struct ChangeFeedPolicy {
    pub row_filters: Vec<control_plane_core::RowFilter>,
    pub denied: Vec<String>,
    pub masked: Vec<String>,
}
```

Add the three default methods to `trait ServingEngine` (after `vector_search`, mirroring its default-error shape):

```rust
    /// The per-bucket high-water positions of `table`'s changelog (`None` = not
    /// a declared CDC table). Doubles as the "is this subscribable" probe.
    async fn changelog_latest(
        &self,
        table: &control_plane_core::TableRef,
    ) -> Result<Option<std::collections::BTreeMap<i32, i64>>, ServingError> {
        let _ = table;
        Err(ServingError::Unsupported("changelog feed".into()))
    }

    /// One bounded, governed, ordered page of `table`'s changelog feed from
    /// per-bucket `positions`. See `engine_serving::changelog_feed_scan`.
    async fn changelog_feed(
        &self,
        table: &control_plane_core::TableRef,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: usize,
        policy: &ChangeFeedPolicy,
    ) -> Result<control_plane_core::ChangeFeedPage, ServingError> {
        let _ = (table, positions, limit, policy);
        Err(ServingError::Unsupported("changelog feed".into()))
    }

    /// Block until a CDC write commits against `table` or `timeout` elapses
    /// (the notify wakeup + poll fallback). Ok on timeout.
    async fn await_changelog(
        &self,
        table: &control_plane_core::TableRef,
        timeout: std::time::Duration,
    ) -> Result<(), ServingError> {
        let _ = (table, timeout);
        Err(ServingError::Unsupported("changelog feed".into()))
    }
```

- [ ] **Step 4: The NDJSON stream builder in `subscribe.rs`**

```rust
/// Feed pacing: max events fetched per scan pass (memory bound / natural
/// backpressure) and the poll-fallback interval bounding a missed notify.
pub const FEED_BATCH_LIMIT: usize = 256;
pub const FEED_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

struct FeedState {
    serving: std::sync::Arc<dyn crate::serving::ServingEngine>,
    table: control_plane_core::TableRef,
    type_name: String,
    positions: std::collections::BTreeMap<i32, i64>,
    policy: crate::serving::ChangeFeedPolicy,
    /// `None` = all governed columns; `Some` = the validated `?fields=` subset.
    fields: Option<Vec<String>>,
    /// Remaining `?max_events=` budget; `None` = endless tail.
    remaining: Option<u64>,
}

/// The chunked NDJSON body: loop { scan a page; emit its lines; else wait for
/// the notify/poll wakeup }. Ends when the max_events budget is exhausted or an
/// engine fault occurs (logged server-side; a stream in flight cannot change
/// its status code). Client disconnect drops the stream (axum drops the body),
/// cancelling any in-flight scan/wait — no orphaned long-poll.
pub(crate) fn ndjson_feed_stream(
    st: FeedState,
) -> impl futures::Stream<Item = Result<axum::body::Bytes, std::convert::Infallible>> {
    futures::stream::unfold(Some(st), |state| async move {
        let mut st = state?;
        loop {
            let batch = match st.remaining {
                Some(0) => return None,
                Some(r) => FEED_BATCH_LIMIT.min(usize::try_from(r).unwrap_or(FEED_BATCH_LIMIT)),
                None => FEED_BATCH_LIMIT,
            };
            match st
                .serving
                .changelog_feed(&st.table, &st.positions, batch, &st.policy)
                .await
            {
                Ok(page) if page.events.is_empty() => {
                    if let Err(e) = st.serving.await_changelog(&st.table, FEED_POLL_INTERVAL).await
                    {
                        tracing::error!(error = %e, "changelog wait fault; closing stream");
                        return None;
                    }
                }
                Ok(page) => {
                    st.positions = page.next.clone();
                    let mut running = page.next; // rebuilt per event below
                    // Recompute per-event cursors: walk events, tracking the
                    // position AFTER each one.
                    let mut buf = String::new();
                    let mut per_event = std::collections::BTreeMap::new();
                    // start from the pre-page positions minus this page's advance:
                    // simpler — re-derive: clone pre-page positions before the
                    // scan call and fold ev.offset + 1 per event (implementation
                    // detail; the sketch keeps `running` from before the call).
                    let _ = (&mut running, &mut per_event);
                    for ev in &page.events {
                        // fields projection (validated at connect).
                        let fields: serde_json::Map<String, serde_json::Value> = match &st.fields {
                            None => ev.fields.clone(),
                            Some(want) => ev
                                .fields
                                .iter()
                                .filter(|(k, _)| want.iter().any(|w| w == *k))
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect(),
                        };
                        let cursor = SubscribeCursor {
                            t: st.type_name.clone(),
                            b: /* positions folded through this event */ Default::default(),
                        };
                        let line = serde_json::json!({
                            "bucket": ev.bucket,
                            "offset": ev.offset,
                            "change_kind": ev.change_kind,
                            "fields": fields,
                            "cursor": cursor.to_opaque(),
                        });
                        // json! of Values never fails to serialize.
                        if let Ok(s) = serde_json::to_string(&line) {
                            buf.push_str(&s);
                            buf.push('\n');
                        }
                        if let Some(r) = st.remaining.as_mut() {
                            *r = r.saturating_sub(1);
                        }
                    }
                    let done = matches!(st.remaining, Some(0));
                    let next = if done { None } else { Some(st) };
                    return Some((Ok(axum::body::Bytes::from(buf)), next));
                }
                Err(e) => {
                    tracing::error!(error = %e, "changelog feed fault; closing stream");
                    return None;
                }
            }
        }
    })
}
```

(Implementation note on the per-event cursor: clone the positions map BEFORE calling `changelog_feed`, then per event do `pre.insert(ev.bucket, ev.offset + 1)` and mint the cursor from `pre` — after the last event `pre == page.next`. The sketch above marks where; write it that way, not with the placeholder `Default::default()`.)

- [ ] **Step 5: The handler in `http.rs`**

Register the route after line 96: `.route("/objects/:type_name/changes", get(get_changes))`.

```rust
/// Subscribe to a CDC type's ordered change-event feed (chunked NDJSON).
///
/// Each line is one change event `{bucket, offset, change_kind, fields, cursor}`;
/// `cursor` is the opaque resume position AFTER that event. `?cursor=` accepts
/// `earliest` (default), `latest` (join the tail), or a previously returned
/// opaque cursor (type-bound). Ordering is per-bucket (gapless offsets); the
/// stream stays open (notify-driven, poll-fallback) unless `?max_events=` bounds
/// it. Policy is resolved at connect; a mid-stream policy change takes effect on
/// the next reconnect.
#[utoipa::path(
    get, path = "/objects/{type_name}/changes",
    params(
        ("type_name" = String, Path, description = "Ontology object type (must back a declared CDC table)"),
        ("cursor" = Option<String>, Query, description = "`earliest` (default) | `latest` | an opaque cursor from a previous event line"),
        ("fields" = Option<String>, Query, description = "Comma-separated projection over the governed columns"),
        ("max_events" = Option<u64>, Query, description = "Close the stream after N events (unset = endless tail)"),
    ),
    responses(
        (status = 200, description = "NDJSON change-event stream, one JSON object per line", body = String, content_type = "application/x-ndjson"),
        (status = 400, description = "Malformed/foreign cursor, unknown field, bad max_events, or the type's table is not a declared CDC table"),
        (status = 403, description = "Forbidden by ACL policy (checked before existence)"),
        (status = 404, description = "Unknown type"),
        (status = 501, description = "The serving engine does not implement the changelog feed"),
    ),
    security(("bearer_auth" = [])),
    tag = "objects",
)]
async fn get_changes(
    State(st): State<AppState>,
    Path(type_name): Path<String>,
    Query(q): Query<crate::subscribe::ChangesQuery>,
    subject: Subject,
) -> axum::response::Response {
    use crate::governed::{OnMissing, resolve_governed};
    use crate::subscribe::{CursorSpec, parse_cursor};

    // 1. Governance prologue: coarse Read gate before existence, then policy.
    let g = match resolve_governed(
        st.cp.ontology(),
        st.cp.acl(),
        &subject.0,
        &TypeName(type_name.clone()),
        OnMissing::NotFound,
    )
    .await
    {
        Ok(g) => g,
        Err(e) => return crate::http::query_error_response(e, "changes read gate fault"),
    };
    let table = g.otype.table.clone();

    // 2. Cursor syntax + type binding (client faults before any engine call).
    let spec = match parse_cursor(q.cursor.as_deref()) {
        Ok(s) => s,
        Err(m) => return (StatusCode::BAD_REQUEST, m).into_response(),
    };
    if let CursorSpec::Resume(c) = &spec {
        if c.t != type_name {
            return (StatusCode::BAD_REQUEST, "cursor was minted for another type").into_response();
        }
    }

    // 3. ?fields= intersects the governed allowed set.
    let allowed = g.allowed();
    let fields = match q.fields.as_deref() {
        None => None,
        Some(raw) => {
            let want: Vec<String> = raw.split(',').map(str::trim).filter(|s| !s.is_empty())
                .map(String::from).collect();
            if let Some(bad) = want.iter().find(|f| !allowed.contains(f)) {
                return (StatusCode::BAD_REQUEST, format!("unknown or denied field `{bad}`"))
                    .into_response();
            }
            Some(want)
        }
    };

    // 4. Probe the engine: subscribable? (also yields the bucket set).
    let latest = match st.serving.changelog_latest(&table).await {
        Ok(Some(l)) => l,
        Ok(None) => {
            return (StatusCode::BAD_REQUEST, "type is not backed by a declared CDC table")
                .into_response();
        }
        Err(crate::serving::ServingError::Unsupported(_)) => {
            return (StatusCode::NOT_IMPLEMENTED, "changelog feed not available on this engine")
                .into_response();
        }
        Err(e) => return internal_error("changelog latest fault", e),
    };

    // 5. Boot positions: earliest = 0 per bucket; latest = the high-water map;
    // resume = the cursor's map normalized onto the live bucket set (missing
    // buckets start at 0; unknown buckets dropped).
    let positions = match spec {
        CursorSpec::Earliest => latest.keys().map(|b| (*b, 0)).collect(),
        CursorSpec::Latest => latest,
        CursorSpec::Resume(c) => latest
            .keys()
            .map(|b| (*b, c.b.get(b).copied().unwrap_or(0)))
            .collect(),
    };

    let policy = crate::serving::ChangeFeedPolicy {
        row_filters: g.row_filters.clone(),
        denied: g.denied.iter().cloned().collect(),
        masked: g.masked.iter().cloned().collect(),
    };
    let stream = crate::subscribe::ndjson_feed_stream(/* FeedState { serving: st.serving.clone(), table, type_name, positions, policy, fields, remaining: q.max_events } */);
    match axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
    {
        Ok(resp) => resp,
        Err(e) => internal_error("changes response build fault", e),
    }
}
```

`ChangesQuery` in `subscribe.rs`:

```rust
/// `GET /objects/{type}/changes` query parameters.
#[derive(Debug, serde::Deserialize)]
pub struct ChangesQuery {
    pub cursor: Option<String>,
    pub fields: Option<String>,
    pub max_events: Option<u64>,
}
```

(Make `FeedState`'s fields `pub(crate)` or give it a constructor so http.rs can build it; `query_error_response` is already `pub` at `http.rs:751`. `futures` is already a `:query-api` dep — `BUCK:20`.)

- [ ] **Step 6: OpenAPI registration**

Add `crate::http::get_changes,` to the `paths(...)` list in `src/services/query-api/src/openapi.rs:195-212`, and `("get", "/objects/{type_name}/changes")` to `expected()` in `src/services/query-api/tests/openapi.rs`.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `buck2 test --console none //src/services/query-api:subscribe-http //src/services/query-api:openapi //src/services/query-api:http-smoke //src/services/query-api:subscribe-cursor`
Expected: PASS.

- [ ] **Step 8: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(query-api): governed GET /objects/{type}/changes NDJSON feed route

The subscribe endpoint: resolve_governed at connect (deny-before-existence,
policy folded into a ChangeFeedPolicy the engine re-applies per batch), opaque
type-bound cursor (earliest/latest/resume), ?fields= over the governed
columns, ?max_events= bounded mode, and a chunked NDJSON body driven by the
new ServingEngine feed seam (changelog_latest/changelog_feed/await_changelog,
default-unsupported => 501 on engines without the feed — the wire hop is a
registered follow-up)."
```

---

## Task 6: e2e wiring + subscribe lifecycle e2e (flush boundary, resume, latest, multi-consumer)

**Files:**
- Modify: `src/services/query-api/tests/e2e_support.rs` — `InProcessServingEngine` feed overrides + `get_ndjson` streaming reader
- Create: `src/services/query-api/tests/stream_subscribe_e2e.rs` + `loom_fixture_test` target `stream-subscribe-e2e` mirroring `stream-cdc-e2e` (`query-api/BUCK:1950`; add `"//third-party:http-body-util"` if the mirror lacks it)

**Interfaces:**
- Consumes: `changelog_feed_scan` (T4), `await_changelog`/`changelog_positions_latest` (T3), the seam (T5), `TablePolicy` (`engine-serving/governed.rs:143`), `e2e_support` harness (`get`/`session_token` shape at `e2e_support.rs:250`).
- Produces:
  - `InProcessServingEngine` implements the three feed methods (in-process twin of the future wire client).
  - `pub async fn get_ndjson(cp: Arc<PgControlPlane>, eng: Arc<dyn ServingEngine>, uri: &str, subject: &str, max_lines: usize, timeout: Duration) -> (StatusCode, Vec<serde_json::Value>)` in `e2e_support` — drives the router, reads body frames incrementally until `max_lines` NDJSON lines arrive or the stream ends, then drops the body (client disconnect).

- [ ] **Step 1: Implement the e2e_support wiring**

In `impl query_api::serving::ServingEngine for InProcessServingEngine` (the struct is at `e2e_support.rs:107`; its existing trait-method impls are around `:150-158` — add these three methods in that same `impl` block), add:

```rust
    async fn changelog_latest(
        &self,
        table: &control_plane_core::TableRef,
    ) -> Result<Option<std::collections::BTreeMap<i32, i64>>, ServingError> {
        control_plane_postgres::stream::changelog_positions_latest(&self.catalog.pool, table)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn changelog_feed(
        &self,
        table: &control_plane_core::TableRef,
        positions: &std::collections::BTreeMap<i32, i64>,
        limit: usize,
        policy: &query_api::serving::ChangeFeedPolicy,
    ) -> Result<control_plane_core::ChangeFeedPage, ServingError> {
        let tp = engine_serving::TablePolicy {
            row_filters: policy.row_filters.clone(),
            denied: policy.denied.iter().cloned().collect(),
            masked: policy.masked.iter().cloned().collect(),
        };
        engine_serving::feed::changelog_feed_scan(&self.catalog, table, None, positions, limit, &tp)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }

    async fn await_changelog(
        &self,
        table: &control_plane_core::TableRef,
        timeout: std::time::Duration,
    ) -> Result<(), ServingError> {
        control_plane_postgres::stream::await_changelog(&self.catalog.pool, table, timeout)
            .await
            .map_err(|e| ServingError::Engine(e.to_string()))
    }
```

(`self.catalog.pool` is the public `IcebergCatalog.pool`. If `catalog` is a private field consumed elsewhere, mirror how the existing methods reach it.)

Add `get_ndjson` next to `get` (`e2e_support.rs:250`), same app construction, incremental body read via `http_body_util::BodyExt::frame`:

```rust
/// Drive the router and read the chunked NDJSON body incrementally: parse lines
/// as frames arrive, stop after `max_lines` (dropping the body = client
/// disconnect) or when the stream ends (`?max_events=` bounded mode). Panics if
/// `timeout` elapses first. Non-200 responses collect the whole body into a
/// single element.
pub async fn get_ndjson(
    cp: Arc<PgControlPlane>,
    eng: Arc<dyn query_api::serving::ServingEngine>,
    uri: &str,
    subject: &str,
    max_lines: usize,
    timeout: std::time::Duration,
) -> (StatusCode, Vec<serde_json::Value>) {
    let token = session_token(&cp, subject).await;
    let app = /* identical protect(router(AppState{..})) block to `get` */;
    let res = app
        .oneshot(/* GET uri with Bearer token, empty body */)
        .await
        .unwrap();
    let status = res.status();
    if status != StatusCode::OK {
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v = serde_json::from_slice(&bytes)
            .unwrap_or(serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        return (status, vec![v]);
    }
    let deadline = tokio::time::Instant::now() + timeout;
    let mut body = res.into_body();
    let mut buf: Vec<u8> = Vec::new();
    let mut lines = Vec::new();
    while lines.len() < max_lines {
        let frame = tokio::time::timeout_at(deadline, http_body_util::BodyExt::frame(&mut body))
            .await
            .expect("get_ndjson timed out waiting for a frame");
        let Some(frame) = frame else { break }; // stream ended (bounded mode)
        let frame = frame.expect("body frame");
        if let Some(data) = frame.data_ref() {
            buf.extend_from_slice(data);
            while let Some(nl) = buf.iter().position(|b| *b == b'\n') {
                let line: Vec<u8> = buf.drain(..=nl).collect();
                let line = &line[..line.len() - 1];
                if !line.is_empty() {
                    lines.push(serde_json::from_slice(line).expect("NDJSON line parses"));
                }
            }
        }
    }
    (status, lines)
}
```

- [ ] **Step 2: Write the failing e2e test**

Create `src/services/query-api/tests/stream_subscribe_e2e.rs` with the standard harness (identical setup block to `stream_subscribe_scan.rs`: declare CDC 2 buckets, `define_widget`, `grant_writer`, `spawn_engine_writer`, `connect_gov_client`), then one test fn per concern:

```rust
//! GET /objects/Widget/changes e2e (road-stream-subscribe): the full governed
//! NDJSON feed through the real router.
//! loom_fixture_test (Postgres + LocalFsStorage warehouse).
```

**(a) `full_stream_across_a_flush_boundary`** — write `+I(1)`, `−U/+U(1)` (3 events), `gov.flush_table`, then `+I(2)`, `−D(2)` (2 more). `get_ndjson(cp, eng, "/objects/Widget/changes?max_events=5", "writer", 5, 10s)`:
- 5 lines; every line has `bucket`/`offset`/`change_kind`/`fields`/`cursor` keys and NO top-level or `fields` `loom_*` key;
- the `(bucket, offset)` sequence is sorted and per-bucket gapless from 0 (reuse `assert_ordered_gapless` adapted to JSON lines);
- change-kind multiset is `{+I, -U, +U, +I, -D}`; the `-U` line's `fields.qty == 1` (before-image); the `-D` carries the full prior image (`fields.qty == 2` for id=2 — a full-image tombstone, not id-only);
- the stream ENDED on its own (the helper returned exactly 5 without hitting the timeout — bounded mode closes).

**(b) `resume_is_gapless_and_dup_free`** — after (a)'s writes: read 2 lines (`?max_events=2`); take line 2's `cursor` string; reconnect `?cursor={it}&max_events=3`; assert the concatenated `(bucket, offset, kind)` sequences equal the full 5-event read — no overlap, no gap (the inline-XOR-files disjointness through a reconnect).

**(c) `latest_cursor_joins_the_tail`** — after the 5 events: spawn `get_ndjson(..., "/objects/Widget/changes?cursor=latest&max_events=2", 2, 10s)` as a `tokio::spawn` task, sleep 300ms, `run_action("updateWidget", {id:1, qty:20})` (emits `−U,+U`). Join the task: exactly the 2 NEW events (`-U` then `+U` — same bucket, consecutive offsets strictly greater than every pre-subscribe offset in that bucket), none of the 5 old events.

**(d) `two_consumers_are_independent`** — `tokio::join!` two concurrent readers: one from `earliest` (`max_events=5`), one from the (b) mid-cursor (`max_events=3`); assert each returns exactly its own correct slice (byte-compare against the (a)/(b) expectations).

Wire `stream-subscribe-e2e` in `query-api/BUCK` mirroring `stream-cdc-e2e` (deps: `:query-api`, `:e2e-support`, core, postgres, axum, serde_json, sqlx, tempfile, tokio).

- [ ] **Step 3: Run to verify it fails, then passes**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-e2e`
Expected first run: FAIL (compile — the e2e_support methods land in Step 1 of this task, so if Steps 1–2 were done together, expected FAIL only if Step 1 was skipped; keep the TDD order test-first where practical).
After implementation: PASS.

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(stream): subscribe feed e2e — flush boundary, resume, latest, multi-consumer

Wire the in-process engine's feed seam (changelog_feed_scan + pg waiters) into
e2e_support, add the incremental NDJSON reader, and prove through the real
router: the complete ordered event stream across a flush boundary (files ∪
inline, -U included), gapless dup-free resume from a returned opaque cursor,
cursor=latest join-the-tail, and independent concurrent consumers."
```

---

## Task 7: Governance + freshness e2e

**Files:**
- Create: `src/services/query-api/tests/stream_subscribe_gov_e2e.rs` + `loom_fixture_test` target `stream-subscribe-gov-e2e` (same mirror as Task 6; add `"//third-party:time"` if needed)

**Interfaces:**
- Consumes: everything above; `subject_with_role`/`grant_read_filtered`/`grant_read_columns` (`e2e_support.rs:205`/`:647`/`:671`); `RowFilter::Compare` (`core/src/acl.rs:188`).

- [ ] **Step 1: Write the test**

Same harness; seed 5 events for id=1/id=2 as in Task 6(a) (no flush needed — governance is tier-agnostic; ONE case re-runs after a flush to prove file-tier parity):

**(a) `row_filter_hides_every_event_of_a_filtered_identity`** — `subject_with_role(cp, "restricted")` + `grant_read_filtered(&cp, &role, "Widget", RowFilter::Compare { property: "id".into(), op: CompareOp::Eq, value: ScalarValue::Int(1) })` (copy the exact `Compare` construction from `action_e2e.rs:357`). `get_ndjson(..., "?max_events=3", "restricted", 3, 10s)`: exactly id=1's 3 events; NO id=2 event ever appears (consume with a short follow-up read at the returned cursor + `max_events=1` and assert it times out blocked — wrap in `tokio::time::timeout` and expect `Err`, or simpler: read `?max_events=5` with a 5s helper timeout via `std::panic::catch_unwind`-free pattern — prefer: read 3, then assert a `?cursor=<after>&max_events=1` read against a *governed* stream stays empty by using a 2s-timeout spawned reader that gets aborted). Keep it deterministic: assert the FIRST 3 lines are id=1's and that after a flush the same read returns the same 3 (file-tier parity).
**(b) `masked_column_is_starred_on_every_event`** — `grant_read_columns(&cp, &role2, "Widget", vec![], vec!["qty".into()])`: every line's `fields.qty == "***"`, incl. the `-U` before-image.
**(c) `reconnect_regates`** — a granted subject reads fine; revoke (grant a different subject nothing / use a role with no grant), reconnect with the SAME valid cursor → 403: the cursor carries no authority (`get_ndjson` returns `(FORBIDDEN, _)` before any line).
**(d) `unflushed_write_is_fresh`** — with all events consumed (`cursor=latest`), spawn a blocked reader (`max_events=1`, helper timeout 10s), sleep 300ms, commit one inline write via `run_action` (NO flush): the reader completes with that event. With `FEED_POLL_INTERVAL = 1s` this passes even on a missed notify; the notify-vs-poll distinction is already pinned deterministically by Task 3(b) at the postgres layer.

- [ ] **Step 2: Wire the target + run**

Run: `buck2 test --console none //src/services/query-api:stream-subscribe-gov-e2e`
Expected: PASS (after implementation fixes any handler/scan gaps it reveals).

- [ ] **Step 3: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(stream): subscribe governance + freshness e2e

Row-filtered subjects receive zero events for filtered identities (both
tiers), masked columns arrive as '***' on every event incl. -U before-images,
a reconnecting cursor re-runs the full governance gate (revocation => 403 —
the cursor carries no authority), and an unflushed inline write reaches a
blocked stream within the notify/poll window."
```

---

## Task 8: Documentation + registers

**Files:**
- Modify: `docs/system-capabilities/stream.md` — a **Subscribe / tail feed** section: the endpoint, the event + cursor contract (opaque, unsigned, type-bound, per-event `cursor` field), earliest/latest/resume, `?fields=`/`?max_events=`, the disjoint-union read (no watermark), the notify channel `loom_changelog:{tid}`, per-bucket ordering semantics, governance-at-connect + per-batch enforcement, and the 501-on-wire-engine caveat. Bump the `_As of <commit>._` line.
- Modify registers via the **`loom-docs-update` skill**: close `road-stream-subscribe` in `docs/ROADMAP.md`; file the deferred follow-ups in `docs/FUTURE.md` (each as a proper register item, `[[road-stream-subscribe]]`-linked where the grammar wants it):
  - `fut-stream-subscribe-wire` — serve the feed on the production wire client: a `ChangelogFeedTicket` Flight `do_get` (mirroring `VectorSearchTicket`, `engine-wire/src/flight.rs:47`) + a unary long-poll `AwaitChangelog` RPC (mirroring `AwaitJobs`) implementing the T5 seam on `EngineServingClient`; until then the route is 501 on the wire deployment.
  - `fut-stream-feed-pruning` — push the bucket/offset resume predicate through `GovernedTableProvider` into the mirror provider's stat-pruning scan.
  - (Check first with `bash tools/docs.sh query open --area stream` whether existing items already cover consumer-offset storage / log-table subscribe / SSE — the spec's other non-goals — and only add what's missing.)
- Validate: `bash tools/docs.sh validate`.

- [ ] **Step 1: Write the capability doc + register edits (via loom-docs-update)**
- [ ] **Step 2: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "docs(stream): document the subscribe/tail feed; close road-stream-subscribe

Document the governed NDJSON changelog feed (event + cursor contract, ordered
disjoint union, notify wakeup) in the stream capability; close the ROADMAP
item and file the wire-transport and pruning follow-ups."
```

---

## Task 9: Final verification sweep

- [ ] **Step 1: Whole-tree build**

Run: `buck2 build -v0 --console none //src/...`
Expected: silent success (exit 0).

- [ ] **Step 2: The new suites**

Run: `buck2 test --console none //src/control-plane/core:change-event //src/services/query-api:subscribe-cursor //src/services/query-api:subscribe-http //src/control-plane/postgres:stream-changelog-notify //src/services/query-api:stream-subscribe-scan //src/services/query-api:stream-subscribe-e2e //src/services/query-api:stream-subscribe-gov-e2e //src/services/query-api:openapi`
Expected: `Tests finished: Pass N. Fail 0`.

- [ ] **Step 3: Non-regression — the touched-crate stream/CDC suites**

Run: `buck2 test --console none //src/control-plane/postgres:stream-cdc-emission //src/control-plane/postgres:stream-cdc-bucket //src/control-plane/postgres:stream-cdc-dual-flush //src/control-plane/postgres:stream-inline //src/control-plane/postgres:stream-merge-declare //src/services/query-api:stream-cdc-e2e //src/services/query-api:stream-cdc-read-mid-window //src/services/query-api:stream-cdc-consolidate //src/services/query-api:stream-merge-firstrow //src/services/query-api:stream-merge-versioned //src/services/query-api:http-smoke`
Expected: PASS, unchanged test code.

- [ ] **Step 4: Full local suite (dev machine)**

Run: `buck2 test -j 8 --console none //src/...` (the `-j 8` cap per MEMORY: the 8 PG boot-slots starve otherwise). On a cloud/root session: scope to the Step 2+3 targets instead and build with `-M none`.
Expected: PASS.

- [ ] **Step 5: prek, final**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: all hooks pass (incl. `reindeer-check` for the base64 dep and the markdown hooks for the docs).

---

## Self-Review (run after writing the plan)

**1. Spec coverage.** Endpoint + route + governance gate ⇒ T5; opaque cursor + sentinels ⇒ T2; stateless server / multi-consumer ⇒ T5 design + T6(d); reconnect re-gates ⇒ T7(c); NDJSON transport + transport-agnostic contract ⇒ T1 (core types) + T5 (framing); notify-in-commit ⇒ T3; poll fallback ⇒ T3/T5 (`FEED_POLL_INTERVAL`); the invented `changelog_feed_scan` (files ∪ inline, ordered, bounded, `−U` included, governed by providers) ⇒ T4; per-batch `GovernedTableProvider` enforcement + `?fields=` intersection ⇒ T4/T5/T7; every Testing-section case ⇒ T3(b,e) wakeup latency, T4/T6(a) flush-boundary full stream, T6(b) resume/disjointness, T6(c) latest, T6(d) multi-consumer, T7(a,b,c) governance, T7(d) freshness. Non-goals stay out (no consumer-offset storage, no watermark, no gRPC transport, no log-table subscribe) and the wire follow-up is registered ⇒ T8.

**2. Spec deviations are explicit** — the *Architecture placement* section: contract types in core (dependency-forced), positions-not-cursor return, per-line `cursor` field, `?max_events=`, 501-on-wire-engine, deferred pruning, corrected line refs.

**3. Type consistency.** `ChangeEvent`/`ChangeFeedPage` (T1) are consumed identically by T4 (scan return), T5 (seam + NDJSON), T6 (in-process impl). `BTreeMap<i32, i64>` positions flow T3 → T4 → T5 → T2's `SubscribeCursor.b`. `ChangeFeedPolicy` (T5) ↔ `TablePolicy` (T4) conversion lives in exactly one place (e2e_support's impl, the future wire client's twin). The channel literal `loom_changelog:{tid}` appears in exactly two production sites (notifier T3, waiter T3) — one crate.

**4. Command hygiene.** All builds `-v0 --console none`; all tests `--console none` with explicit targets; prek before every commit; no compile-time SQL (no sqlx-prepare dependency, so the plan is cloud-executable except the full `-j 8` local sweep, which has a scoped cloud alternative in Task 9 Step 4).
