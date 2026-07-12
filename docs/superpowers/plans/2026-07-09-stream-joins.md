# Stream Joins / Delta-Join Analog (slice 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **PREREQUISITE — slice 4 must be fully landed.** This plan builds ON TOP of
> the slice-4 plan (`docs/superpowers/plans/2026-07-09-stream-continuous.md`,
> `road-stream-continuous`) and presumes every interface it produces exists
> under its exact name: `TransformBody::MicroBatch`, `STREAM_MV_JOB_KIND` +
> `StreamMvJob` (`core/src/stream_mv_job.rs`), `MvWatermarks`/
> `WatermarkAdvance`/`mv_key`, `MvDeltaTicket`/`fetch_mv_delta`,
> `mv_delta_scan`, `CommitMicroBatch`/`commit_micro_batch`, `MvCommit`/
> `inline_append_mv`, `handle_stream_mv`/`StreamMvCtx`
> (`worker/src/stream_mv.rs`), and the slice-4 e2e harness/BUCK targets.
> File:line references into that code are **symbolic (name-based)**;
> references into shipped code are real, verified file:line. Before Task 1,
> verify the prerequisite: `git log --oneline | head` shows the slice-4 plan's
> final commit, and `grep -n "MicroBatch" src/control-plane/core/src/transforms.rs`
> hits the variant. If it does not, STOP — this plan cannot start.

**Goal:** Stream joins as micro-batch MVs: a new `TransformBody::MicroBatchJoin`
whose SQL joins the source stream's delta (slice 4's watermarked delta read)
against a second table's folded current state — fetched whole (**state-join**)
or by the delta's distinct join keys (**lookup-join**, the delta-join analog) —
and committed as an enriched `+I` log through slice 4's `CommitMicroBatch`,
unchanged.

**Architecture:** One new fetch on the slice-4 loop. Core gains the
`MicroBatchJoin` body variant + `LookupOn`; `StreamMvJob` widens with two
`#[serde(default)]` fields (same job kind, same handler). The engine data plane
gains `MvEnrichTicket` → `mv_enrich_scan` — `build_serving_provider`'s folded
current state (`serving.rs:74`) with an optional typed `IN` key predicate.
`handle_stream_mv` grows one branch: derive keys, fetch enrich state, register
it beside the delta. No migration, no proto change, no new job kind, no
`.sqlx` change. Spec: `docs/superpowers/specs/2026-07-09-stream-joins-design.md`.

**Tech Stack:** Rust, DataFusion, Arrow Flight (tonic), buck2
`loom_fixture_test`/`rust_test`, serde JSON tickets.

## Global Constraints

Carried verbatim from the spec; every task implicitly includes these:

- **Tests are `rust_test` / `loom_fixture_test` integration targets only — never inline `#[cfg(test)]`.** New fixture tests MUST use `loom_fixture_test` (`src/control-plane/postgres/defs.bzl`), not a bare `rust_test`. The `no-inline-tests` prek hook fails the build on any inline `#[test]`.
- **No new SQL.** No migration; no `query!`/`query_scalar!` touched; no `tools/sqlx-prepare.sh` run. If adapter SQL becomes unavoidable, use runtime `sqlx::query(AssertSqlSafe(...))` mirroring `version_for_table` (`postgres/src/ontology.rs:709`).
- **Clippy is strict (pedantic + restriction):** no `unwrap`/`expect`/`indexing_slicing`/`panic`/`todo`/`map_err_ignore` in production lib/bin code; `#[expect(lint, reason = "...")]` for justified local exceptions. Test code is exempted from the panic-safety lints via `loom_rust_test`/`loom_fixture_test`.
- **Run prek before every commit:** `buck2 run //tools:prek -- run --all-files` (`git add` new files first; rustfmt is a separate hook — clippy-clean ≠ lint-clean). Markdown ends with exactly one trailing newline, no trailing whitespace.
- **The engine owns Postgres; the worker stays zero-pool.** The enrich read is a Flight ticket; the commit stays the single `CommitMicroBatch` RPC; no postgres dep may enter the worker's BUCK closure.
- **`CommitMicroBatch` is consumed byte-for-byte unchanged** — enrichment carries no commit state; lineage inputs ride the existing `lineage_json` field.
- **The enrich read is the internal, ungoverned data plane** (parity with the transform-input read, `worker/src/transform.rs:256-298`); it returns user columns only (logical read). The delta read stays framed; the MV SQL sees no `loom_*` column from either side.
- **Slice-4 paths must stay byte-identical.** A `StreamMvJob` without `enrich`/`on` takes exactly the slice-4 code path; existing slice-4 MV suites stay green and unmodified except mechanical enum-match additions.
- **Build/test commands** (from CLAUDE.md): build `buck2 build -v0 --console none //src/...`; test `buck2 test --console none <targets>` (cloud: add `-M none` to builds, scope tests, `buck2 clean` between heavy phases; full suite locally needs `-j 8`).

---

## File Structure

**Create:**
- `src/control-plane/core/tests/microbatch_join.rs` — body variant / job payload / validation unit tests.
- `src/services/engine-wire/tests/mv_enrich_ticket.rs` — ticket round-trip + decode-chain disjointness.
- `src/services/engine-serving/src/mv_enrich.rs` — `mv_enrich_scan`.
- `src/services/engine-serving/tests/mv_enrich_scan.rs` — folded/keyed/empty/unknown scan fixture test.
- `src/services/engine/tests/mv_enrich_flight.rs` — do_get dispatch + `fetch_mv_enrich` over UDS.
- `src/services/worker/tests/stream_mv_join_e2e.rs` — the headline lookup-join e2e.
- `src/services/worker/tests/stream_mv_join_triggers.rs` — enrich-edge trigger/cycle/composability e2e.

**Modify (production):**
- `src/control-plane/core/src/transforms.rs` — `MicroBatchJoin` variant; `to_job` / `TriggerNode::resolve` / `validate_transform_def` arms.
- `src/control-plane/core/src/stream_mv_job.rs` (slice-4-produced) — `LookupOn`; `StreamMvJob.enrich`/`.on`; `MAX_LOOKUP_KEYS`.
- `src/control-plane/core/src/lib.rs` — re-export `LookupOn`.
- `src/services/engine-wire/src/flight.rs` — `MvEnrichTicket`; `EngineTicket::MvEnrich` + decode arm; `FlightTableClient::fetch_mv_enrich`.
- `src/services/engine/src/flight.rs` — `do_get` dispatch arm → `do_get_mv_enrich`.
- `src/services/engine-serving/src/lib.rs` — `pub mod mv_enrich`.
- `src/services/worker/src/stream_mv.rs` (slice-4-produced) — the enrich fetch/registration branch + key extraction.
- BUCK files beside each new test (targets mirror named siblings).

---

## Task 1: Core — `LookupOn`, `TransformBody::MicroBatchJoin`, `StreamMvJob` widening, validation + trigger arms

**Files:**
- Modify: `src/control-plane/core/src/transforms.rs` (`TransformBody` at `:62`, `to_job` — a method on `TransformBody` — at `:75`, `validate_transform_def` at `:261`, `TriggerNode::resolve` at `:344`)
- Modify: `src/control-plane/core/src/stream_mv_job.rs` (slice-4-produced: `StreamMvJob` at `:14`, `STREAM_MV_JOB_KIND` at `:8`)
- Modify: `src/control-plane/core/src/lib.rs` (re-exports — `LookupOn`/`MAX_LOOKUP_KEYS` beside the existing `StreamMvJob`/`STREAM_MV_JOB_KIND` re-exports at `:80`,`:85-89`)
- Test: `src/control-plane/core/tests/microbatch_join.rs` (new) + `src/control-plane/core/BUCK` (new `rust_test`, mirror `transform-job` at `:253`)

**Interfaces:**
- Consumes: `TransformBody`/`to_job`/`validate_transform_def`/`TriggerNode::resolve` (`core/src/transforms.rs:62`/`:75`/`:261`/`:344` — shipped); `STREAM_MV_JOB_KIND`/`StreamMvJob { source, output, buckets, sql, run_id }` (`stream_mv_job.rs:8`/`:14` — slice-4).
- Produces (later tasks rely on these EXACT names/types):
  - `pub struct LookupOn { pub source_col: String, pub enrich_col: String }` (serde derive, `deny_unknown_fields` not required) in `core/src/stream_mv_job.rs`, re-exported as `control_plane_core::LookupOn`.
  - `TransformBody::MicroBatchJoin { source: TableRef, enrich: TableRef, on: Option<LookupOn>, output: TableRef, buckets: i32, sql: String }` (serde tag `"microbatch_join"`).
  - `StreamMvJob` widened: `#[serde(default)] pub enrich: Option<TableRef>`, `#[serde(default)] pub on: Option<LookupOn>`.
  - `pub const MAX_LOOKUP_KEYS: usize = 10_000` in `core/src/stream_mv_job.rs`.

- [ ] **Step 1: Write the failing test**

Create `src/control-plane/core/tests/microbatch_join.rs`:

```rust
//! Unit tests for TransformBody::MicroBatchJoin, LookupOn, and the widened
//! StreamMvJob payload. External rust_test (no inline #[cfg(test)]).

use control_plane_core::{
    LookupOn, STREAM_MV_JOB_KIND, StreamMvJob, TableRef, TransformBody, TransformDef,
    TransformName, TriggerNode, validate_transform_def,
};

fn tref(schema: &str, name: &str) -> TableRef {
    TableRef { schema: schema.into(), name: name.into() }
}

fn join_def(on: Option<LookupOn>) -> TransformDef {
    TransformDef {
        name: TransformName("enrich_orders".into()),
        body: TransformBody::MicroBatchJoin {
            source: tref("s", "orders"),
            enrich: tref("s", "customers"),
            on,
            output: tref("s", "enriched_orders"),
            buckets: 2,
            sql: "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
                .into(),
        },
        schedule: None,
        on_input_commit: true,
    }
}

#[test]
fn microbatch_join_serde_round_trips_with_tag() {
    let def = join_def(Some(LookupOn {
        source_col: "customer_id".into(),
        enrich_col: "id".into(),
    }));
    let json = serde_json::to_value(&def.body).unwrap();
    assert_eq!(json["kind"], "microbatch_join", "serde tag");
    let back: TransformBody = serde_json::from_value(json).unwrap();
    assert_eq!(back, def.body);
}

#[test]
fn to_job_emits_stream_mv_kind_with_enrich_and_on() {
    let def = join_def(Some(LookupOn {
        source_col: "customer_id".into(),
        enrich_col: "id".into(),
    }));
    let run_id = uuid::Uuid::new_v4();
    // `to_job` is a method on `TransformBody`, not `TransformDef` (transforms.rs:75).
    let job = def.body.to_job(run_id);
    assert_eq!(job.kind, STREAM_MV_JOB_KIND, "same kind as slice-4 MVs");
    let payload: StreamMvJob = serde_json::from_value(job.payload).unwrap();
    assert_eq!(payload.enrich, Some(tref("s", "customers")));
    assert_eq!(payload.on.as_ref().map(|o| o.source_col.as_str()), Some("customer_id"));
    assert_eq!(payload.run_id, Some(run_id));
}

#[test]
fn stream_mv_job_without_enrich_keys_decodes_back_compat() {
    // A slice-4 payload (no enrich/on keys) must decode with None/None.
    let slice4 = serde_json::json!({
        "source": {"schema": "s", "name": "a"},
        "output": {"schema": "s", "name": "b"},
        "buckets": 1,
        "sql": "SELECT * FROM a",
        "run_id": null,
    });
    let payload: StreamMvJob = serde_json::from_value(slice4).unwrap();
    assert!(payload.enrich.is_none() && payload.on.is_none());
}

#[test]
fn trigger_node_resolves_source_and_enrich_as_inputs() {
    let def = join_def(None);
    // Real signature (transforms.rs:344): resolve(&TransformName, &TransformBody,
    // &HashMap<String, TableRef>) -> Self (no Result — no `.unwrap()`).
    let node = TriggerNode::resolve(&def.name, &def.body, &std::collections::HashMap::new());
    assert_eq!(node.inputs, vec![tref("s", "orders"), tref("s", "customers")]);
    assert_eq!(node.output, Some(tref("s", "enriched_orders")));
}

#[test]
fn validate_rejects_degenerate_join_defs() {
    let mut cases: Vec<(TransformDef, &str)> = Vec::new();
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { buckets, .. } = &mut d.body { *buckets = 0; }
    cases.push((d, "buckets < 1"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { sql, .. } = &mut d.body { sql.clear(); }
    cases.push((d, "empty sql"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { output, .. } = &mut d.body { *output = tref("s", "orders"); }
    cases.push((d, "source == output"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { output, .. } = &mut d.body { *output = tref("s", "customers"); }
    cases.push((d, "enrich == output"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { enrich, .. } = &mut d.body { *enrich = tref("s", "orders"); }
    cases.push((d, "source == enrich"));
    let mut d = join_def(None);
    if let TransformBody::MicroBatchJoin { enrich, .. } = &mut d.body { *enrich = tref("other", "orders"); }
    cases.push((d, "registration-name collision (source.name == enrich.name)"));
    let d = join_def(Some(LookupOn { source_col: String::new(), enrich_col: "id".into() }));
    cases.push((d, "empty LookupOn.source_col"));
    let d = join_def(Some(LookupOn { source_col: "customer_id".into(), enrich_col: String::new() }));
    cases.push((d, "empty LookupOn.enrich_col"));
    for (def, why) in cases {
        assert!(validate_transform_def(&def).is_err(), "must reject: {why}");
    }
    // The well-formed def validates.
    assert!(validate_transform_def(&join_def(None)).is_ok());
}
```

(Adjust `TransformDef`/`TriggerNode` field spellings to the shipped + slice-4
shapes at implementation time — the assertions, not the scaffolding, are the
contract.)

- [ ] **Step 2: Wire the test target**

In `src/control-plane/core/BUCK`, mirror the `transform-job` `rust_test`
target: add `microbatch-join` with `srcs = ["tests/microbatch_join.rs"]`,
deps `[":core", "//third-party:serde_json", "//third-party:uuid"]`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/control-plane/core:microbatch-join`
Expected: FAIL — `MicroBatchJoin` variant / `LookupOn` / `enrich` field missing (compile error).

- [ ] **Step 4: Implement**

In `src/control-plane/core/src/stream_mv_job.rs` (slice-4-produced), add:

```rust
/// The lookup-join key contract: the delta column whose distinct values probe
/// the enrich table's `enrich_col`. MUST name the SQL's equijoin columns —
/// v1 does not parse the SQL to verify (a mismatch silently drops join
/// partners; `on: None` is the always-correct full-state default). See the
/// slice-5 spec's correctness contract.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LookupOn {
    pub source_col: String,
    pub enrich_col: String,
}

/// Above this many distinct lookup keys the worker falls back to the
/// full-state fetch (a superset — always correct); keeps the enrich ticket
/// bounded.
pub const MAX_LOOKUP_KEYS: usize = 10_000;
```

and widen `StreamMvJob` (additive, back-compatible):

```rust
    /// Slice-5 join MVs: the state-side table. `None` => a plain slice-4 MV.
    #[serde(default)]
    pub enrich: Option<TableRef>,
    /// Slice-5 lookup-join key contract. `None` with `enrich: Some` => state-join.
    #[serde(default)]
    pub on: Option<LookupOn>,
}
```

In `src/control-plane/core/src/transforms.rs`:

- `TransformBody` (`:62`) gains the variant, mirroring slice-4's `MicroBatch`
  arm shape. **The enum is `#[serde(tag = "kind", rename_all = "lowercase")]`
  and `MicroBatch` carries an explicit `#[serde(rename = "microbatch")]`
  (`:61`); `MicroBatchJoin` MUST likewise get an explicit
  `#[serde(rename = "microbatch_join")]`, else `rename_all = "lowercase"`
  yields `"microbatchjoin"` and the Task-1 tag assertion fails.**
- `to_job` (a method on `TransformBody`, `:75`) gains a `MicroBatchJoin` arm
  emitting `STREAM_MV_JOB_KIND` with the widened `StreamMvJob`
  (`enrich: Some(..)`, `on` threaded through). **The existing `MicroBatch` arm
  constructs `StreamMvJob { .. }` as a struct literal (`:114`); widening the
  struct breaks it — add `enrich: None, on: None` there.**
- `TriggerNode::resolve` (`:344`): `inputs = vec![source.clone(), enrich.clone()]`,
  `output = Some(output.clone())` — both edges trigger AND cycle-check.
- `validate_transform_def` (`:261`): reject `buckets < 1`, empty `sql`,
  `source == output`, `enrich == output`, `source == enrich`,
  `source.name == enrich.name` (DataFusion registration collision — mirror the
  ambiguous-name error text of `worker/src/transform.rs:236-242`), and an `on`
  with an empty `source_col` or `enrich_col` (all `Validation` errors with
  actionable messages).

In `src/control-plane/core/src/lib.rs`, re-export `LookupOn` (and
`MAX_LOOKUP_KEYS`) beside the slice-4 `StreamMvJob` re-export.

- [ ] **Step 5: Run the test + sweep the tree**

Run: `buck2 test --console none //src/control-plane/core:microbatch-join`
Expected: PASS.

Run: `buck2 build -v0 --console none //src/...`
Expected: exit 0. If slice-4 code constructs `StreamMvJob { .. }` literally
(the widened struct breaks literal constructions), the compiler enumerates
each site — add `enrich: None, on: None`. Any exhaustive `match` on
`TransformBody` (poison-body scan, schedule claim, admin listing) gains a
mechanical `MicroBatchJoin` arm mirroring its `MicroBatch` arm.

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(stream): TransformBody::MicroBatchJoin + LookupOn payload

The slice-5 join-MV body: source delta + enrich state + optional lookup-key
contract, emitted on the existing stream_mv job kind (StreamMvJob widened
with serde-default enrich/on — slice-4 payloads decode unchanged). Both the
source and enrich edges are trigger + cycle edges. No behavior change in
the worker yet."
```

---

## Task 2: engine-wire — `MvEnrichTicket`, decode arm, `fetch_mv_enrich`

**Files:**
- Modify: `src/services/engine-wire/src/flight.rs` (`EngineTicket` at `:180`, `decode` at `:239`, terminal `Files(FlightTicket)` at `:277`, `FlightTableClient` at `:300`, `fetch`/`fetch_mv_delta` at `:337`/`:342`, `do_get_batches` helper at `:349`)
- Test: `src/services/engine-wire/tests/mv_enrich_ticket.rs` (new) + `src/services/engine-wire/BUCK`

**Interfaces:**
- Consumes: `EngineTicket`/`decode` chain (`engine-wire/src/flight.rs:180`/`:239` — shipped, lines drifted ~+30 from slice-4 landing); `MvDeltaTicket` (`:123`) + its `EngineTicket::MvDelta` decode arm (`:188`, slice-4 — this arm slots beside it); `FlightTicket { schema, name, files }` (`:25`, `encode()` at `:33`); `do_get_batches` (`:349`).
- Produces: `pub struct MvEnrichTicket { pub enrich_schema: String, pub enrich_name: String, pub key: Option<String>, pub keys: Vec<serde_json::Value> }` (serde `deny_unknown_fields`, `keys` `#[serde(default)]`) with `encode()`/`decode()`; `EngineTicket::MvEnrich(MvEnrichTicket)`; `FlightTableClient::fetch_mv_enrich(&self, ticket: MvEnrichTicket) -> Result<Vec<RecordBatch>>`.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-wire/tests/mv_enrich_ticket.rs`:

```rust
//! MvEnrichTicket round-trip + EngineTicket decode-chain routing. The required
//! enrich_schema/enrich_name fields keep it deny_unknown_fields-disjoint from
//! every other JSON ticket shape.

use engine_wire::flight::{EngineTicket, FlightTicket, MvEnrichTicket};

#[test]
fn mv_enrich_ticket_round_trips_keyed_and_unkeyed() {
    for ticket in [
        MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: Some("id".into()),
            keys: vec![serde_json::json!(1), serde_json::json!(2)],
        },
        MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: None,
            keys: vec![],
        },
    ] {
        let bytes = ticket.encode();
        match EngineTicket::decode(&bytes).expect("decodes") {
            EngineTicket::MvEnrich(back) => assert_eq!(back, ticket),
            other => panic!("misrouted: {other:?}"),
        }
    }
}

#[test]
fn keys_field_defaults_empty() {
    let bytes = br#"{"enrich_schema":"s","enrich_name":"t","key":null}"#;
    match EngineTicket::decode(bytes).expect("decodes") {
        EngineTicket::MvEnrich(t) => assert!(t.keys.is_empty() && t.key.is_none()),
        other => panic!("misrouted: {other:?}"),
    }
}

#[test]
fn existing_tickets_still_route_unchanged() {
    // The terminal file ticket must not be shadowed by the new arm.
    let files = FlightTicket { schema: "s".into(), name: "t".into(), files: vec!["f".into()] };
    match EngineTicket::decode(&files.encode()).expect("decodes") {
        EngineTicket::Files(back) => assert_eq!(back, files),
        other => panic!("misrouted: {other:?}"),
    }
}
```

(Mirror `FlightTicket`'s real field spelling from `flight.rs:25` when
implementing; the routing assertions are the contract.)

- [ ] **Step 2: Wire the test target**

In `src/services/engine-wire/BUCK`, mirror the `engine-ticket` `rust_test`
target: add `mv-enrich-ticket` with `srcs = ["tests/mv_enrich_ticket.rs"]`,
deps `[":engine-wire", "//third-party:serde_json"]`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/engine-wire:mv-enrich-ticket`
Expected: FAIL — `MvEnrichTicket` not found.

- [ ] **Step 4: Implement**

In `src/services/engine-wire/src/flight.rs`:

- Add `MvEnrichTicket` beside slice-4's `MvDeltaTicket` (`:123`), mirroring the
  existing JSON-ticket idiom (`#[derive(Debug, Clone, PartialEq,
  serde::Serialize, serde::Deserialize)] #[serde(deny_unknown_fields)]`, with
  `#[serde(default)] pub keys: Vec<serde_json::Value>`, plus the standard
  `encode`/`decode` inherent methods — `encode` may mirror `MvDeltaTicket`'s
  `serde_json::to_vec(self).expect(...)` infallible-serialization idiom
  (`:137`)).
- Add `EngineTicket::MvEnrich(MvEnrichTicket)` and a decode arm in the JSON
  fall-through chain (`decode`, `:239`) beside slice-4's `MvDelta` arm (`:188`),
  before the terminal `Files(FlightTicket)` arm (`:277`) — required
  `enrich_schema`/`enrich_name` fields keep it disjoint. Document the
  disjointness in the arm comment, matching the chain's existing comment style.
- Add `FlightTableClient::fetch_mv_enrich` mirroring `fetch`/`fetch_mv_delta`
  (`:337`/`:342`), which route through the private helper
  `do_get_batches(ticket.encode())` (`:349`) — call that, don't hand-roll the
  `do_get` + decode.

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/services/engine-wire:mv-enrich-ticket //src/services/engine-wire:engine-ticket //src/services/engine:ticket-errors`
Expected: PASS — the new arm routes; the pinned decode-error messages are untouched.

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine-wire): MvEnrichTicket + fetch_mv_enrich

The slice-5 enrich-read ticket: (table, optional key column, JSON key set)
-> folded current-state rows. Joins the deny_unknown_fields JSON decode
chain beside MvDelta; existing ticket routing byte-identical."
```

---

## Task 3: engine-serving — `mv_enrich_scan` (folded state, optional typed key predicate)

**Files:**
- Create: `src/services/engine-serving/src/mv_enrich.rs`
- Modify: `src/services/engine-serving/src/lib.rs` (`pub mod mv_enrich;`)
- Test: `src/services/engine-serving/tests/mv_enrich_scan.rs` (new) + `src/services/engine-serving/BUCK`

**Interfaces:**
- Consumes: `build_serving_provider` (`engine-serving/src/serving.rs:80` — signature `(ctx, catalog: &IcebergCatalog, table, serving_store: Option<&ServingStore>, at: Option<SnapshotId>)`; the CDC fold via `Precedence::Offset`/`build_merge_view`); `EngineServingError` (`:36`); the `Catalog` trait's declared-columns lookup + `datafusion_io::logical_arrow_schema` (`datafusion-io/src/infer.rs:52`) for the empty-table fallback. `ServingStore` is `store_config::ServingStore` (re-imported at `serving.rs:30`) — `mv_enrich.rs` must `use store_config::ServingStore;`. `IcebergCatalog` is `control_plane_postgres::iceberg_catalog::IcebergCatalog`.
- **Test harness — mirror `engine-serving/tests/merge_on_read.rs`'s ACTUAL seeding, not `land_cdc`/`flush_table`/`local_sql_catalog` (those are prod-only or worker-test helpers):** `PgFixture::shared()` for the DB; `IcebergWriter::{seed_arrays (file rows), inline (update/tombstone rows)}`; `define_type(cp, "s", "t", Some("id"))` to declare the CDC identity; `IcebergCatalog::new(pool)` for the serving catalog (`merge_on_read.rs:14-15,129-151`).
- Produces: `pub async fn mv_enrich_scan(catalog: &IcebergCatalog, table: &TableRef, key: Option<(&str, &[serde_json::Value])>, serving_store: Option<&ServingStore>) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError>` in `engine_serving::mv_enrich`. **Deterministic refusals (unknown table, non-coercible/wrong-type key) return `EngineServingError::Engine(format!("mv enrich: …"))` — a stable `"mv enrich:"` message prefix the wire status mapping and the worker key off (the message survives the gRPC-status flattening; a code does not).**

- [ ] **Step 1: Write the failing test**

Create `src/services/engine-serving/tests/mv_enrich_scan.rs`, mirroring
`merge_on_read.rs`'s ACTUAL harness (`PgFixture::shared()` +
`IcebergCatalog::new(pool)` + `define_type(cp, "s", "customers", Some("id"))`
+ `IcebergWriter::{seed_arrays, inline}` — NOT `land_cdc`/`local_sql_catalog`)
seeding a CDC table `("s","customers")` with identity `id`, columns
`id: Long, name: String`:

```rust
//! mv_enrich_scan: the slice-5 enrich read — folded current state (merge
//! engine applied), optional typed IN key predicate, user columns only.
//! loom_fixture_test (Postgres + local warehouse).

use engine_serving::mv_enrich::mv_enrich_scan;
// ...mirror merge_on_read.rs's imports + seed helpers...

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enrich_scan_folds_filters_and_hides_framing() {
    // Seed: +I id=1 "ada", +I id=2 "bob", update id=1 -> "ada2", delete id=2,
    // +I id=3 "cyd"; flush mid-history so state spans files ∪ inline.
    // ... land_cdc / flush_table calls per the merge_on_read.rs pattern ...

    // 1. Unkeyed: the full folded current state (LastRow): {1:"ada2", 3:"cyd"}.
    let (schema, batches) = mv_enrich_scan(&catalog, &table, None, None).await.unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(names.iter().all(|n| !n.starts_with("loom_")), "logical read: no framing");
    assert_eq!(ids(&batches), vec![1, 3], "folded: update applied, delete dropped");

    // 2. Keyed: only the requested keys' folded rows.
    let keys = [serde_json::json!(1), serde_json::json!(99)];
    let (_, keyed) = mv_enrich_scan(&catalog, &table, Some(("id", &keys)), None).await.unwrap();
    assert_eq!(ids(&keyed), vec![1], "key 99 has no row; key 1 folded");

    // 3. Live-but-empty table: declared schema, zero rows.
    // ... ensure_table for ("s","empty") with columns, no rows ...
    let (empty_schema, empty) = mv_enrich_scan(&catalog, &empty_table, None, None).await.unwrap();
    assert!(empty.iter().all(|b| b.num_rows() == 0));
    assert!(empty_schema.fields().len() > 0, "declared logical schema");

    // 4. Unknown table: deterministic error.
    assert!(mv_enrich_scan(&catalog, &tref("s", "nope"), None, None).await.is_err());

    // 5. Non-coercible key value: deterministic error, not a silent miss.
    let bad = [serde_json::json!({"not": "a scalar"})];
    assert!(mv_enrich_scan(&catalog, &table, Some(("id", &bad)), None).await.is_err());
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/engine-serving/BUCK`, mirror the `merge-on-read`
`loom_fixture_test` target: add `mv-enrich-scan` with
`srcs = ["tests/mv_enrich_scan.rs"]` and the same dep set plus
`//third-party:serde_json`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/engine-serving:mv-enrich-scan`
Expected: FAIL — `mv_enrich` module not found.

- [ ] **Step 4: Implement `mv_enrich_scan`**

Create `src/services/engine-serving/src/mv_enrich.rs`:

```rust
//! The slice-5 enrich read: a table's folded current state (the merge engine
//! applied for CDC tables; files ∪ inline for log/plain tables), optionally
//! filtered to a lookup-key set. A LOGICAL read — framing/reserved columns
//! are hidden by the serving provider. This is the point-lookup API the
//! deferred fut-stream-pk-index later re-backs with an index probe; v1 backs
//! it with the predicated merge-on-read scan (correctness never depends on
//! pruning). Internal, ungoverned data plane — parity with transform inputs.

pub async fn mv_enrich_scan(
    catalog: &IcebergCatalog,
    table: &TableRef,
    key: Option<(&str, &[serde_json::Value])>,
    serving_store: Option<&ServingStore>,
) -> Result<(SchemaRef, Vec<RecordBatch>), EngineServingError> {
    let ctx = SessionContext::new();
    let Some(provider) =
        build_serving_provider(&ctx, catalog, table, serving_store, None).await?
    else {
        // Live-but-empty: serve the declared logical schema with zero rows
        // (the transform-input posture). An UNKNOWN table errors here.
        let columns = declared_columns(catalog, table).await?; // Catalog trait lookup
        let schema = logical_arrow_schema(&columns).map_err(to_serving_infer)?;
        return Ok((schema, Vec::new()));
    };
    let df = ctx.read_table(provider).map_err(to_serving)?;
    let df = match key {
        None => df,
        Some((col, keys)) => {
            let field = df.schema().field_with_unqualified_name(col).map_err(|_| {
                EngineServingError::Engine(format!(
                    "mv enrich: key column '{col}' not in {}.{}", table.schema, table.name
                ))
            })?;
            let literals = coerce_keys(field.data_type(), keys)?; // typed, loud on mismatch
            df.filter(col_expr(col).in_list(literals, false)).map_err(to_serving)?
        }
    };
    let schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.map_err(to_serving)?;
    Ok((schema, batches))
}
```

`coerce_keys` maps JSON scalars to typed `Expr` literals for `Int32`/`Int64`/
`Utf8` key columns (a JSON number that doesn't fit, a non-scalar, or any other
column type is a loud `EngineServingError::Engine(format!("mv enrich: …"))` with
the stable prefix — deterministic worker abandon, never a silent empty result).
The empty-table fallback's unknown-table error likewise carries the `"mv enrich:"`
prefix. Filtering above the merge view is
correct for any column; when `col` is the CDC identity DataFusion may push the
predicate below the fold (its partition key) toward Parquet pruning — the
optimization, never the correctness. Resolve the exact declared-columns lookup
(`Catalog` trait via the catalog handle, as `build_serving_provider` itself
does at `serving.rs:74`) at implementation time.

- [ ] **Step 5: Run the test to verify it passes**

Run: `buck2 test --console none //src/services/engine-serving:mv-enrich-scan //src/services/engine-serving:merge-on-read`
Expected: PASS — and `merge-on-read` untouched/green.

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine-serving): mv_enrich_scan — folded state + typed key predicate

The slice-5 enrich read over build_serving_provider's merged current state
(CDC fold per declared engine), with an optional IN key filter coerced to
the column's Arrow type. Empty tables serve the declared schema; unknown
tables and bad keys error loudly. Framing stays hidden (logical read)."
```

---

## Task 4: engine — `do_get` dispatch arm + Flight round-trip over UDS

**Files:**
- Modify: `src/services/engine/src/flight.rs` (`do_get` at `:240`; new `do_get_mv_enrich` beside `do_get_governed_sql` at `:117`)
- Test: `src/services/engine/tests/mv_enrich_flight.rs` (new) + `src/services/engine/BUCK`

**Interfaces:**
- Consumes: `EngineTicket::MvEnrich` (T2); `mv_enrich_scan` (T3); the engine struct `FlightDataService`'s `serving_catalog: IcebergCatalog` field (`engine/src/flight.rs:56`) and `serving_store` field (`:58`); **`do_get_governed_sql` (`:122`) as the placement/delegation template — NOT `do_get_mv_delta` (`:177`), which passes the raw `SqlCatalog` (`self.catalog`, `:53`) because `mv_delta_scan` needs it. `mv_enrich_scan` wants the `IcebergCatalog`, so mirror the governed-sql arm.**
- Produces: the engine serves `MvEnrichTicket` end-to-end; `fetch_mv_enrich` works against a live engine.

- [ ] **Step 1: Write the failing test**

Create `src/services/engine/tests/mv_enrich_flight.rs`, mirroring the
`governed-flight` test's harness (spawn the engine over a UDS with a seeded
fixture — reuse the same spawn/seed helpers that test uses; seed a CDC table
as in Task 3):

```rust
//! do_get(MvEnrichTicket) end-to-end over the engine's Flight UDS:
//! keyed and unkeyed enrich reads return the folded state; an unknown
//! table maps to a gRPC error status. loom_fixture_test.

use engine_wire::flight::{FlightTableClient, MvEnrichTicket};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mv_enrich_round_trips_over_flight() {
    // ...spawn engine + seed customers CDC per the governed-flight pattern...
    let client = FlightTableClient::connect(socket).await.unwrap();

    let unkeyed = client
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: None,
            keys: vec![],
        })
        .await
        .unwrap();
    assert_eq!(row_count(&unkeyed), 2, "full folded state");

    let keyed = client
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "customers".into(),
            key: Some("id".into()),
            keys: vec![serde_json::json!(1)],
        })
        .await
        .unwrap();
    assert_eq!(row_count(&keyed), 1, "keyed lookup");

    let missing = client
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: "s".into(),
            enrich_name: "nope".into(),
            key: None,
            keys: vec![],
        })
        .await;
    assert!(missing.is_err(), "unknown table is a wire error");
}
```

- [ ] **Step 2: Wire the test target**

In `src/services/engine/BUCK`, mirror the `governed-flight`
`loom_fixture_test` target: add `mv-enrich-flight` with
`srcs = ["tests/mv_enrich_flight.rs"]` and the same dep set plus
`//third-party:serde_json`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/engine:mv-enrich-flight`
Expected: FAIL — the decode succeeds (T2) but `do_get` has no `MvEnrich` arm
(non-exhaustive match compile error, or the terminal-arm error at runtime).

- [ ] **Step 4: Implement the dispatch arm**

In `src/services/engine/src/flight.rs`, add `EngineTicket::MvEnrich(t)` to the
`do_get` match (`:240`) beside slice-4's `MvDelta` arm, delegating to a new
`do_get_mv_enrich` that mirrors `do_get_governed_sql`'s shape (`:122`): call
`engine_serving::mv_enrich::mv_enrich_scan(&self.serving_catalog, &table,
key_as_option_tuple, self.serving_store.as_ref())` — **note `&self.serving_catalog`
(the `IcebergCatalog`, `:56`), NOT `&self.catalog` (the `SqlCatalog`, `:53`, which
`do_get_mv_delta` uses)**. Map `EngineServingError` onto the module's existing
status mapping. **Deterministic refusals from `mv_enrich_scan` (unknown table,
bad key) carry the `"mv enrich:"` message prefix and map to `failed_precondition`,
mirroring `do_get_mv_delta` (`:193-198`) — the worker branches on that prefix, not
a gRPC code (see Task 5).** Stream the `(schema, batches)` schema-first — the same
encode path the other unary `do_get_*` handlers use.

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/services/engine:mv-enrich-flight //src/services/engine:ticket-errors //src/services/engine:flight-ticket-membership`
Expected: PASS — new arm serves; ticket-error pins and membership tests green
(extend the membership test's variant list mechanically if it enumerates
`EngineTicket` variants).

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(engine): serve MvEnrichTicket on the Flight data plane

do_get gains the MvEnrich arm -> mv_enrich_scan; keyed and unkeyed enrich
reads round-trip over the UDS; unknown tables map to gRPC status."
```

---

## Task 5: worker — the enrich branch in `handle_stream_mv` + the headline lookup-join e2e

**Files:**
- Modify: `src/services/worker/src/stream_mv.rs` (slice-4-produced — symbolic)
- Test: `src/services/worker/tests/stream_mv_join_e2e.rs` (new) + `src/services/worker/BUCK`

**Interfaces:**
- Consumes: `handle_stream_mv`/`StreamMvCtx`, `fetch_mv_delta`, `commit_micro_batch` (slice-4, by name); `fetch_mv_enrich` (T2/T4); `StreamMvJob.enrich`/`.on`, `MAX_LOOKUP_KEYS` (T1); `datafusion_io::{register_batches, register_empty_table}` (`datafusion-io/src/scan.rs:125`/`:115`); `loom_test_flight::spawn_engine_uds` + `loom_test_seed::local_sql_catalog` (`worker/tests/transform_e2e.rs:7-8`); slice-4's `stream-mv-e2e` harness/BUCK target (by name).
- Produces: `handle_stream_mv` runs join MVs end-to-end (state-join and lookup-join); a worker-private `distinct_lookup_keys(batches, col) -> Result<Vec<serde_json::Value>, JobFailure>` helper.

- [ ] **Step 1: Write the failing test**

Create `src/services/worker/tests/stream_mv_join_e2e.rs`, mirroring slice-4's
`stream_mv_e2e` harness (spawn engine UDS, seed via the engine wire, drive
`define_transform` + `submit_run` + `handle_stream_mv`). Topology: `s.orders`
(declared log source; columns `id: Long, customer_id: Long, amount: Long`),
`s.customers` (CDC enrich; identity `id`, columns `id: Long, name: String`),
output `s.enriched_orders` (buckets 2). Join SQL:
`SELECT o.id, o.customer_id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id`.

```rust
//! Slice-5 headline e2e: a lookup-join MV emits the enriched stream.
//! Multi-micro-batch (flush between), processing-time enrichment semantics,
//! keyed ≡ unkeyed equivalence, exactly-once rerun. loom_fixture_test.
```

Cases (one `#[tokio::test]` each or a shared-seed sequence, per the slice-4
e2e's structure):

1. **The lookup-join emits the enriched stream** (the spec's testing intent):
   seed customers `{1:"ada", 2:"bob"}`; append orders batch 1 (customer_ids
   1,2); run the MV with `on = LookupOn { source_col: "customer_id",
   enrich_col: "id" }` → the output's physical rows are the enriched rows
   (`name` joined in), framed `+I`, per-bucket gapless from 0.
2. **Multi-batch convergence with a flush boundary**: append orders batch 2,
   `flush_table` the source between batches, run again → cumulative output ==
   the batch-equivalent join of ALL orders against (quiescent) customers, no
   duplicates, offsets still gapless.
3. **Processing-time semantics**: update customer 1's name, append orders
   batch 3, run → batch-3 output rows carry the NEW name; batch-1/2 output
   rows are untouched (no retro re-emission — the deferred incremental join's
   job).
4. **Keyed ≡ unkeyed**: run the same topology as a state-join (`on: None`)
   into a second output table → identical enriched row-sets per batch.
5. **Exactly-once rerun**: re-submit with no new source rows → empty-delta
   no-op; output unchanged.
6. **Deterministic abandons**: enrich table that doesn't exist → the run
   fails terminally with the unknown-table error; a `LookupOn.source_col`
   missing from the delta → terminal failure.

- [ ] **Step 2: Wire the test target**

In `src/services/worker/BUCK`, mirror slice-4's `stream-mv-e2e`
`loom_fixture_test` target (itself mirroring `transform-e2e`, `BUCK:184`): add
`stream-mv-join-e2e` with `srcs = ["tests/stream_mv_join_e2e.rs"]` and the
same dep set.

- [ ] **Step 3: Run the test to verify it fails**

Run: `buck2 test --console none //src/services/worker:stream-mv-join-e2e`
Expected: FAIL — `handle_stream_mv` ignores `job.enrich` (the SQL references
an unregistered `customers` table → SQL-planning abandon).

- [ ] **Step 4: Implement the enrich branch**

In `src/services/worker/src/stream_mv.rs`, between slice-4's
"strip framing / register the delta under `source.name`" step (the real vars
are `stripped` for the user-column delta batches and `df_ctx` for the
DataFusion context — the enrich branch goes in the non-empty `else` between
`register_batches` (`:108`) and `df_ctx.sql` (`:111`)) and "run the SQL" step,
add (only when `job.enrich` is `Some(enrich)`):

```rust
    // Slice 5: register the enrich table's folded current state beside the
    // delta. Keyed (lookup-join) when `on` is present and the delta's
    // distinct key count is bounded; full state (state-join) otherwise —
    // a superset is always correct.
    let key_payload = match &job.on {
        None => None,
        Some(on) => {
            let keys = distinct_lookup_keys(&stripped, &on.source_col)?;
            (keys.len() <= MAX_LOOKUP_KEYS).then(|| (on.enrich_col.clone(), keys))
        }
    };
    let enrich_batches = ctx
        .table
        .fetch_mv_enrich(MvEnrichTicket {
            enrich_schema: enrich.schema.clone(),
            enrich_name: enrich.name.clone(),
            key: key_payload.as_ref().map(|(c, _)| c.clone()),
            keys: key_payload.map(|(_, k)| k).unwrap_or_default(),
        })
        .await
        .map_err(classify_enrich_error)?; // msg.contains("mv enrich:") => abandon; else retry+backoff
    match enrich_batches.first().map(|b| b.schema()) {
        None => { /* empty state: register_empty_table under enrich.name is not
                     possible without a schema — the engine always streams
                     schema-first, so decode yields the schema even for zero
                     rows; register it (register_empty_table) */ }
        Some(schema) => register_batches(&df_ctx, &enrich.name, schema, enrich_batches)
            .map_err(|e| JobFailure::abandon(format!("register enrich: {e}")))?,
    }
```

`distinct_lookup_keys` (worker-private, in `stream_mv.rs`): downcast the named
column per batch to `Int64Array`/`Int32Array`/`StringArray`, collect distinct
non-null values into a `BTreeSet`, emit as `serde_json::Value`s; a missing
column or any other array type is `JobFailure::abandon` (deterministic — the
def's `on` doesn't match the delta schema). **`classify_enrich_error`
(worker-private) mirrors slice-4's `fetch_mv_delta` error handling
(`stream_mv.rs:63-84`): the `FlightTableClient` flattens gRPC status so only the
error *message* survives — abandon when `msg.contains("mv enrich:")` (the
engine-side deterministic-refusal prefix), else retry with
`worker_tuning.backoff`. Do NOT branch on `tonic::Code` — it does not survive
the flattening.** Error taxonomy otherwise stays slice-4's:
decode/SQL/register → abandon; wire/store → retry; the CAS "superseded" path is
untouched. Lineage:
extend the run's `lineage_json` inputs to `[source, enrich]` and the payload
with `"enrich"` (+ `"on"` when keyed) beside slice-4's
`{"sql", "mv", "offsets"}`.

Resolve at implementation time (slice-4 code, symbolic here): the exact
variable names for the stripped user-column delta and the `df_ctx`, and
whether the engine's schema-first stream lets the empty-enrich case flow
through `register_batches` with zero-row batches (preferred) or needs
`register_empty_table`.

- [ ] **Step 5: Run the tests**

Run: `buck2 test --console none //src/services/worker:stream-mv-join-e2e //src/services/worker:stream-mv-e2e`
Expected: PASS — the join e2e lands and slice-4's MV e2e (non-join path)
stays green, byte-identical behavior.

- [ ] **Step 6: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "feat(worker): lookup-join + state-join in handle_stream_mv

The slice-5 enrich branch: distinct delta keys -> keyed MvEnrichTicket
(state-join full fetch when unkeyed or over MAX_LOOKUP_KEYS), registered
beside the delta; SQL, commit, watermark CAS, and error taxonomy unchanged.
The lookup-join emits the enriched stream (headline e2e: multi-batch
convergence across a flush, processing-time enrichment, keyed==unkeyed,
no-op rerun)."
```

---

## Task 6: triggers, cycles, composability — the enrich-edge e2e

**Files:**
- Test: `src/services/worker/tests/stream_mv_join_triggers.rs` (new) + `src/services/worker/BUCK`
- Modify (only if a case fails): `src/control-plane/core/src/transforms.rs` arm wiring from Task 1

**Interfaces:**
- Consumes: `TriggerNode::resolve` arm (T1); `pg_fire_data_triggers` (`postgres/src/transforms.rs:137` — shipped); `validate_no_trigger_cycle` (`core/src/transforms.rs:325` — shipped); slice-4's composability e2e harness (by name).
- Produces: proof that the enrich edge triggers, no-ops, cycle-checks, and composes.

- [ ] **Step 1: Write the failing-or-passing test** (this task is
      verification-heavy: most behavior falls out of Task 1's `resolve` arm —
      the test pins it)

Create `src/services/worker/tests/stream_mv_join_triggers.rs`, mirroring
slice-4's trigger/composability e2e harness:

1. **Enrich commit wakes, then no-ops**: define the Task-5 join MV with
   `on_input_commit: true`; commit rows to `customers` only → a run is
   enqueued (debounced), `handle_stream_mv` processes it as an empty source
   delta → no output rows, watermark unchanged, run Succeeded.
2. **Source commit produces output**: commit to `orders` → the triggered run
   emits enriched rows.
3. **Join MV composes downstream**: a plain slice-4 `MicroBatch` MV over
   `s.enriched_orders` with `on_input_commit: true` fires off the join MV's
   output commit and produces its own output.
4. **Enrich-edge cycle rejected at define time**: MV₁ =
   `MicroBatchJoin { source: s.a, enrich: s.out2, output: s.out1 }`, MV₂ =
   `MicroBatchJoin { source: s.out1, enrich: s.b, output: s.out2 }` — the
   second `define_transform` fails with the cycle `Validation` error
   (`validate_no_trigger_cycle` sees the enrich edge because `resolve` lists
   it as an input).

- [ ] **Step 2: Wire the test target**

In `src/services/worker/BUCK`, add `stream-mv-join-triggers` mirroring the
Task-5 target.

- [ ] **Step 3: Run the test**

Run: `buck2 test --console none //src/services/worker:stream-mv-join-triggers`
Expected: cases 1–3 may pass immediately (they fall out of T1+T5); case 4
must pass via the `resolve` arm. Fix any gap in the Task-1 arm (not in the
test) until all four pass.

- [ ] **Step 4: Run prek + commit**

```bash
buck2 run //tools:prek -- run --all-files
git add -A
git commit -m "test(stream): join-MV trigger/cycle/composability e2e

Enrich commits debounce-wake the join MV (empty-delta no-op), source
commits produce enriched output, the output composes into a downstream
slice-4 MV, and an enrich-edge cycle is rejected at define time."
```

---

## Task 7: Full verification

- [ ] **Step 1: Whole-tree build**

Run: `buck2 build -v0 --console none //src/...`
Expected: exit 0.

- [ ] **Step 2: The slice's suites + every touched crate's neighbors**

Run: `buck2 test --console none //src/control-plane/core:microbatch-join //src/services/engine-wire:mv-enrich-ticket //src/services/engine-wire:engine-ticket //src/services/engine-serving:mv-enrich-scan //src/services/engine-serving:merge-on-read //src/services/engine:mv-enrich-flight //src/services/engine:ticket-errors //src/services/worker:stream-mv-join-e2e //src/services/worker:stream-mv-join-triggers //src/services/worker:stream-mv-e2e //src/services/worker:transform-e2e`
Expected: `Tests finished: Pass N. Fail 0.`

- [ ] **Step 3: Full suite**

Run (local, non-root): `buck2 test --console none -j 8 //src/...`
(cloud: scope to the btd-affected targets instead — never a bare whole-tree
test run; `buck2 clean` between heavy phases).
Expected: all green — in particular slice 4's MV suites, the `stream_*`
suites, and the ticket/decode pins, unmodified.

- [ ] **Step 4: Lint gate**

Run: `buck2 run //tools:prek -- run --all-files`
Expected: clean (rustfmt, clippy pedantic+restriction, markdown hooks).

- [ ] **Step 5: Close out the registers**

Run the `loom-docs-update` skill: close `road-stream-joins` (remove its
register entry in the closing PR; document the landed capability under
`docs/system-capabilities/`); confirm `fut-stream-pk-index` and
`fut-stream-incremental-join` remain open in `docs/FUTURE.md` with this
slice's spec as their `from:` context (the ticket-seam framing for the PK
index is worth a one-line note on the item). Then finish the branch per
`superpowers:finishing-a-development-branch` (push + PR; CI green per the
polling-CI method).
