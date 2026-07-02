# Pillar idioms audit — abstraction, simplification & test-tooling programme

**Status:** approved design
**Date:** 2026-07-02
**Audit base:** `e354b04` (line references below are as of that commit; PRs #292/#293
landed mid-audit — see *Post-audit drift* at the end)
**Inputs:** full-source review of the four service pillars + control plane by five
parallel audit agents; `docs/code-health/complexity.md` (93 hotspots, as of
`ba068d4`); `docs/code-health/duplication.md` (161 pairs, as of `a20c351`).

## Summary

A systematic audit of query-api, the control plane, the engine/wire/worker
cluster, ingest, and the test estate found ~60 improvement opportunities. Six
are genuine defects (filed in `ISSUES.md`). The rest cluster around **one
dominant production pattern** — *a sibling path was added later and copied from
the first* (governed↔ungoverned, streaming↔unary, wire↔direct-PG,
inline-read↔PG-provider, and query-api's seven copies of the governance
prologue) — and **one dominant test pattern** (per-file copies of seed/boot
helpers that were never hoisted, or were hoisted and never migrated to).

This design packages the work as: 6 `ISSUES` entries (Wave 0), 17 `ROADMAP`
items in two waves (7 foundations, 10 consumers/cleanups), and 5 `FUTURE`
items that need their own design pass or are opt-in tooling. Each `road-` item
is sized for one `work/<id>` branch/PR and references this spec.

Scope ground rules agreed with the maintainer:

- Production and test code both in scope, ranked by value.
- **Anything goes**: pre-1.0, no external users — internal APIs, wire details,
  and endpoint semantics may change where it substantially simplifies.
- Everything lands via the register workflow (`loom-work-checkout`), TDD per
  repo norms; duplication items prove themselves with a register diff
  (`/loom-duplication diff`), complexity items with a metric drop
  (`/loom-complexity diff`).

## Wave 0 — defects found by the audit (ISSUES)

Filed as open ISSUES entries; each names the ROADMAP item that fixes it.

1. **`iss-pg-provider-vector-drift`** — `engine-serving/src/provider.rs::pg_rows_to_arrays`
   (:116-202) is a drifted copy of `control-plane/postgres/src/iceberg_inline.rs::column_array`:
   it lacks the `vector(...)` arm, so a SQL read projecting a vector column of a
   table with live inline rows fails at scan time with "unsupported logical
   type" even though `arrow_schema_from_mirror` presents the column. The two
   authorities also disagree on the Arrow list element name (`base_to_arrow`
   says `"element"`, `arrow_field` says `"item"`). Fixed by
   `road-value-bridge`.
2. **`iss-inline-downcast-panic`** — `iceberg_inline.rs::cell_from_arrow`'s `dc!`
   macro (:126-131) expands to `.expect("inline arrow downcast")`, a production
   panic reachable whenever a decoded IPC batch's arrow type mismatches the
   declared logical column (nothing upstream validates arrow types —
   `align_to_columns` matches by name only). The macro expansion slips past the
   enforced `clippy::expect_used` gate. The same function's vector branch
   returns a proper `Err`. Fix: fallible macro returning
   `ControlPlaneError::Validation` (+ optional batch-level arrow-type check in
   `align_to_columns`). Small fix; rides with `road-cp-adapter-hygiene` or ships
   alone.
3. **`iss-vector-build-lineage-ref`** — `postgres/src/vector_index.rs::build_vector_index`
   emits `DatasetRef { namespace: table.schema, name: table.name }` (:506-509)
   while the flush path emits the canonical `DatasetId::from(table).dataset_ref()`
   — the same physical table gets two disconnected lineage nodes depending on
   which job touched it, breaking upstream/downstream joins. Small standalone
   fix; also folded into `road-vector-build-decomposition`.
4. **`iss-inline-delta-string-identity`** — `postgres/src/vector_index.rs::inline_delta_batch`
   hardcodes the identity column as `Int64` (:339, :345, :358) while
   `extract_rows` supports `Utf8` identities — a string-identity type silently
   breaks the hot-delta search path. Fixed by `road-vector-build-decomposition`
   (support both, or reject string identities at `define_vector_index` time).
5. **`iss-config-silent-fallbacks`** — stragglers that violate the config seam's
   own "present-but-malformed fails startup" rule:
   `runtime/src/auth.rs::{service_token_max_ttl_from_env,session_ttl_from_env}`
   (`.ok().and_then(|s| s.parse().ok())` — a typo'd TTL silently yields the
   default), `engine/src/run.rs::parse_env_or` (reads live env, bypassing the
   snapshot), and raw `LOOM_BOOTSTRAP_ADMIN_*` reads in mains/standalone. Fixed
   by `road-service-bootstrap`.
6. **`iss-transform-catalog-local-only`** — `transform/src/main.rs::build_iceberg_catalog`
   (:100-116) hardcodes `LocalFsStorageFactory` + `file://{data_path}`,
   ignoring `cfg.object_store` — the transform binary cannot commit to an S3
   warehouse even though every other writer goes through
   `build_storage_factory`. Fixed by `road-dead-path-sweep`.

## Wave 1 — foundations (ROADMAP)

### road-qa-governance-layer — query-api governed-read spine

Every query-api entry point re-implements the same ~20-line prologue — build
`TypeName`/`PolicyTarget`, `acl.check == Deny → Forbidden` (deny **before**
existence leak), `get_type` with `NotFound` mapping, `load_policy` →
`(row_filters, denied, masked)` — seven times (`handler.rs:303-319, 542-561,
718-730, 940-959, 1416-1431, 1572-1587, 1713-1728`, plus per-hop variants).
This is the direct source of the register's cc-33..49 handler hotspots, and
`read_object_page` runs the prologue itself *and then* calls
`compile_object_read` which runs it again — 2× `acl.check`, 2× `get_type`,
2× `policies_for` per paginated read.

Design:

```rust
pub(crate) struct GovernedType {
    otype: ObjectType,
    row_filters: Vec<RowFilter>,
    denied: HashSet<String>,
    masked: HashSet<String>,
}
enum OnMissing { NotFound, Forbidden } // 404 vs no-leak 403 (vector_search)
async fn resolve_governed(ontology, acl, subject, name, on_missing)
    -> Result<GovernedType, QueryError>;
```

plus three companions that collapse the per-endpoint copies:

- `Projection` value type (`visible()` fail-closed on empty; owns the
  mask-set + logical-types zip + the `debug_assert_eq!` column-order check +
  `into_object_rows`) — closes the census pair `handler.rs:1221-1247 ≈
  1094-1120` and its four siblings. Also absorbs the ~10×
  `properties.iter().find(...).unwrap_or("")` sentinel with an explicit
  `prop_ty(...) -> Option<&str>` lookup.
- `seed_predicates(g, allowed, filters, ids)` — the visibility-gate + coerce +
  `_ids` loop copied 3× across the graph paths (and half-copied twice more);
  also fixes `resolve_chain`'s `project_allowed` recomputation inside its
  per-filter loop.
- `resolve_hop(ontology, current, hop) -> (TypeName, LinkBacking)` — the
  forward/inverse link match (`links` vs `links_to`, ambiguity check,
  `backing.reversed()`) duplicated verbatim between `resolve_chain` and
  `resolve_graph`.

`compile_object_read` splits into a thin governed wrapper over
`compile_object_read_with(&GovernedType, …)` so `read_object_page` resolves
once. Behavior-preserving; the e2e suite pins the 403/404 distinctions.

### road-value-bridge — one authoritative PG↔Arrow conversion layer

`pg_rows_to_arrays` is deleted in favour of the postgres adapter's
`column_array`/`arrow_type` (engine-serving already depends on
`control_plane_postgres` — no new crate). `base_to_arrow` goes too; the
`"element"`/`"item"` divergence collapses to one name (needs a check of the
union path + vector e2es). `PgTableProvider.logical_types: Vec<String>`
becomes `Vec<BaseType>` resolved once at provider construction, so unsupported
types are rejected up front, not mid-scan. One authoritative
`BaseType → DataType` map (core already owns `BaseType`/`resolve_logical`)
that all four conversion sites consult; `datafusion-io::infer` stays
deliberately narrow but delegates to the same map.

Deliberately **not** a grand unified value-codec trait: `one_cell`
(SqlValue→Arrow) and `cell_from_arrow` (Arrow→PG bind) are different
directions with different domains — unifying them all is over-abstraction.
The full SqlValue codec consolidation stays with `fut-coercion-taxonomy`
(this audit's concretization: seven per-direction dispatch functions —
`params::parse_value`, `filter::coerce_filter`, `render::render_typed`,
`serving::one_cell` + `build_object_batches`, `serving_datafusion::arrow_to_sqlvalue`,
`write_filter::eq_cell/order_cell`, `flight_export::base_to_arrow` — should
land in a `loom-value` module in core when that item is picked up, including
the Arrow legs, and `build_object_batches` should get real per-column builders
instead of N single-cell arrays + `concat`).

Fixes `iss-pg-provider-vector-drift`.

### road-iceberg-commit-skeleton — CommitExtras unification

Six sites repeat the mirror snapshot-commit skeleton (`begin` →
`next_snapshot` → `ensure_table` → end-caps → `register_files`/project →
`pg_emit` → jobs → `commit`): `iceberg_landing.rs::{append_parquet_snapshot,
land_additive, overwrite_truncate}`, `iceberg_sql_catalog/catalog.rs::{write_mirror,
do_update_table}`, `iceberg_control_plane.rs::IcebergTx::commit`. The
inline-row end-cap SQL is copy-pasted **verbatim** in `iceberg_landing.rs:374-391`
and `catalog.rs:553-567`. The crate already has the right aggregate —
`CommitExtras<'a> { lineage, end_cap, overwrite, jobs }` — but the landing
functions take 8-9 loose params under `#[allow(too_many_arguments)]` instead.

Design: (1) extract `end_cap_inline_rows_by_id(conn, table_id, row_ids, at)`
into `iceberg_inline.rs`, called from both duplicate sites; (2) landing
signatures carry `CommitExtras<'_>`, deleting three `too_many_arguments`
allows; (3) `apply_commit_extras(conn, at, &CommitExtras)` (end-cap + lineage
+ jobs) shared by `land_additive` and `do_update_table` — the latter becomes
CAS → `write_mirror` → `apply_commit_extras` → commit; (4) move the loom-added
members of the **vendored** `catalog.rs` (`CommitExtras`, `InlineEndCap`,
`write_mirror`, `do_update_table`, `delete_file`) into a loom-owned sibling
module (`iceberg_sql_catalog/commit_mirror.rs`) so the vendored file stays
close to upstream for re-vendoring diffs.

### road-vector-index-codec — split core/vector_index.rs behind a shared codec

The three (de)serializers (`FlatIndex` cc 28, `IvfFlatIndex` cc 37, `HnswIndex`
cc 49) each hand-roll the identical header (`LVIX` magic | version | metric |
kind), the identical bounded-`with_capacity` f32-section pattern, and the
identical keys section — ~120 duplicated lines. `decode()` peeks a hard-coded
byte offset that must stay in sync with all three writers; the private
`Cursor` shadows `core::page::Cursor`.

Verdict on hand-rolled binary: **keep it** — the format is deliberately
compact/self-describing for Puffin blobs and the corrupt-header
`remaining()`-bound guard is a real property a serde codec would need
re-verification to preserve. Extraction, not replacement:

restructure as `core/src/vector_index/{mod,codec,flat,ivf,hnsw,kmeans}.rs`
with `codec.rs` owning `ByteReader` (renamed `Cursor`), `write_header`/
`read_header`, `write_f32s`/`read_f32s` (owns the `remaining()` bound),
`write_keys`/`read_keys`, `pack_rows` (the rows→`(keys,data)` packing with
dim-check that all three `build`s repeat), and `KIND_OFFSET`. Each
`deserialize` becomes ~15 lines of format-specific fields. Cognitive hotspots:
`kmeans` extracts `nearest_centroid` (shared by assign + init loops);
`hnsw_search_layer` extracts `pop_nearest`/`evict_worst` (linear scans stay —
deliberate, bounded by `ef`). Byte-identical output; characterize with a
round-trip/golden-bytes test **first**.

### road-service-bootstrap — shared service bootstrap + config-parse helpers

The config→pool→control-plane→auth→bootstrap-admin sequence is copied across
`query-api/src/main.rs`, `ingest/src/main.rs`, `standalone/src/lib.rs`, and
(prefix) `engine/src/main.rs` — including the embedded-PG keep-alive handle
(`_pg`) that each caller must remember to bind. `runtime/src/lib.rs::from_map`
(cc 35) hand-rolls the shapes `loom-config` exists to eliminate.

Design: `service_runtime` owns

```rust
pub struct ServiceContext {
    pub cfg: Config, pub pool: PgPool, pub pg: Arc<PgControlPlane>,
    pub auth: AuthState, pub admin_subject: Option<SubjectId>,
    pub max_ttl: Duration,
    _embedded: Option<managed_postgres::EmbeddedPg>, // kept alive by ownership
}
pub enum Boot { Migrated, Ready(ServiceContext) }
pub async fn bootstrap(env: &HashMap<String, String>) -> Result<Boot, RuntimeError>;
```

plus two `loom-config` primitives used tree-wide — `req_var(vars, key)` and
`parse_var<T: FromStr>(vars, key, default)` — and `impl From<StoreConfigError>
for ConfigError`. `from_map` decomposes onto `DbConfig::from_map` /
`EmbeddedSettings::from_map` / `parse_migrate_on_boot` (~15-line composition).
The `iss-config-silent-fallbacks` sites convert to fail-loud fallible readers
over the env snapshot. Explicitly **not** adopting figment/config-rs — the
repo already has a typed seam with deliberate semantics; converge, don't
replace. No overlap with `fut-config-yaml-format` (format untouched).
The worker stays out (zero-pool by design); engine uses the prefix only.

### road-test-shared-pg-fixture — one PG cluster per test process

116 files / 340 `PgFixture::start()` call sites boot a full `initdb` +
`postgres` per test **function**; the 8-slot `BootThrottle` and the recorded
`-j 8` workaround exist because of it. `fresh_db()` already gives
database-level isolation within one cluster.

Design: `PgFixture::shared() -> &'static PgFixture` via `OnceLock`; migration
is a mechanical `start()` → `shared()` sweep (call sites already pass `&fx`).
Caveats to verify in the spec'd work: cluster-level state `fresh_db` doesn't
isolate (roles, template DB); `Drop` never runs for the shared cluster
(acceptable — that's process exit anyway); tests that genuinely need
cluster-level isolation (`boot_throttle.rs`, `s3_storage.rs`) keep `start()`.
Biggest wall-clock/flake win available; verify with the full suite. Land
**before** the harness items so new harnesses take `&PgFixture` and default to
`shared()`.

### road-test-seed-dsl — ObjectType/ActionDef builders (+ update_delete migration)

The ~25-line `ObjectType { properties: vec![PropertyDef {…}, …], … }` literal
appears in every vector test, every update_delete test, testkit's contracts,
and ingest's tests — it is the root cause underneath most register pairs (the
≥20-line threshold is exactly what these literals trip). Design: a small
builder in `control_plane_core` (plain construction, production-legit) or
`testkit::dsl`:

```rust
ObjectType::build("Docs", ("wh", "docs"))
    .prop_req("id", "Long").prop_req("embedding", "vector(4)")
    .identity("id").done();
ActionDef::build("updateWidget", "Widget", ActionKind::Update)
    .param_req("id", "Long").param_req("qty", "Long").done();
```

Proving ground: the **update_delete e2e family** — its `define_widget`/
`grant_writer` helpers were already promoted into `e2e_support.rs:837-967`
but the three original files were never migrated and keep ~130-line private
copies (the finished-promotion/unfinished-migration anti-pattern; the
register's top-6 pairs, 91-127 lines each). Pure deletion + import swap +
a `grant_writer_role` variant absorbing the one signature delta, then the DSL
lands under the survivors.

## Wave 2 — consumers & cleanups (ROADMAP)

### road-qa-read-path-consolidation — sql.rs + graph entry points + http.rs

- **sql.rs finish-the-decomposition**: `compile_graph_reach_union` and
  `recursive_reach_cte` re-inline `reach_seed_where` (now in **three** places
  in one file), `masked_col_exprs`, and `reach_projection_where` — helpers
  that already exist a few functions up. Pure substitution. A `ReachSpec<'a>`
  params struct replaces the six `#[allow(too_many_arguments)]` (strengthened
  by #293's new identity param).
- **Slice patterns over panic-as-fail-closed**: the three
  `#[expect(clippy::indexing_slicing)]` in `caller_predicate_sql` claim
  fail-closed but panic the request thread on a violated invariant;
  `let [lo, hi] = p.values.as_slice() else { return Err(...) }` makes the
  invariant total and lint-clean on the injection-boundary path.
- **Graph entry-point collapse**: `read_graph_reach_union` and
  `read_graph_reach_with_tail` are full parallel copies of `resolve_graph`
  differing only in the ~30-line recursion-structure middle; after the Wave-1
  layer, either three thin functions or one `GraphReadSpec` enum
  (`PathCycle`/`UnionSelfLinks`/`CoreTail`) — the enum also simplifies
  `http.rs::get_graph_path` (cc 25). Watch the tail path's
  `final_type/final_denied/final_masked` fold (subtle; e2e-pinned).
- **http.rs**: fallible `parse_ids`/`parse_depth` extractors + a
  `ReservedParams` splitter (the reserved-param scraper loop is hand-rolled
  5×; the census pair `http.rs:404-430 ≈ 487-513` is the get_linked/chain
  tails — share one `respond_shaped`); one **total** `QueryError → Response`
  mapping (`impl IntoResponse` or `QueryError::status()`) replacing four
  partial copies; `AppState::deps()` replacing 7 hand-built `QueryDeps`
  literals; `flight_export.rs`'s duplicated governed-read block gets a
  `governed(&self, cmd, subject)` helper. Sequenced after
  `road-qa-governance-layer`.

### road-qa-action-decomposition — run_mutate/run_insert phases

`run_mutate` (cc 44, 165 lines) interleaves six concerns; split into
`locate_unique_row` (keeps the corrupt-PK guard), `enforce_mutate_policy`
(the three policy legs), `validate_constraints` (moves from `run_insert`),
`expand_to_full_row`, and a response epilogue shared with `run_insert` (whose
logical-type zip re-implements the handler epilogue — reuse `Projection` from
Wave 1). Behavior-preserving; each phase gets a unit-test seam.

### road-engine-wire-dedup — ticket dispatch, client dedup, structured errors

- `EngineTicket` decode enum **in engine-wire** (where the ticket types and
  their `deny_unknown_fields` disjointness invariants live), carrying the
  load-bearing decode-order comments; engine's `do_get` becomes a flat match,
  each arm ~5 lines via one `encode_response(stream)` helper (the
  encode tail is currently copied 4×). Delete `FlightTicketReq` (a 1:1 field
  copy of `FlightTicket`).
- Client: `execute` = `execute_stream(sql).await?.try_collect()`; shared
  `decode_batches`; a `gov_rpc!` macro for the ~20 clone-shaped governance
  getters in `client.rs` (~200 lines; multi-field methods stay hand-written).
- **Structured wire errors**: `EngineServingError::Engine(String)` +
  `to_serving` erase error classes, so a user's bad SQL surfaces as HTTP 500.
  Split `Plan(DataFusionError)` from `Execution`/`Backend`; one
  `From<EngineServingError> for tonic::Status` (Plan → `invalid_argument`,
  NoIndex → `not_found`, DimMismatch → `invalid_argument`); client maps
  `InvalidArgument` → `ControlPlaneError::Validation` (mirroring the existing
  `cp_status` pattern) so query-api returns 400. Classify conservatively
  (only `ctx.sql()` errors → Plan). After this, `be` (flatten-everything)
  should be rare enough to deserve a warning comment at its definition.

### road-ingest-api-error — typed ApiError + honest wire DTO

Handlers return `Result<Response, ApiError>`; `ApiError::Internal { context,
source }` logs structurally in `IntoResponse` (closing
`iss-ingest-model-500-unlogged` **as a class** — the 4×-copied
`IngestError → status` mapping becomes one `into_api`), `Violations(Vec<_>)`
owns the 422 body. Header parsing collapses into `parse_header<T>`. The
hand-rolled `violations_json` + the parallel doc-only OpenAPI `Violation`
struct become one `WireViolation` deriving `Serialize + ToSchema` (byte-identical
JSON asserted in tests) — the single conversion point that will absorb
`fut-conformance-enum-consolidation`. Follow-on (not in scope): hoist the
pattern into `service_runtime` so all governance-fronted services share one
error idiom.

### road-ingest-bind-decomposition — pure violation collectors + gate/idiom pass

- `bind` (cc 30): steps 1-2 are an inline copy of the same file's
  `schema_of_table` helper (added later, never back-applied); the rest is
  three independent **pure** validation passes → `structural_violations(ty,
  schema) -> Vec<BindViolation>` with `property_violations`/
  `identity_violation`/`reserved_name_violations`, each unit-testable without
  fakes. The two reserved-name loops become one iterator chain.
- `validate_derived` (cc 22): `target_column_type(...)` helper collapsing the
  triplicated `NotFound → MissingAggColumn` arm; one cohesive typing unit on
  `Aggregation` (`column()/label()/column_applicable()/result_expectation()`),
  which deletes the `Count => true // unreachable` arm and gives
  `fut-coercion-taxonomy` a single landing site. These extractions turn both
  tracked consolidation items into *moves* instead of rewrites.
- `gate::validate_values`: five hand-rolled downcast loops (~55 lines) →
  `AsArray` + `.iter().flatten()` dispatch (~20 lines).
- `LineageEvent::completed(outputs, payload)` / `completed_with_run(...)` core
  constructors (the five-field ritual is hand-assembled in both handlers and
  other emitters).
- `extract_pg` (cc 16): guard-clause inversion + `unpack_to_temp`/`publish`
  split.

### road-cp-adapter-hygiene — postgres/memory adapter idioms batch

- Exists-check helpers (`role_exists`/`object_type_exists`/`ensure_*` over
  `PgExecutor`) — the `select exists(...)` boilerplate appears 9× and is most
  of `set_policy`'s cc 17.
- `links`/`links_to` dedup via `query_as!` + a named `LinkRow` (the census
  pair `ontology.rs:204-226 ≈ 246-268`); shared policy-target validation
  ("pure decision function in core, adapter supplies lookups" — the pattern
  `validate_constraints` already proved) for the memory/postgres duplicated
  validation blocks.
- Enum↔string codecs move to core with **fail-loud** unknown-token handling
  (`Cardinality`, `EventType`, `Action`, `Effect`, `ActionKind` — today
  `cardinality_from_str` silently coerces corrupt rows to `One`, etc.;
  `Metric`/`IndexKind` already model the right pattern);
  `PolicyTarget::key_parts()` replaces the twice-written target encoding.
- One `backend()` boxing helper (four coexisting variants, two of which
  Display-flatten and lose sources, against the paid-down `map_err_ignore`
  debt); reclassify parse/dim failures from `Backend` (→500) to `Validation`
  (→4xx) — deliberate, testkit-visible contract change.
- Inline-table access preamble → `existing_inline_table(conn, table)` +
  `mvcc_live_pred(at)` + `quote_ident` (concentrating the `AssertSqlSafe`
  safety argument, currently re-justified per site); move `inline_append`'s
  invariant SQL construction out of its per-row loop; `unnest` the
  `project_files` stat inserts.
- `PageReq::fetch_limit()` (the keyset "+1 sentinel" is re-derived per adapter
  site in two overflow styles); memory adapter keyed by `TableRef` (already
  `Eq + Hash`) instead of cloned tuples; N+1 fixes (`vector_indexes_for` one
  query; `events_for` `event_id = any($1)` — coordinate with
  `fut-lineage-events-page-hydration`); memory `transaction.rs::commit`
  (cc 26/cog 37) → ordered `StagedOp` log mirroring the postgres `WriteMode`
  (also removes a latent replay-order divergence vs postgres semantics).
- The `dc!` fallible-macro fix (`iss-inline-downcast-panic`) if not already
  shipped.

`.sqlx` cache refresh (`tools/sqlx-prepare.sh`) required.

### road-vector-build-decomposition — build_vector_index + IndexSpec::build

`build_vector_index` (cc 41, 182 sloc, 10 numbered jobs) decomposes into
`resolve_build_inputs` / `collect_vectors` (cold Parquet + hot inline) /
`infer_dim` / `bind_index_and_emit`; the `Box<dyn VectorIndex>` construction
match moves to core as `IndexSpec::build(dim, metric, rows)`. Ships the
canonical-lineage fix (`iss-vector-build-lineage-ref`) and string-identity
support on the hot-delta path (`iss-inline-delta-string-identity`), sharing an
`identity_array` helper with `extract_rows`.

### road-dead-path-sweep — delete superseded/dead paths, small structural fixes

- Ingest: delete the production-dead `materialize()`/`land()`/
  `MaterializeRequest` pipeline (only consumer is its own test; module doc
  presents it as *the* orchestrator; keep `resolve_columns`), and **eliminate
  the double Arrow-IPC decode** — `iceberg_landing::land` takes
  `(SchemaRef, Vec<RecordBatch>)` instead of `ipc_body` (the whole tree is on
  one arrow major; the version-boundary rationale is gone), `LandRequest`
  drops its dead `ipc_body` field, the 3-crate `decode_ipc` copies retire to
  one in `datafusion-io`.
- Transform/worker: delete `transform::compact_table`/`CompactConfig`
  (superseded by the worker path; used only by its own e2e — migrate it);
  hoist the byte-identical `TransformConfig`/`WorkerConfig` into `loom-config`;
  fix `build_iceberg_catalog` to use `build_storage_factory`
  (`iss-transform-catalog-local-only`); move `small_files` out of transform so
  the zero-pool worker drops the transform dep; `JobFailure::abandon/retry`
  constructors + a generic `run_wire_job<P, F>` wrapper collapsing the three
  clone-shaped worker handlers.
- Engine: build **one** `Arc<SqlCatalog>` instead of three (≈20 idle PG
  connections per engine); delete `IcebergMirrorTableProvider::try_new`
  (footer-inference contradicts the mirror-authoritative invariant; rewrite
  its two tests over `try_new_with_schema`); `ServingStore` becomes a named
  struct (`bucket`, `store`) instead of a tuple typedef destructured at six
  hops; `VectorQuery<'a>` params struct deleting the deferred
  `too_many_arguments` expect; `register_qualified` +
  `execute_query = collect(execute_query_stream)` dedup in serving;
  store-config: `build_write_store`'s `expect` restructured away, `for_s3_test`
  marked `#[doc(hidden)]`/moved.

### road-test-wire-harness — cross-crate fixtures for the vector/wire clusters

One test-support home (a `//src/testing` `rust_library` or an expanded
`postgres::fixture` + siblings — decide in-work; the cross-crate pattern is
proven by testkit/e2e-support, and all consumers already carry the needed
deps):

- **Vector fixtures** (clears the ~90-pair register cluster across 4 crates):
  `vec4_columns`, `vec4_ipc(rows)`, `test_lineage`, `local_sql_catalog`,
  `seed_docs_vector(fx, db, index, metric, spec, build) -> VectorSeed`,
  `ids_i64`/`distances_f32`/`assert_knn` (the 49-line search-assert block that
  `vector_search.rs` self-duplicates 4×). Also retires query-api
  e2e-support's private fourth copy.
- **Engine UDS harness**: `spawn_engine_uds(fx, db, EngineOpts) -> EngineGuard`
  with **connect-retry readiness** replacing the `sleep(20ms)`-and-hope
  pattern (19 sleep-based syncs tree-wide are the classic flake source);
  hoists what query-api's `spawn_engine`/`spawn_engine_writer` already proved,
  to where engine/worker/engine-serving tests can reach it. Clears the
  wire/worker register pairs.
- **Ingest router support** (`ingest_router(fx, db)`, `sample_batch`,
  `post_ipc`) and shared landed-table asserts (`assert_table_rows`,
  `assert_no_inline`) wanted by postgres/engine/worker tests alike.
- **Fixture telemetry**: `tracing` spans in `PgFixture::start()`/`fresh_db()`
  (slot-wait/initdb/migrate durations) + `LOOM_FIXTURE_TIMING=1` — makes the
  next contention incident diagnosable; adopt `#[traced_test]`/`logs_contain`
  as the standard for error-path logging assertions.

Sequenced after `road-test-shared-pg-fixture` and `road-test-seed-dsl` (the
harnesses take `&PgFixture` defaulting to `shared()` and use the DSL
internally). Non-`rust_test` support sources carry the crate-level
`#![allow(...reason)]` per the lint policy.

### road-test-property-invariants — proptest at the parser/compiler/decoder seams

proptest exists in exactly 2 files; the highest-value missing targets are the
seams where this codebase has *demonstrably* had the bug class (the HNSW and
Flat/IVF deserialize-bounds issues were found by manual adversarial review):

- `sql_compile_props.rs` — over generated filter trees: placeholder count ==
  param count; every emitted identifier quote-wrapped; caller values never
  appear verbatim in emitted SQL (the injection property; currently asserted
  only by example).
- `vector_index_decode_props.rs` — `decode(arbitrary bytes)` never
  panics/overallocates; `decode(encode(x)) == x`.
- `path_parse_props.rs` — arbitrary caller strings through
  `path_parse`/`filter_coerce`.

Pure-logic `rust_test` targets → run on RE, cheap.

## FUTURE items (design-first or opt-in)

- **`fut-tx-trait-segregation`** — `Tx` bundles the backend-neutral unit of
  work with the table-format staging surface; `PgTx` stubs half its own
  interface with runtime `Validation` errors. Split `Tx` /
  `TableTx: Tx`; also decide `Auth`'s place on the `ControlPlane` facade (a
  sixth concern with two adapter impls, unreachable through the facade —
  either add `auth()` or document the asymmetry). One design note **before**
  `fut-wider-tx-composition` (which would otherwise multiply the stub
  surface). Watch `Ontology` (12 methods) for a `VectorIndexRegistry`
  sub-trait when vector-index deletion lands.
- **`fut-transform-wire-migration`** — move `run_transform`-shaped jobs onto
  the zero-pool worker: inputs over Flight (`FlightTicket` already streams
  file sets), commit via a new `EngineControl::CommitTransform` RPC (shape
  proven by `CompactTable`). Matches the recorded engine-owns-Postgres
  direction; the audit found the worker crate (290 lines) dramatically simpler
  than transform (770) for comparable surface — the direction is validated.
  Spec-sized; until then, no new direct-PG job handlers.
- **`fut-sql-compile-snapshots`** — insta snapshots for `sql_compile.rs`
  (1020 lines, 54 hand-maintained SQL literals) + candidates (`render`,
  `openapi_gen`, governed SQL). The fiddly part is buck2 wiring (snapshots as
  declared inputs, like the `.sqlx` cache; `cargo insta` via the dev shell);
  police blind `--accept` in review. Pairs with future SQL-emission churn
  (external wire).
- **`fut-testkit-contract-split`** — decompose testkit's 3293-line `lib.rs`
  (`acl_contract` 681 sloc) into per-concern modules with named sub-contracts
  (keep the public `*_contract` entry points; split along `fresh_db`
  boundaries; migrate seeding to the seed DSL). Better failure attribution +
  parallelism.
- **`fut-macro-panic-lint-hook`** — a prek hook greping for
  `.expect(`/`.unwrap(` inside `macro_rules!` bodies under `src/**` (outside
  tests) — closes the loophole `iss-inline-downcast-panic` exposed in the
  panic-safety lint gate.

## Sequencing

```
Wave 0 (defect fixes, small, anytime):
  iss-vector-build-lineage-ref, iss-inline-downcast-panic  (standalone-able)

Wave 1 (foundations, mutually independent):
  road-qa-governance-layer ──────────► road-qa-read-path-consolidation
  road-value-bridge          (before further logical-type additions)
  road-iceberg-commit-skeleton
  road-vector-index-codec
  road-service-bootstrap ────────────► (dead-path-sweep's config bits)
  road-test-shared-pg-fixture ──┐
  road-test-seed-dsl ───────────┴────► road-test-wire-harness

Wave 2 (rest, independent unless noted):
  road-qa-action-decomposition, road-engine-wire-dedup,
  road-ingest-api-error, road-ingest-bind-decomposition,
  road-cp-adapter-hygiene, road-vector-build-decomposition,
  road-dead-path-sweep, road-test-property-invariants
```

Verification per item: TDD; duplication items prove with `/loom-duplication
diff`, complexity items with `/loom-complexity diff`; full `buck2 test
//src/...` for anything touching the iceberg pin's blast radius or shared
fixtures. Refresh both code-health registers after Wave 1 lands (several
census entries are already stale — e.g. `query-api/main.rs::main` cc 19
predates the `serve.rs` extraction).

## Post-audit drift

PRs #292 (type↔table lineage binding edge) and #293 (object-identity dedup)
landed while the audit ran. #293 added an identity param + windowed
`ROW_NUMBER()` dedup to `compile_chain_with`/`compile_graph_reach_tail` —
this **strengthens** the `ReachSpec` case (more params) and does not conflict
with any item; handler.rs/sql.rs line references above shift by up to ~100
lines. Re-anchor from symbols, not line numbers, when claiming items.

## Cross-cutting observations worth keeping (no item)

- The "sibling path copied from the first" disease suggests a review
  heuristic: when a PR adds a streaming/governed/wire sibling of an existing
  path, it should leave exactly **one** body behind.
- query-api's error handling is otherwise in good shape (transparent
  `thiserror` chains, fail-closed defaults); the control plane's five-concern
  trait decomposition is genuinely cohesive — no god traits; managed-postgres
  is idiomatic throughout.
- Coverage thin spots (structural, not measured): `ui/` (960 src lines, 2 test
  files), `store-config` (205/1), `engine-wire` (921/8).
