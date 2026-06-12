# Design: object-safe `ControlPlane` facade

> **Status:** approved design (2026-06-12). Closes the lone open **[M]** finding in
> `2026-06-06-control-plane-critical-review.md` ("`ControlPlane` is nearly useless as a
> `dyn` trait"). Makes `&dyn ControlPlane` / `Arc<dyn ControlPlane>` reach every concern,
> so a type-erased holder no longer depends on the concrete adapter or carries a
> five-trait generic bound.

## Goal

Add five accessor methods to the `ControlPlane` trait — one per concern — so that a
`&dyn ControlPlane` (or `Arc<dyn ControlPlane>`) gets you `.catalog()`, `.ontology()`,
`.acl()`, `.lineage()`, `.queue()`. Today `ControlPlane` exposes only `begin()`; the five
concern traits are impl'd directly on the concrete adapter structs (`PgControlPlane`,
`MemoryControlPlane`) with no accessor, so a consumer that wants more than one concern
either names the concrete adapter type or holds a separate `&dyn Concern` per concern.
After this slice, a single type-erased facade value reaches all five — without forcing
narrow consumers to widen.

## Why this matters now

The review's [M]: *"a consumer must depend on the concrete `PgControlPlane` /
`MemoryControlPlane` type, or carry a five-trait generic bound — there's no
`&dyn ControlPlane` that gets you `.lineage()`, `.acl()`, etc. … reconsider it (or a
single object-safe facade) before three services each invent their own bound soup."*

`query-api::AppState` is the canonical type-erased holder: it stores `Arc<dyn …>` precisely
to stay adapter-agnostic, and today it carries **two** separate erased concern Arcs
(`ontology`, `acl`) because there is no single facade to hold. As more services land
(ingest shell, transform workers), each would repeat that per-concern Arc soup. One
accessor-bearing facade fixes the shape once, in `core`.

## The trait change (`core/src/transaction.rs`)

```rust
#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// The DuckLake catalog read surface.
    fn catalog(&self) -> &(dyn Catalog + Send + Sync);
    /// The object/link ontology.
    fn ontology(&self) -> &(dyn Ontology + Send + Sync);
    /// The access-control policy surface.
    fn acl(&self) -> &(dyn Acl + Send + Sync);
    /// The lineage event log.
    fn lineage(&self) -> &(dyn Lineage + Send + Sync);
    /// The job queue.
    fn queue(&self) -> &(dyn Queue + Send + Sync);

    /// Open a unit of work. Issue operations on the returned `Tx`, then `commit`
    /// or `rollback`. (Unchanged.)
    async fn begin(&self) -> Result<Box<dyn Tx + Send>>;
}
```

- **Object-safe.** Every accessor returns a `&dyn` reference; `begin` is already boxed.
  `&dyn ControlPlane` remains constructible.
- **Send + Sync on the returned objects.** The concern traits carry no `Send + Sync`
  supertrait — callers add the bound at each trait-object site (e.g. `query-api` holds
  `dyn Ontology + Send + Sync`). The accessors return `&(dyn Concern + Send + Sync)` to
  match that established convention, so the borrowed ref is usable wherever the per-concern
  Arcs are today.
- **The five concern traits are unchanged.** This slice only adds the bridge; it does not
  touch `Catalog`/`Ontology`/`Acl`/`Lineage`/`Queue` or `Tx`.

### Why accessors, not a supertrait

The rejected alternative is `trait ControlPlane: Catalog + Ontology + Acl + Lineage + Queue`.
Two problems: (1) method-name collisions across concerns become ambiguous on
`dyn ControlPlane`; (2) narrowing a `&dyn ControlPlane` back to `&dyn Acl` to pass into a
segregated function needs trait upcasting and loses the explicit concern boundary.
Accessor methods keep each concern a distinct, namable trait object and were the roadmap's
original sketch.

## Adapter implementations (one-liners)

Both adapters already implement all five concern traits on the struct itself, so each
accessor is a self-coercion:

```rust
// src/control-plane/postgres/src/lib.rs  (inside impl ControlPlane for PgControlPlane)
fn catalog(&self)  -> &(dyn Catalog  + Send + Sync) { self }
fn ontology(&self) -> &(dyn Ontology + Send + Sync) { self }
fn acl(&self)      -> &(dyn Acl      + Send + Sync) { self }
fn lineage(&self)  -> &(dyn Lineage  + Send + Sync) { self }
fn queue(&self)    -> &(dyn Queue    + Send + Sync) { self }
```

Identical bodies in `src/control-plane/memory/src/lib.rs` (inside
`impl ControlPlane for MemoryControlPlane`). Both structs are already `Send + Sync`
(`PgControlPlane` wraps a `PgPool`; `MemoryControlPlane` wraps `Arc<Mutex<…>>`/`Arc<Notify>`),
so the `self` coercion to `&(dyn Concern + Send + Sync)` type-checks.

## Adoption (prove at one site)

`query-api::AppState` swaps its two erased concern Arcs for one facade Arc:

```rust
// src/services/query-api/src/http.rs
#[derive(Clone)]
pub struct AppState {
    pub cp: Arc<dyn ControlPlane>,        // was: ontology: Arc<dyn Ontology…>, acl: Arc<dyn Acl…>
    pub serving: Arc<dyn ServingEngine>,
}
```

`get_object` builds `QueryDeps` from the facade, handing each narrow concern object into the
unchanged read path:

```rust
let deps = QueryDeps {
    ontology: st.cp.ontology(),
    acl: st.cp.acl(),
    serving: st.serving.as_ref(),
};
```

`QueryDeps` and `read_object` are **unchanged** — they still take `&dyn Ontology` / `&dyn Acl`.
Interface segregation is preserved at the function boundary; only the *holder's* concrete /
multi-Arc dependency is removed. (`Arc<dyn ControlPlane>` auto-derefs, so `st.cp.ontology()`
yields a `&(dyn Ontology + Send + Sync)` borrowed for the duration of the handler — valid for
the synchronous `read_object` call.)

`governed_read`/`bind_read_e2e` deliberately stay as-is: they hold a **concrete**
`PgControlPlane` and pass `&cp` as both `ontology` and `acl`. There is no type erasure there,
so the facade adds nothing — changing them would be churn without value.

## Testing

### (a) Facade contract (`testkit`, both adapters)

New `control_plane_facade_contract<CP: ControlPlane>(cp: &CP)` in
`src/control-plane/testkit/src/lib.rs`. It coerces `cp` to `&dyn ControlPlane` and exercises
the concerns **through the facade accessors** on a freshly-empty control plane:

- **queue round-trip:** `cp.queue().enqueue(job)` then `cp.queue().dequeue(&kinds, worker)`
  returns that job (id matches) — proves the accessor returns a live, correctly-wired object,
  not a panic stub.
- **ontology:** `cp.ontology().list_types(PageReq::default()).await` is empty.
- **acl:** `cp.acl().check(&unknown_subject, Action::Read, &some_target).await` is
  `Decision::Deny` (deny-by-default through the facade).
- **lineage:** `cp.lineage().events_for(&run, PageReq::default()).await` is empty.
- **catalog:** bind `let _cat: &(dyn Catalog + Send + Sync) = cp.catalog();` — proves the
  accessor exists, is object-safe, and returns. (Behavioral catalog reads hit `ducklake_*`
  tables that need an attached DuckLake catalog; that dispatch is already covered by
  `catalog_contract`, and keeping it out here lets the postgres facade test stay DuckLake-free.)

Bound is just `CP: ControlPlane` — the accessors live on `ControlPlane`, so no per-concern
bound is needed on the contract.

Wired as:
- `src/control-plane/memory/tests/facade.rs` — plain `rust_test`, constructs
  `MemoryControlPlane::new(lock_timeout)` and calls the contract.
- `src/control-plane/postgres/tests/facade.rs` — `loom_fixture_test` (default
  `duckdb = False`: postgres + migrations only, no DuckLake), boots a fresh DB and calls the
  contract. The contract touches only queue/ontology/acl/lineage, none of which need an
  attached catalog.

### (b) `http_smoke.rs` rework (the adoption test)

`AppState` now needs a `ControlPlane`, so the smoke test replaces its `StubOntology` +
`StubAcl` (~130 lines of `unimplemented!()`) with a **seeded `MemoryControlPlane`**:

1. `define_type` an `Order` type → table `main.orders`, property `id: Long`.
2. `define_subject("analyst")`, `define_role(role)`, `assign_role`, `grant(role, Read,
   PolicyTarget::Type("Order"), Allow)` so the request subject is permitted.
3. `AppState { cp: Arc::new(mem_cp), serving: Arc::new(StubServing) }`.

`StubServing` stays (canned `id = 1` row), so the test remains DuckDB-free and still asserts
the typed-object JSON envelope (`json["objects"][0]["id"] == "1"`, Long → JSON string). This
both exercises the facade end-to-end through the real HTTP surface and deletes the stub
boilerplate. Adds `//src/control-plane/memory:memory` to the `http-smoke` test deps.

## Verification

- `buck2 test //src/...` green — including the two new `facade` targets and the reworked
  `http-smoke`.
- `tools/clippy-all.sh` clean; `buck2 run //tools:prek -- run --all-files` green.
- After landing, flip the [M] callout in `2026-06-06-control-plane-critical-review.md` from
  ❌ Open (sidestepped) to ✅ Done, and note it in the five-things summary.

## Scope / non-goals

- **In:** the five `ControlPlane` accessor methods; their one-line impls on both adapters;
  the `control_plane_facade_contract` + its two adapter targets; `AppState` adoption + the
  `http_smoke` rework; the critical-review doc flip.
- **Out:**
  - **Supertrait form** (`ControlPlane: Catalog + …`) — rejected above.
  - **Migrating `governed_read`/`bind_read_e2e`** — they hold concrete `PgControlPlane`; no
    erasure to fix.
  - **Widening `QueryDeps`/`read_object` to take `&dyn ControlPlane`** — would weaken
    interface segregation; the read path legitimately needs only ontology + acl.
  - **Any change to the five concern traits or `Tx`** — untouched.
  - **An owned/`Arc`-returning accessor variant** (e.g. `fn acl_arc(&self) -> Arc<dyn Acl>`) —
    not needed; the borrowed `&dyn` accessor covers the holder-hands-to-narrow-fn pattern, and
    `Arc<dyn ControlPlane>` itself is the shareable handle.

## Open risks

- **Borrow ergonomics.** Accessors return a borrow tied to `&self`, so a caller cannot hold
  `cp.acl()` past the borrow of `cp`. This is the right default (it mirrors today's
  pass-into-a-call usage) and `Arc<dyn ControlPlane>` remains the cloneable share point. If a
  future consumer genuinely needs an owned `Arc<dyn Concern>`, that is an additive accessor,
  not a breaking change.
