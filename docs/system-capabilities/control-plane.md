# Control plane capabilities

The control plane is loom's shared metadata brain: the `src/control-plane/` crates
(`core` traits + domain types, `memory` fake, `postgres` adapter, `testkit`
contracts, and a generic `worker`) that hold the job queue, the Iceberg mirror
catalog, the ontology, ACL policy, auth credentials, and lineage events as
loom-owned schemas of one Postgres database. Every concern is a backend-agnostic
trait exercised by one contract suite against both the in-memory fake and real
Postgres, so services program against `core` and tests get a faithful fake. This
document describes what those concerns can do today, the guarantees they carry,
and the design decisions behind them.

_As of 4861433b._

## The concern library, transactions, and hardening

The library ships the five original concerns — queue, catalog, ontology, ACL,
lineage — as ports and adapters, each with a contract test run against the memory
fake and a hermetic Postgres fixture, plus a sixth `auth` concern added later
(see **Auth**). The cross-concern transaction seam is the headline property:
`ControlPlane::begin` opens a `Tx` on which operations commit together or roll
back together, so a snapshot commit and the downstream job it enqueues (or the
lineage event it emits) are atomic — no lost work, no orphan jobs. A decision
record keeps the `Tx` seam deliberately flat rather than growing a nested
transaction-composition API; wider composition is tracked as a deferred idea.

That guarantee is contract-tested, not assumed. The deterministic
`tx_isolation_contract` pins open-transaction invisibility (an uncommitted
`enqueue` + `emit` is invisible to the autocommit read path, then wholly visible
on commit) and rollback invisibility on both adapters (#14), and the memory
adapter's `Tx::commit` — originally two separate critical sections — was made
genuinely atomic across queue, lineage, and catalog by holding all locks in one
consistent-order critical section and validating fallible checks before any
mutation (#112).

The error model is a `#[non_exhaustive]` `ControlPlaneError` with distinct
`NotFound`, `Conflict` (uniqueness/optimistic-concurrency races — both ACL
adapters raise it), `Unauthorized` (originally a documented reservation, now
produced by the auth middleware), `Validation`, `Serialization`, and `Backend`
variants; the identity-hardening pass that reserved these seams also documented
the OpenLineage `DatasetRef` convention (#16). Cross-adapter reference
validation was closed as a defect: `Acl::grant`, `Acl::set_policy` (Type
targets), and `Ontology::define_action` all reject a non-existent ontology type
with `Validation`, identically on both adapters, pinned by an
`existence_validation_contract` (#149); catalog-table references deliberately
remain unvalidated for now.

The adapters themselves went through a hardening pass: both were split into
per-concern modules mirroring `core`; list reads follow one cursor + limit
pagination convention (#22); every concern's trait methods carry a
`#[tracing::instrument]` span; and the postgres adapter's SQL is compile-time
checked via sqlx `query!` macros against a committed `.sqlx` offline cache,
whose freshness is enforced by a fixture-backed test that re-describes every
cached query against the live schema.

## Queue and worker

The queue is a Postgres-backed, graphile_worker-shaped job queue: `SELECT … FOR
UPDATE SKIP LOCKED` claims plus `LISTEN`/`NOTIFY` wakeups, exposed through a
`Queue` trait with a typed job envelope over opaque JSON payloads, per-job
priority, and delayed eligibility (`run_at`). Lifecycle is deliberately lean:
`complete` deletes the row; `fail` is caller-driven via `RetryPolicy` —
`Retry { delay }` reschedules, `Abandon` retains a terminal `failed` row for
inspection. There is no `max_attempts` column by design: retry is caller policy,
and the `attempts` counter exists for observability and caller-computed backoff.
A `queue_concurrency_contract` (N workers racing over M jobs, each claimed
exactly once) pins the SKIP LOCKED semantics on both adapters.

The generic `Worker` runs the dequeue → handle → complete/fail loop with
`LISTEN`/`NOTIFY` low-latency wakeups, a polling fallback bounding the wait when
a notification is missed, and graceful shutdown via a `CancellationToken`.
Robustness against the two classic failure modes is built in: the worker leases
its in-flight job and heartbeats it at a third of the lease interval, so a
handler outliving the lock timeout is no longer reclaimed and double-executed
(#13); and handlers are wrapped in `catch_unwind`, so a panicking handler fails
its job `Abandon` instead of poisoning the loop.

## Catalog and the Iceberg mirror

The catalog concern is a read-only MVCC view over the active table-format
catalog: snapshots are catalog-global, monotonic ids; tables, files, and columns
are versioned by `begin`/`end` snapshot ranges, so every read is "this table *at*
that snapshot". The Iceberg adapter populates the mirror; loom reads it. The
delete contract makes drops part of that MVCC story: dropping a table end-caps
its rows, so it remains time-travellable up to the drop snapshot while reads at
or after the drop return `NotFound` — exercised on both adapters, with the
postgres side driving a real catalog `DROP` (#15).

Compaction exists at two layers. The library primitive — `Tx::compact_files`
plus a `compact_table` service function — coalesces a table's sub-threshold
Parquet files into fewer size-targeted files, preserving the exact row set and
time travel, adjusting stats by delta, and rejecting a concurrent-compaction
race with `Conflict`. On top of it rides the queue-driven job (#192): an
operator `POST /tables/{schema}/{table}/compact` enqueues a `compact_table` job;
a zero-pool worker (no Postgres in its dependency closure) lists the live file
set over an `EngineControl::ListFiles` RPC, streams the small files from the
engine over Arrow Flight, rewrites them coalesced with DataFusion, and commits
the swap over `EngineControl::CompactTable` — the engine owns the Postgres/
Iceberg commit, the worker owns compute, and bulk data never rides in an RPC
request/response. A raced compaction gets `Conflict`, a bounded worker retry,
and converges to a clean no-op; compacted files get fresh row-ids (a recorded
assumption to revisit if row-level deletes land).

Two correctness items round the area out. `SqlCatalog::execute`'s auto-commit
branch used to discard its commit `Result`, returning success for a write that
may not have persisted; it now commits only after a successful statement and
propagates commit failure (#239). And garbage collection covers dropped tables:
`gc_table` resolves every dropped incarnation of a `(schema, name)` — not just
the live table — reclaims their aged-out Parquet under the same retention
horizon, and once a drop snapshot ages past the horizon physically drops the
`inline_<tid>` table and deletes the mirror rows, gated on full reclaim so
nothing time-travellable vanishes.

## Ontology and typed writes (actions)

The ontology concern is loom's user-facing typed model: object types with a
logical property vocabulary deliberately decoupled from physical column types,
links between types (FK- and join-table-backed), derived properties, and the
binding from a type to its backing Iceberg table via `Ontology::resolve`.
Authoring is validated at define time rather than failing at read time (#176):
the `bind` conformance seam validates each derived property's link, target,
aggregation column, and result type, and a sibling `bind_link` checks that a
link's declared physical backing columns actually exist in the catalog schema —
keeping the `Ontology` trait itself decoupled from the catalog. Define-time
chain validation was deliberately dropped as subsumed: once every link is
column-validated and endpoint-typed, any chain of defined links is structurally
sound by construction, and ad-hoc multi-hop query paths correctly stay a
read-time 400.

Actions are the governed write path. Part 1 delivered named `ActionDef`s invoked
via `POST /actions/{name}` — the first live `Action::Write` enforcement — as an
insert-only typed write validated against the type contract; invoke-time
conformance was then hardened so every parameter must name a property of a
compatible logical type and every required property must be covered, surfacing a
structured misconfiguration error instead of an opaque insert failure (#106).
Update and delete followed (#208): an `ActionKind` discriminator
(`Insert`/`Update`/`Delete`) extends the surface to identity-targeted mutations —
`Update` locates the object by its declared identity column, applies the
caller-supplied field delta, and recommits; `Delete` removes it — initially via
whole-table copy-on-write, with two-layer governance (the coarse `Action::Write`
check plus fine-grained column/row-filter enforcement on the affected row) and
vector-bearing types rejected. The scalability slice (#331) makes those
mutations O(change) instead of O(table) for identity-bearing types: a
`loom_tombstone` column extends the inline tier to identity-keyed version and
tombstone rows, the engine read path switches from additive `UNION ALL` to
identity-dedup merge-on-read (running below the governed layer, so ACL is
unaffected), and the read→commit window is closed by a per-identity CAS — the
delta commits only if no newer version of that identity appeared, aborting into
a bounded retry. Identity-less types keep whole-table COW.

Two declarative layers govern what an action may write. Model constraints
(#265) add an all-optional `PropertyConstraints { range, length, pattern,
one_of }` to `PropertyDef` — type-tied, with mismatched or invalid-regex
constraints rejected at `define_type` — enforced by a pure `core` validator on
both write paths (the typed-insert action and the ingest land gate), returning
aggregated violations as a 422. Param→property mapping (#269) breaks the
original 1:1 name matching: `ParamDef` gains `binds` (rename) and `ActionDef`
gains constant assignments, a property resolving to its bound param, else its
constant, else unset, with generalized define-time conformance (no double-bind,
required coverage via param-or-constant) — and the resolved row still flows
through the unchanged ACL and constraint gates.

## ACL

The ACL concern stores and serves policy; it never enforces. Subjects hold
roles; roles hold coarse grants powering `Acl::check` and fine-grained policies
powering `policies_for`; the query layer folds a policy's `RowFilter` into a
DataFusion `Expr` and projects out masked or denied columns — the control plane
never interprets a filter. The decision model is deny-wins: grants carry an
`Effect`, and a deny anywhere in the subject's effective-role closure overrides
any allow. Policies support row filters (a `Compare`/`And`/`Or`/`Not` predicate
tree) and column masking (`mask_columns`, a read-render concept deliberately
ignored on writes). Policies are action-scoped — keyed on `(role, action,
target)` — so a role's read and write policies are independent rows, and
clearing one leaves the other intact.

Roles form an inheritance DAG: a role transitively receives the grants and
policies of the roles it inherits, resolved by a recursive-CTE closure at
decision time, with the DAG invariant enforced at edge-insert time. That cycle
check is atomic (#125): existence check, recursive-CTE cycle check, and edge
insert run in one transaction under a transaction-scoped advisory lock, so two
racing opposite-edge inserts serialize and the loser gets `Conflict` — pinned by
a fixture-backed concurrency test.

Write enforcement is live in `run_action` (#72): a pure `write_filter` evaluator
with SQL three-valued logic rejects an insert that sets a denied column or
produces a row failing the policy's row filter — fail-closed, so an UNKNOWN
comparison denies, with multi-policy row filters ANDed and deny-column sets
unioned. The resulting 403 carries a caller-scoped structured body (#146):
`{error: "write_denied", reason: "column"|"row_filter", column?}` names the
offending caller-supplied column and the column-vs-row-filter distinction while
keeping the predicate, policy id, and role server-side.

## Auth

Auth is the sixth concern, mirroring the others' store-don't-interpret split:
the trait persists Argon2 PHC password verifiers and opaque server-side sessions
but performs no cryptography — hashing, verification, and token minting live in
the service layer. `POST /auth/login` exchanges username + password for a bearer
session token of which only the SHA-256 is stored; verification is
constant-time with a dummy hash on unknown usernames; and a shared
`service_runtime` middleware resolves the bearer to a *verified* `SubjectId`,
rejecting absent/invalid/expired tokens with 401 — the first producer of
`ControlPlaneError::Unauthorized`, replacing the previously self-asserted
`X-Loom-Subject` header while ACL `check` runs unchanged on the verified subject
(#195). Sessions are opaque and server-side rather than JWTs, chosen for
immediate revocability and zero key management.

Machine identity is separate from human identity (#271): a distinct
`service_account` entity (no password credential, but a `SubjectId` so ACL
governs it uniformly) carries API tokens in their own table with mandatory TTL —
no immortal tokens; rotation is overlapping mint-then-revoke, capped by config —
revocation is soft for audit, the plaintext is shown exactly once, and the
middleware resolves session-or-service-token through one `resolve_bearer` path.

Administration is layered on top. Admin-gated user provisioning (#272) adds
create (initial password + bundled starting roles, sequenced
`create_user` → `define_subject` → `assign_role`, idempotent and retry-safe),
list, and disable/enable — disable immediately revokes the user's sessions,
blocks login, and is honored by session resolution as defense in depth. The
password lifecycle (#326) adds self-service change (verify current, revoke all
*other* sessions), admin reset (no current-password check, revoke *all*
sessions), and store-backed account lockout — configurable threshold, window,
and auto-unlocking duration, surviving restarts and preserving the
no-enumeration login failure. First-admin bootstrap is a one-time, out-of-band
`loom create-admin` host CLI that seeds and seals the instance in one
transaction — a one-way `Uninitialized→Sealed` state machine with no in-app
unseal — and the resulting admin is a normal identity holding the reserved
`admin` role, not a standing ACL-bypass superuser: the `/admin/*` gate checks
`Acl::has_role` fail-closed, and the admin reads data only via explicit
self-grant.

## Lineage

Lineage is loom's provenance record: every snapshot-producing run emits an
OpenLineage-shaped event with its input and output datasets, keyed by
`DatasetRef` — OpenLineage's own `{namespace, name}` identity, deliberately
decoupled from `TableRef`/`TypeName` so one graph spans physical tables,
ontology types, and external datasets. Lineage stores and serves provenance; it
enforces nothing. Emission is atomic with the write it describes (#123): action
writes route through loom's snapshot-commit primitive so the row and its
`LineageEvent` commit in one Postgres transaction, and `run_action` surfaces its
`run_id` on the `X-Loom-Run-Id` response header.

The read surface is a real graph, not one hop (#263): `upstream`/`downstream`
take a `depth` parameter (default 1, hard-capped at `LINEAGE_MAX_DEPTH`)
implemented as a depth-bounded `WITH RECURSIVE` CTE whose `UNION` set semantics
double as the cycle guard, so re-run cycles terminate; the memory fake mirrors
it with a visited-set BFS. All three reads honor the cursor + limit pagination
convention with stable ordering, and pagination composes with the closure (the
CTE materializes the deduped set, the outer select windows it). That capability
is exposed over HTTP (#286): three authenticated `GET` routes in query-api —
per-dataset `upstream`/`downstream` with `depth`, and per-run events — returning
paginated JSON, OpenAPI-annotated.

Naming and layering were bridged so the graph is navigable and governable. The
dataset naming bridge (#291) is a postgres-free `lineage-naming` crate providing
the OpenLineage-conformant mapping from a loom `TableRef`/`TypeName` to
`{namespace, name}` and a total reverse `resolve` into
`ResolvedDataset::{Table, Type, Unresolvable, External}` — external datasets are
first-class, not rejected, while a ref under a loom-owned namespace (logical or
this deployment's storage `site_namespace`) whose name fails to parse resolves to
`Unresolvable` so consumers can fail closed rather than default-allow it (#337).
The type↔table layer join (#292) emits a binding edge at bind
time connecting a type-named lineage node to its backing table node, so typed
and physical provenance read as one graph rather than two disjoint layers.

Lineage reads are ACL-filtered least-disclosure (#321): a query-api
`LineageVisibility` filter drives the closure one hop at a time, gating each
frontier node through the naming bridge to a `PolicyTarget` and `Acl::check`.
The semantics are cut-not-skip (a denied intermediate is dropped *and* not
expanded, so nodes reachable only through it are never discovered), seed-gated
(an unreadable seed yields an empty page — denied is indistinguishable from
unknown), and redact-within for events (denied refs are dropped from
`inputs`/`outputs` while the envelope and cursor stay intact, and the opaque
event `payload` is gated all-or-nothing — served verbatim only when every typed
ref was readable, else nulled, so free-form dataset names in it cannot leak past
the redaction applied to the typed refs (#337)). External refs default-allow,
but a ref under any loom-owned namespace — logical *or* the deployment's storage
`site_namespace` — that fails to resolve is `Unresolvable` and fail-closed:
denied *and* cut (#337). The visible set is assembled before windowing — a pure
function of `(seed,
depth, subject)` — so pagination stays complete with no short pages, and a scan
cap bounds the ACL fan-out with a 422. `core` stays subject-free; the governance
lives entirely in the service layer.

## Known gaps

- `#road-action-computed-assignments` — expression-valued action properties (bounded pure grammar), spec'd not built
- `#road-action-multi-object` — multi-object/multi-step actions in one transaction, spec'd not built
- `#road-action-enqueue-downstream` — actions that atomically enqueue a downstream job, spec'd not built
- `#road-auth-comprehensive` — build-vs-adopt decision for a fuller identity layer
- `#fut-lineage-stitching` — run-grouped lifecycle stitching of events
- `#fut-openlineage-validation` — validate emitted OpenLineage payloads
- `#fut-lineage-events-page-hydration` — hydrate only kept rows in paginated `events_for`
- `#fut-lineage-datasetref-validation` — validate lineage `DatasetRef`s at emit
- `#fut-storage-derived-lineage-emit` — emit canonical storage-derived dataset names
- `#fut-lineage-filter-batch-resolve` — batch/CTE-pushdown of the governed lineage closure
- `#fut-compaction-incremental` — incremental (append-delta) compaction output
- `#fut-compaction-auto-trigger` — automatic compaction triggering
- `#fut-fgac-subject-attribute` — fine-grained subject-attribute access control
- `#fut-cow-inline-shadow` — remaining scalable-COW slices (tombstone-aware consolidation)
- `#fut-cow-file-granular` — file-granular copy-on-write
- `#fut-cow-identity-change` — identity-change / upsert mutations
- `#fut-ontology-versioning` — ontology versioning
- `#fut-ontology-migrations` — migrations for ontology changes
- `#fut-model-constraint-uniqueness` — cross-row uniqueness constraint
- `#fut-bloom-filter-index-blob` — bloom-filter index blob to accelerate uniqueness
- `#fut-model-constraint-backfill` — retroactive constraint validation / backfill
- `#fut-ontology-semantic-descriptions` — semantic description fields across the ontology
- `#fut-breakglass-godmode` — break-glass / quorum god-mode
- `#fut-engine-wire-role-rpcs` — `gov_has_role`/`gov_list_roles` engine-wire RPCs
- `#fut-admin-governance-http-surface` — fuller governance authoring over HTTP
- `#fut-auth-totp-mfa` — TOTP/OTP second factor
- `#fut-auth-passkeys` — passkeys (WebAuthn)
- `#fut-auth-saml` — SAML bridge
- `#fut-auth-admin-capability` — first-class auth-admin ACL capability
- `#fut-auth-token-scoping` — service-token scoping
- `#fut-auth-acl-provisioning-tx` — atomic cross-concern auth+ACL provisioning
- `#fut-auth-email-reset` — email password reset + verification
- `#fut-auth-login-rate-limit` — per-IP login rate-limiting
- `#fut-auth-password-policy` — forced rotation + strength policy
- `#fut-auth-session-refresh` — session refresh / sliding expiry
- `#fut-auth-credential-debug-redact` — redact password verifier from credential `Debug`
- `#fut-multi-tenancy` — tenant_id partitioning
- `#fut-wider-tx-composition` — wider `Tx` composition
- `#fut-metrics-crate` — metrics counters and histograms
