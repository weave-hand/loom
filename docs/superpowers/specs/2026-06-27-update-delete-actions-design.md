# Design: Governed UPDATE / DELETE actions (A5)

> **Status:** approved design (2026-06-27). The second slice of loom's **Actions**
> capability — the ontology's typed, governed write-backs. Part-1
> (`road-actions-typed-insert`) shipped **insert-only** actions; this slice adds the
> two **mutating** kinds, **UPDATE** and **DELETE**, completing the create/update/delete
> triad for typed objects. Promotes `fut-update-delete-actions`. The Grimoire /
> personal-KG use case's **A5** ask: write-back upserts mutable state, so insert-only is
> not enough.

## Goal

Make loom run **UPDATE** and **DELETE** actions: a named, ontology-defined operation
that a subject invokes over HTTP to **mutate** or **remove** one existing typed object,
located by its declared primary-key (`identity`) value. Invoking the action resolves it
to its target type, enforces the `Action::Write` ACL (coarse) and the fine-grained
write policy (column-denial + row-filter) on the affected row, and commits the mutation
as a new mirror snapshot with an atomic lineage event. The mutation is immediately
readable through the governed read path, and **prior snapshots still time-travel to the
pre-mutation state**.

End-to-end: **POST an UPDATE/DELETE action with the identity (+ for UPDATE, the changed
columns) → governed (coarse Write + fine-grained row/column policy) → whole-table
copy-on-write commit → new snapshot + lineage → the change reads back.**

This brings the **mutating** half of the write-back pillar online. Its genuinely-new
infrastructure is **row supersession over loom's two-tier MVCC mirror**, realized by
**whole-table copy-on-write** layered on the shipped A1 overwrite primitive
(`road-iceberg-overwrite-mode`) — exactly the "A5 rides A1" path the agenda predicts.

## Decisions (settled in brainstorming)

- **A — Scope is A5 only.** UPDATE/DELETE actions as their own slice. A6 (atomic
  per-session check-in: batching a session's insert/update/delete delta into one
  snapshot+lineage) is a thin follow-on that composes this primitive, specced
  separately.
- **B — Whole-table copy-on-write via A1 overwrite.** A mutation reads the live table
  (inline ∪ files), applies the single-object change in memory, and re-commits the whole
  table through `overwrite_parquet_snapshot`. Fully correct across both MVCC tiers,
  time-travel preserved, reuses the shipped overwrite primitive. Cost is O(table) per
  mutation — acceptable at this use case's scale; file-granular COW and Iceberg-native
  delete-files are deferred (the latter is blocked by iceberg-rust 0.9 having no
  delete-file writer regardless).
- **C — Targeting by declared `identity`.** UPDATE/DELETE require the target type to
  declare an `identity` property and locate the single live object by its value. No
  predicate/bulk targeting in this slice. The identity column is **immutable** (changing
  it = delete + insert).
- **D — UPDATE is partial/merge (PATCH).** Only the columns named by the action's
  parameters change; every other column retains the existing row's value.
- **E — Not-found is 404, no upsert.** A mutation whose identity matches no live row
  returns 404. Upsert (insert-on-miss) is out of scope.

## Current state

- **Action handler** (`src/services/query-api/src/action.rs`): `run_action` resolves the
  action, enforces the coarse `Action::Write` gate, runs `check_conformance`, parses
  typed params, applies the fine-grained write policy
  (`write_filter::check_write_policy` → `WriteDenialReason` → structured 403), then calls
  `ActionEngine::write_object` to insert one row + lineage atomically. Returns the created
  object + `RunId`.
- **Action engine seam** (`src/services/query-api/src/serving.rs`): `ActionEngine` trait
  with `write_object(table, columns, values, logical_types, lineage)`; the
  `IcebergActionWriter` impl builds a one-row Arrow batch (`build_object_batch`), encodes
  IPC, and routes through `iceberg_landing::land`.
- **ActionDef** (`src/control-plane/core/src/ontology.rs`): `{ name, target, parameters }`
  — insert semantics are implicit (no kind discriminator yet).
- **Identity** (`ObjectType::identity: Option<String>`): a declared primary-key property,
  already used by the read path as the dedup/join key (`handler.rs`), flight export, and
  ingest bind.
- **Overwrite primitive** (`src/control-plane/postgres/src/iceberg_landing.rs`):
  `overwrite_parquet_snapshot` replaces a table's live data with new `batches` in one
  Postgres tx — end-caps every live `iceberg_mirror.data_file` at the new snapshot,
  projects the new files, `fast_append`s, emits lineage; a zero-file call truncates via
  `overwrite_truncate`. **It does NOT end-cap the inline tier** (`inline_<tid>`) — transform
  replace, its only caller, only ran on file-only tables.
- **Inline MVCC** (`src/control-plane/postgres/src/iceberg_inline.rs`,
  `iceberg_mirror.rs`): `inline_<tid>` rows carry `loom_row_id` / `begin_snapshot` /
  `end_snapshot`; end-cap machinery exists for the flush path.

## Design

### 1. Action kind on `ActionDef`

Add an operation discriminator to the ontology type:

```rust
pub enum ActionKind { Insert, Update, Delete }   // serde default = Insert (back-compat)
pub struct ActionDef { name, target, parameters, kind }
```

`Insert` is the deserialize/storage default so every existing `ActionDef` keeps its
meaning with no migration of in-memory semantics. The ontology adapter persists `kind`
(a new nullable column defaulting to `'insert'`; sqlx cache refreshed). Invocation stays
`POST /actions/{name}`; the handler dispatches on `action.kind`.

### 2. Conformance (extends `check_conformance`)

For `Update`/`Delete`, in addition to the part-1 parameter/property rules:

- The target type **must** declare `identity`; otherwise `Misconfigured` (operator-facing,
  like the read path's `NoIdentity`).
- A **required** parameter must name the identity property.
- **Delete**: the identity parameter is the only parameter.
- **Update**: the remaining parameters are the mutable columns; none may name the identity
  property (immutable). The part-1 "required property covered by required parameter" rule
  is **relaxed** for Update — a PATCH need not resupply every required column (the existing
  row already satisfies required-ness); only type-compatibility of supplied params is
  checked.

### 3. Mutation flow (query-api)

New `run_update` / `run_delete` siblings to `run_action` (sharing resolution + the coarse
Write gate + conformance):

1. **Privileged full-table read** over the serving engine: `SELECT <all columns> FROM
   <type>` (the canonical "live as of current snapshot" read), **unfiltered by the
   subject's READ ACL**. COW must faithfully rewrite *every* live row, including rows the
   subject cannot read; those rows are never returned to the subject (only the affected
   object is), so this discloses nothing. The write is still fully governed (step 4).
2. **Locate** the row whose identity column equals the supplied value. Zero matches → 404
   (`ActionError::NotFound`). The identity invariant guarantees ≤1; >1 is a server fault
   (500).
3. **Apply** the mutation in memory: Delete drops the row; Update produces a new row =
   existing row with the named columns overwritten.
4. **Govern the affected row** (fine-grained write policy):
   - Delete: the policy `row_filter` must admit the **existing** row.
   - Update: column-denial on the set columns (as insert); the `row_filter` must admit
     **both** the existing row and the resulting row.
   - Denials reuse `WriteDenialReason` → structured 403 (predicate/policy-id server-side).
5. **Commit** the full new live set via a new `ActionEngine::overwrite_table`, routed
   through `overwrite_parquet_snapshot`, with the lineage event committed atomically. A
   Delete that empties the table commits the zero-file truncate branch.
6. **Return** the affected object (Update: the new version; Delete: the removed row's
   values) + `RunId`.

### 3a. Vector-column guard (data-loss safety)

The COW read leg (`Rows`/`SqlValue`) and write leg (`build_object_batch`/`one_cell`)
are **scalar-only** — `SqlValue` has no list variant and `one_cell` errors on
`BaseType::Vector`. A whole-table rewrite of a `vector(N)`-bearing table would therefore
silently **drop every other row's vector** (data loss). So UPDATE/DELETE on a type with
any vector property is **rejected** up front (`ActionError::Unsupported` → 422), before
any read or write. This guard is lifted when the Arrow-native COW read leg lands (the
deferred follow-on). (Action *inserts* already can't carry vectors either, so this is a
consistent inherited limitation, not a new one.)

### 4. Overwrite must supersede the inline tier (required plumbing)

A5 adds a new `end_cap_live_inline_rows(conn, tid, at)` and calls it in **both** overwrite
commit paths so overwrite supersedes *both* MVCC tiers (the new full set is the sole live
data; stale inline rows must not survive):

- the non-empty path's `write_mirror` (catalog.rs), right after `end_cap_live_data_files`
  inside the `if overwrite { … }` block — same commit tx, no new snapshot;
- the zero-file `overwrite_truncate` branch (iceberg_landing.rs).

The helper end-caps every live inline row (`update iceberg_mirror.inline_<tid> set
end_snapshot = $at where end_snapshot is null`), guarded by a `to_regclass` existence
check (the inline table is created lazily and may be absent). This is net-new behavior A5
needs, and it makes A1 overwrite correct for any inline-bearing table as a bonus.

### 5. Lineage

Each mutation emits one `LineageEvent` (`EventType::Complete`), committed atomically with
the overwrite. `outputs = [target type]`; `inputs = [target type]` (a mutation derives the
table from itself); `payload = { action, op: "update"|"delete", identity: <value> }`. The
event commits *with* the snapshot (structural linkage), as part-1's insert does.

### 6. ActionEngine trait change

```rust
async fn overwrite_table(
    &self,
    table: &TableRef,
    columns: &[String],
    rows: &[Vec<SqlValue>],      // the FULL new live set
    logical_types: &[String],
    lineage: LineageEvent,
) -> Result<(), ServingError>;
```

The `IcebergActionWriter` impl generalizes `build_object_batch` to N rows, encodes IPC,
and calls `overwrite_parquet_snapshot`. (A faithful read leg that pulls Arrow batches
directly from the mirror — avoiding the SqlValue read/re-encode round-trip — is a noted
follow-on; the first slice reuses the serving read + `build_object_batch`, the same
fidelity the insert path already relies on.)

## Error handling

- **404 NotFound** — identity matches no live row (new `ActionError::NotFound`).
- **403 Forbidden** — coarse Write gate denial (unit, as part-1).
- **403 WriteDenied** — fine-grained column/row-filter denial (structured body; for Update,
  either the old or the new row failing the filter denies).
- **422 Unsupported** — UPDATE/DELETE on a type with a `vector(N)` property (§3a guard).
- **BadParams** — malformed/typed-param failure, same status mapping as part-1's insert.
- **500 Misconfigured/ControlPlane** — identity-less target type, >1 identity match, or a
  backend fault. The overwrite runs in one Postgres tx; any failure rolls back with no
  snapshot, no lineage, no partial state.

## Concurrency (documented limitation)

The full-table read (step 1) and the overwrite commit (step 5) are **not** one
transaction, so a concurrent writer landing between them is lost (read-modify-write race).
**Accepted** for the Grimoire single-session-writer use case. Logged as a deferred item;
the fix (a follow-on) is snapshot-version optimistic concurrency: capture the snapshot id
at read time and have the overwrite assert it is still current (else retry), reusing the
CAS already in the Iceberg writer.

## Testing

Fixture e2e (`loom_fixture_test`, hermetic Postgres), reusing the e2e-support seed/ACL
helpers:

- Update merges: named columns change, others retained; reads back the new version.
- Delete removes the row; subsequent read omits it.
- **Time travel**: the pre-mutation snapshot still reads the original row (both kinds).
- Not-found identity → 404 (both kinds).
- Write-denied: column-denied Update → 403; row-filter-denied Update (old row outside the
  set, and new row outside the set, separately) → 403; row-filter-denied Delete → 403.
- Identity-less target type → Misconfigured.
- Both tiers: a mutation against an **inline-resident** object and a **flushed
  (file-resident)** object each commit correctly (proves the inline end-cap of §4).
- Vector guard: UPDATE/DELETE on a type with a `vector(N)` property → 422 Unsupported
  (§3a), and the table is left untouched (no data loss).

Plus unit tests for the extended conformance rules (identity required, immutable identity
param, Delete-has-only-identity, Update PATCH relaxation).

## Out of scope (→ registers)

- **A6** atomic per-session check-in (batch insert/update/delete → one snapshot+lineage) —
  the thin follow-on composing this primitive.
- **File-granular copy-on-write** (prune to containing files, rewrite only those) — the
  O(table)→O(touched) optimization.
- **Inline-shadow + merge-on-read + compaction-consolidation** — the scalable destination
  (PR #208 review): an UPDATE writes a new inline row-version and a DELETE writes an inline
  **tombstone**; the engine read path makes inline rows **shadow** file-tier rows by
  identity (today's union is additive), and compaction consolidates the inline deltas into
  files and clears them. O(change) per mutation, but it changes the **engine read path** and
  **compaction** (not just query-api) and needs a tombstone concept the inline tier lacks —
  hence sequenced after this whole-table-COW slice rather than built first.
- **Iceberg-native delete-files** (positional/equality deletes + merge-on-read) — blocked
  by iceberg-rust 0.9; the long-term ideal.
- **Identity change** and **upsert** (insert-on-miss).
- **Concurrency CAS guard** (snapshot-version optimistic concurrency for the read→commit
  race).
- **Arrow-native COW read leg** (read mirror batches directly, skip the SqlValue
  round-trip).

## Register impact

- Promotes `fut-update-delete-actions` → a new `road-update-delete-actions` (area:
  ontology).
- New deferred items: per-session check-in (A6), file-granular COW, COW concurrency CAS,
  Arrow-native COW read, identity-change/upsert.
