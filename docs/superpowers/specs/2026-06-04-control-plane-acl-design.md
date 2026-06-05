# Design: Control-Plane ACL (Phase 4)

> **Status:** approved design for the control plane's fourth concern — access control.
> Sits under the umbrella roadmap (`2026-06-03-control-plane-roadmap-design.md`).
> Built in **one cycle** (like queue and ontology): it is a loom-owned schema with no
> external substrate, so it follows that shape — trait → contract → fake →
> pg adapter + migration.

## Goal

ACL is loom's **policy store and decision surface**. It holds subjects, roles,
coarse access grants, and fine-grained row/column policy, and answers two
questions for the services that enforce:

- `check(subject, action, target) -> Decision` — the coarse yes/no gate.
- `policies_for(subject, target) -> Vec<Policy>` — the row filters and column
  restrictions the Query API pushes into DataFusion scans.

The `Acl` trait exposes this through the control-plane traits, satisfied by both
the in-memory fake and the Postgres adapter via one contract suite.

## Scope boundary (the framing that makes this tractable)

The control plane is a **library**, and the roadmap puts *DataFusion plan
rewriting* explicitly out of scope (that belongs to the Query API service). So
P4's job is to **store and serve** the policy model — not to enforce it. That
collapses the roadmap's "ACL pushdown completeness" open question into a
representation question: **return predicates in a form the Query API can consume**,
and let the planner decide what is pushable into a single scan versus what needs a
plan-level filter. P4 has no opinion on pushability and **no dependency on
DataFusion**.

Like the ontology (and unlike the catalog), the `acl` schema is **loom-owned**:
loom reads *and writes* it. So the trait carries the write ops and the contract
**self-seeds through them** (the queue/ontology pattern) — no seeding seam, no
external substrate.

## Why one cycle

ACL is a loom-owned CRUD surface over typed metadata — the same shape as the queue
and ontology. No DuckDB, no hermetic external binary, no read-only seeding problem.
So it is a single trait → testkit contract → memory fake → pg adapter + `acl`
migration cycle, in one plan/PR.

## The authorization model

### Subjects, roles, and the flat-role indirection

- **Subjects** (`SubjectId`) are users or service accounts.
- **Roles** (`RoleId`) are the unit governance is expressed in.
- **Grants and policies attach to roles, never directly to subjects.** A subject's
  effective permissions are the **union over the roles it is assigned**. A
  per-user grant is modelled as a per-user role.
- **Flat — no hierarchy or inheritance.** Roles do not contain other roles. (A
  role-hierarchy is a safe additive future cycle if a consumer needs it.)

Rationale: grants bound directly to individual subjects are the one thing that is
genuinely painful to migrate away from later (you would rewrite every grant row),
whereas a flat role layer is cheap now and is the natural unit governance is
expressed in. Hierarchy is the part safe to defer.

### `check` — default-deny, allow-only

`check(subject, action, target)` returns `Allow` iff **any** role assigned to the
subject has a grant matching `(action, target)` exactly; otherwise `Deny`. An
unknown subject returns `Deny` (not an error). There are **no explicit-deny
rows** — you remove access by revoking the grant. Composition is therefore a
trivial union of allows with no precedence rule.

The carve-out use case ("read everything *except* the PII table") that explicit
deny would buy is better expressed at the **row/column policy** layer, which is
where real loom governance lives. If table-level explicit-deny is ever needed it
arrives as an additive `effect` column — no rewrite. `Decision` is a bare
`enum { Allow, Deny }`: no reason/obligations payload (the Query API gets
obligations from `policies_for`, not `check`).

### Targets — ontology type or physical table, matched as stored

`PolicyTarget` is `Type(TypeName)` or `Table(TableRef)`, reusing the ontology and
catalog vocabularies already in `core`. Governance can be expressed against the
user-facing typed model (`Customer`) **or** a raw physical table that has no
ontology type yet.

P4 matches targets **exactly as stored**: a `Type` policy matches a query against
that type, a `Table` policy matches a query against that table. P4 does **not**
resolve types to tables to union them — that resolution (and the question of
whether a type-level and table-level policy on the same backing table should both
apply) is the Query API's job, exactly like pushdown. This keeps the control plane
from needing the ontology at query time while still letting both vocabularies be
governed, honouring the architecture's "bound to ontology types or physical
tables" wording literally.

### Actions

`enum Action { Read, Write }` — the Query API reads; Ingest and Transform write.
Two actions cover the data plane; finer actions (e.g. a manage/admin verb) are
additive later.

## Row/column policy representation (the crux)

`policies_for(subject, target)` returns a `Vec<Policy>`:

```rust
pub struct Policy {
    pub target: PolicyTarget,
    pub row_filter: Option<RowFilter>,   // None = no row restriction
    pub deny_columns: Vec<String>,       // columns projected out
}

/// A row filter as a boolean expression tree over ontology properties.
pub enum RowFilter {
    Compare { property: String, op: CompareOp, value: ScalarValue },
    And(Vec<RowFilter>),
    Or(Vec<RowFilter>),
    Not(Box<RowFilter>),
}
```

- **Row filter = a recursive boolean tree** of property comparisons combined with
  `And`/`Or`/`Not`. Grouping is the nesting, so it expresses arbitrary boolean
  structure unambiguously — including the common `scope AND (this OR that)` shape
  (e.g. `tenant = X AND (is_public OR owner = me)`) that a flat conjunct or
  connective-per-predicate list cannot. Leaves reference **ontology properties** by
  name; the Query API folds the tree into a DataFusion `Expr` (`col(p).eq(lit(v))`,
  combined with `.and()`/`.or()`/`.not()`) — **no SQL parser, no injection or
  dialect surface, no DataFusion dependency in `core`.**
- **Column restriction = a denied-column name set** (`deny_columns`), project-out
  only. Masking (replace with NULL/hash) is deferred.
- **One `Policy` per matching role; P4 does no merging.** The Query API ANDs the
  row filters and unions the denied columns across the returned policies.

Why a structured tree rather than an opaque SQL string: the tree is
**push-down-classifiable by construction** — the Query API can walk it and see it
references only the scanned table's columns, which is exactly the discrimination
the roadmap's "pushdown completeness" open question asks for; a string would have
to be parsed and analysed to learn the same thing, and carries an injection/dialect
hazard when spliced into a plan. It also serializes cleanly to one `jsonb` column
via `serde_json` (already a `core` dependency; the plan adds the `serde` derive
macro alongside it) and translates to a DataFusion `Expr` by a pure fold with no
parser. The cost — a small recursive enum plus
`CompareOp`/`ScalarValue` to maintain, and value typing that stays structural
(well-formed comparisons) rather than semantic until the ontology grows a real type
registry — is accepted: the structure is its own guard, and disjunction is a
first-class need for row policies, not a someday-maybe.

The leaf references a **property name**. For `Type` targets that is an ontology
property (resolved to a column by-name, per the ontology spec); for `Table` targets
it is a column name directly. Same by-name correspondence either way.

## Trait surface (`core`)

Domain types live in `core`; all ops `async`, returning `Result<_, ControlPlaneError>`.
Reuses `core::TypeName` (ontology) and `core::TableRef` (catalog). Runtime-free
(no tokio), like the other concern traits.

```rust
/// A user or service account.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubjectId(pub String);

/// The unit governance is expressed in. Grants and policies attach here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoleId(pub String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action { Read, Write }

/// What a grant or policy is bound to. Matched exactly as stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyTarget {
    Type(TypeName),    // ontology type
    Table(TableRef),   // physical DuckLake table
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision { Allow, Deny }

/// A comparison operator in a row-filter leaf.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq, Ne, Lt, Le, Gt, Ge,
    In, NotIn,           // `value` is ScalarValue::List
    IsNull, IsNotNull,   // `value` ignored
}

/// A literal on the right-hand side of a comparison. Text/Int/Bool/List this
/// cycle; float and temporal variants are additive later (kept out now so the
/// type derives `Eq` — f64 is not `Eq`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalarValue {
    Text(String),
    Int(i64),
    Bool(bool),
    List(Vec<ScalarValue>),
}

/// A row filter as a boolean expression tree over ontology properties. Folded
/// into a DataFusion Expr by the Query API; the control plane never interprets it.
/// Serializes to one `jsonb` column.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowFilter {
    Compare { property: String, op: CompareOp, value: ScalarValue },
    And(Vec<RowFilter>),
    Or(Vec<RowFilter>),
    Not(Box<RowFilter>),
}

/// A fine-grained row/column restriction for one (role, target).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub target: PolicyTarget,
    pub row_filter: Option<RowFilter>,   // None = no row restriction
    pub deny_columns: Vec<String>,
}

#[async_trait]
pub trait Acl {
    // --- authoring (write) ---
    /// Create a subject. Idempotent.
    async fn define_subject(&self, id: &SubjectId) -> Result<()>;
    /// Create a role. Idempotent.
    async fn define_role(&self, id: &RoleId) -> Result<()>;
    /// Assign a role to a subject. Both must already exist, else `NotFound`. Idempotent.
    async fn assign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()>;
    /// Remove a role assignment. Idempotent (no-op if absent).
    async fn unassign_role(&self, subject: &SubjectId, role: &RoleId) -> Result<()>;
    /// Grant a coarse `(action, target)` allow to a role. Role must exist, else
    /// `NotFound`. Idempotent.
    async fn grant(&self, role: &RoleId, action: Action, target: PolicyTarget) -> Result<()>;
    /// Remove a grant. Idempotent (no-op if absent).
    async fn revoke(&self, role: &RoleId, action: Action, target: &PolicyTarget) -> Result<()>;
    /// Create or replace the row/column policy for `(role, policy.target)`. Role
    /// must exist, else `NotFound`. Upsert.
    async fn set_policy(&self, role: &RoleId, policy: Policy) -> Result<()>;
    /// Remove the policy for `(role, target)`. Idempotent (no-op if absent).
    async fn clear_policy(&self, role: &RoleId, target: &PolicyTarget) -> Result<()>;

    // --- enforcement queries (read) ---
    /// `Allow` iff any role assigned to `subject` has a grant matching
    /// `(action, target)`. Unknown subject → `Deny` (not an error).
    async fn check(&self, subject: &SubjectId, action: Action, target: &PolicyTarget) -> Result<Decision>;
    /// All policies across `subject`'s roles whose target equals `target`
    /// (order unspecified). Unknown subject → empty vec. No merging.
    async fn policies_for(&self, subject: &SubjectId, target: &PolicyTarget) -> Result<Vec<Policy>>;
}
```

Decisions pinned here:
- **Write ops on the trait** — loom owns the schema; these are the authoring
  substrate and let the contract self-seed.
- **Inverse ops kept** (`unassign_role`/`revoke`/`clear_policy`) — "grant access
  but never remove it" is a real security footgun, so removal is in from the start
  (unlike ontology, which deferred all deletes).
- **Subject/role *deletion* deferred** — only assign/grant/policy have inverses;
  deleting a whole subject or role (and cascading) has no consumer yet.
- **Store, do not validate** — `grant`/`set_policy` do not check the target exists
  in the ontology/catalog, and do not check that a `RowFilter` leaf's `property`
  exists on the type or that its `value` matches the property's type. The tree is
  well-formed by construction (that is the structure's guard); semantic validation
  against the ontology stays deferred (cross-concern, and the property type system
  is still opaque). The only existence check is `assign_role` (subject and role must
  exist → `NotFound`).
- **Autocommit, not `Tx`** — ACL writes are their own transactions. ACL joins the
  `ControlPlane` accessor (`fn acl(&self) -> &dyn Acl`) but does not participate in
  `Tx` this cycle; lineage (P5) remains the first real cross-concern `Tx`.
- **Single-tenant** — no tenant partitioning. Adding a `tenant_id` later is an
  additive column + a lookup predicate; nothing in this model gets harder to
  multi-tenant.

## Schema: `acl` (new migration)

A new `acl` schema migration ships in the postgres adapter
(`migrations/0003_acl.sql`), applied like the queue's `0001` and ontology's `0002`.

The `PolicyTarget` is encoded into three **NOT NULL** columns so it sits in a
primary key cleanly (Postgres PKs cannot rely on nullable columns):
- `target_kind = 'type'`  → `target_a = TypeName`,        `target_b = ''`
- `target_kind = 'table'` → `target_a = TableRef.schema`, `target_b = TableRef.name`

The adapter maps `PolicyTarget <-> (kind, a, b)`; the encoding is an internal
detail of the adapter and never leaks through the trait.

```
acl.subject
  id  text  primary key                  -- SubjectId

acl.role
  id  text  primary key                  -- RoleId

acl.role_member
  subject_id  text  not null  references acl.subject(id) on delete cascade
  role_id     text  not null  references acl.role(id)    on delete cascade
  primary key (subject_id, role_id)

acl.grant                                 -- coarse allow; powers check()
  role_id      text  not null  references acl.role(id) on delete cascade
  action       text  not null            -- 'read' | 'write'
  target_kind  text  not null            -- 'type' | 'table'
  target_a     text  not null
  target_b     text  not null
  primary key (role_id, action, target_kind, target_a, target_b)

acl.policy                                -- fine row/col; powers policies_for()
  role_id       text     not null  references acl.role(id) on delete cascade
  target_kind   text     not null
  target_a      text     not null
  target_b      text     not null
  row_filter    jsonb                     -- nullable; serialized RowFilter tree
  deny_columns  text[]   not null  default '{}'
  primary key (role_id, target_kind, target_a, target_b)
```

- **`grant` / `revoke`:** insert / delete the `grant` row (`grant` uses
  `ON CONFLICT DO NOTHING` for idempotency; `revoke` is a plain delete).
- **`set_policy` (upsert):** serialize the `RowFilter` with `serde_json` and insert
  the `policy` row, `ON CONFLICT (role_id, target_kind, target_a, target_b) DO
  UPDATE` the `row_filter`/`deny_columns`; `row_filter` is `NULL` when the policy
  has no row restriction. `policies_for` deserializes the `jsonb` back into a
  `RowFilter`. `clear_policy` is a plain delete. The in-memory fake stores the
  `RowFilter` value directly under one lock (no serialization).
- **`assign_role`:** verify both the subject and role exist (explicit lookups, →
  `NotFound` otherwise), then upsert the `role_member` row. The FKs are a backstop,
  not the error path (so both backends return the same variant).
- **`action` enum** maps to/from `'read'`/`'write'` via small helpers (mirrors the
  ontology adapter's `cardinality_to_str`/`_from_str`).
- The in-memory fake needs no migration (maps/sets behind a `Mutex`).

## Testing

Queue/ontology-style self-seeding contract (`testkit`, run against both adapters):

- **check / grant / revoke:** define subject + role, assign, `grant(role, Read,
  T)`, then `check(subject, Read, T) == Allow`; `check` for an ungranted action or
  a different target → `Deny`; `check` for an unknown subject → `Deny`;
  `revoke` then `check` → `Deny`; `grant` is idempotent (double-grant, single
  revoke ⇒ `Deny`).
- **role union:** a subject in two roles is `Allow` if *either* role grants;
  revoking one still leaves the other's grant effective.
- **policies:** `set_policy(role, {target, Some(filter), [cols]})` then
  `policies_for(subject, target)` returns the row filter and deny-columns;
  `set_policy` again with different contents **replaces** (upsert, no duplicate);
  `policies_for` across two roles returns **both** policies (no merge);
  `clear_policy` then `policies_for` → empty; unknown subject → empty vec;
  a policy with `row_filter: None` round-trips as `None` (not an empty tree).
- **row-filter tree round-trips intact:** a **nested** filter —
  `And([Compare(tenant Eq Text), Or([Compare(is_public Eq Bool), Compare(owner Eq
  Text)]), Not(Compare(region In List))])` — survives `set_policy` →
  `policies_for` structurally equal (exercises every variant and the `jsonb`
  serialize/deserialize round-trip in the pg adapter).
- **targets:** a `Type` policy and a `Table` policy are distinct; `policies_for`
  for one target kind does not return the other; both `Type` and `Table` round-trip
  through `grant`/`check` and `set_policy`/`policies_for`.
- **referential integrity:** `assign_role` with a missing subject **or** missing
  role → `NotFound`.
- **idempotency:** `unassign_role`, `revoke`, `clear_policy` on absent rows are
  no-ops (`Ok`), not errors.

Assertions are on values/variants, never on backend-specific messages.

## Non-goals (this phase)

- **DataFusion plan rewriting & pushdown decisions** — the Query API folds
  `RowFilter` into an `Expr`, decides what pushes into a single scan versus a
  plan-level filter, and projects out `deny_columns`. P4 only stores and serves.
- **Explicit-deny rules** — default-deny, allow-only; deny-wins precedence is a
  future additive `effect` column.
- **Role hierarchy / inheritance** — flat roles only.
- **Column masking** — `deny_columns` projects out; value masking (NULL/hash) is
  later.
- **Subject/role deletion** — only assign/grant/policy have inverses.
- **Richer `RowFilter` leaves** — `ScalarValue` is Text/Int/Bool/List this cycle;
  float and temporal literals, and functions/computed expressions in leaves, are
  additive later. The tree is `Compare`/`And`/`Or`/`Not` only.
- **Semantic filter validation** — `set_policy` does not check a leaf's `property`
  exists on the type or that its `value` type matches; the tree's well-formedness is
  the only guard until the ontology has a real type registry.
- **Cross-concern validation** — `grant`/`set_policy` do not check the target
  exists in the ontology or catalog.
- **Tenancy** — single-tenant; partitioning deferred.
- **`Tx` participation** — ACL writes are autocommit; cross-concern atomicity
  arrives with lineage (P5).
