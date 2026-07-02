# First-admin bootstrap via host CLI + minimal admin governance surface — design

**Status:** approved for planning
**Date:** 2026-07-02
**Area:** acl

## Goal

Give a fresh loom instance a safe, first-class path from "nobody exists" to "an
admin can log in and set up governance," without ever exposing a standing
all-seeing superuser. Bootstrapping the first admin becomes a one-time,
**out-of-band host CLI** act (`loom create-admin`); afterward the admin is a
normal authenticated identity with **administrative authority** — it defines
models, roles, and coarse ACL grants over an admin-gated HTTP surface, and grants
read/write explicitly (including to itself). Bootstrap is a one-way state machine:
once the first admin exists the instance is **Sealed**, with no in-app path back.
This replaces the current `LOOM_BOOTSTRAP_ADMIN_*` env bootstrap.

## Security posture (why this shape)

A permanent all-seeing superuser is the high-value target you then have to guard.
Industry practice splits the concern:

1. **Bootstrapping the first admin** — a one-time act bound to a trust boundary
   that is *already* hardened (host / DB access), **never an in-band HTTP-reachable
   feature**. Self-hosted ⇒ a host CLI command (the Django `createsuperuser`
   archetype).
2. **Ongoing authority** — *not* a standing god account. Day-to-day runs on
   decomposed least-privilege roles; the admin's power is administrative, and data
   access is by explicit, auditable grant.

Sealing is modelled as a one-way state machine (`Uninitialized | Sealed { at }`)
with the illegal `Sealed → Uninitialized` transition simply **not implemented** —
re-bootstrapping requires direct DB/host access. A true break-glass / quorum
god-mode path is explicitly **out of scope** here (future work).

## Non-goals (this slice)

- Any HTTP-reachable way to create the first admin (deliberately CLI-only).
- A standing `is_superuser` / ACL-bypass identity of any kind.
- Break-glass / quorum / time-boxed god-mode, MFA, external IdP delegation.
- Fine-grained policy authoring over HTTP (row filters, column masks).
- Ontology **links** and **actions** over HTTP (only `define_type`/"model").
- Hidden (no-echo) password entry if it requires a new third-party dep (see
  *Password handling*); `--password-stdin` is the dep-free mechanism.
- Multi-admin lifecycle beyond "an admin can mint another admin by assigning the
  reserved role."

## Architecture

Three cooperating pieces: a **host CLI** that seeds+seals, a **sealing state +
`admin` role** in the control plane, and an **admin HTTP surface** the seeded
admin uses.

```
loom create-admin (host, out-of-band)
    └─ control-plane tx: create_user (Argon2) → define+assign `admin` role → seal
Sealed state (one-way)  ── gates re-bootstrap (CLI refuses when sealed)
`admin` role            ── gates /admin/* (require_admin = holds `admin` role)
/admin/{models,roles,roles/{r}/grants,users}  ── admin builds governance
```

### 1. Host CLI — `loom create-admin`

- A new argv subcommand on the standalone `loom` binary
  (`src/services/standalone/src/main.rs`), dispatched *before* the composite
  boots — sibling to the existing `LOOM_MIGRATE=apply` migrate-and-exit path
  (`service_runtime::migrate_requested`). Detected via
  `std::env::args().nth(1) == "create-admin"`; hand-rolled flag parse (`--username`,
  `--password-stdin`) to avoid a new arg-parser dep.
- **Connects** to the control plane as a plain client using the normal
  `LOOM_DB_*`/`Config::from_map(env_map())` resolution + `build_pool` +
  `control_plane` — it does **not** own the embedded-PG lifecycle. Operationally
  it runs against an external DB, or against a *running* embedded instance's PG
  socket (`LOOM_DB_HOST` = `<LOOM_DATA_PATH>/pgrun`). Migrations must already be
  applied (embedded auto-migrates on server boot; external via `LOOM_MIGRATE=apply`);
  a missing bootstrap table is a clear "run migrations first" error.
- **Password handling:** read from stdin (`--password-stdin`; prompt "Password:"
  to stderr, read one line). Works interactively and piped
  (`printf '%s' "$pw" | loom create-admin --username jack --password-stdin`). A
  no-echo interactive prompt is deferred (would need e.g. `rpassword`); out of scope.
- **Behaviour** (single control-plane transaction):
  - **Uninitialized** (not sealed): `create_user` (subject id = username, Argon2
    PHC via `hash_password`) → `define_subject` → `define_role(admin)` (idempotent)
    → `assign_role(subject, admin)` → **seal** (`seal_bootstrap`). Print the created
    admin username; exit 0.
  - **Sealed**: refuse — stderr "an admin already exists; re-bootstrap requires
    direct DB/host access", exit non-zero. No in-app unseal.
  - Empty/whitespace password or username ⇒ reject, exit non-zero.
- **`standalone::run` change:** delete the `LOOM_BOOTSTRAP_ADMIN_USERNAME/PASSWORD`
  → `bootstrap_admin` block (`src/services/standalone/src/lib.rs:56-62`). The
  server boots and serves with zero users; login simply fails until
  `create-admin` runs. `service_runtime::bootstrap_admin` and its env are removed.

### 2. Sealing state + reserved `admin` role (control plane)

- **Sealing** is a single-row control-plane record — new migration adding an
  `acl.bootstrap` table `(id smallint primary key default 1 check (id=1),
  sealed_at timestamptz not null)`; absence of the row = `Uninitialized`. Two new
  `Auth` (or a small dedicated) trait methods: `is_bootstrap_sealed() -> bool` and
  `seal_bootstrap()` (insert-once; a second call is a `Conflict`, which the CLI's
  single-tx flow never triggers). Chosen over inferring from `has_any_user` because
  an explicit, terminal flag was wanted.
- **Reserved `admin` role:** a `pub const ADMIN_ROLE: &str = "admin"` in
  `control-plane/core`. The CLI `define_role`s + `assign_role`s it. Membership is
  the admin gate.
- **New `Acl::has_role(subject, role) -> bool`** on the trait
  (`control-plane/core/src/acl.rs`), implemented in the postgres adapter
  (`control-plane/postgres/src/acl.rs`, a `select exists(... subject_role ...)`)
  and the memory fake, with a testkit contract case. `require_admin` uses it.

### 3. Admin HTTP surface (all `require_admin`-gated)

Extends the existing `admin_routes` (`src/services/runtime/src/admin.rs`, already
mounted at `query-api/src/serve.rs:73`).

- **Gate change:** `require_admin` currently checks `subject.id == admin_username`
  (config). It changes to `acl.has_role(subject, ADMIN_ROLE)`. `AdminState` drops
  `admin_username` and gains an `Ontology` facet (`Arc<dyn Ontology + Send + Sync>`)
  for `define_type`; it already carries `auth` + `acl`. `serve.rs`'s `AdminState`
  construction updates accordingly (the `PgControlPlane` already provides all facets).
- **Routes:**
  - `POST /admin/models` → `Ontology::define_type`. Body: `{ name, table: {schema,
    name}, identity, properties: [{ name, ty, required }] }` (mirrors the
    `ObjectType`/`PropertyDef` shape; `derived: []`). 201 on create; existing
    define_type upsert semantics apply.
  - `POST /admin/roles` (+ `GET /admin/roles` list) → `Acl::define_role`.
  - `POST /admin/roles/{role}/grants` → `Acl::grant(role, action, Type(name),
    Allow)`, `action ∈ {Read, Write}`; `DELETE`/revoke → `Acl::revoke`. Unknown
    type ⇒ 400 (existing existence-validation in `grant`).
  - `POST /admin/users` (**unchanged**) — already creates a user + assigns existing
    roles; an admin can assign `admin` to mint another admin.
- The admin is **not** an ACL bypass: to read a type's objects it grants a role
  `Read` on it (its own `admin` role or a dedicated one) via the grant route, then
  the normal governed read path applies.

## Error handling

- **CLI:** sealed → non-zero + guidance; DB unreachable / missing bootstrap table
  → non-zero naming the cause; empty username/password → non-zero. The seed+seal
  runs in one transaction, so a mid-way failure leaves the instance Uninitialized
  (retryable) — never a half-seeded, sealed state.
- **Routes:** unauth → 401; non-admin (lacks `admin` role) → 403; unknown grant
  target type → 400; define_type/role conflicts follow existing control-plane
  semantics. Opaque-500 arms log server-side via the existing `internal_error`
  idiom.

## Testing

- **CLI (fixture, `loom_fixture_test`):** create-admin on a migrated fresh DB →
  the `admin` user exists, holds `ADMIN_ROLE`, instance is sealed; the admin can
  authenticate (`/auth/login` 200). A second `create-admin` → refused (non-zero,
  instance still single-admin). Empty-password → refused.
- **`has_role` + seal contracts** in `testkit`, run against both the memory fake
  and postgres adapters (mirrors existing ACL/auth contract tests).
- **Admin routes (fixture e2e, extending `query-api/tests/admin_e2e.rs`):** an
  `admin`-role subject defines a model, creates a `reader` role, grants it `Read`
  on the model, and creates a reader user; the reader then reads the type's objects
  and a non-admin gets 403 on every `/admin/*` route. Gate regression: a subject
  *without* `admin` role is 403.
- **`.sqlx` cache** regenerated (`tools/sqlx-prepare.sh`) for the new
  `has_role`/`seal`/`is_sealed` queries and committed; the `sqlx-cache-check` test
  covers freshness.

## Global constraints

- Tests are `rust_test`/`loom_fixture_test` targets only — never inline
  `#[cfg(test)]` (`no-inline-tests` hook). New control-plane SQL uses sqlx
  compile-time macros with a committed `.sqlx` cache.
- Strict clippy (pedantic + restriction) on all production lib/bin code, including
  the CLI path — no `unwrap`/`expect`/`indexing_slicing`/`panic`; propagate with
  `?` and typed errors.
- Both control-plane adapters (memory + postgres) implement every new trait method
  and are pinned by a testkit contract — no adapter divergence.
- The CLI must have **no HTTP path**; it is argv-only on the `loom` binary.
- No new standing-privilege identity; the admin is governed like any subject for
  data access.

## File structure

- **Modify** `src/control-plane/core/src/acl.rs` — `Acl::has_role`; `ADMIN_ROLE`
  const (or in `lib.rs`).
- **Modify** `src/control-plane/core/src/auth.rs` — `is_bootstrap_sealed` /
  `seal_bootstrap` (sealing trait methods).
- **Create** `src/control-plane/postgres/migrations/<n>_bootstrap_seal.sql` — the
  `acl.bootstrap` table.
- **Modify** `src/control-plane/postgres/src/{acl.rs,auth.rs}` — impls +
  `.sqlx` cache.
- **Modify** `src/control-plane/memory/*` — memory-fake impls.
- **Modify** `src/control-plane/testkit/*` — contract cases for `has_role` + seal.
- **Modify** `src/services/runtime/src/admin.rs` — `AdminState` (drop
  `admin_username`, add `Ontology`), `require_admin` → `has_role`, new
  `define_model` / `create_role` / `list_roles` / `grant` handlers + routes.
- **Modify** `src/services/runtime/src/auth.rs` — remove `bootstrap_admin` + its env.
- **Modify** `src/services/runtime/src/lib.rs` — re-exports.
- **Modify** `src/services/query-api/src/serve.rs` — `AdminState` construction.
- **Create** `src/services/standalone/src/admin_cli.rs` — `create-admin` logic.
- **Modify** `src/services/standalone/src/main.rs` — argv dispatch of `create-admin`.
- **Modify** `src/services/standalone/src/lib.rs` — delete the env bootstrap block.
- **Modify** `src/services/{runtime,standalone,query-api}/BUCK` — new test targets.
- **Modify** `tools/dev-up.sh` — drop `LOOM_BOOTSTRAP_ADMIN_*`; after the server
  is ready, run `loom create-admin --username admin --password-stdin`.
- **Modify** `docs/ISSUES.md` — none required; **`docs/ROADMAP.md`** — add the
  register item on planning.

## Register updates

- `docs/ROADMAP.md`: add `road-first-admin-bootstrap` (area `acl`, status
  `planned` → `done`), `spec:` this file, linking the retired
  `LOOM_BOOTSTRAP_ADMIN_*` env path.
- On completion, `loom-docs-update` records deferrals: break-glass/quorum god-mode,
  fine-grained policy authoring over HTTP, ontology links/actions over HTTP, and
  no-echo password entry — as FUTURE items.
