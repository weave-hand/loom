# Embed migrations into the binary via `sqlx::migrate!` (slice 2, PR-a)

Status: design. Author: brainstorm session 2026-06-28.

## Why

The single-binary arc (see
[`2026-06-28-embedded-postgres-lifecycle-design.md`](2026-06-28-embedded-postgres-lifecycle-design.md))
wants loom to run with **no files on disk beyond the binary**. Slice 1 landed
the embedded-Postgres lifecycle but still applies migrations from an on-disk
directory (`LOOM_MIGRATIONS_DIR` → `run_migrations(pool, dir)`). This spec
removes the on-disk migrations dependency by **baking the migrations into the
binary at compile time**.

Slice 2 was split (brainstorm decision) into two PRs:

- **PR-a (this spec):** embed the 76 KB of migrations. Small, proves the
  compile-time embed mechanism, and is independently shippable.
- **PR-b (later):** embed the 11.7 MB Postgres distribution. A genuinely
  different mechanism (`include_bytes!` of a tarball + `flate2` extract to a
  content-addressed cache + target gating) — its own spec.

## The mechanism (verified, not assumed)

The historical BUCK comment in `src/control-plane/postgres/BUCK` claims the
compile-time `sqlx::migrate!` macro "needs an absolute `CARGO_MANIFEST_DIR`,
which buck2 can't provide." **That is false**, and this design rests on a
mechanism proven empirically with a throwaway probe target built on
BuildBuddy RE:

1. **`sqlx::migrate!` reads the dir at compile time** (sqlx 0.9.0
   `common::resolve_path`): it rejects absolute paths outright ("absolute paths
   will only work on the current machine"), only accepts a string **literal**
   (so `env!(...)` does not parse), rejects a bare single-component relative
   path (the parent must be non-empty), and otherwise joins the relative path
   onto `CARGO_MANIFEST_DIR`.
2. **buck2 already pins `CARGO_MANIFEST_DIR="."`** on the `postgres`
   rust_library — `"."` is the package root in the action sandbox (this is what
   lets clippy-driver find `clippy.toml` by walking up, and satisfies the
   `query!` offline resolver).
3. **`mapped_srcs` materializes the migrations as declared inputs.** Adding
   `mapped_srcs = {f: f for f in glob(["migrations/*.sql"])}` places the SQL
   files at `<sandbox>/__srcs/migrations/` — a *declared* input, so the action
   is hermetic on RE.
4. Therefore **`sqlx::migrate!("./migrations")`** (note the leading `./`, which
   gives the path a non-empty parent) resolves to the mapped directory and
   embeds every migration, with checksums, at compile time.

Evidence: with `mapped_srcs` present the probe **built on RE** (`Commands: 2
remote`, `BUILD SUCCEEDED`); with `mapped_srcs` removed it failed with `error
canonicalizing migration directory .../__srcs/./migrations: No such file or
directory` — proving the macro genuinely reads the mapped files rather than
silently embedding an empty set.

This is strictly better than a hand-rolled `MigrationSource`, a genrule that
generates a `(version, name, sql)` array, or a tar-and-extract scheme: it is the
blessed, tested sqlx path, adds **no new dependencies and no codegen**, and the
embedded `Migrator` tracks `_sqlx_migrations` identically to the dir runner
(idempotent, re-runs as a no-op).

## Scope

Decision (brainstorm): **consolidate everything onto the embedded migrator and
delete the on-disk migration machinery** — one blessed migration path
repo-wide, not just the embedded runtime arm.

**In:**

- `control_plane_postgres`: add the embedded migrator + a thin runner; wire
  `mapped_srcs` into the `postgres` rust_library; correct the false BUCK
  comment.
- Switch **every** migration-application site to the embedded migrator.
- Delete `run_migrations(pool, dir)`, the `:migrations` filegroup, and all
  `LOOM_MIGRATIONS_DIR` plumbing.
- `service_runtime`: drop `migrations_dir` from `EmbeddedSettings`; embedded
  mode no longer needs `LOOM_MIGRATIONS_DIR`.

**Out:**

- Embedding the Postgres **binaries** (PR-b).
- Any change to `tools/sqlx-prepare.sh` — it applies migrations by globbing the
  **source** `migrations/*.sql` through `psql` directly (not the Rust runner),
  so it is unaffected. The source files stay on disk in the repo; only the buck
  `:migrations` *filegroup* and the runtime *dir* dependency are removed.

## Design

### `control_plane_postgres` — the embed

`src/control-plane/postgres/BUCK`, on the `postgres` rust_library:

```python
mapped_srcs = {f: f for f in glob(["migrations/*.sql"])},
```

`CARGO_MANIFEST_DIR="."` is unchanged. The `:migrations` filegroup (currently
only feeding `LOOM_MIGRATIONS_DIR`) is deleted. The misleading comment above it
is replaced with a one-line pointer to this mechanism.

New surface (`src/control-plane/postgres/src/lib.rs`, or a small
`embedded_migrations` module):

```rust
/// Migrations baked into the binary at compile time (no migrations-on-disk).
pub fn embedded_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

/// Apply the embedded migrations (tracked in `_sqlx_migrations`, idempotent).
pub async fn run_embedded_migrations(pool: &PgPool) -> Result<()> {
    embedded_migrator()
        .run(pool)
        .await
        .map_err(|e| ControlPlaneError::Backend(Box::new(e)))?;
    Ok(())
}
```

`run_migrations(pool, &Path)` is **deleted**.

### Migration-application sites (all switch to `run_embedded_migrations`)

| Site | Today | After |
| --- | --- | --- |
| `fixture.rs:306` | reads `LOOM_MIGRATIONS_DIR`, `run_migrations(pool, dir)` | `run_embedded_migrations(pool)`; drop the env read |
| `runtime/src/lib.rs:278` (embedded arm) | `run_migrations(pool, &e.migrations_dir)` | `run_embedded_migrations(pool)` |
| `managed-postgres/tests/embedded_lifecycle.rs:37,60` | `run_migrations(pool, dir)` | `run_embedded_migrations(pool)`; drop the env read (line 26) + doc (line 3) |

Indirect beneficiaries (no direct edit): the `.sqlx` freshness test
(`tests/sqlx_cache.rs`) applies migrations via `PgFixture::start()`, so it
inherits the embedded path automatically.

### `service_runtime` seam

- Remove `migrations_dir: PathBuf` from `EmbeddedSettings`
  (`runtime/src/lib.rs:41`) and its parse (`:168`, `:180`).
- Embedded mode stops requiring `LOOM_MIGRATIONS_DIR`.
- Update `runtime/tests/embedded_config.rs` (drop the `LOOM_MIGRATIONS_DIR`
  insert at `:32` and any `migrations_dir` assertion).

### buck `loom_fixture_test` macro

Remove the `LOOM_MIGRATIONS_DIR` entry from `defs.bzl:40` (it is set for every
fixture test repo-wide; `fixture.rs` is its only reader, and that reader is
switching to the embedded migrator).

## Testing

- **Pure-logic unit** (`rust_test`, RE-eligible, no DB): assert
  `embedded_migrator()` contains exactly the on-disk migration count and that
  versions form the expected contiguous `1..=N` sequence — proves the macro
  embedded the real set. (Uses the `Migrator`'s public iteration API; exact
  method confirmed at implementation.)
- **Fixture** (`loom_fixture_test`, hermetic PG): `run_embedded_migrations` on a
  fresh db → assert the loom schema/tables exist; **re-run → zero pending**
  (`_sqlx_migrations` count unchanged); idempotent.
- The existing fixture suite (query-api, worker, postgres contract tests, the
  `.sqlx` freshness test) is the regression backstop — all boot through
  `fixture.rs`, which now uses the embedded migrator. A green
  `buck2 test //src/...` is the acceptance gate.

## Risks / open questions

- **Macro + `query!` coexistence in one crate.** The `postgres` lib already
  expands the offline `query!` macros (`SQLX_OFFLINE`/`SQLX_OFFLINE_DIR`); adding
  the `migrate!` expansion is independent, but the first implementation task
  builds the lib on RE to confirm the two coexist (the probe validated
  `migrate!` in this crate's exact env, sans `query!`).
- **`mapped_srcs` glob drift.** The glob is evaluated at buck-parse time, so a
  new `migrations/NNNN_*.sql` is picked up automatically; no manual list to keep
  in sync. The unit count-assertion guards against an accidental empty/short
  embed.
- **macOS.** Mechanism is host-agnostic (compile-time file read); no per-OS
  concern.

## Register note

On completion: mark the migrations half of `fut-embedded-postgres-embed-extract`
done (or split it — migrations done, PG binaries pending), add a ROADMAP item
for slice-2a with this spec as `spec:` and PR-b as a `[[link]]`, and record in
`docs/ISSUES.md`/cleanup that the BUCK "migrate! can't work" comment was
corrected.
