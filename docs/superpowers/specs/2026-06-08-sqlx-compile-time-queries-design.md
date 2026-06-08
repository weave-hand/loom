# Design: compile-time sqlx queries under hermetic buck2 (Step 2b, group 4 item 1)

> **Status:** approved design. The original "sqlx offline metadata" item from the
> control-plane critical review, de-risked by the spike on `spike/sqlx-offline`
> (now discarded). This is its own effort, larger than the rest of Group 4 — items
> 2–4 (proptest, pagination, error variants) ship separately.

## Goal

Migrate the postgres adapter from sqlx's **runtime** `query()` API to the
**compile-time-checked** `query!`/`query_as!` macros, so SQL is verified against the
real schema at build time — working under loom's hermetic, RE-based, cargo-less
buck2 build.

## Proven mechanism (from the spike — see memory `sqlx-query-macro-offline-under-buck2`)

`query!` offline resolution checks dirs in order: `[SQLX_OFFLINE_DIR,
manifest_dir/.sqlx, workspace_root/.sqlx]`. Only the last calls `cargo metadata`
(lazily). So with the prepared cache in `SQLX_OFFLINE_DIR` (dirs[0]), `cargo
metadata` is never invoked and no cargo/abspaths are needed. **Verified building on
BuildBuddy RE (1018 cmds, local: 0).** The `rust_library` env:

```python
env = {
    "CARGO_MANIFEST_DIR": "control_plane_postgres",   # read but never dereferenced when dirs[0] hits
    "SQLX_OFFLINE": "true",
    "SQLX_OFFLINE_DIR": "$(location :sqlx-cache)/.sqlx",
}
```
with `filegroup(name = "sqlx-cache", srcs = glob([".sqlx/**"]))` (globbed at the BUCK
package root = the crate root, where `cargo sqlx prepare` writes `.sqlx/`; the
filegroup preserves the `.sqlx/` prefix so the env appends it — same idiom as
`:migrations`, and exactly what the spike used).

**Requires sqlx 0.9.0** — `SQLX_OFFLINE_DIR`-from-env precedence landed there
(PR #3962); it is broken in loom's current 0.8.6 (#3961) and no 0.8.x fix exists.

## Decisions

- **sqlx-cli via `tools/sqlx-prepare.sh`.** sqlx-cli ships no prebuilt binaries, so
  the script builds it once with loom's hermetic cargo into `.loom/` (cached), then
  boots the pinned `:postgres-bin`, applies the migrations, and runs `cargo sqlx
  prepare`. It's a codegen/CI-time tool, not a build-graph dep, so a script is
  proportionate (no reindeer/fetch infra).
- **Freshness enforced by a prek pre-push hook** (`sqlx-prepare`), alongside
  `buck2-build`/`buck2-test`. It runs the script in check mode and `git diff
  --exit-code`s `src/control-plane/postgres/.sqlx`. Pre-commit would be too heavy
  (boots pg); the CI build is a backstop anyway — a stale/missing cache makes `query!`
  fall through to the cargo-metadata path and **fail the build**.
- **`.sqlx` lives at `src/control-plane/postgres/.sqlx/`** (crate root, where `cargo
  sqlx prepare` writes it; checked in), globbed into the `:sqlx-cache` filegroup.

## sqlx 0.9.0 breaking changes to handle

The whole `postgres` crate moves 0.8.6 → 0.9.0 (sqlx is only used there; no
dual-version collision). Relevant breaks:

- **#3723 `SqlSafeStr`** — runtime `query()/query_as()` now take `impl SqlSafeStr`.
  The adapter's own SQL becomes `query!` (string literal, unaffected). The TWO
  `query_scalar` calls in `fixture.rs` **must stay runtime** (wrapped in
  `AssertSqlSafe(...)`): they read DuckDB-created `ducklake_*` catalog tables that the
  loom migrations don't create, so they don't exist in the freshly-migrated pg at
  `cargo sqlx prepare` time and `query!` can't validate them.
- **#3383 Migrate trait / `sqlx.toml`** — loom uses runtime
  `sqlx::migrate::Migrator::new(dir).run(&pool)` (`lib.rs:48`). Verify/port the
  `Migrator::new` signature and `Migrate` trait usage for 0.9.
- **#3821 MSRV 1.86** — satisfied: the pinned nightly is `2026-03-28`, and the spike
  compiled sqlx 0.9 on it.
- **#3541 / #3800** (generic plans, conn-option escaping) — transparent to our usage;
  no action beyond confirming tests stay green.

## Migration pattern (per query)

Today: `sqlx::query("…").bind(a).bind(b).fetch_optional(&pool)` → manual
`row_to_job(&PgRow)` using `row.get(...)`.

After: `sqlx::query!("…", a, b).fetch_optional(&pool)` → map the macro's anonymous
struct into the loom domain type. Example (`dequeue`):

```rust
let cutoff = OffsetDateTime::now_utc() - self.lock_timeout;
let row = sqlx::query!(
    r#"update queue.jobs set state='running', locked_at=now(), locked_by=$1,
           attempts=attempts+1, updated_at=now()
       where id = ( select id from queue.jobs
           where kind = any($2) and run_at <= now()
             and (state='available' or (state='running' and locked_at < $3))
           order by priority desc, run_at asc
           for update skip locked limit 1)
       returning id, kind, payload, attempts, run_at"#,
    worker, kinds, cutoff,
)
.fetch_optional(&self.pool)
.await
.map_err(backend)?;
Ok(row.map(|r| Job {
    id: JobId(r.id),
    kind: r.kind,
    payload: r.payload,
    attempts: r.attempts,
    run_at: r.run_at,
}))
```

Caveats the implementer must handle (build will force them):
- **Nullability overrides.** `query!` infers each column's nullability; columns it
  can't prove non-null become `Option<T>`. Where the domain type is non-`Option`, use
  the `column as "name!"` force-non-null alias (or `as "name?"` to force nullable). A
  `returning` from an `update`/`insert` of declared-NOT-NULL columns is usually proven
  non-null, but expressions/joins are not.
- **Type overrides.** For columns sqlx maps to a different Rust type than the domain
  needs, use `as "name: Type"` (e.g. a jsonb decoded to a concrete type). `payload`
  (jsonb) → `serde_json::Value` is automatic with the `json` feature.
- The shared `row_to_job`/`row_to_snapshot`/etc. helpers in `lib.rs` are replaced by
  per-`query!` mapping closures (each macro yields its own anonymous struct), or by
  `query_as!(DomainStruct, …)` where the domain struct's fields line up. Prefer
  `query_as!` into a small private row struct when several queries share a shape;
  otherwise inline the map. The implementer chooses per query for readability.

## `.sqlx` generation harness (`tools/sqlx-prepare.sh`)

1. Build/cache sqlx-cli 0.9 via the hermetic cargo (`cargo install --root .loom
   --version ^0.9 sqlx-cli --no-default-features --features postgres,rustls`), skip if
   already present.
2. Materialize the pinned postgres + libxml2 (`buck2 build //src/control-plane/postgres:postgres-bin :libxml2`),
   `initdb` a temp cluster, start it on a private socket (mirrors `PgFixture`).
3. Apply the loom migrations (the same `migrations/` dir the fixture uses) so the
   schema exists for `query!` validation.
4. `DATABASE_URL=… cargo sqlx prepare --workspace -- -p control-plane-postgres`
   (writes `src/control-plane/postgres/.sqlx/query-*.json`).
5. Stop the cluster, remove the temp dir.
6. A `--check` flag runs steps 1–5 then `git diff --exit-code …/.sqlx` for the hook/CI.

Booting pg + applying migrations duplicates a little of `PgFixture`; that's acceptable
for a standalone tool (the fixture is Rust test code, not reusable from a shell hook).

## Build wiring

- `postgres/Cargo.toml`: `sqlx = "0.9"` (same features + `macros`).
- `tools/buckify.sh` + `reindeer update` regenerate `third-party/BUCK` for 0.9.
- `postgres/BUCK`: add the `:sqlx-cache` filegroup and the three `env` entries to the
  `rust_library`. The `rust_test` targets already boot a real pg (PgFixture), so they
  keep working unchanged; the env only affects the library's compile.

## Verification

- `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...` — all green, counts
  unchanged (behaviour preserved; `query!` is a compile-time change).
- A clean **remote-only** build of `//src/control-plane/postgres:postgres` succeeds
  (the CI-critical proof; the spike confirmed the mechanism on RE).
- `tools/clippy-all.sh` clean; `prek run --all-files` green; the new `sqlx-prepare`
  pre-push hook passes (no `.sqlx` diff).
- `git grep 'sqlx::query('` in the adapter's concern files (queue/catalog/ontology/
  acl/lineage + the `pg_insert`/`pg_emit` helpers) returns nothing — all migrated to
  `query!`. `fixture.rs`'s two `query_scalar` + `AssertSqlSafe` calls remain.

## Scope / non-goals

- **postgres adapter only.** `memory` has no SQL; `core`/`testkit`/`worker` unaffected.
- **`fixture.rs` sqlx queries stay runtime** (`AssertSqlSafe`): they read DuckDB's
  `ducklake_*` catalog tables, absent from the loom-migrated schema at prepare time.
  Its DuckDB-CLI `push_str` scripts are **not** sqlx and stay as-is.
- No change to the `Queue`/`Catalog`/… trait signatures or behaviour — purely the SQL
  layer's compile-time checking.
- Incremental delivery: one concern per task, regenerating `.sqlx` and testing after
  each, so a mid-migration tree always builds.
