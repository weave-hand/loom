# Fix `SqlCatalog::execute` swallowed commit error

- **Date:** 2026-06-29
- **Area:** iceberg
- **Register item:** [[iss-sqlcatalog-execute-commit-swallow]]
- **Status:** spec (ready for a work agent to plan + build)

## Problem

`SqlCatalog` (`src/control-plane/postgres/src/iceberg_sql_catalog/catalog.rs`) is
loom's hand-vendored implementation of the apache/iceberg-rust `Catalog` trait and
the single object through which every table mutation in the system commits — ingest
landing, the transform/flush/GC worker paths, and the serving-side action writer all
route their writes through it (it is the highest-betweenness node in the codebase).
A silent failure here is therefore maximally load-bearing.

In the **no-transaction** branch of `SqlCatalog::execute` (catalog.rs:386-394):

```rust
async fn execute(
    &self,
    query: &str,
    args: Vec<Option<&str>>,
    transaction: Option<&mut Transaction<'_, Postgres>>,
) -> Result<PgQueryResult> {
    // ...
    match transaction {
        Some(t) => sqlx_query.execute(&mut **t).await.map_err(from_sqlx_error),
        None => {
            let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
            let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error);
            drop(tx.commit().await.map_err(from_sqlx_error));   // <-- bug
            result
        }
    }
}
```

Two defects in the `None` arm:

1. **Swallowed commit failure.** `tx.commit()`'s `Result` is built into an error by
   `.map_err(from_sqlx_error)` and then immediately `drop`ped. If the commit fails,
   the function still returns the earlier (successful) `execute` result — the caller
   believes the statement persisted when it did not. The `.map_err` inside the
   `drop(...)` is dead work.
2. **Commit-on-failed-statement.** `result` captures the statement error but is only
   returned *after* `tx.commit()` runs, so the transaction is committed even when the
   statement itself errored. (In practice a failed statement leaves nothing to
   persist, but committing a known-bad unit of work is the wrong shape and masks the
   intent.)

Origin: pre-existing. On `main` this was `let _ = tx.commit().await…`; the
strict-clippy pass (`from:clippy-strict-lints`) mechanically rewrote it to
`drop(…)` without changing the swallow semantics, and the behavior change was
deliberately deferred out of the lint PR to keep that PR free of transactional-
semantics changes.

## Fix

Replace the `None` arm with straight-line `?` propagation:

```rust
None => {
    let mut tx = self.connection.begin().await.map_err(from_sqlx_error)?;
    let result = sqlx_query.execute(&mut *tx).await.map_err(from_sqlx_error)?;
    tx.commit().await.map_err(from_sqlx_error)?;
    Ok(result)
}
```

Semantics after the fix:

- On **statement error**, `?` returns early; the dropped `tx` rolls back implicitly,
  so nothing is committed.
- The transaction is **committed only after** a successful statement.
- A **commit failure** now surfaces to the caller as the error it is.

No signature or API change — `execute` already returns `Result<PgQueryResult>`, and
the `Some(t)` (caller-owned transaction) arm is unchanged.

## Scope

Exactly the one branch above. Audit of the rest of `catalog.rs` confirms this is the
only swallowed commit:

- catalog.rs:391 — the `drop(tx.commit()…)` being fixed (the only `drop(...commit)`
  in the whole `control-plane/postgres` crate).
- catalog.rs:568, :826, :982 — already propagate via `?`; unchanged.
- catalog.rs:526 — `drop(tx.rollback().await)` on the error-cleanup path; discarding
  a rollback error while already unwinding is acceptable and stays.

Out of scope: any broader transactional-semantics review of the other commit sites,
the `Some(t)` arm, or the `do_update_table` path. This is a single-defect fix.

## Testing

Per the planning decision, cover the **statement-failure path only** — it is
deterministic in the hermetic Postgres fixture, whereas a genuine *commit* failure
requires fault injection we are deliberately not adding.

Add a `rust_test` (extend the existing hermetic fixture suite
`src/control-plane/postgres/tests/iceberg_catalog.rs`, or a sibling `rust_test`
target wired the same way via `loom_fixture_test`) that:

1. Drives a statement guaranteed to fail at execution time through
   `execute(query, args, None)` — e.g. invalid DDL or a constraint/uniqueness
   violation against the live loom schema.
2. Asserts `execute` returns `Err`.
3. Asserts, via a follow-up read, that **nothing was persisted** (the implicit
   rollback held).

Commit-error propagation is correct-by-construction from the added `?` and is **not**
separately exercised — triggering a deterministic commit failure (e.g. a `DEFERRABLE`
constraint that fails at `COMMIT`, or a poisoned connection) is fixture-fragile and
out of scope. This gap is recorded here intentionally rather than left implicit.

**Implementation outcome (2026-06-29).** The commit-failure path *was* covered after
all: a `DEFERRABLE INITIALLY DEFERRED` unique constraint proved to be a deterministic
(not fixture-fragile) way to make a single-statement insert succeed at statement time
and fail at `COMMIT`. The shipped suite
(`src/control-plane/postgres/tests/sqlcatalog_execute_commit.rs`) therefore carries
**both** tests: `statement_failure_rolls_back_and_errors` (the statement-failure lock
specified above) and `commit_failure_propagates_as_error` (a genuine red→green
regression test for the swallowed-commit bug — it fails on the pre-fix `drop(...)`
body and passes after the `?`-propagation fix). The earlier "out of scope" judgment is
left above as the original reasoning; this note records that it was superseded in
implementation.

The test follows loom's testing rules: a `rust_test` integration target (never an
inline `#[cfg(test)]` module), routed local via `loom_fixture_test` because it boots
the hermetic Postgres fixture.

## Risk

Strictly a correctness improvement. The only behavior change visible to callers is
that a previously-spurious `Ok` (on a failed commit) becomes the real `Err`, and a
failed statement no longer reaches `commit`. Blast radius is one local branch; the
catalog contract tests already cover the success path, so the fix cannot silently
regress normal commits.
