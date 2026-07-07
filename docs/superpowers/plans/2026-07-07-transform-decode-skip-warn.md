# Transform body decode: per-row skip-and-warn — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the two hard-failing `de_body` batch decode sites (`define_transform`'s trigger-cycle scan and `claim_due_schedules`'s loop) to per-row skip-and-warn — matching the already-immune commit seam — so one undecodable `transforms.transform.body` no longer 500s every subsequent `on_input_commit` define nor starves every schedule.

**Architecture:** `pg_fire_data_triggers` already decodes with skip-and-warn at both its points (`transforms.rs:129-136`, `:172-179`). The two batch decoders still use `?`/`collect::<Result<_>>()?`. Site 2 (trigger-cycle scan) becomes a loop that warns and omits an undecodable def from the edge set (sound: an undecodable node cannot fire, so it forms no live cycle — repairing it re-runs the scan). Site 1 (claim loop) becomes advance-and-warn: on decode failure, warn and still advance `next_run_at` from the row's own `schedule` column so the poison row leaves the due window (a bare skip would leave it perpetually due), while not pushing it to `claimed`.

**Tech Stack:** Rust, sqlx (all SQL text reused verbatim — **no `.sqlx` regen**), buck2 `loom_fixture_test` (hermetic Postgres), `tracing::warn!`.

## Global Constraints

- **No `.sqlx` regen** — both sites reuse existing `query!`/`query_scalar!` text verbatim (the claim loop's `update … next_run_at` is the same statement the healthy path already runs; the trigger scan's `select` is unchanged). `//src/control-plane/postgres:sqlx-cache-check` must stay green.
- **Mirror the commit seam** — warn with `tracing::warn!(transform = %<name>, error = %e, "<site>: undecodable body skipped")`, carrying the offending transform name and the decode error, exactly as `transforms.rs:129-136` / `:172-179` do.
- **No change to `pg_fire_data_triggers`** — it is already correct; this makes the other two consistent.
- **No API / write-time validation change** — `define_transform` still validates + re-serializes on write; this is read-path defense-in-depth only.
- **Tests are `rust_test` integration targets** (`//src/control-plane/postgres:data-triggers`, `:transforms`), not inline modules. Seed an undecodable body by writing invalid JSON directly with raw SQL (`update transforms.transform set body = '"nonsense"'::jsonb where name = $1`), bypassing `define_transform`'s validation — the idiom `broken_def_body_is_skipped` (`data_triggers.rs:528`) already uses. Assert **behavior** (the seam's tests do not capture tracing output).
- **Clippy strict** on production code — no `unwrap`/`expect`/indexing; errors go through `map_err(backend)` / `?` on the healthy path exactly as today.
- Commit messages end with the two required trailers; subjects follow Conventional Commits.

---

### Task 1: Trigger-cycle scan omits undecodable defs (skip-and-warn)

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs:312-315` (the `collect::<Result<_>>()?` in `define_transform`)
- Test: `src/control-plane/postgres/tests/data_triggers.rs` (a new `define_survives_poison_existing_def` test)

**Interfaces:**
- Consumes: `de_body(v: serde_json::Value) -> Result<TransformBody>` (`transforms.rs:24`); the existing `existing` row set (`select name, body …`, `transforms.rs:304-311`).
- Produces: no new surface — `define_transform` no longer 500s on an undecodable existing def.

- [ ] **Step 1: Write the failing test**

In `src/control-plane/postgres/tests/data_triggers.rs`, add (it already imports `Transforms`, `TransformDef`, `TransformName`, `TransformBody`, `OutputMode`, `PgFixture`, and has `dt_def`/`tref`):

```rust
/// An undecodable EXISTING on_input_commit def must not 500 a subsequent define:
/// the trigger-cycle scan skips it and validates the decodable subset.
#[tokio::test]
async fn define_survives_poison_existing_def() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    let input = tref("main", "input1");
    pg.define_transform(dt_def("existing", &input, ("main", "out_existing")))
        .await
        .expect("define existing");

    // Corrupt the existing def's body directly (bypasses define_transform).
    sqlx::query("update transforms.transform set body = '\"nonsense\"'::jsonb where name = $1")
        .bind("existing")
        .execute(&pool)
        .await
        .expect("corrupt existing body");

    // A fresh on_input_commit define runs the trigger-cycle scan over every other
    // def — the poison one must be skipped, not fatal.
    pg.define_transform(dt_def("fresh", &input, ("main", "out_fresh")))
        .await
        .expect("define fresh succeeds despite a poison existing def");
}
```

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/postgres:data-triggers`
Expected: `define_survives_poison_existing_def` FAILS — `define_transform` returns `Err(Serialization(_))` (the `?` on the poison row), so `.expect("define fresh succeeds…")` panics.

- [ ] **Step 3: Convert the scan to skip-and-warn**

In `src/control-plane/postgres/src/transforms.rs`, replace the `collect::<Result<_>>()?` block (lines 312-315):

```rust
            let mut bodies: Vec<(TransformName, TransformBody)> = existing
                .into_iter()
                .map(|r| Ok((TransformName(r.name), de_body(r.body)?)))
                .collect::<Result<_>>()?;
            bodies.push((def.name.clone(), def.body.clone()));
```

with a loop that warns and omits an undecodable existing def:

```rust
            let mut bodies: Vec<(TransformName, TransformBody)> =
                Vec::with_capacity(existing.len() + 1);
            for r in existing {
                match de_body(r.body) {
                    Ok(b) => bodies.push((TransformName(r.name), b)),
                    Err(e) => {
                        tracing::warn!(transform = %r.name, error = %e,
                            "trigger-cycle scan: undecodable body skipped");
                    }
                }
            }
            // The candidate being defined is always in-memory and decodable, so
            // the def under construction is always validated against the cycle set.
            bodies.push((def.name.clone(), def.body.clone()));
```

- [ ] **Step 4: Run the test to verify it passes (GREEN)**

Run: `buck2 test --console none //src/control-plane/postgres:data-triggers`
Expected: `Pass N. Fail 0` (including `define_survives_poison_existing_def` and the pre-existing `broken_def_body_is_skipped`).

- [ ] **Step 5: Commit**

```bash
git add src/control-plane/postgres/src/transforms.rs src/control-plane/postgres/tests/data_triggers.rs
git commit -m "fix(transform): trigger-cycle scan skips an undecodable def instead of 500ing"
```

(Commit body carries the two required trailers.)

---

### Task 2: Claim loop advance-and-warn on undecodable body

**Files:**
- Modify: `src/control-plane/postgres/src/transforms.rs:533-553` (the `claim_due_schedules` loop)
- Modify: `src/control-plane/postgres/BUCK:328-340` (the `transforms` test target — add the deps the new test needs)
- Test: `src/control-plane/postgres/tests/transforms.rs` (a new `claim_survives_poison_schedule_row` test)

**Interfaces:**
- Consumes: `de_body` (`transforms.rs:24`); `next_cron_occurrence(expr, now) -> Result<OffsetDateTime>` (already used at `transforms.rs:543`); the claim query row `r` with fields `name`, `body`, `schedule`, `on_input_commit`; `Transforms::claim_due_schedules` (`core/src/transforms.rs:414`).
- Produces: no new surface — a poison schedule row is skipped from the claim result and its `next_run_at` advances so it stops starving the batch.

- [ ] **Step 1a: Add the test target's missing deps**

The `transforms` `loom_fixture_test` target (`src/control-plane/postgres/BUCK:333-339`) does not yet depend on `core`/`time`/`sqlx` (the sibling `data-triggers` target does). Add these three to its `deps` list so the new test compiles:

```
        "//src/control-plane/core:core",
        "//third-party:sqlx",
        "//third-party:time",
```

(Insert alongside the existing `//src/testing:seed`, `:postgres`, `//src/control-plane/testkit:testkit`, `//third-party:tempfile`, `//third-party:tokio`.)

- [ ] **Step 1b: Write the failing test**

In `src/control-plane/postgres/tests/transforms.rs`, add these imports at the top (there is no existing `control_plane_core`/`time` `use` block here — add new `use` lines):

```rust
use control_plane_core::{
    OutputMode, TableRef, TransformBody, TransformDef, TransformName, Transforms,
};
use time::OffsetDateTime;
```

and add the test plus a small def builder:

```rust
fn sched_def(name: &str) -> TransformDef {
    TransformDef {
        name: TransformName(name.into()),
        body: TransformBody::Physical {
            inputs: vec![TableRef { schema: "main".into(), name: "in".into() }],
            output: TableRef { schema: "main".into(), name: format!("out_{name}") },
            sql: "select 1".into(),
            output_mode: OutputMode::Append,
        },
        schedule: Some("0 0 * * *".into()), // daily; define sets next_run_at to the next occurrence
        on_input_commit: false,
    }
}

/// One undecodable due schedule must not starve the batch: healthy schedules are
/// still claimed, and the poison row's next_run_at advances out of the due window.
#[tokio::test]
async fn claim_survives_poison_schedule_row() {
    let fx = PgFixture::shared();
    let (pg, db) = fx.fresh_db().await;
    let pool = fx.pool_for(&db).await;

    pg.define_transform(sched_def("healthy")).await.expect("define healthy");
    pg.define_transform(sched_def("poison")).await.expect("define poison");

    // Force both into the due window, then corrupt the poison body.
    sqlx::query("update transforms.transform set next_run_at = now() - interval '1 hour'")
        .execute(&pool)
        .await
        .expect("make schedules due");
    sqlx::query("update transforms.transform set body = '\"nonsense\"'::jsonb where name = $1")
        .bind("poison")
        .execute(&pool)
        .await
        .expect("corrupt poison body");

    let now = OffsetDateTime::now_utc();
    let claimed = pg
        .claim_due_schedules(now, 10)
        .await
        .expect("claim commits despite a poison row");

    let names: Vec<String> = claimed.into_iter().map(|d| d.name.0).collect();
    assert!(names.contains(&"healthy".to_string()), "healthy schedule is claimed");
    assert!(!names.contains(&"poison".to_string()), "poison schedule is skipped");

    // Regression guard: the poison row's next_run_at advanced past `now`, so a
    // second tick no longer re-surfaces it earliest and starves the batch.
    let poison_next: OffsetDateTime =
        sqlx::query_scalar("select next_run_at from transforms.transform where name = 'poison'")
            .fetch_one(&pool)
            .await
            .expect("poison next_run_at");
    assert!(poison_next > now, "poison next_run_at advanced out of the due window");
}
```

- [ ] **Step 2: Run it to verify it fails (RED)**

Run: `buck2 test --console none //src/control-plane/postgres:transforms`
Expected: `claim_survives_poison_schedule_row` FAILS — `claim_due_schedules` returns `Err(Serialization(_))` on the poison row (the `de_body(r.body)?` at `transforms.rs:536`), so `.expect("claim commits…")` panics and nothing is rescheduled.

- [ ] **Step 3: Convert the claim loop to advance-and-warn**

In `src/control-plane/postgres/src/transforms.rs`, replace the loop body (lines 533-553):

```rust
        for r in rows {
            let def = TransformDef {
                name: TransformName(r.name),
                body: de_body(r.body)?,
                schedule: r.schedule,
                on_input_commit: r.on_input_commit,
            };
            let Some(expr) = def.schedule.as_deref() else {
                continue;
            };
            let next = next_cron_occurrence(expr, now)?;
            sqlx::query!(
                "update transforms.transform set next_run_at = $2 where name = $1",
                def.name.0,
                next,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            claimed.push(def);
        }
```

with:

```rust
        for r in rows {
            let body = match de_body(r.body) {
                Ok(b) => b,
                Err(e) => {
                    tracing::warn!(transform = %r.name, error = %e,
                        "schedule claim: undecodable body skipped");
                    // Advance next_run_at anyway (from the row's own `schedule`
                    // column, no body needed) so the poison row leaves the due
                    // window and stops starving healthy schedules; it is not
                    // pushed to `claimed` because it cannot run until repaired.
                    if let Some(expr) = r.schedule.as_deref() {
                        let next = next_cron_occurrence(expr, now)?;
                        sqlx::query!(
                            "update transforms.transform set next_run_at = $2 where name = $1",
                            r.name,
                            next,
                        )
                        .execute(&mut *tx)
                        .await
                        .map_err(backend)?;
                    }
                    continue;
                }
            };
            let def = TransformDef {
                name: TransformName(r.name),
                body,
                schedule: r.schedule,
                on_input_commit: r.on_input_commit,
            };
            let Some(expr) = def.schedule.as_deref() else {
                continue;
            };
            let next = next_cron_occurrence(expr, now)?;
            sqlx::query!(
                "update transforms.transform set next_run_at = $2 where name = $1",
                def.name.0,
                next,
            )
            .execute(&mut *tx)
            .await
            .map_err(backend)?;
            claimed.push(def);
        }
```

(The poison branch's `update` reuses the identical SQL text as the healthy path — no new `.sqlx` entry. In the `Err` arm `r.schedule` is borrowed for the cron and `r.name` is moved into the query, then `continue`; the `Ok` arm moves `r.name`/`r.schedule` into `def` — the arms are mutually exclusive, so no double-move.)

- [ ] **Step 4: Run the test to verify it passes (GREEN)**

Run: `buck2 test --console none //src/control-plane/postgres:transforms`
Expected: `Pass N. Fail 0` — `claim_survives_poison_schedule_row` passes; the `transforms_contract` and other transform tests unaffected.

- [ ] **Step 5: Run the full postgres sweep**

Run: `buck2 test --console none //src/control-plane/postgres/...`
Expected: `Pass N. Fail 0`, including `sqlx-cache-check` (no SQL text changed).

- [ ] **Step 6: Commit**

```bash
git add src/control-plane/postgres/src/transforms.rs \
        src/control-plane/postgres/tests/transforms.rs \
        src/control-plane/postgres/BUCK
git commit -m "fix(transform): claim loop advances-and-warns past an undecodable schedule body"
```

(Commit body carries the two required trailers.)
