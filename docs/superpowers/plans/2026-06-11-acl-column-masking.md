# ACL Column Masking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `mask_columns` to ACL policies — a column stays in the result but every value is replaced with a constant `'***'` redaction marker, and the column is unfilterable. Distinct from `deny_columns` (which drops the column).

**Architecture:** `Policy` gains a `mask_columns: Vec<String>` field, persisted by both adapters (postgres via a new column, memory automatically). The query-api `read_object` unions masked columns and rejects filters on them; `compile_select` emits `'***' AS "col"` for masked columns. Deny wins over mask. The marker is a constant (no value is read), so masking is leak-proof and masked columns retype to text.

**Tech Stack:** Rust, buck2, sqlx compile-time queries (postgres), hermetic Postgres via `PgFixture`, shared `control-plane-testkit` contract, embedded DuckDB (query-api tests).

**Reference spec:** `docs/superpowers/specs/2026-06-11-acl-column-masking-design.md`.

**Conventions (do not deviate):**
- Tests run via `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/...`. **Do NOT pipe `buck2 test` through `tail`** — it can stall; redirect to a file and grep it: `buck2 test ... > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t.log`.
- **rustfmt is a check-only commit hook** — run `buck2 run //tools:rustfmt -- <files>` before each commit or it silently aborts; confirm the SHA changed.
- Lint: `tools/clippy-all.sh` + `buck2 run //tools:prek -- run --all-files`. Keep lint-clean; NO `--no-verify`.
- After changing any SQL (`query!`) or a migration, run `tools/sqlx-prepare.sh` and commit `.sqlx/`; `//src/control-plane/postgres:sqlx-cache-check` gates it.
- Conventional Commits; end each commit with `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Never weaken a test assertion to pass.

---

## File Structure

- `src/control-plane/core/src/acl.rs` — **Modify:** add `mask_columns` to `Policy`.
- `src/control-plane/postgres/migrations/0006_acl_policy_mask.sql` — **Create:** add the `mask_columns` column.
- `src/control-plane/postgres/src/acl.rs` — **Modify:** `set_policy` insert/upsert + `policies_for` select include `mask_columns`.
- `src/control-plane/postgres/.sqlx/` — **Regenerate.**
- `src/control-plane/testkit/src/lib.rs` — **Modify:** add `mask_columns` to existing `Policy` literals; add a mask round-trip assertion.
- `src/services/query-api/src/sql.rs` — **Modify:** `compile_select` gains `mask_cols` param + `MASK_MARKER`; emits `'***' AS "col"`.
- `src/services/query-api/tests/sql_compile.rs` — **Modify:** add `&[]` mask arg to existing calls; add masking tests.
- `src/services/query-api/src/handler.rs` — **Modify:** gather masked, reject filters on masked, pass to `compile_select`.
- `src/services/query-api/tests/governed_read.rs` — **Modify:** existing `Policy` literal gains `mask_columns`; add a masking end-to-end assertion.

---

## Task 1: Core `Policy.mask_columns` + adapter persistence + contract round-trip

Adds the field and persists it in both adapters (no enforcement yet — masking happens in Tasks 2–3). The shared contract proves storage round-trips on both adapters.

**Files:**
- Modify: `src/control-plane/core/src/acl.rs`
- Create: `src/control-plane/postgres/migrations/0006_acl_policy_mask.sql`
- Modify: `src/control-plane/postgres/src/acl.rs`, `src/control-plane/testkit/src/lib.rs`, `src/services/query-api/tests/governed_read.rs`
- Regenerate: `src/control-plane/postgres/.sqlx/`

- [ ] **Step 1: Add the field to `Policy`.**

In `src/control-plane/core/src/acl.rs`, the `Policy` struct currently ends with `pub deny_columns: Vec<String>`. Add after it:
```rust
    /// Columns shown but value-masked (redacted to a marker). Distinct from
    /// `deny_columns`, which removes the column. Order unspecified.
    pub mask_columns: Vec<String>,
```

- [ ] **Step 2: Update every `Policy { ... }` literal to compile.**

`Policy` is constructed in these spots; add `mask_columns: vec![]` to each (they keep current behavior):
- `src/control-plane/postgres/src/acl.rs` — the `out.push(Policy { ... })` in `policies_for` (around line 262): this one gets the DB value, NOT empty — handled in Step 5 (it will read `mask_columns: r.mask_columns`). For now (so the crate compiles before Step 5), you will edit it in Step 5; if you need an intermediate compile, use `mask_columns: vec![]` and replace in Step 5.
- `src/control-plane/testkit/src/lib.rs` — the literals at ~823 (`pol`), ~841 (`pol2`), ~859 (`pol_w`), ~910: add `mask_columns: vec![]` to each.
- `src/services/query-api/tests/governed_read.rs` — the `Policy { ... }` at ~153 (currently `deny_columns: vec!["secret".into()]`): add `mask_columns: vec![]`.

- [ ] **Step 3: Postgres migration — add the `mask_columns` column.**

Create `src/control-plane/postgres/migrations/0006_acl_policy_mask.sql`:
```sql
-- Column masking: a policy can list columns shown-but-redacted (distinct from
-- deny_columns, which removes the column). The query API emits a '***' marker for
-- these. Existing rows default to no masked columns (backward-compatible).
alter table acl.policy
    add column mask_columns text[] not null default '{}';
```

- [ ] **Step 4: Postgres `set_policy` — persist `mask_columns`.**

In `src/control-plane/postgres/src/acl.rs` `set_policy`, change the insert to include `mask_columns` (mirrors `deny_columns`):
```rust
        sqlx::query!(
            "insert into acl.policy \
                 (role_id, target_kind, target_a, target_b, row_filter, deny_columns, mask_columns) \
             values ($1, $2, $3, $4, $5, $6, $7) \
             on conflict (role_id, target_kind, target_a, target_b) do update set \
                 row_filter = excluded.row_filter, \
                 deny_columns = excluded.deny_columns, \
                 mask_columns = excluded.mask_columns",
            &role.0,
            kind,
            &a,
            &b,
            row_filter,
            &policy.deny_columns,
            &policy.mask_columns,
        )
        .execute(&self.pool)
        .await
        .map_err(backend)?;
```

- [ ] **Step 5: Postgres `policies_for` — read `mask_columns`.**

In the same file's `policies_for`, add `p.mask_columns` to the select and the constructed `Policy`:
```rust
        let rows = sqlx::query!(
            "select p.row_filter, p.deny_columns, p.mask_columns from acl.role_member m \
             join acl.policy p on p.role_id = m.role_id \
             where m.subject_id = $1 and p.target_kind = $2 \
               and p.target_a = $3 and p.target_b = $4",
            &subject.0,
            kind,
            &a,
            &b,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(backend)?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let row_filter = match r.row_filter {
                Some(v) => Some(
                    serde_json::from_value(v)
                        .map_err(|e| ControlPlaneError::Serialization(e.to_string()))?,
                ),
                None => None,
            };
            out.push(Policy {
                target: target.clone(),
                row_filter,
                deny_columns: r.deny_columns,
                mask_columns: r.mask_columns,
            });
        }
        Ok(Page::from_full(out))
```
(Memory adapter needs no change — it stores/returns the whole `Policy` struct, so `mask_columns` is carried automatically.)

- [ ] **Step 6: Write the failing contract round-trip assertion.**

In `src/control-plane/testkit/src/lib.rs`, in the ACL contract's policy section (near the existing `pol`/`pol2` round-trips around line 823), add a policy with a non-empty `mask_columns` and assert it round-trips through `policies_for`. Use the existing helpers (`rid`, `sid`, `ttype`, `set_policy`, `policies_for`):
```rust
    // mask_columns round-trips (stored + returned, distinct from deny_columns).
    let pol_mask = Policy {
        target: ttype("Customer"),
        row_filter: None,
        deny_columns: vec!["ssn".into()],
        mask_columns: vec!["email".into(), "phone".into()],
    };
    a.set_policy(&rid("reader"), pol_mask.clone())
        .await
        .expect("set_policy with mask_columns");
    let got_mask = a
        .policies_for(&sid("alice"), &ttype("Customer"), PageReq::unbounded())
        .await
        .expect("policies_for after mask set");
    assert_eq!(
        got_mask.items, vec![pol_mask],
        "mask_columns must round-trip alongside deny_columns",
    );
```
(`set_policy` upserts by `(role, target)`, so this replaces `reader`'s Customer policy — place it AFTER any earlier assertion that depends on the prior `reader`/Customer policy state, or adjust the role/target if the contract reuses it later. Read the surrounding assertions first; if `reader`/Customer is reused afterward, use a fresh role like `rid("masker")` (define + assign alice) and `ttype("Customer")`, or a fresh type, to avoid disturbing later checks.)

- [ ] **Step 7: Regenerate the sqlx cache.**

Run: `tools/sqlx-prepare.sh`
Expected: rewrites `.sqlx/` — `set_policy` (7-col insert) and `policies_for` (now selects `mask_columns`) entries update.

- [ ] **Step 8: Build + run the sweep.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: all PASS — `memory:acl`, `postgres:acl` (incl. the new mask round-trip), `sqlx-cache-check`, and every pre-existing target. No enforcement yet, so query reads are unchanged.

- [ ] **Step 9: Commit.**
```bash
buck2 run //tools:rustfmt -- src/control-plane/core/src/acl.rs src/control-plane/postgres/src/acl.rs src/control-plane/testkit/src/lib.rs src/services/query-api/tests/governed_read.rs
git add -A
git commit -m "feat(acl): add mask_columns to Policy (persist + round-trip)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 2: `compile_select` masking (sql.rs, pure)

Pure SQL-compilation change: a `mask_cols` parameter makes masked columns emit `'***' AS "col"`. Unit-tested with no fixture.

**Files:**
- Modify: `src/services/query-api/src/sql.rs`, `src/services/query-api/tests/sql_compile.rs`

- [ ] **Step 1: Update existing call sites + write the failing masking tests.**

In `src/services/query-api/tests/sql_compile.rs`:
- The 8 existing `compile_select(&t(), <allowed>, <filters>, <eq>, <limit>)` calls gain a `mask_cols` arg **immediately after the allowed-cols arg**. Each existing call becomes `compile_select(&t(), <allowed>, &[], <filters>, <eq>, <limit>)` (empty mask → unchanged expected SQL, so the existing asserted strings stay correct).
- Add two new tests:
```rust
#[test]
fn masks_a_column_with_marker() {
    let (sql, params) = compile_select(
        &t(),
        &["id".into(), "secret".into()],
        &["secret".into()],
        &[],
        &[],
        100,
    );
    assert_eq!(
        sql,
        r#"SELECT "id", '***' AS "secret" FROM "main"."orders" LIMIT 100"#
    );
    assert!(params.is_empty(), "the marker is a constant, not a bound param");
}

#[test]
fn masking_preserves_projection_order_and_other_columns() {
    let (sql, _params) = compile_select(
        &t(),
        &["a".into(), "b".into(), "c".into()],
        &["b".into()],
        &[],
        &[],
        10,
    );
    assert_eq!(
        sql,
        r#"SELECT "a", '***' AS "b", "c" FROM "main"."orders" LIMIT 10"#
    );
}
```

- [ ] **Step 2: Run — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:sql-compile > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[" /tmp/t.log`
Expected: FAIL to compile (`compile_select` arity mismatch).

- [ ] **Step 3: Implement masking in `compile_select`.**

In `src/services/query-api/src/sql.rs`, add the marker constant near the top (after the imports):
```rust
/// The value substituted for a masked column. A compile-time constant (never caller
/// data), so inlining it as a SQL literal is not an injection vector.
const MASK_MARKER: &str = "***";
```
Change `compile_select` to take `mask_cols: &[String]` right after `allowed_cols`, and build the projection per-column:
```rust
pub fn compile_select(
    table: &TableRef,
    allowed_cols: &[String],
    mask_cols: &[String],
    row_filters: &[RowFilter],
    eq_filters: &[(String, SqlValue)],
    limit: u32,
) -> (String, Vec<SqlValue>) {
    let mut params = Vec::new();
    let cols = allowed_cols
        .iter()
        .map(|c| {
            if mask_cols.iter().any(|m| m == c) {
                // Masked: emit the constant marker, never the column's value.
                format!("'{MASK_MARKER}' AS {}", quote_ident(c))
            } else {
                quote_ident(c)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let from = format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    );

    let mut conjuncts: Vec<String> = Vec::new();
    for f in row_filters {
        conjuncts.push(filter_sql(f, &mut params));
    }
    for (col, val) in eq_filters {
        conjuncts.push(format!("({} = ?)", quote_ident(col)));
        params.push(val.clone());
    }

    let mut sql = format!("SELECT {cols} FROM {from}");
    if !conjuncts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conjuncts.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    (sql, params)
}
```
(`MASK_MARKER` is `***`, which contains no single-quote, so `'{MASK_MARKER}'` is a well-formed safe literal. Do not bind it as a param — it's a constant, and binding would make the test expect a `?`.)

- [ ] **Step 4: Run — verify pass.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:sql-compile > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS (all prior tests + the 2 new masking tests).

- [ ] **Step 5: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/sql.rs src/services/query-api/tests/sql_compile.rs
git add -A
git commit -m "feat(query-api): compile_select emits '***' for masked columns

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 3: `read_object` enforcement + end-to-end masking

Wire masking through the handler: gather masked columns, reject filters on them, pass them to `compile_select`. Prove it end-to-end.

**Files:**
- Modify: `src/services/query-api/src/handler.rs`, `src/services/query-api/tests/governed_read.rs`

- [ ] **Step 1: Write the failing end-to-end assertion.**

In `src/services/query-api/tests/governed_read.rs`, after the existing assertions in `governed_object_read`, add a masking case. It changes the policy to MASK `secret` (instead of deny) via a fresh role so it doesn't disturb the earlier deny/deny-override assertions, then reads as a subject who only has that masking policy. Simplest: define a new subject `masker` in a new role `mask_role` with an Allow grant + a policy that masks `secret`, and read:
```rust
    // Masking: a policy that MASKS `secret` (vs denying it) -> the column is present
    // but every value is the marker, and it cannot be filtered on.
    let masker = SubjectId("masker".into());
    let mrole = RoleId("mask_role".into());
    cp.define_subject(&masker).await.unwrap();
    cp.define_role(&mrole).await.unwrap();
    cp.assign_role(&masker, &mrole).await.unwrap();
    cp.grant(
        &mrole,
        Action::Read,
        PolicyTarget::Type(TypeName("Order".into())),
        Effect::Allow,
    )
    .await
    .unwrap();
    cp.set_policy(
        &mrole,
        Policy {
            target: PolicyTarget::Type(TypeName("Order".into())),
            row_filter: None,
            deny_columns: vec![],
            mask_columns: vec!["secret".into()],
        },
    )
    .await
    .unwrap();
    let masked_rows = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![],
        },
        &Subject(masker.clone()),
        &deps,
    )
    .await
    .unwrap();
    // secret is present in the projection but every value is the marker, not s1/s2/s3.
    let secret_idx = masked_rows
        .columns
        .iter()
        .position(|c| c == "secret")
        .expect("secret column present (masked, not dropped)");
    assert!(
        masked_rows
            .rows
            .iter()
            .all(|r| r[secret_idx] == SqlValue::Text("***".into())),
        "every secret value is the redaction marker",
    );
    assert!(
        masked_rows.rows.iter().all(|r| r[secret_idx] != SqlValue::Text("s1".into())),
        "no real secret value leaks",
    );
    // Filtering on a masked column is rejected.
    let bad = read_object(
        &ObjectQuery {
            type_name: "Order".into(),
            eq_filters: vec![("secret".into(), SqlValue::Text("s1".into()))],
        },
        &Subject(masker),
        &deps,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(bad, QueryError::BadFilter(ref c) if c == "secret"),
        "a filter on a masked column is rejected, got {bad:?}",
    );
```
Ensure `Policy` is imported in the test's `use control_plane_core::{...}` (add it if absent). `masker` has no row policy so all 3 rows return; `secret` is masked.

- [ ] **Step 2: Run — verify it fails.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:governed-read > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL|error\[|panicked" /tmp/t.log`
Expected: FAIL — `read_object` doesn't yet gather/pass masked columns, so `secret` returns real values (and the filter isn't rejected / the `compile_select` call has the wrong arity).

- [ ] **Step 3: Implement masking in `read_object`.**

In `src/services/query-api/src/handler.rs`, update the policy-gather + projection + filter-check + `compile_select` call. Replace the relevant region with:
```rust
    let mut row_filters: Vec<RowFilter> = Vec::new();
    let mut denied: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut masked: std::collections::HashSet<String> = std::collections::HashSet::new();
    for p in policies.items {
        if let Some(f) = p.row_filter {
            row_filters.push(f);
        }
        denied.extend(p.deny_columns);
        masked.extend(p.mask_columns);
    }

    // projection: type properties minus denied columns, preserving property order.
    let allowed: Vec<String> = object_type
        .properties
        .iter()
        .map(|p| p.name.clone())
        .filter(|n| !denied.contains(n))
        .collect();
    if allowed.is_empty() {
        return Err(QueryError::Forbidden);
    }
    // masked columns to actually apply: those still visible (deny wins over mask).
    let mask_cols: Vec<String> = allowed
        .iter()
        .filter(|c| masked.contains(*c))
        .cloned()
        .collect();

    // request equality filters must target a visible, non-masked column.
    for (col, _) in &q.eq_filters {
        if !allowed.contains(col) || masked.contains(col) {
            return Err(QueryError::BadFilter(col.clone()));
        }
    }

    let (sql, params) = compile_select(
        &object_type.table,
        &allowed,
        &mask_cols,
        &row_filters,
        &q.eq_filters,
        DEFAULT_LIMIT,
    );
    Ok(deps.serving.fetch_rows(&sql, &params).await?)
```
(The only changes vs. today: the `masked` set, the `mask_cols` intersection, the `|| masked.contains(col)` in the filter check, and the extra `&mask_cols` arg to `compile_select`. `read_object` already imports what it needs; no new imports.)

- [ ] **Step 4: Run — verify pass.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/services/query-api:governed-read > /tmp/t.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/t.log`
Expected: PASS — masked `secret` returns `***`, real value never leaks, filter on `secret` is `BadFilter`, and all earlier governed-read assertions (deny, deny-override, deny-by-default) still hold.

- [ ] **Step 5: Commit.**
```bash
buck2 run //tools:rustfmt -- src/services/query-api/src/handler.rs src/services/query-api/tests/governed_read.rs
git add -A
git commit -m "feat(query-api): apply column masking in read_object (mask + unfilterable)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Task 4: Full sweep, lint, final review, finish branch

**Files:** none (verification + finish).

- [ ] **Step 1: Full sweep.**

Run: `env -u BUCK_PREFER_REMOTE buck2 test --local-only //src/... > /tmp/sweep.log 2>&1; echo "exit: $?"; grep -E "Tests finished|FAIL" /tmp/sweep.log`
Expected: all PASS (acl contract with mask round-trip, sql-compile masking tests, governed-read masking e2e, sqlx-cache-check, and every pre-existing target).

- [ ] **Step 2: Lint.**

Run:
```bash
tools/clippy-all.sh > /tmp/clippy.log 2>&1; echo "clippy warnings: $(grep -cE 'warning|error' /tmp/clippy.log)"
buck2 run //tools:prek -- run --all-files
```
Expected: clippy 0 warnings; prek all PASS (rustfmt, clippy, reindeer-in-sync, file checks).

- [ ] **Step 3: Confirm against the spec.** Spot-check: `mask_columns` on `Policy`, persisted + round-tripped both adapters (Task 1); `compile_select` emits `'***' AS "col"` for masked, marker not bound, empty mask unchanged (Task 2); `read_object` unions masked, deny-wins via `masked ∩ allowed`, rejects filters on masked, passes `mask_cols` (Task 3); end-to-end masked value + filter rejection. Out-of-scope (NULL/hash, configurable marker, link masking) not implemented — correct.

- [ ] **Step 4: Finish the branch.** Use superpowers:finishing-a-development-branch (verify tests pass → present options). Branch: `feat/acl-column-masking` (already carries the spec commit).

---

## Self-review notes

- **Spec coverage:** `mask_columns` field (Task 1) · postgres column + set_policy/policies_for + memory automatic + contract round-trip (Task 1) · `compile_select` `'***' AS "col"`, constant (not bound), empty-mask-unchanged (Task 2) · `read_object` union + deny-wins intersection + unfilterable + pass-through (Task 3) · end-to-end masked-value + filter-rejection (Task 3). Non-goals (NULL/hash, configurable/per-column marker, link/FK masking, role hierarchy) untasked — correct per spec.
- **Type consistency:** `Policy { target, row_filter, deny_columns, mask_columns }` used identically across core/adapters/tests; `compile_select(&TableRef, allowed: &[String], mask_cols: &[String], row_filters, eq_filters, limit)` is the new 6-arg signature, applied at all 9 call sites (8 sql-compile + 1 handler); `MASK_MARKER = "***"` matches the `'***'` asserted in tests and the `***` matched in the e2e (`SqlValue::Text("***")`).
- **Migration ordering:** `0006_acl_policy_mask.sql` is next (existing 0001–0005); `acl.policy` rows default `mask_columns` to `'{}'`, mirroring `deny_columns` — no backfill.
- **sqlx regen:** Task 1 changes `set_policy` (7-col) and `policies_for` (selects `mask_columns`) — run `tools/sqlx-prepare.sh` and commit `.sqlx/` in Task 1, or `sqlx-cache-check` fails. Tasks 2–3 touch no SQL macros (query-api uses runtime DuckDB), so no further regen.
- **Contract placement caution:** the mask round-trip in Task 1 Step 6 upserts `reader`/Customer — if a later contract assertion depends on the prior `reader`/Customer policy, use a fresh role/type instead (noted in-step).
