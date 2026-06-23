# Existence validation for loom-owned ontology references Implementation Plan

> **Spec:** `docs/superpowers/specs/2026-06-21-existence-validation-design.md`
> Closes the loom-owned portion of `iss-existence-validation`.

**Goal:** Reject, at the control-plane write, a reference to a non-existent
ontology type on three loom-owned paths — `Acl::grant` (Type target),
`Acl::set_policy` (Type target, **always**, not only when a `row_filter` is
present), and `Ontology::define_action` (`target` type) — **consistently on both
the Postgres adapter and the in-memory fake**, returning
`ControlPlaneError::Validation`, with a both-adapter contract test.

**Architecture:** An explicit in-Rust existence check in each adapter. No new
core seam, trait change, or error variant (`ControlPlaneError::Validation`
already exists and is `#[non_exhaustive]`). Postgres reuses the existing
`select exists (select 1 from ontology.object_type where name = $1)` query
string (already in `.sqlx` — **no cache regeneration**); the in-memory fake reads
its ontology map. The `define_action` Postgres FK (`0009_actions.sql`) stays as
the atomic backstop, with the new explicit check running inside its existing
transaction.

**Tech Stack:** Rust 2024, buck2, async-trait, sqlx (compile-time, offline),
Postgres control plane.

## Global Constraints

- **Tests are `rust_test`/`loom_fixture_test` integration targets only** — NO
  inline `#[cfg(test)]` modules (the `no-inline-tests` prek hook fails the build
  otherwise). The new contract is a function in the testkit, run by a sibling
  `tests/existence_validation.rs` in each adapter, wired as its own BUCK target.
- **Fixture-backed (real Postgres) tests** MUST use the `loom_fixture_test`
  macro (postgres adapter), not a bare `rust_test`. The memory adapter test is a
  pure `rust_test`.
- **Run the suite with** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E
  "Tests finished|FAIL" /tmp/t.log` — never pipe `buck2 test` through
  `tail`/`head` (it stalls). **The full sweep matters:** this changes shared ACL
  write behavior used across every query-api e2e test, so per-crate green is not
  enough.
- **No `.sqlx` change:** all three Postgres checks reuse the already-cached query
  string verbatim. Do not introduce a new SQL string (it would require running
  `tools/sqlx-prepare.sh`).
- **Markdown lint:** any `.md` file must end with exactly one trailing newline
  and have no trailing whitespace.
- **Error messages don't matter for tests** (contract tests assert on the
  `Validation` *variant*, never message text — see `core/src/error.rs:2`). Use
  the spec's listed messages for the two new paths; leave `set_policy`'s existing
  message verbatim ("(unchanged)" per the spec).
- **Lock ordering (memory):** never hold the `acl` lock while taking the
  `ontology` lock other than in the established acl→ontology order. Mirror
  `set_policy`'s pattern (short acl lock for the role check, release, then read
  the ontology map).

---

### Task 1: Postgres adapter — existence checks

**Files:**
- Modify: `src/control-plane/postgres/src/acl.rs` (`grant`, `set_policy`).
- Modify: `src/control-plane/postgres/src/ontology.rs` (`define_action`).

**`grant`** (`acl.rs:168`): after the existing role-exists check, before the
insert, add — when `target` is `PolicyTarget::Type(name)` — a type-exists guard:

```rust
if let PolicyTarget::Type(name) = &target {
    let type_exists = sqlx::query_scalar!(
        "select exists (select 1 from ontology.object_type where name = $1)",
        &name.0,
    )
    .fetch_one(&self.pool)
    .await
    .map_err(backend)?
    .unwrap_or(false);
    if !type_exists {
        return Err(ControlPlaneError::Validation(format!(
            "grant references unknown type `{}`",
            name.0
        )));
    }
}
```

(`PolicyTarget::Table(_)` targets are left unvalidated — deferred boundary.)

**`set_policy`** (`acl.rs:224`): restructure the validation block so the
Type-target existence check runs **always** (not only when `row_filter` is
`Some`), while the row-filter column validation stays gated on `row_filter`. Keep
the "best-effort / non-transactional" comment. Keep the existing message
`"policy references unknown type {}"` verbatim:

```rust
// Best-effort, non-transactional validation (separate round-trips from the
// insert below, like the role-exists check above): a concurrent type deletion
// between this check and the insert is tolerated. Fine for current usage.
match &policy.target {
    PolicyTarget::Type(name) => {
        let type_exists = sqlx::query_scalar!(
            "select exists (select 1 from ontology.object_type where name = $1)",
            &name.0,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?
        .unwrap_or(false);
        if !type_exists {
            return Err(ControlPlaneError::Validation(format!(
                "policy references unknown type {}",
                name.0
            )));
        }
        if let Some(f) = &policy.row_filter {
            let names = sqlx::query_scalar!(
                "select name from ontology.property where type_name = $1",
                &name.0,
            )
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
            let set: std::collections::HashSet<String> = names.into_iter().collect();
            validate_row_filter(f, Some(&set)).map_err(ControlPlaneError::Validation)?;
        }
    }
    PolicyTarget::Table(_) => {
        if let Some(f) = &policy.row_filter {
            validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
        }
    }
}
```

**`define_action`** (`ontology.rs:287`): inside the existing transaction, before
the `insert into ontology.action`, add an explicit target-exists guard (the FK
stays as the atomic backstop):

```rust
let mut tx = self.pool.begin().await.map_err(backend)?;
let target_exists = sqlx::query_scalar!(
    "select exists (select 1 from ontology.object_type where name = $1)",
    action.target.0,
)
.fetch_one(&mut *tx)
.await
.map_err(backend)?
.unwrap_or(false);
if !target_exists {
    return Err(ControlPlaneError::Validation(format!(
        "action `{}` references unknown target type `{}`",
        action.name.0, action.target.0
    )));
}
// ... existing insert into ontology.action + params, tx.commit()
```

(Returning early drops `tx`, rolling back — no rows written.)

- [ ] **Step 1:** Apply the three edits above. Verify the SQL string is byte-identical to the cached one (so `.sqlx` is untouched). `buck2 build //src/control-plane/postgres:postgres` to confirm the offline macros resolve from cache.

---

### Task 2: Memory adapter — existence checks

**Files:**
- Modify: `src/control-plane/memory/src/acl.rs` (`grant`, `set_policy`, add a
  `type_exists` helper next to `type_properties`).
- Modify: `src/control-plane/memory/src/ontology.rs` (`define_action`).

**`type_exists` helper** (next to `type_properties`, `acl.rs:69`):

```rust
/// True if an ontology type with this name is defined.
fn type_exists(&self, name: &str) -> bool {
    self.ontology.lock().unwrap().types.contains_key(name)
}
```

**`grant`** (`acl.rs:147`): split the single acl-lock body so the role check runs
under a short acl lock (released), then the type-exists check reads the ontology
map (acl→ontology order, no acl lock held), then the insert takes a short acl
lock:

```rust
{
    let acl = self.acl.lock().unwrap();
    if !acl.roles.contains(&role.0) {
        return Err(ControlPlaneError::NotFound(format!("role {}", role.0)));
    }
}
if let PolicyTarget::Type(name) = &target {
    if !self.type_exists(&name.0) {
        return Err(ControlPlaneError::Validation(format!(
            "grant references unknown type `{}`",
            name.0
        )));
    }
}
self.acl
    .lock()
    .unwrap()
    .grants
    .insert((role.0.clone(), action, target_key(&target)), effect);
Ok(())
```

**`set_policy`** (`acl.rs:174`): make the Type-target existence check
unconditional (independent of `row_filter`), reusing `type_properties` for the
single lookup:

```rust
// validation (may lock ontology) — no acl lock held here, to avoid a
// lock-ordering deadlock between the acl and ontology mutexes.
match &policy.target {
    PolicyTarget::Type(name) => {
        let props = self.type_properties(&name.0).ok_or_else(|| {
            ControlPlaneError::Validation(format!(
                "policy references unknown type {}",
                name.0
            ))
        })?;
        if let Some(f) = &policy.row_filter {
            let set: HashSet<String> = props.into_iter().collect();
            validate_row_filter(f, Some(&set)).map_err(ControlPlaneError::Validation)?;
        }
    }
    PolicyTarget::Table(_) => {
        if let Some(f) = &policy.row_filter {
            validate_row_filter(f, None).map_err(ControlPlaneError::Validation)?;
        }
    }
}
```

**`define_action`** (`ontology.rs:98`): check the target exists within the
ontology lock before inserting:

```rust
async fn define_action(&self, action: ActionDef) -> Result<()> {
    let mut ont = self.ontology.lock().unwrap();
    if !ont.types.contains_key(&action.target.0) {
        return Err(ControlPlaneError::Validation(format!(
            "action `{}` references unknown target type `{}`",
            action.name.0, action.target.0
        )));
    }
    ont.actions.insert(action.name.0.clone(), action);
    Ok(())
}
```

- [ ] **Step 1:** Apply the edits. `buck2 build //src/control-plane/memory:memory`.

---

### Task 3: testkit — new contract + repair `acl_contract`

Adding existence validation to `grant`/`set_policy` breaks the existing
`acl_contract`, which grants/policies several **undefined** Type targets
(`Customer` is granted at line 916 before its define at 1033; `Invoice`,
`Ticket`, `Widget`, `Gadget` are policied/granted with `row_filter: None` and
never defined). Both must be repaired in the same change.

**Files:**
- Modify: `src/control-plane/testkit/src/lib.rs`.

**Step 1 — add a `define_min_type` helper** (module scope, near `job()` at
line 17), reused by both the repaired `acl_contract` and the new contract:

```rust
/// Define a minimal ontology type (all-`String`, optional properties) so a
/// contract can reference it as a Type target. Table name is the lowercased type.
async fn define_min_type<O: Ontology>(o: &O, name: &str, props: &[&str]) {
    o.define_type(ObjectType {
        name: TypeName(name.into()),
        properties: props
            .iter()
            .map(|n| PropertyDef {
                name: (*n).into(),
                ty: "String".into(),
                required: false,
            })
            .collect(),
        derived: vec![],
        table: TableRef {
            schema: "main".into(),
            name: name.to_lowercase(),
        },
        identity: None,
    })
    .await
    .expect("define type");
}
```

**Step 2 — repair `acl_contract`** (`lib.rs:901`): immediately after the
closure definitions (before the first `grant` at line 916), define every Type
target the contract touches; then remove the now-redundant inline `Customer`
`define_type` block (lines 1032–1051, plus its preceding comment). `Customer`
keeps the five properties its row-filter assertions reference; the others only
need to exist:

```rust
// grant/set_policy now reject Type targets that don't exist, so every Type
// this contract references must be defined first. Customer carries the
// properties the row-filter assertions use; the rest only need to exist.
define_min_type(a, "Customer", &["tenant", "is_public", "owner", "region", "active"]).await;
for t in ["Invoice", "Ticket", "Widget", "Gadget"] {
    define_min_type(a, t, &[]).await;
}
```

(Types only used by `check`/`revoke`/`clear_policy`/`policies_for` — e.g. `Order`,
`Nothing` — and the role-`NotFound`-first cases `X`/`NoSuchType` need **no**
definition, since those paths don't run the new existence check or fail earlier.)

**Step 3 — add `existence_validation_contract`** (new `pub async fn`, after
`acl_contract` ends at line 1586):

```rust
/// Both-adapter contract for type-existence validation on the three loom-owned
/// write paths: `grant`, `set_policy`, and `define_action`. `cp` must be empty.
pub async fn existence_validation_contract<CP: Acl + Ontology>(cp: &CP) {
    let rid = |s: &str| RoleId(s.to_string());
    let tn = |s: &str| TypeName(s.to_string());
    let ttype = |s: &str| PolicyTarget::Type(TypeName(s.to_string()));
    let policy = |target: PolicyTarget, row_filter: Option<RowFilter>| Policy {
        target,
        row_filter,
        deny_columns: vec![],
        mask_columns: vec![],
    };

    define_min_type(cp, "T", &["col"]).await;
    cp.define_role(&rid("R")).await.unwrap();

    // grant: existing Type ok; unknown Type -> Validation.
    cp.grant(&rid("R"), Action::Read, ttype("T"), Effect::Allow)
        .await
        .expect("grant on existing type");
    assert!(matches!(
        cp.grant(&rid("R"), Action::Read, ttype("Nope"), Effect::Allow)
            .await,
        Err(ControlPlaneError::Validation(_))
    ));

    // set_policy: existing Type ok with and without a row_filter; unknown -> Validation.
    cp.set_policy(&rid("R"), Action::Read, policy(ttype("T"), None))
        .await
        .expect("set_policy without row_filter");
    cp.set_policy(
        &rid("R"),
        Action::Read,
        policy(
            ttype("T"),
            Some(RowFilter::Compare {
                property: "col".into(),
                op: CompareOp::Eq,
                value: ScalarValue::Text("x".into()),
            }),
        ),
    )
    .await
    .expect("set_policy with row_filter");
    assert!(matches!(
        cp.set_policy(&rid("R"), Action::Read, policy(ttype("Nope"), None))
            .await,
        Err(ControlPlaneError::Validation(_))
    ));

    // define_action: existing target ok; unknown target -> Validation.
    cp.define_action(ActionDef {
        name: ActionName("act".into()),
        target: tn("T"),
        parameters: vec![],
    })
    .await
    .expect("define_action on existing target");
    assert!(matches!(
        cp.define_action(ActionDef {
            name: ActionName("actBad".into()),
            target: tn("Nope"),
            parameters: vec![],
        })
        .await,
        Err(ControlPlaneError::Validation(_))
    ));

    // Deferred boundary: Table targets are NOT existence-checked.
    cp.grant(
        &rid("R"),
        Action::Read,
        PolicyTarget::Table(TableRef {
            schema: "main".into(),
            name: "raw".into(),
        }),
        Effect::Allow,
    )
    .await
    .expect("Table targets are not existence-checked");
}
```

- [ ] **Step 4:** `buck2 build //src/control-plane/testkit:testkit`, then run the existing memory + postgres `acl` contract tests to confirm the repair: `buck2 test //src/control-plane/memory:acl //src/control-plane/postgres:acl > /tmp/acl.log 2>&1; grep -E "Tests finished|FAIL" /tmp/acl.log`.

---

### Task 4: wire the new contract into both adapter suites

**Files:**
- Create: `src/control-plane/memory/tests/existence_validation.rs`
- Create: `src/control-plane/postgres/tests/existence_validation.rs`
- Modify: `src/control-plane/memory/BUCK` (add `rust_test` `existence-validation`).
- Modify: `src/control-plane/postgres/BUCK` (add `loom_fixture_test`
  `existence-validation`).

Memory test:

```rust
#[tokio::test]
async fn memory_passes_existence_validation_contract() {
    let cp = control_plane_memory::MemoryControlPlane::new(std::time::Duration::from_millis(300));
    control_plane_testkit::existence_validation_contract(&cp).await;
}
```

Postgres test:

```rust
use control_plane_postgres::fixture::PgFixture;

#[tokio::test]
async fn postgres_passes_existence_validation_contract() {
    let fixture = PgFixture::start();
    let cp = fixture.fresh_control_plane().await;
    control_plane_testkit::existence_validation_contract(&cp).await;
}
```

BUCK targets mirror the `acl` targets exactly (memory `rust_test` with
`edition = "2024"` + the same three deps; postgres `loom_fixture_test` with the
same deps).

- [ ] **Step 1:** Add files + targets. `buck2 test //src/control-plane/memory:existence-validation //src/control-plane/postgres:existence-validation > /tmp/ev.log 2>&1; grep -E "Tests finished|FAIL" /tmp/ev.log`.

---

### Task 5: full-suite regression sweep

Because `grant`/`set_policy` are exercised by every query-api e2e test (via
`e2e_support::grant_read`) and the action tests call `define_action`, run the
**whole** suite and fix any test that grants/policies/actions an undefined type
(define it first). The standard `e2e_support::setup` defines its types before
granting, so breakage (if any) is expected only in tests with bespoke topologies.

- [ ] **Step 1:** `buck2 test //src/... > /tmp/t.log 2>&1; grep -E "Tests finished|FAIL|BUILD FAILED" /tmp/t.log`. Triage and fix any failure; re-run until green.

---

### Task 6: close the register item + mint follow-ons

**Files:**
- Modify: `docs/ISSUES.md` (close `iss-existence-validation`, scoped).
- Modify: `docs/FUTURE.md` (mint the two out-of-scope follow-ons).

Close `iss-existence-validation`: `- [ ]`→`- [x]`, `status:open`→`status:fixed`,
`pr:-`→`pr:#N` (the PR number, filled after the PR is opened), and rewrite the
prose to a "Fixed (scoped):" note describing what landed (the three loom-owned
type-reference paths, both adapters, the contract test) and that catalog-table /
lineage-`DatasetRef` / `Table`-target validation are carved out to the follow-ons.

Mint in `docs/FUTURE.md` (per the spec's "Out of scope → follow-on items"):

- `## catalog`: `fut-catalog-reference-validation` (`area:catalog
  status:deferred from:2026-06-21-existence-validation-design pr:- spec:-`) —
  validate `define_type`'s backing `TableRef` and ACL `Table` targets against the
  DuckLake catalog; needs a `Catalog::exists(&TableRef)` read seam.
- `## lineage`: `fut-lineage-datasetref-validation` (`area:lineage
  status:deferred from:2026-06-21-existence-validation-design pr:- spec:-`) —
  validate lineage `DatasetRef`s; needs an internal-vs-external namespace
  convention first.

Cross-link from the closed issue with `[[fut-catalog-reference-validation]]` and
`[[fut-lineage-datasetref-validation]]`. (`fut-define-link-validation` already
tracks the `define_link` backing-column case — leave it unchanged.)

- [ ] **Step 1:** Edit registers; `bash tools/docs.sh validate` must pass.

---

## Done when

- All three write paths reject unknown Type targets with `Validation` on **both**
  adapters; `Table` targets stay unvalidated.
- `existence_validation_contract` passes on memory and postgres.
- `buck2 test //src/...` is fully green; `buck2 run //tools:prek -- run
  --all-files` is clean.
- `.sqlx` is unchanged.
- `iss-existence-validation` is closed (scoped) and the two follow-ons are minted;
  `bash tools/docs.sh validate` passes.
