//! The `0049_mv_output_admin_grant` backfill: micro-batch transform outputs defined
//! before define-time granting existed get the reserved admin role a Read grant on
//! their bare output table. The fixture applies every migration to an empty database,
//! so the backfill has already matched nothing by the time a test can seed rows —
//! these tests therefore re-execute the migration's OWN embedded SQL against seeded
//! state, which keeps the assertions pinned to the shipped statement.

use control_plane_postgres::fixture::PgFixture;
use sqlx::PgPool;

/// The backfill statement, read out of the compile-time migration embed.
///
/// Returns `sqlx::SqlStr` (the type `Migration.sql` already is) because that is one of
/// the three types `sqlx::raw_sql`'s `SqlSafeStr` bound accepts — a `String`/`&str`
/// would not compile, and `Migration.sql` has no `Display` so `.to_string()` is not
/// available either.
fn backfill_sql() -> sqlx::SqlStr {
    let migrator = control_plane_postgres::embedded_migrator();
    let mut matches = migrator
        .iter()
        .filter(|m| m.description.contains("mv output admin grant"));
    let m = matches
        .next()
        .expect("the mv-output-admin-grant migration must be embedded");
    assert!(
        matches.next().is_none(),
        "more than one migration matched 'mv output admin grant'"
    );
    m.sql.clone()
}

/// Seed a transform definition row directly, bypassing the concern API (the point is
/// to simulate rows written by a build that predates define-time granting).
async fn seed_def(pool: &PgPool, name: &str, body: serde_json::Value) {
    sqlx::query("insert into transforms.transform (name, body) values ($1, $2)")
        .bind(name)
        .bind(body)
        .execute(pool)
        .await
        .expect("seed transform def");
}

async fn define_role(pool: &PgPool, id: &str) {
    sqlx::query("insert into acl.role (id) values ($1)")
        .bind(id)
        .execute(pool)
        .await
        .expect("define role");
}

/// A whole `acl.role_grant` row: (role_id, action, target_kind, effect, target_a, target_b).
type Grant = (String, String, String, String, String, String);

/// EVERY grant row in the database, sorted — deliberately unfiltered.
///
/// Filtering by `role_id = 'admin'` would hide a fan-out regression: if the migration's
/// `join acl.role r on r.id = 'admin'` degraded to `on true`, every role in the
/// deployment would receive the grant and a role-scoped assertion would still pass.
/// `effect` is included for the same reason — a grant inserted as `'deny'` is strictly
/// worse than no grant at all, because `check()` short-circuits
/// `bool_or(g.effect = 'deny')` straight to `Decision::Deny` (`src/acl.rs`), so the
/// column that decides allow-vs-deny is the last one a statement-pinning test should omit.
async fn all_grants(pool: &PgPool) -> Vec<Grant> {
    sqlx::query_as::<_, Grant>(
        "select role_id, action, target_kind, effect, target_a, target_b \
         from acl.role_grant \
         order by role_id, action, target_kind, effect, target_a, target_b",
    )
    .fetch_all(pool)
    .await
    .expect("read grants")
}

/// The exact row the runtime writes at define time: the reserved admin role holding a
/// coarse *allow* Read on a bare output table.
fn admin_read_allow(schema: &str, table: &str) -> Grant {
    (
        "admin".to_string(),
        "read".to_string(),
        "table".to_string(),
        "allow".to_string(),
        schema.to_string(),
        table.to_string(),
    )
}

fn microbatch(output_schema: &str, output_name: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": "microbatch",
        "source": {"schema": "main", "name": "events"},
        "output": {"schema": output_schema, "name": output_name},
        "buckets": 4,
        "sql": "select 1"
    })
}

fn microbatch_join(output_schema: &str, output_name: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": "microbatch_join",
        "source": {"schema": "main", "name": "events"},
        "enrich": {"schema": "main", "name": "users"},
        "output": {"schema": output_schema, "name": output_name},
        "buckets": 2,
        "sql": "select 1"
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfills_both_micro_batch_variants_and_is_idempotent() {
    let fixture = PgFixture::shared();
    let (_cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;
    define_role(&pool, "admin").await;
    // Fan-out canary: a second, ordinary role. The backfill must not grant it anything —
    // asserting on the unfiltered grant table is what makes a widened join visible.
    define_role(&pool, "analyst").await;

    // Distinct output schemas ('main' vs 'warehouse') so a statement that emitted a
    // literal schema instead of reading `body -> 'output' ->> 'schema'` would be caught.
    seed_def(&pool, "rollup_mv", microbatch("main", "rollup")).await;
    seed_def(
        &pool,
        "enriched_mv",
        microbatch_join("warehouse", "enriched"),
    )
    .await;
    assert_eq!(all_grants(&pool).await, vec![]);

    sqlx::raw_sql(backfill_sql())
        .execute(&pool)
        .await
        .expect("run backfill");
    assert_eq!(
        all_grants(&pool).await,
        vec![
            admin_read_allow("main", "rollup"),
            admin_read_allow("warehouse", "enriched"),
        ]
    );

    // Re-running is a clean no-op: the PK is the grant tuple MINUS `effect`
    // (role_id, action, target_kind, target_a, target_b), so `on conflict do nothing`
    // also means an operator's existing `deny` on the output survives the backfill.
    sqlx::raw_sql(backfill_sql())
        .execute(&pool)
        .await
        .expect("re-run backfill");
    assert_eq!(
        all_grants(&pool).await,
        vec![
            admin_read_allow("main", "rollup"),
            admin_read_allow("warehouse", "enriched"),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leaves_physical_and_typed_outputs_alone() {
    let fixture = PgFixture::shared();
    let (_cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;
    define_role(&pool, "admin").await;

    seed_def(
        &pool,
        "daily",
        serde_json::json!({
            "kind": "physical",
            "inputs": [{"schema": "main", "name": "src"}],
            "output": {"schema": "main", "name": "dst"},
            "sql": "select * from src"
        }),
    )
    .await;
    seed_def(
        &pool,
        "typed_daily",
        serde_json::json!({
            "kind": "typed",
            "inputs": ["Src"],
            "output": "Dst",
            "sql": "select 1"
        }),
    )
    .await;

    sqlx::raw_sql(backfill_sql())
        .execute(&pool)
        .await
        .expect("run backfill");
    // `physical` is granted at define time and needs no backfill; `typed` rides its
    // bound type's grants and must never get a table grant. Neither is touched here.
    assert_eq!(all_grants(&pool).await, vec![]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn is_a_clean_no_op_when_no_admin_role_has_been_bootstrapped() {
    let fixture = PgFixture::shared();
    let (_cp, db) = fixture.fresh_db().await;
    let pool = fixture.pool_for(&db).await;
    // Deliberately no 'admin' role: `loom create-admin` has not run.
    seed_def(&pool, "rollup_mv", microbatch("main", "rollup")).await;

    sqlx::raw_sql(backfill_sql())
        .execute(&pool)
        .await
        .expect("backfill must succeed, not violate the role FK, with no admin role");
    assert_eq!(all_grants(&pool).await, vec![]);
}
