use control_plane_postgres::fixture::PgFixture;
use tracing_test::traced_test;

// NOTE: the test fn name becomes the tracing-test span name, and `logs_contain`
// filters on " {span_name}:" substring-matching lines before searching each for
// the assertion value. Naming this test `fresh_db_emits_tracing_event` would make
// `logs_contain("fresh_db")` trivially true for ANY captured line (sqlx's own
// ambient query logging included), since "fresh_db" is a substring of the span
// name itself — defeating the RED step. Kept substring-free of "fresh_db" so the
// assertion is only satisfied by our own instrumentation firing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[traced_test]
async fn db_creation_emits_tracing_event() {
    let fx = PgFixture::shared();
    let (_cp, db) = fx.fresh_db().await;
    assert!(
        logs_contain("fresh_db"),
        "fresh_db should emit an instrumented tracing event"
    );
    assert!(logs_contain(&db), "the event carries the created db name");
}
