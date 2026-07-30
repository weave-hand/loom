//! `#[tracing::instrument]` records every non-skipped argument as a span field.
//! `Auth::update_password` takes the raw Argon2 verifier, so it MUST be skipped —
//! otherwise every password change writes the verifier to the log at debug level.
//!
//! This asserts on span *creation* (`FmtSpan::NEW`), not on events: the method
//! emits no event of its own, so an event-only capture (`tracing_test`) would
//! pass vacuously whether or not the argument is skipped.
use std::sync::{Arc, Mutex};

use control_plane_core::{Auth, NewUser, Redacted, SubjectId};
use control_plane_memory::MemoryControlPlane;
use tracing_subscriber::fmt::format::FmtSpan;

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn update_password_does_not_log_the_verifier() {
    let sink = Arc::new(Mutex::new(Vec::new()));
    let writer = Buffer(Arc::clone(&sink));
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::NEW)
        .with_writer(move || writer.clone())
        .finish();

    // `MemoryControlPlane::new` takes the lock timeout (src/lib.rs:92) — it is
    // irrelevant here, so pass any value.
    let cp = MemoryControlPlane::new(std::time::Duration::from_secs(5));
    cp.create_user(&NewUser {
        subject_id: SubjectId("u-secret".into()),
        username: "secret-user".into(),
        password_phc: Redacted::new("$argon2id$v=19$m=1$OLD".to_owned()),
    })
    .await
    .unwrap();

    let _guard = tracing::subscriber::set_default(subscriber);
    cp.update_password(&SubjectId("u-secret".into()), "$argon2id$v=19$m=1$LEAKME")
        .await
        .unwrap();
    drop(_guard);

    let logged = String::from_utf8(sink.lock().unwrap().clone()).unwrap();
    assert!(
        !logged.contains("LEAKME"),
        "update_password recorded the Argon2 verifier in its tracing span:\n{logged}"
    );
    // Guard against the assertion going vacuous: the span must actually have been
    // captured, otherwise this test would pass with the argument unskipped.
    assert!(
        logged.contains("update_password"),
        "the span was not captured at all — this test proves nothing:\n{logged}"
    );
}
