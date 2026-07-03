#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    reason = "dev-only fixture emitter for tools/dev-up.sh, not a production path"
)]
//! Emit the [`loom_test_seed::employees_ipc`] demo fixture as an Arrow IPC
//! stream file. `tools/dev-up.sh` builds and runs this to produce the bytes it
//! `curl`s into ingest's `POST /models/employees`, seeding a freshly booted
//! local stack so the object-explorer has something to render.
//!
//! Usage: `emit-employees <output-path>`

use std::io::Write;

fn main() {
    let out = std::env::args()
        .nth(1)
        .expect("usage: emit-employees <output-path>");
    let bytes = loom_test_seed::employees_ipc();
    let mut file = std::fs::File::create(&out).expect("create output file");
    file.write_all(&bytes).expect("write ipc bytes");
    file.flush().expect("flush");
    eprintln!("emit-employees: wrote {} ({} bytes)", out, bytes.len());
}
