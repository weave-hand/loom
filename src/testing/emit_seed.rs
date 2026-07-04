#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stderr,
    clippy::panic,
    reason = "dev-only fixture emitter for tools/dev-up.sh, not a production path"
)]
//! Emit a `loom_test_seed` demo fixture as an Arrow IPC stream file.
//! `tools/dev-up.sh` builds and runs this to produce the bytes it `curl`s into
//! ingest's `POST /models/{dataset}`, seeding a freshly booted local stack so
//! the object-explorer has linked object data to render.
//!
//! Usage: `emit-seed <dataset> <output-path>`, where `dataset` is one of
//! `employees` | `departments`.

use std::io::Write;

fn main() {
    let mut args = std::env::args().skip(1);
    let dataset = args
        .next()
        .expect("usage: emit-seed <employees|departments> <output-path>");
    let out = args
        .next()
        .expect("usage: emit-seed <employees|departments> <output-path>");
    let bytes = match dataset.as_str() {
        "employees" => loom_test_seed::employees_ipc(),
        "departments" => loom_test_seed::departments_ipc(),
        other => panic!("unknown dataset '{other}' (want employees|departments)"),
    };
    let mut file = std::fs::File::create(&out).expect("create output file");
    file.write_all(&bytes).expect("write ipc bytes");
    file.flush().expect("flush");
    eprintln!(
        "emit-seed: wrote {dataset} -> {out} ({} bytes)",
        bytes.len()
    );
}
