//! Guards that the assembled host toolchain (`//tools:rust-host-toolchain`) ships
//! the rust-analyzer proc-macro server.
//!
//! `rust-project develop` (the prelude's rust-analyzer integration) points
//! rust-analyzer at this assembled toolchain as its `sysroot`. rust-analyzer then
//! derives the proc-macro server path as `<sysroot>/libexec/rust-analyzer-proc-macro-srv`.
//! If that binary is absent, rust-analyzer cannot expand attribute/derive
//! proc-macros (`#[tokio::main]`, `#[derive(...)]`); the unexpanded items leave the
//! whole function/struct unresolved, which silently poisons every downstream
//! diagnostic in the editor with thousands of false errors. This test makes that
//! contract explicit so a future change to the toolchain genrule can't drop the
//! server unnoticed.
//!
//! The toolchain dir is supplied as a buck input via `LOOM_RUST_HOST_TOOLCHAIN`
//! (a `$(location //tools:rust-host-toolchain)` in the test's `env`).

use std::path::Path;
use std::process::{Command, Stdio};

#[test]
fn proc_macro_server_present_and_runnable() {
    let root = std::env::var("LOOM_RUST_HOST_TOOLCHAIN")
        .expect("LOOM_RUST_HOST_TOOLCHAIN must be set to $(location //tools:rust-host-toolchain)");
    let srv = Path::new(&root).join("libexec/rust-analyzer-proc-macro-srv");

    assert!(
        srv.is_file(),
        "proc-macro server missing from the assembled sysroot: {}\n\
         rust-analyzer derives <sysroot>/libexec/rust-analyzer-proc-macro-srv; without it,\n\
         #[tokio::main]/#[derive(...)] never expand and editor diagnostics are all false.",
        srv.display()
    );

    // It must actually run from inside the assembled toolchain: the binary's rpath
    // ($ORIGIN/../lib) has to resolve librustc_driver and libLLVM out of the
    // toolchain's own lib/. Real work is gated behind RUST_ANALYZER_INTERNALS_DO_NOT_USE;
    // with it set and stdin closed it does a clean startup-then-exit, which fails fast
    // (nonzero / loader error) if those shared libs don't resolve.
    let status = Command::new(&srv)
        .env("RUST_ANALYZER_INTERNALS_DO_NOT_USE", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap_or_else(|e| panic!("spawn {}: {e}", srv.display()));

    assert!(
        status.success(),
        "proc-macro server failed to start from the assembled toolchain (shared libs \
         unresolved?): {status}"
    );
}
