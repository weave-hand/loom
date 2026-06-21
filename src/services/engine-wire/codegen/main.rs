//! Pure-Rust protobuf + tonic codegen tool for the engine-wire crate.
//!
//! argv: `<proto_file> <out_dir>`. Uses `protox` (no `protoc`/external compiler)
//! to compile the proto into a `FileDescriptorSet`, then hands that to
//! `tonic_prost_build` to emit client + server stubs into `<out_dir>`. Driven by
//! the `:pb-gen` buck2 genrule; the library `include!`s the result via the
//! `ENGINE_PB` env-location (mirrors the postgres crate's `SQLX_OFFLINE_DIR`).

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let proto = PathBuf::from(args.next().expect("usage: codegen <proto> <out_dir>"));
    let out_dir = PathBuf::from(args.next().expect("usage: codegen <proto> <out_dir>"));
    let include = proto.parent().unwrap().to_path_buf();

    // protox: pure-Rust protobuf compiler — emits a FileDescriptorSet, no protoc.
    let fds = protox::compile([&proto], [&include])?;

    // tonic-prost-build consumes the FDS directly (no protoc invocation).
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir(&out_dir)
        .compile_fds(fds)?;
    Ok(())
}
