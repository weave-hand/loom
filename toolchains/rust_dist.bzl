"""Hermetic Rust toolchain rule that uses downloaded toolchain artifacts."""

load("@prelude//rust/rust_toolchain.bzl", "PanicRuntime", "RustToolchainInfo")

def _hermetic_rust_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    rustc_dist = ctx.attrs.rustc_dist[DefaultInfo].default_outputs[0]
    std_dist = ctx.attrs.std_dist[DefaultInfo].default_outputs[0]
    clippy_dist = ctx.attrs.clippy_dist[DefaultInfo].default_outputs[0]

    rustc = cmd_args(rustc_dist, format = "{}/bin/rustc")
    rustdoc = cmd_args(rustc_dist, format = "{}/bin/rustdoc")

    # Assemble sysroot: rustc expects lib/rustlib/<triple>/lib/ under --sysroot
    # The rustc dist already has the right layout, and we symlink std into it.
    # We also create a clippy-driver wrapper that sets LD_LIBRARY_PATH to the
    # sysroot libs, since clippy is downloaded separately from rustc.
    sysroot = ctx.actions.declare_output("sysroot", dir = True)
    target = ctx.attrs.rustc_target_triple

    ctx.actions.run(
        cmd_args(
            "bash", "-c",
            cmd_args(
                "set -euo pipefail;",
                "SYSROOT=\"$1\"; RUSTC_DIST=\"$2\"; STD_DIST=\"$3\"; TARGET=\"$4\"; CLIPPY_DIST=\"$5\";",
                "mkdir -p \"$SYSROOT\"/lib/rustlib;",
                # Copy rustc's own libs (codegen backends, etc.)
                "cp -rfL \"$RUSTC_DIST\"/lib/* \"$SYSROOT\"/lib/ 2>/dev/null || true;",
                # Copy std libs for the target (merge contents, not the directory itself)
                "mkdir -p \"$SYSROOT\"/lib/rustlib/\"$TARGET\";",
                "cp -rfL \"$STD_DIST\"/lib/rustlib/\"$TARGET\"/* \"$SYSROOT\"/lib/rustlib/\"$TARGET\"/;",
                # Copy clippy-driver binary into sysroot and create a wrapper
                # that finds it relative to itself (so it works on both local and RE).
                "mkdir -p \"$SYSROOT\"/bin;",
                "cp -L \"$CLIPPY_DIST\"/bin/clippy-driver \"$SYSROOT\"/bin/clippy-driver-bin;",
                "printf '#!/usr/bin/env bash\\nSCRIPT_DIR=\"$(cd \"$(dirname \"$0\")/..\" && pwd)\"\\nexport LD_LIBRARY_PATH=\"$SCRIPT_DIR/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}\"\\nexec \"$SCRIPT_DIR/bin/clippy-driver-bin\" \"$@\"\\n' > \"$SYSROOT\"/bin/clippy-driver;",
                "chmod +x \"$SYSROOT\"/bin/clippy-driver;",
                delimiter = " ",
            ),
            "_",  # dummy $0
            sysroot.as_output(),
            rustc_dist,
            std_dist,
            target,
            clippy_dist,
        ),
        category = "assemble_sysroot",
        # RE-eligible (not local_only): on a fresh CI runner this keeps the
        # rustc/std dists in CAS instead of materializing them locally every
        # build. allow_cache_upload lets a local dev run populate the shared
        # remote cache too. (The script only does cp/printf/chmod, no rustc
        # execution, so it runs fine in the RE container.)
        allow_cache_upload = True,
    )

    clippy_driver = cmd_args(sysroot, format = "{}/bin/clippy-driver")

    rustc_flags = list(ctx.attrs.rustc_flags)

    # Use bundled rust-lld to avoid needing a system linker
    if ctx.attrs.use_bundled_linker:
        rustc_flags.extend([
            "-Clinker-flavor=gnu-lld-cc",
            cmd_args(rustc_dist, format = "-Clinker={}/lib/rustlib/" + ctx.attrs.host_triple + "/bin/rust-lld"),
        ])

    return [
        DefaultInfo(),
        RustToolchainInfo(
            compiler = RunInfo(args = [rustc]),
            rustdoc = RunInfo(args = [rustdoc]),
            clippy_driver = RunInfo(args = [clippy_driver]),
            panic_runtime = PanicRuntime("unwind"),
            default_edition = ctx.attrs.default_edition,
            rustc_target_triple = target,
            sysroot_path = sysroot,
            rustc_flags = rustc_flags,
            allow_lints = ctx.attrs.allow_lints,
            deny_lints = ctx.attrs.deny_lints,
            warn_lints = ctx.attrs.warn_lints,
            clippy_toml = ctx.attrs.clippy_toml[DefaultInfo].default_outputs[0] if ctx.attrs.clippy_toml else None,
        ),
    ]

hermetic_rust_toolchain = rule(
    impl = _hermetic_rust_toolchain_impl,
    is_toolchain_rule = True,
    attrs = {
        "rustc_dist": attrs.dep(),
        "std_dist": attrs.dep(),
        "clippy_dist": attrs.dep(),
        "rustc_target_triple": attrs.string(default = "x86_64-unknown-linux-gnu"),
        "host_triple": attrs.string(default = "x86_64-unknown-linux-gnu"),
        "default_edition": attrs.string(default = "2024"),
        "rustc_flags": attrs.list(attrs.string(), default = []),
        "use_bundled_linker": attrs.bool(default = False),
        # Lint policy: forwarded into RustToolchainInfo. clippy::* lints in
        # warn_lints/allow_lints are applied to the clippy action; see
        # prelude/rust/build.bzl:_lintify. clippy_toml configures lint
        # parameters (e.g. allow-*-in-tests) for the clippy action only.
        #
        # FOOTGUN: allow_lints can NOT override a lint enabled via a *group* in
        # warn_lints (e.g. allowing clippy::implicit_return while warn_lints has
        # clippy::restriction). _lint_flags emits -A before -W, so the group -W
        # wins. To allowlist members of an enabled group, put -Aclippy::<lint> in
        # rustc_flags instead (emitted after lints — see toolchains/BUCK
        # CLIPPY_ALLOWS). allow_lints/deny_lints are for standalone (non-group)
        # lints, where order does not matter.
        "allow_lints": attrs.list(attrs.string(), default = []),
        "deny_lints": attrs.list(attrs.string(), default = []),
        "warn_lints": attrs.list(attrs.string(), default = []),
        # NOTE: deliberately NOT exposing the prelude's `rustc_test_flags` toolchain
        # field — it is unusable (rust_binary.bzl:561 mutates the frozen provider list
        # via `extra_flags += ["--test"]`, breaking every rust_test build when it is
        # non-empty). Test-only lint flags are injected per-target by the
        # loom_rust_test wrapper (src/loom_test.bzl) instead.
        "clippy_toml": attrs.option(attrs.dep(providers = [DefaultInfo]), default = None),
    },
)
