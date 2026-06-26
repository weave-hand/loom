# Shared macro for hermetic-fixture rust_test targets.
#
# Fixture tests boot real initdb/postgres processes, which refuse to run as
# root. buck2/tpx runs the test-RUN action on the LOCAL executor by default (it only
# dispatches to RE when told, via --unstable-allow-all-tests-on-re — there is no
# buckconfig key and NO remote test-result cache; tests re-run every invocation). So
# WHERE a fixture runs, and whether root rejects it, is decided by the INVOCATION's
# environment, not by this macro:
#   - dev machines / the non-root BuildBuddy CI runners: local executor, non-root → pass;
#   - a root host (e.g. a cloud session): local executor would run initdb as root → fail,
#     so the test run must be routed to RE (non-root `buildbuddy` worker) instead. The
#     cloud buck2 shim injects that flag for `buck2 test`; CI passes it explicitly in
#     buildbuddy.yaml.
# Builds always go to RE regardless (the RE platform's `dockerUser` is `buildbuddy`, a
# non-root user — see platforms/defs.bzl), so a fixture's *build* is never the problem;
# only the local *test run* is. This macro deliberately sets no per-target
# `remote_execution` profile: that would force RE for fixtures in EVERY environment and
# break local dev without an RE backend. Placement stays an invocation-level choice.
# Centralising the env here means a new fixture test gets correct wiring by
# construction — there is no separate lint to forget.
#
# The $(location //src/control-plane/postgres:...) labels are absolute, so this
# produces identical env whether called from postgres/, query-api/, or worker/.

def loom_fixture_test(
        name,
        crate,
        srcs,
        crate_root,
        deps,
        minio = False,
        edition = "2024",
        env = {},
        **kwargs):
    fixture_env = {
        "POSTGRES_BIN_DIR": "$(location //src/control-plane/postgres:postgres-bin)/bin",
        "POSTGRES_LD_LIBRARY_PATH": "$(location //src/control-plane/postgres:postgres-bin)/lib:$(location //src/control-plane/postgres:libxml2)",
        "LOOM_MIGRATIONS_DIR": "$(location //src/control-plane/postgres:migrations)/migrations",
        # Host-stable shared dir so the fixture boot throttle's K slots are shared
        # across ALL fixture-test processes (buck2's local executor may hand each
        # action a per-action TMPDIR; keying the slot dir off that would
        # un-throttle the cross-target axis). The fixture code defaults to this
        # same literal, so a direct cargo test behaves identically. Override-safe:
        # placed before fixture_env.update(env), so a per-target `env` (and ambient
        # env) wins.
        "LOOM_PG_FIXTURE_SLOT_DIR": "/tmp/loom-pg-fixture-slots",
    }
    if minio:
        fixture_env["MINIO_BIN"] = "$(location //src/control-plane/postgres:minio-bin)"
    fixture_env.update(env)
    native.rust_test(
        name = name,
        crate = crate,
        srcs = srcs,
        crate_root = crate_root,
        edition = edition,
        env = fixture_env,
        deps = deps,
        **kwargs
    )
