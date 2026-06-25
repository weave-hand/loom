# Shared macro for hermetic-fixture rust_test targets.
#
# Fixture tests boot real initdb/postgres/duckdb processes, which refuse to run as
# root. This macro sets no `remote_execution` profile, so the test command runs with
# buck2's default (local) test executor — fine on a non-root host (dev machines, the
# non-root BuildBuddy CI runners), where it passes and its result is cached remotely.
# Builds still go to RE: the RE platform runs as the non-root `buildbuddy` user
# (`dockerUser` in platforms/defs.bzl), so a fixture's *build* on RE is unaffected by
# the root constraint; only the local *test run* is. A warm session (incl. cloud)
# hits that remote test-result cache instead of re-running, so it never executes the
# fixture locally. NOTE: a root-only session that misses the cache will run the test
# locally and fail at `initdb` — that is a property of the host user, not this macro.
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
        duckdb = False,
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
    if duckdb:
        fixture_env["DUCKDB_BIN"] = "$(location //src/control-plane/postgres:duckdb-cli)"
        fixture_env["DUCKDB_EXTENSION_DIR"] = "$(location //src/control-plane/postgres:duckdb-extensions)"
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
