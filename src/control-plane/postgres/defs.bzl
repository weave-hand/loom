# Shared macro for hermetic-fixture rust_test targets.
#
# Fixture tests boot real initdb/postgres/duckdb processes, which refuse to run
# as root. BuildBuddy RE runs as root, so these tests must run their test command
# LOCALLY. `remote_execution = "disabled"` gives the test a local-only run
# executor WITHOUT forcing its build off RE (buck2 separates build execution from
# test-run execution). Centralising it here means a new fixture test gets correct
# routing by construction — there is no separate lint to forget.
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
