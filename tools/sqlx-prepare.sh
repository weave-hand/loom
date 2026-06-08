#!/usr/bin/env bash
# Generate src/control-plane/postgres/.sqlx/ (the sqlx offline query cache used by
# compile-time query! validation) by running `cargo sqlx prepare` against a freshly
# migrated, ephemeral Postgres — the SAME pinned binary the hermetic tests use.
#
# The committed .sqlx/ cache is what lets the postgres rust_library build offline
# (SQLX_OFFLINE_DIR) with no cargo / no `cargo metadata`, including on remote
# execution. Re-run this whenever a query! in the postgres crate changes.
#
#   tools/sqlx-prepare.sh          # regenerate the cache
#   tools/sqlx-prepare.sh --check  # regenerate + fail if it drifts from git
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# 1. Build/cache sqlx-cli 0.9 via the hermetic toolchain (skip if present).
if [ ! -x .loom/bin/sqlx ]; then
  eval "$(./tools/env.sh)"
  cargo install --root "$PWD/.loom" --version '^0.9' --no-default-features --features postgres,rustls sqlx-cli
fi
export PATH="$PWD/.loom/bin:$PATH"

# 2. Materialize the pinned postgres + libxml2.
BIN="$PWD/$(env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:postgres-bin --show-output 2>/dev/null | awk '{print $2}')"
XML="$PWD/$(env -u BUCK_PREFER_REMOTE buck2 build //src/control-plane/postgres:libxml2 --show-output 2>/dev/null | awk '{print $2}')"
export LD_LIBRARY_PATH="$BIN/lib:$XML"

# 3. initdb + start a private cluster; trap cleanup.
DATA="$(mktemp -d)/pgdata"
SOCK="$(mktemp -d)"
PORT=54399
"$BIN/bin/initdb" -D "$DATA" -U postgres --auth=trust >/dev/null
"$BIN/bin/pg_ctl" -D "$DATA" -o "-p $PORT -k $SOCK -c listen_addresses=''" -w -l "$DATA/log" start
cleanup() {
  "$BIN/bin/pg_ctl" -D "$DATA" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$DATA" "$SOCK"
}
trap cleanup EXIT
"$BIN/bin/createdb" -h "$SOCK" -p "$PORT" -U postgres loom

# 4. Apply loom migrations so the schema exists for query! validation.
for f in src/control-plane/postgres/migrations/*.sql; do
  "$BIN/bin/psql" -h "$SOCK" -p "$PORT" -U postgres -d loom -v ON_ERROR_STOP=1 -q -f "$f"
done

# 5. Prepare.
export DATABASE_URL="postgres://postgres@localhost:$PORT/loom?host=$SOCK"
(cd src/control-plane/postgres && unset SQLX_OFFLINE && cargo sqlx prepare -- --lib)

# 6. --check mode for the hook/CI.
if [ "${1:-}" = "--check" ]; then
  git diff --exit-code src/control-plane/postgres/.sqlx
fi
