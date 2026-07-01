#!/usr/bin/env bash
# Cloud (root-session) variant of tools/sqlx-prepare.sh.
#
# initdb/postgres refuse to run as root, and cloud routine sessions are root. So
# this runs the Postgres *server* (initdb + pg_ctl) as a dedicated unprivileged
# user ($PG_RUNNER, default `pgrunner`), while the *clients* (createdb/psql/
# `cargo sqlx prepare`) run as root over the shared unix socket. Root can reach a
# non-root-owned socket, and the hermetic toolchain / buck2 outputs are already
# root-owned from the ambient session.
#
#   tools/sqlx-prepare-cloud.sh          # regenerate the cache
#   tools/sqlx-prepare-cloud.sh --check  # regenerate + fail if it drifts from git
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

PG_RUNNER="${PG_RUNNER:-pgrunner}"
id "$PG_RUNNER" >/dev/null 2>&1 || useradd -m "$PG_RUNNER"

# 1. Hermetic Rust toolchain (cargo/rustc) on PATH — runs as root, same as the
#    original script.
eval "$(./tools/env.sh)"
if [ ! -x .loom/bin/sqlx ]; then
  cargo install --root "$PWD/.loom" --version '^0.9' --no-default-features --features postgres,rustls sqlx-cli
fi
export PATH="$PWD/.loom/bin:$PATH"

# 2. Materialize the pinned postgres + libxml2 (root buck2, local materialization).
BIN="$PWD/$(buck2 build //src/control-plane/postgres:postgres-bin --show-output 2>/dev/null | awk '{print $2}')"
XML="$PWD/$(buck2 build //src/control-plane/postgres:libxml2 --show-output 2>/dev/null | awk '{print $2}')"
export LD_LIBRARY_PATH="$BIN/lib:$XML"

# 3. A world-traversable working root so both root and $PG_RUNNER can reach the
#    data dir + socket.
WORK="$(mktemp -d)"
chmod 0711 "$WORK"
DATA="$WORK/pgdata"
SOCK="$WORK/sock"
mkdir -p "$SOCK"
chown "$PG_RUNNER":"$PG_RUNNER" "$WORK" "$SOCK"
PORT=54399

run_pg() { sudo -u "$PG_RUNNER" env LD_LIBRARY_PATH="$LD_LIBRARY_PATH" "$@"; }

run_pg "$BIN/bin/initdb" -D "$DATA" -U postgres --auth=trust >/dev/null
run_pg "$BIN/bin/pg_ctl" -D "$DATA" -o "-p $PORT -k $SOCK -c listen_addresses=''" -w -l "$DATA/log" start
cleanup() {
  run_pg "$BIN/bin/pg_ctl" -D "$DATA" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# 4. createdb + migrations (clients as root over the socket).
"$BIN/bin/createdb" -h "$SOCK" -p "$PORT" -U postgres loom
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
