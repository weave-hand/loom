#!/usr/bin/env bash
# Stand up a complete, throwaway loom stack on localhost so you can exercise the
# web UI (login + authed view) end-to-end in a browser — the "tight" deploy where
# query-api serves the UI bundle itself (same origin, no CORS).
#
#   tools/dev-up.sh                 # build everything, boot the stack, print the URL + creds
#   LOOM_DEV_HTTP_PORT=9000 tools/dev-up.sh
#   LOOM_DEV_ADMIN_USER=me LOOM_DEV_ADMIN_PASS=hunter2 tools/dev-up.sh
#
# What it runs (all from buck2-built binaries; nothing is installed):
#   * an ephemeral Postgres (the pinned //src/control-plane/postgres:postgres-bin,
#     the same one the hermetic tests use), migrated with the loom control-plane
#     migrations — the SAME initdb/migrate recipe as tools/sqlx-prepare.sh;
#   * engine-bin   — DataFusion serving over a local file:// Iceberg warehouse,
#     listening on a private unix socket (loom's sole serving path);
#   * query-api-bin — the governed HTTP API + auth, serving the wasm UI bundle
#     from LOOM_UI_DIR, with a bootstrap admin seeded from env.
#
# Everything lives under a single mktemp dir and is torn down on Ctrl-C / exit:
# the Postgres cluster is stopped and all temp state (pgdata, warehouse, sockets)
# is removed. No state survives. This is a developer convenience, NOT a deploy
# path — the deploy// cell + Helm chart are the real thing.
#
# Detached deploy (UI on its own origin): instead of LOOM_UI_DIR, run query-api
# with LOOM_CORS_ALLOWED_ORIGINS=<ui-origin> and serve the bundle separately with
# `buck2 run //src/ui:serve`. This script does the tight variant; see README/UI docs.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

HTTP_PORT="${LOOM_DEV_HTTP_PORT:-8080}"
ADMIN_USER="${LOOM_DEV_ADMIN_USER:-admin}"
ADMIN_PASS="${LOOM_DEV_ADMIN_PASS:-loom-dev}"
PG_PORT="${LOOM_DEV_PG_PORT:-54398}"

log() { printf '\033[36m[dev-up]\033[0m %s\n' "$*"; }

# 1. Build the binaries + the UI bundle + the pinned Postgres dist. `buck2 build`
#    materializes outputs locally; --show-output prints `<target> <relpath>`.
log "building binaries, UI bundle, and the pinned Postgres (first run is slow, then cached)…"
buck2 build \
  //src/services/engine:engine-bin \
  //src/services/query-api:query-api-bin \
  //src/ui:bundle \
  //src/control-plane/postgres:postgres-bin \
  //src/control-plane/postgres:libxml2 > /dev/null 2>&1

out() { buck2 build "$1" --show-output 2>/dev/null | awk '{print $2}'; }
ENGINE_BIN="$PWD/$(out //src/services/engine:engine-bin)"
QUERY_BIN="$PWD/$(out //src/services/query-api:query-api-bin)"
DIST="$PWD/$(out //src/ui:bundle)"
PGDIST="$PWD/$(out //src/control-plane/postgres:postgres-bin)"
XML="$PWD/$(out //src/control-plane/postgres:libxml2)"
export LD_LIBRARY_PATH="$PGDIST/lib:$XML${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# 2. Scratch workspace (all teardown is `rm -rf $WORK` + a pg stop).
WORK="$(mktemp -d "${TMPDIR:-/tmp}/loom-dev.XXXXXX")"
PGDATA="$WORK/pgdata"
PGSOCK="$WORK/pgsock"
WAREHOUSE="$WORK/warehouse"
ENGINE_SOCK="$WORK/engine.sock"
mkdir -p "$PGSOCK" "$WAREHOUSE"

ENGINE_PID="" QUERY_PID="" PG_STARTED=""
cleanup() {
  log "shutting down…"
  [ -n "$QUERY_PID" ] && kill "$QUERY_PID" 2>/dev/null || true
  [ -n "$ENGINE_PID" ] && kill "$ENGINE_PID" 2>/dev/null || true
  [ -n "$PG_STARTED" ] && "$PGDIST/bin/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

# 3. Boot an ephemeral Postgres and apply the loom migrations (mirrors
#    tools/sqlx-prepare.sh — trust auth, unix socket, listen_addresses='').
log "initialising ephemeral Postgres…"
"$PGDIST/bin/initdb" -D "$PGDATA" -U postgres --auth=trust >/dev/null
# Listen on TCP localhost (services connect over TCP) AND a private socket dir
# (used only for the createdb/psql setup below).
"$PGDIST/bin/pg_ctl" -D "$PGDATA" \
  -o "-p $PG_PORT -k $PGSOCK -c listen_addresses=127.0.0.1" -w -l "$PGDATA/log" start >/dev/null
PG_STARTED=1
"$PGDIST/bin/createdb" -h "$PGSOCK" -p "$PG_PORT" -U postgres loom
log "applying control-plane migrations…"
for f in src/control-plane/postgres/migrations/*.sql; do
  "$PGDIST/bin/psql" -h "$PGSOCK" -p "$PG_PORT" -U postgres -d loom \
    -v ON_ERROR_STOP=1 -q -f "$f" >/dev/null
done

# 4. Shared service env. Services connect over TCP to 127.0.0.1:$PG_PORT — NOT the
#    unix socket: DbConfig::pg_connect_options only sets `.port()` on the TCP branch,
#    so a socket host would be probed at the default 5432 and miss our port.
#    LOOM_BIND_ADDR is required by Config::from_env for every service even though the
#    engine binds a unix socket, not the addr.
export LOOM_DB_HOST=127.0.0.1 LOOM_DB_PORT="$PG_PORT" \
       LOOM_DB_USER=postgres LOOM_DB_PASSWORD= LOOM_DB_NAME=loom \
       LOOM_DATA_PATH="$WORK" LOOM_WAREHOUSE_URI="file://$WAREHOUSE" \
       LOOM_ENGINE_SOCKET="$ENGINE_SOCK"

# 5. Engine (sole serving path) on the unix socket. LOOM_BIND_ADDR is a placeholder.
log "starting engine…"
LOOM_BIND_ADDR=127.0.0.1:1 "$ENGINE_BIN" > "$WORK/engine.log" 2>&1 &
ENGINE_PID=$!
for _ in $(seq 1 100); do
  [ -S "$ENGINE_SOCK" ] && break
  kill -0 "$ENGINE_PID" 2>/dev/null || { log "engine exited early — log:"; cat "$WORK/engine.log"; exit 1; }
  sleep 0.1
done
[ -S "$ENGINE_SOCK" ] || { log "engine socket never appeared — log:"; cat "$WORK/engine.log"; exit 1; }

# 6. query-api: serves the UI bundle (tight deploy) + seeds the bootstrap admin.
log "starting query-api…"
LOOM_BIND_ADDR="127.0.0.1:$HTTP_PORT" \
LOOM_UI_DIR="$DIST" \
LOOM_BOOTSTRAP_ADMIN_USERNAME="$ADMIN_USER" \
LOOM_BOOTSTRAP_ADMIN_PASSWORD="$ADMIN_PASS" \
  "$QUERY_BIN" > "$WORK/query-api.log" 2>&1 &
QUERY_PID=$!
for _ in $(seq 1 100); do
  (exec 3<>"/dev/tcp/127.0.0.1/$HTTP_PORT") 2>/dev/null && { exec 3>&- 3<&-; break; }
  kill -0 "$QUERY_PID" 2>/dev/null || { log "query-api exited early — log:"; cat "$WORK/query-api.log"; exit 1; }
  sleep 0.1
done

cat <<BANNER

  loom is up — tight deploy (query-api serves the UI):

    URL:       http://127.0.0.1:$HTTP_PORT
    username:  $ADMIN_USER
    password:  $ADMIN_PASS

  Open the URL, log in, and you should land on the authed view. Logs:
    engine:     $WORK/engine.log
    query-api:  $WORK/query-api.log

  Ctrl-C to stop and remove all temp state.

BANNER

# 7. Wait until interrupted; surface an early crash of either service.
while kill -0 "$ENGINE_PID" 2>/dev/null && kill -0 "$QUERY_PID" 2>/dev/null; do
  sleep 1
done
log "a service exited; see logs above."
