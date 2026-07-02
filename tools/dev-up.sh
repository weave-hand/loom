#!/usr/bin/env bash
# dev-up.sh — boot loom locally as the single all-in-one binary and serve the UI.
#
# Builds the UI bundle and //src/services/standalone:loom, then runs the composite
# (embedded Postgres self-extracted from the binary + engine + ingest + query-api)
# with a local file:// warehouse. query-api serves the built UI bundle via
# LOOM_UI_DIR, so one origin hosts both the API and the login page, and bootstraps
# an admin user you can log in with.
#
# Usage:
#   tools/dev-up.sh                        # boot on http://127.0.0.1:18080
#   LOOM_DATA_PATH=/tmp/loom tools/dev-up.sh   # persist data + PG cache across runs
#   LOOM_QUERY_API_BIND_ADDR=127.0.0.1:9000 tools/dev-up.sh
#
# Ports default to 18080/18081 to stay clear of a local k3d cluster on 8080/8081.
#
# Log in with LOOM_ADMIN_USER / LOOM_ADMIN_PASS (defaults admin / admin).
# Ctrl-C stops the composite and the embedded Postgres cleanly.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

ADMIN_USER="${LOOM_ADMIN_USER:-admin}"
ADMIN_PASS="${LOOM_ADMIN_PASS:-admin}"
QAPI_ADDR="${LOOM_QUERY_API_BIND_ADDR:-127.0.0.1:18080}"
INGEST_ADDR="${LOOM_INGEST_BIND_ADDR:-127.0.0.1:18081}"

# Data dir. Postgres' Unix-domain socket path caps at ~107 bytes, so DATA_PATH
# must stay short ("<DATA_PATH>/pgrun/.s.PGSQL.5432" has to fit). Default to a
# short ephemeral /tmp dir; a caller-set LOOM_DATA_PATH persists across runs.
if [[ -n "${LOOM_DATA_PATH:-}" ]]; then
  DATA_PATH="$LOOM_DATA_PATH"; mkdir -p "$DATA_PATH"
  echo "dev-up: persistent data dir $DATA_PATH"
else
  DATA_PATH="$(mktemp -d -t loom-XXXXXX)"
  echo "dev-up: ephemeral data dir $DATA_PATH (removed on exit)"
  trap 'rm -rf "$DATA_PATH"' EXIT
fi
if (( ${#DATA_PATH} > 80 )); then
  echo "dev-up: LOOM_DATA_PATH is ${#DATA_PATH} chars — too long for Postgres' ~107-byte" >&2
  echo "        Unix socket path. Use a shorter path (e.g. /tmp/loom)." >&2
  exit 1
fi
mkdir -p "$DATA_PATH/pgrun" "$DATA_PATH/warehouse" "$DATA_PATH/cache"

echo "dev-up: building UI bundle + standalone binary (buck2)…"
buck2 build //src/ui:bundle //src/services/standalone:loom 2>&1 | tail -1
UI_DIR="$(buck2 build --show-full-output //src/ui:bundle 2>/dev/null | awk '{print $2}')"
LOOM_BIN="$(buck2 build --show-full-output //src/services/standalone:loom 2>/dev/null | awk '{print $2}')"
[[ -d "$UI_DIR" ]]   || { echo "dev-up: UI bundle dir not found ($UI_DIR)" >&2; exit 1; }
[[ -x "$LOOM_BIN" ]] || { echo "dev-up: loom binary not found ($LOOM_BIN)" >&2; exit 1; }

# Env shared by every boot of the composite. LOOM_BIND_ADDR is required by Config
# but unused by the composite (the three real binds come from the addrs below).
common_env=(
  LOOM_BIND_ADDR=127.0.0.1:0
  LOOM_PG_MODE=embedded
  LOOM_DATA_PATH="$DATA_PATH"
  LOOM_DB_HOST="$DATA_PATH/pgrun" LOOM_DB_PORT=5432
  LOOM_DB_USER=postgres LOOM_DB_PASSWORD=postgres LOOM_DB_NAME=loom
  LOOM_WAREHOUSE_URI="file://$DATA_PATH/warehouse"
)

# Phase 1 — ensure the embedded Postgres is extracted. The binary self-extracts
# it (idempotently) on first embedded boot; trigger that once in the background
# on throwaway ephemeral ports, wait until the cache appears, then stop it. This
# lets phase 2 pin LOOM_PG_BIN_DIR/LD_LIBRARY_PATH explicitly (so the libxml2
# shim below is honoured — the binary only sets its own LD path when it extracts).
PGROOT="$(find "$DATA_PATH/cache" -maxdepth 1 -type d -name 'pg-*' 2>/dev/null | head -1)"
if [[ -z "$PGROOT" ]]; then
  echo "dev-up: extracting embedded Postgres (first run)…"
  env "${common_env[@]}" LOOM_ENGINE_SOCKET="$DATA_PATH/extract.sock" \
      LOOM_QUERY_API_BIND_ADDR=127.0.0.1:0 LOOM_INGEST_BIND_ADDR=127.0.0.1:0 \
      "$LOOM_BIN" >/dev/null 2>&1 &
  xpid=$!
  for _ in $(seq 1 120); do
    PGROOT="$(find "$DATA_PATH/cache" -maxdepth 1 -type d -name 'pg-*' 2>/dev/null | head -1)"
    [[ -n "$PGROOT" ]] && break
    kill -0 "$xpid" 2>/dev/null || break
    sleep 0.5
  done
  kill "$xpid" 2>/dev/null || true; wait "$xpid" 2>/dev/null || true
  rm -rf "$DATA_PATH/pgdata"   # discard any partial cluster the extraction boot began
fi
[[ -n "$PGROOT" ]] || { echo "dev-up: embedded Postgres extraction failed" >&2; exit 1; }

# libxml2 shim — the bundled Postgres links libxml2.so.2, which some hosts (e.g.
# Arch, whose system libxml2 carries soname .so.16) don't provide under that name.
# If postgres won't load with only its own lib dir, symlink the newest system
# libxml2 as .so.2 and prepend it. Proper fix: bundle libxml2 in the embed archive
# (docs/ISSUES.md → iss-embedded-pg-libxml2).
PGLD="$PGROOT/lib"
if ! env LD_LIBRARY_PATH="$PGROOT/lib" "$PGROOT/bin/postgres" -V >/dev/null 2>&1; then
  sys_xml="$(ls /usr/lib/libxml2.so.2 2>/dev/null \
             || ls -1 /usr/lib*/libxml2.so.* 2>/dev/null | grep -vE '\.so$' | sort -V | tail -1)"
  if [[ -n "$sys_xml" ]]; then
    mkdir -p "$DATA_PATH/xmlshim"; ln -sf "$sys_xml" "$DATA_PATH/xmlshim/libxml2.so.2"
    PGLD="$DATA_PATH/xmlshim:$PGROOT/lib"
    echo "dev-up: libxml2.so.2 shim → $sys_xml"
  else
    echo "dev-up: warning — no system libxml2 found; embedded Postgres may fail to start" >&2
  fi
fi

echo "dev-up: starting loom on http://$QAPI_ADDR  (login: $ADMIN_USER / $ADMIN_PASS)"
echo "dev-up: UI served from $UI_DIR"

# Run the composite in the background so we can bootstrap the first admin once it is
# listening. There is no LOOM_BOOTSTRAP_ADMIN_* env path anymore — the first admin is
# created out-of-band via `loom create-admin`, the sole admin-creation path (and the
# instance seals after the first one, so re-runs are refused, which is fine here).
env "${common_env[@]}" \
  LOOM_PG_BIN_DIR="$PGROOT/bin" \
  LOOM_PG_LD_LIBRARY_PATH="$PGLD" \
  LOOM_ENGINE_SOCKET="$DATA_PATH/engine.sock" \
  LOOM_QUERY_API_BIND_ADDR="$QAPI_ADDR" \
  LOOM_INGEST_BIND_ADDR="$INGEST_ADDR" \
  LOOM_UI_DIR="$UI_DIR" \
  "$LOOM_BIN" &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null' EXIT INT TERM

# Wait for the query-api port to accept connections, then create the first admin.
# `create-admin` connects to the already-running embedded Postgres as a client via
# the LOOM_DB_* socket in common_env (it does not boot its own PG); the PG bin/lib
# vars are still required for `Config::from_map` to parse in embedded mode.
host="${QAPI_ADDR%:*}"; port="${QAPI_ADDR##*:}"
for _ in $(seq 1 120); do
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null && { exec 3>&- 3<&-; break; }
  sleep 0.5
done
if printf '%s' "$ADMIN_PASS" | env "${common_env[@]}" \
     LOOM_PG_BIN_DIR="$PGROOT/bin" LOOM_PG_LD_LIBRARY_PATH="$PGLD" \
     "$LOOM_BIN" create-admin --username "$ADMIN_USER" --password-stdin; then
  echo "dev-up: created admin '$ADMIN_USER'"
else
  echo "dev-up: create-admin skipped (instance already sealed?)"
fi

wait "$server_pid"
