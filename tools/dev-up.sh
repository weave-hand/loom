#!/usr/bin/env bash
# dev-up.sh — boot loom locally as the single all-in-one binary and serve the UI.
#
# Builds the UI bundle and //src/services/standalone:loom, then runs the composite
# (embedded Postgres self-extracted from the binary + engine + ingest + query-api)
# with a local file:// warehouse, plus a transform worker attached to the engine
# UDS (the composite runs no worker, so without it a queued transform run would
# never execute). query-api serves the built UI bundle via
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
# Ctrl-C stops the composite and the embedded Postgres cleanly (TERM with a 10s
# KILL escalation). A previous run that died without its trap (SIGKILL'd script,
# dead terminal) is self-healed on the next boot: a leftover loom composite on
# the target ports, leftover workers of the same data dir, and a stale embedded
# postmaster are all reclaimed before starting; a foreign process on the port is
# refused up front instead of failing deep in the composite's bind.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

ADMIN_USER="${LOOM_ADMIN_USER:-admin}"
ADMIN_PASS="${LOOM_ADMIN_PASS:-admin}"
QAPI_ADDR="${LOOM_QUERY_API_BIND_ADDR:-127.0.0.1:18080}"
INGEST_ADDR="${LOOM_INGEST_BIND_ADDR:-127.0.0.1:18081}"

# Data dir. Postgres' Unix-domain socket path caps at ~107 bytes, so DATA_PATH
# must stay short ("<DATA_PATH>/pgrun/.s.PGSQL.5432" has to fit). Default to a
# short ephemeral /tmp dir; a caller-set LOOM_DATA_PATH persists across runs.
EPHEMERAL=0
if [[ -n "${LOOM_DATA_PATH:-}" ]]; then
  DATA_PATH="$LOOM_DATA_PATH"; mkdir -p "$DATA_PATH"
  echo "dev-up: persistent data dir $DATA_PATH"
else
  DATA_PATH="$(mktemp -d -t loom-XXXXXX)"
  EPHEMERAL=1
  echo "dev-up: ephemeral data dir $DATA_PATH (removed on exit)"
fi

# Single EXIT handler: stop the composite (if started) and remove the ephemeral
# data dir. Installed once so a later `trap … EXIT` can't clobber cleanup — the
# server-kill trap used to overwrite the dir-removal trap and leak /tmp/loom-*.
server_pid=""
worker_pid=""

# TERM a child and give it 10s to exit before KILL. A bare fire-and-forget
# `kill` used to let a slow composite (and the embedded Postgres it owns)
# outlive the script. $1=pid $2=label.
stop_child() {
  local pid="$1" label="$2"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null || return 0
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || return 0
    sleep 0.5
  done
  echo "dev-up: $label (pid $pid) ignored TERM for 10s — killing" >&2
  kill -9 "$pid" 2>/dev/null || true
}

# If an embedded postmaster for THIS data dir is still alive (a composite that
# crashed or was SIGKILL'd never stops its Postgres), stop it directly.
# postmaster.pid line 1 is the pid; liveness is verified first so a stale file
# is just ignored. pg_ctl stop only signals via the pid file, so it needs no
# LD_LIBRARY_PATH; fall back to a plain kill if the cache was removed.
stop_stale_postgres() {
  local pidfile="$DATA_PATH/pgdata/postmaster.pid" pgpid pgctl
  [[ -f "$pidfile" ]] || return 0
  pgpid="$(head -1 "$pidfile" 2>/dev/null)"
  [[ -n "$pgpid" ]] && kill -0 "$pgpid" 2>/dev/null || return 0
  echo "dev-up: stopping stale embedded Postgres (pid $pgpid)"
  pgctl="$(find "$DATA_PATH/cache" -maxdepth 3 -path '*/bin/pg_ctl' 2>/dev/null | head -1)"
  if [[ -x "$pgctl" ]] && "$pgctl" -D "$DATA_PATH/pgdata" stop -m fast >/dev/null 2>&1; then
    return 0
  fi
  stop_child "$pgpid" "stale postgres"
}

cleanup() {
  stop_child "$worker_pid" "worker"
  stop_child "$server_pid" "composite"
  # The composite normally stops its embedded Postgres on TERM; this catches
  # one it left behind (crash / forced kill).
  stop_stale_postgres
  (( EPHEMERAL )) && rm -rf "$DATA_PATH"
  return 0
}
trap cleanup EXIT INT TERM

# True iff something is listening on host:port ($1). Bash /dev/tcp connect in a
# subshell — no dependency on ss/nc being installed.
port_busy() { (exec 3<>"/dev/tcp/${1%:*}/${1##*:}") 2>/dev/null; }

# Preflight one listen address ($1): fail fast on a colliding listener instead
# of a confusing late bind error deep in the composite. A leftover `loom`
# composite (a previous dev-up whose script died without its trap — SIGKILL,
# dead terminal) is reclaimed automatically; anything else is refused.
reclaim_addr() {
  local addr="$1" port="${1##*:}" pid comm
  port_busy "$addr" || return 0
  pid="$(ss -tlnp 2>/dev/null | sed -n "s/.*:$port .*pid=\([0-9]*\).*/\1/p" | head -1)"
  comm="$(cat "/proc/${pid:-0}/comm" 2>/dev/null || true)"
  if [[ "$comm" == "loom" ]]; then
    echo "dev-up: reclaiming $addr from leftover loom composite (pid $pid)"
    stop_child "$pid" "leftover composite"
    for _ in $(seq 1 10); do port_busy "$addr" || break; sleep 0.5; done
  fi
  if port_busy "$addr"; then
    echo "dev-up: $addr is already in use${comm:+ by '$comm'}${pid:+ (pid $pid)} — stop it or pick another port" >&2
    echo "        find it: ss -tlnp | grep :$port" >&2
    exit 1
  fi
}

# A leftover worker from a previous run against THIS data dir (script died
# without its trap). Not caught by the port preflight (workers hold no port),
# and it would reconnect to the new engine socket and double-drain the queue.
# Matched by LOOM_DATA_PATH in the process environment, so workers of other
# data dirs / other users' runs are untouched.
reclaim_stale_workers() {
  local pid
  for pid in $(pgrep -f worker_bin 2>/dev/null); do
    if tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null \
         | grep -qx "LOOM_DATA_PATH=$DATA_PATH"; then
      echo "dev-up: stopping leftover worker (pid $pid)"
      stop_child "$pid" "leftover worker"
    fi
  done
}

reclaim_addr "$QAPI_ADDR"
reclaim_addr "$INGEST_ADDR"
reclaim_stale_workers
# Self-heal a persistent LOOM_DATA_PATH whose previous run died uncleanly: a
# live stale postmaster would make the embedded boot fail on its lock file.
stop_stale_postgres
if (( ${#DATA_PATH} > 80 )); then
  echo "dev-up: LOOM_DATA_PATH is ${#DATA_PATH} chars — too long for Postgres' ~107-byte" >&2
  echo "        Unix socket path. Use a shorter path (e.g. /tmp/loom)." >&2
  exit 1
fi
mkdir -p "$DATA_PATH/pgrun" "$DATA_PATH/warehouse" "$DATA_PATH/cache"

echo "dev-up: building UI bundle + standalone binary + worker + seed emitter (buck2)…"
buck2 build //src/ui:bundle //src/services/standalone:loom //src/services/worker:worker-bin //src/testing:emit-seed 2>&1 | tail -1
UI_DIR="$(buck2 build --show-full-output //src/ui:bundle 2>/dev/null | awk '{print $2}')"
LOOM_BIN="$(buck2 build --show-full-output //src/services/standalone:loom 2>/dev/null | awk '{print $2}')"
WORKER_BIN="$(buck2 build --show-full-output //src/services/worker:worker-bin 2>/dev/null | awk '{print $2}')"
EMIT_BIN="$(buck2 build --show-full-output //src/testing:emit-seed 2>/dev/null | awk '{print $2}')"
[[ -d "$UI_DIR" ]]     || { echo "dev-up: UI bundle dir not found ($UI_DIR)" >&2; exit 1; }
[[ -x "$LOOM_BIN" ]]   || { echo "dev-up: loom binary not found ($LOOM_BIN)" >&2; exit 1; }
[[ -x "$WORKER_BIN" ]] || { echo "dev-up: worker binary not found ($WORKER_BIN)" >&2; exit 1; }
[[ -x "$EMIT_BIN" ]]   || { echo "dev-up: seed emitter not found ($EMIT_BIN)" >&2; exit 1; }

# Env shared by every boot of the composite. LOOM_BIND_ADDR is required by Config
# but unused by the composite (the three real binds come from the addrs below).
common_env=(
  LOOM_BIND_ADDR=127.0.0.1:0
  LOOM_PG_MODE=embedded
  LOOM_DATA_PATH="$DATA_PATH"
  # LOOM_DB_* are defaulted in embedded mode (host <data>/pgrun, user postgres,
  # trust auth, db loom) — see docs/deploy.md. No placeholders needed.
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
  stop_child "$xpid" "extraction boot"; wait "$xpid" 2>/dev/null || true
  rm -rf "$DATA_PATH/pgdata"   # discard any partial cluster the extraction boot began
fi
[[ -n "$PGROOT" ]] || { echo "dev-up: embedded Postgres extraction failed" >&2; exit 1; }

# libxml2 shim — the bundled Postgres links libxml2.so.2, which some hosts (e.g.
# Arch, whose system libxml2 carries soname .so.16) don't provide under that name.
# If postgres won't load with only its own lib dir, symlink the newest system
# libxml2 as .so.2 and prepend it. Proper fix: bundle libxml2 in the embed archive
# (a since-fixed issue).
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

# Wait for the query-api port to accept connections, then create the first admin.
# `create-admin` connects to the already-running embedded Postgres as a client via
# the defaulted embedded socket in common_env (it does not boot its own PG), so it
# needs neither the PG bin/lib vars nor LOOM_DB_* — Config::from_map defaults them.
host="${QAPI_ADDR%:*}"; port="${QAPI_ADDR##*:}"
for _ in $(seq 1 120); do
  (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null && { exec 3>&- 3<&-; break; }
  sleep 0.5
done
if printf '%s' "$ADMIN_PASS" | env "${common_env[@]}" \
     "$LOOM_BIN" create-admin --username "$ADMIN_USER" --password-stdin; then
  echo "dev-up: created admin '$ADMIN_USER'"
else
  echo "dev-up: create-admin skipped (instance already sealed?)"
fi

# Start a transform worker alongside the composite. The composite runs engine +
# ingest + query-api but NOT the worker, so without this a defined transform's
# run (or a scheduled / on-input-commit firing) enqueues and sits `queued`
# forever with nothing to drain it. The worker is the zero-pool binary: it holds
# no Postgres pool, connecting to the engine over the same UDS (the engine owns
# PG) and reading/writing the shared file:// warehouse. The query-api port check
# above already confirmed the composite is listening, so engine.sock is bound.
env "${common_env[@]}" \
  LOOM_ENGINE_SOCKET="$DATA_PATH/engine.sock" \
  LOOM_WORKER_ID=dev-up \
  "$WORKER_BIN" &
worker_pid=$!
echo "dev-up: started transform worker (pid $worker_pid)"

# Seed a small demo graph so the object-explorer has linked data — not just a
# flat list — to render on a fresh boot. Two linked types + one FK link + one
# action, exercising identities, link traversal, and the action surface:
#   employees ──department──▶ departments   (employees.department = departments.name)
# Best-effort: any failure logs a warning and leaves the server running.
#
# Per type the flow is forced (a grant's target type must exist, and landing
# needs a prior Write grant), so `seed_type` runs: define (POST /admin/models)
# → self-grant admin read+write → emit the Arrow batch → land it (POST
# /models/{type}). The physical `main.{type}` table is created by the land.
# Idempotent per type: skips a type already in the ontology (persistent
# LOOM_DATA_PATH re-run); the link/action defines are best-effort and no-op if
# they already exist.
employees_model='{"name":"employees","table":{"schema":"main","name":"employees"},"identity":"id","properties":[{"name":"id","ty":"long","required":true},{"name":"name","ty":"string","required":true},{"name":"department","ty":"string","required":true},{"name":"salary","ty":"double","required":true},{"name":"active","ty":"boolean","required":true}]}'
departments_model='{"name":"departments","table":{"schema":"main","name":"departments"},"identity":"name","properties":[{"name":"name","ty":"string","required":true},{"name":"building","ty":"string","required":true},{"name":"floor","ty":"integer","required":true}]}'
link_body='{"name":"department","from":"employees","to":"departments","cardinality":"One","backing":{"ForeignKey":{"from_column":"department","to_column":"name"}}}'

# Define (if absent), grant, and land one demo type. Reads $base/$ingest/$tok
# from the caller's dynamic scope. $1=type $2=emit-dataset $3=model-json.
seed_type() {
  local ty="$1" dataset="$2" model="$3"
  local fixture="$DATA_PATH/$ty.arrow"
  if curl -fsS -H "authorization: Bearer $tok" "$base/ontology/types" 2>/dev/null \
       | grep -q "\"$ty\""; then
    echo "dev-up: seed skipped ($ty already present)"; return 0
  fi
  curl -fsS -o /dev/null -X POST "$base/admin/models" \
      -H "authorization: Bearer $tok" -H 'content-type: application/json' -d "$model" \
    || { echo "dev-up: seed skipped ($ty define failed)"; return 1; }
  for act in write read; do
    curl -fsS -o /dev/null -X POST "$base/admin/roles/admin/grants" \
        -H "authorization: Bearer $tok" -H 'content-type: application/json' \
        -d "{\"action\":\"$act\",\"type\":\"$ty\"}" \
      || { echo "dev-up: seed skipped ($ty $act grant failed)"; return 1; }
  done
  "$EMIT_BIN" "$dataset" "$fixture" >/dev/null 2>&1 \
    || { echo "dev-up: seed skipped ($ty fixture emit failed)"; return 1; }
  curl -fsS -o /dev/null -X POST "$ingest/models/$ty" \
      -H "authorization: Bearer $tok" \
      -H 'content-type: application/vnd.apache.arrow.stream' --data-binary "@$fixture" \
    || { echo "dev-up: seed skipped ($ty land failed)"; return 1; }
  echo "dev-up: seeded '$ty'"
}

seed_demo() {
  local base="http://$QAPI_ADDR" ingest="http://$INGEST_ADDR" resp tok
  resp="$(curl -fsS -X POST "$base/auth/login" \
      -H 'content-type: application/json' \
      -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\"}")" \
    || { echo "dev-up: seed skipped (login failed)"; return 0; }
  tok="$(printf '%s' "$resp" | sed -n 's/.*"token":"\([^"]*\)".*/\1/p')"
  [[ -n "$tok" ]] || { echo "dev-up: seed skipped (no session token)"; return 0; }

  seed_type employees   employees   "$employees_model"   || return 0
  seed_type departments departments "$departments_model" || return 0

  # Define the employees.department -> departments FK link (best-effort; a
  # re-run over persistent data 4xx's on the existing link, which is fine).
  if curl -fsS -o /dev/null -X POST "$base/admin/links" \
       -H "authorization: Bearer $tok" -H 'content-type: application/json' \
       -d "$link_body" 2>/dev/null; then
    echo "dev-up: defined link employees.department -> departments"
  else
    echo "dev-up: link define skipped (already defined?)"
  fi

  # Define a demo Insert action so the /actions surface is non-empty.
  if curl -fsS -o /dev/null -X POST "$base/admin/actions" \
       -H "authorization: Bearer $tok" -H 'content-type: application/json' \
       -d '{"name":"hireEmployee","target":"employees"}' 2>/dev/null; then
    echo "dev-up: defined action hireEmployee (Insert employees)"
  else
    echo "dev-up: action define skipped (already defined?)"
  fi
  echo "dev-up: seed complete — log in and open the explorer"
}
seed_demo || true

wait "$server_pid"
