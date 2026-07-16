#!/usr/bin/env bash
# Golden helm-render assertions for the loom chart. Requires `helm` 3.x on PATH.
# Not run in CI (the deploy// cell is off the //src sweep and needs the homelab
# external cell); run locally / in the release path:
#   bash deploy/chart/tests/render_assertions.sh
set -euo pipefail
CHART="$(cd "$(dirname "$0")/../chart" && pwd)"
fail() { echo "ASSERT FAIL: $*" >&2; exit 1; }
has()  { grep -q "$1" || fail "expected to find: $1"; }
hasnt() { if grep -q "$1"; then fail "expected NOT to find: $1"; fi; }

# Extract just the Job document (from its `kind: Job` to the next `---`).
job() { echo "$1" | awk '/^kind: Job$/{f=1} f; /^---$/{if(f)exit}'; }
# Extract the query-api Deployment document (podAffinity lives only there, but the
# `component: query-api` label also tags the Service — scope to the Deployment).
qapi_deploy() { echo "$1" | awk '/^kind: Deployment$/{d=1} d && /component: query-api/{p=1} p; /^---$/{if(p)exit}'; }
# Extract the allow-s3 NetworkPolicy document (scope, don't rely on a fixed window).
allow_s3() { echo "$1" | awk '/name: .*-allow-s3$/{f=1} f; /^---$/{if(f)exit}'; }
# Every rendered doc EXCEPT the worker Deployment. The worker is the one workload
# that legitimately carries LOOM_WAREHOUSE_URI in file:// mode (it is postgres-free,
# so parse_from_env has no data_path fallback — see loom.workerWarehouseEnv); every
# OTHER container must still fall back to LOOM_DATA_PATH.
without_worker() { echo "$1" | awk 'BEGIN{RS="\n---\n"} !/component: worker/{print "---"; print}'; }

echo "== helm lint =="
helm lint "$CHART"

echo "== migrations.mode=job (default): hook Job present, no on-boot env =="
OUT="$(helm template loom "$CHART")"
job "$OUT" | has '"helm.sh/hook": post-install,pre-upgrade'
job "$OUT" | has 'name: LOOM_MIGRATE'
job "$OUT" | has 'restartPolicy: Never'
echo "$OUT" | hasnt 'LOOM_DB_MIGRATE_ON_BOOT'

echo "== migrations.mode=<invalid>: render fails loudly =="
if helm template loom "$CHART" --set migrations.mode=bogus >/dev/null 2>&1; then
  fail "an invalid migrations.mode should fail rendering"
fi

echo "== migrations.mode=onBoot: env on the service Deployments, no Job =="
OUT="$(helm template loom "$CHART" --set migrations.mode=onBoot)"
echo "$OUT" | hasnt 'kind: Job'
# Both service Deployments carry the flag (ingest + query-api, plus the engine
# sidecar makes 3). Assert at least the two Deployments.
[ "$(echo "$OUT" | grep -c 'LOOM_DB_MIGRATE_ON_BOOT')" -ge 2 ] || fail "on-boot flag missing on Deployments"

echo "== migrations.mode=external: neither Job nor on-boot env =="
OUT="$(helm template loom "$CHART" --set migrations.mode=external)"
echo "$OUT" | hasnt 'kind: Job'
echo "$OUT" | hasnt 'LOOM_DB_MIGRATE_ON_BOOT'

echo "== default (no S3): PVC + co-scheduling affinity present =="
OUT="$(helm template loom "$CHART")"
echo "$OUT" | has 'kind: PersistentVolumeClaim'
qapi_deploy "$OUT" | has 'podAffinity'
# loom.objectStoreEnv must NOT emit LOOM_WAREHOUSE_URI in file:// mode — every
# pooled service derives the warehouse from LOOM_DATA_PATH instead. Scoped to
# exclude the worker, which is the documented exception (see without_worker).
without_worker "$OUT" | hasnt 'LOOM_WAREHOUSE_URI'

echo "== objectStore.s3.enabled: S3 env on all 5 containers, no PVC, relaxed affinity, egress =="
OUT="$(helm template loom "$CHART" \
  --set objectStore.s3.enabled=true \
  --set objectStore.s3.bucket=loomwh \
  --set objectStore.s3.endpoint=http://minio:9000 \
  --set objectStore.s3.port=9000 \
  --set objectStore.s3.credentialsSecret=loom-s3)"
echo "$OUT" | hasnt 'kind: PersistentVolumeClaim'
echo "$OUT" | hasnt 'claimName:'
# five containers each get the warehouse URI: ingest, query-api + its engine
# sidecar, and the worker + its engine sidecar. (Under S3 the worker gets it from
# loom.objectStoreEnv like everyone else — loom.workerWarehouseEnv only fires in
# the file:// case, so it adds nothing here.)
[ "$(echo "$OUT" | grep -c 'name: LOOM_WAREHOUSE_URI')" = "5" ] || fail "expected 5 LOOM_WAREHOUSE_URI"
echo "$OUT" | has 's3://loomwh'
echo "$OUT" | has 'name: AWS_ENDPOINT_URL'
echo "$OUT" | has 'name: AWS_ACCESS_KEY_ID'
# NOTE: we deliberately do NOT assert the *absence* of LOOM_DATA_PATH here — the
# binary's Config::from_env requires it, so it stays set (inert) even under S3.
# "no local path" is realised as no PVC + no data-volume mount (asserted above).
# co-scheduling affinity is gone
qapi_deploy "$OUT" | hasnt 'podAffinity'
# egress NetworkPolicy to the S3 port
allow_s3 "$OUT" | has 'port: 9000'

echo "== ui default: LOOM_UI_DIR on query-api, no config.js override =="
OUT="$(helm template loom "$CHART")"
qapi_deploy "$OUT" | has 'LOOM_UI_DIR'
echo "$OUT" | hasnt 'ui-config'

echo "== ui.apiBase: ConfigMap config.js + subPath mount over the bundle =="
OUT="$(helm template loom "$CHART" --set ui.apiBase=/app/loom)"
echo "$OUT" | has 'window.LOOM_CONFIG = { apiBase: "/app/loom" };'
qapi_deploy "$OUT" | has 'subPath: config.js'
qapi_deploy "$OUT" | has 'mountPath: "/usr/share/loom/ui/config.js"'

echo "== ui.enabled=false: no UI env, no override even with apiBase =="
OUT="$(helm template loom "$CHART" --set ui.enabled=false --set ui.apiBase=/x)"
echo "$OUT" | hasnt 'LOOM_UI_DIR'
echo "$OUT" | hasnt 'ui-config'

# --- worker -------------------------------------------------------------------
# Extract the worker Deployment, and the worker CONTAINER within it (containers sit
# at 8 spaces of indent; nested keys are deeper, so the next `- name: ` at that exact
# indent is the sidecar boundary).
worker_deploy() { echo "$1" | awk '/^kind: Deployment$/{d=1} d && /component: worker/{p=1} p; /^---$/{if(p)exit}'; }
# Stop at the sidecar's leading comment block too, not just its `- name:` — otherwise
# those 8-space `#` lines land inside "the worker container" and a future comment
# mentioning LOOM_DB_ would fail the zero-pool assertion spuriously.
worker_container() { worker_deploy "$1" | awk '/^        - name: worker$/{f=1;next} f && (/^        - name: /||/^        #/){exit} f'; }

echo "== worker: Deployment renders with worker + engine containers =="
OUT="$(helm template loom "$CHART")"
S3OUT="$(helm template loom "$CHART" \
  --set objectStore.s3.enabled=true \
  --set objectStore.s3.bucket=b \
  --set objectStore.s3.credentialsSecret=s)"
worker_deploy "$OUT" | has 'name: worker'
worker_deploy "$OUT" | has 'name: engine'

echo "== worker: LOOM_WAREHOUSE_URI is set in BOTH warehouse modes =="
# Default (file:// PVC): loom.objectStoreEnv omits LOOM_WAREHOUSE_URI, so the worker
# needs it injected — worker-bin's parse_from_env is eager and REQUIRES it, so
# without this the container crash-loops on a default install.
worker_container "$OUT" | has 'LOOM_WAREHOUSE_URI'
worker_container "$OUT" | has 'file:///var/lib/loom/data'
worker_container "$S3OUT" | has 's3://b'

echo "== worker: workerWarehouseEnv did not leak onto the pod's engine sidecar =="
# The `without_worker` guard above drops the WHOLE worker document, so it cannot
# see this pod's own engine sidecar — which is pooled and must still fall back to
# LOOM_DATA_PATH. Exactly one LOOM_WAREHOUSE_URI in the file:// worker pod: the
# worker container's. Without this, pasting the helper onto the sidecar would
# render the var on a pooled container and no assertion would fail.
[ "$(worker_deploy "$OUT" | grep -c 'name: LOOM_WAREHOUSE_URI')" = "1" ] \
  || fail "workerWarehouseEnv leaked past the worker container"

echo "== worker: container carries NO DB credentials (zero-pool) =="
# Only the engine sidecar talks to PG.
worker_container "$OUT" | hasnt 'LOOM_DB_'

echo "== worker: no probes (binary-only image, no port to probe) =="
worker_container "$OUT" | hasnt 'Probe'

echo "== worker: stable per-pod lease identity from the pod name =="
worker_container "$OUT" | has 'LOOM_WORKER_ID'
worker_container "$OUT" | has 'fieldPath: metadata.name'

echo "== worker: data mount + podAffinity only when !s3 =="
worker_deploy "$OUT" | has 'persistentVolumeClaim'
worker_deploy "$OUT" | has 'podAffinity'
worker_deploy "$S3OUT" | hasnt 'persistentVolumeClaim'
worker_deploy "$S3OUT" | hasnt 'podAffinity'

# The guard that loom.workerWarehouseEnv stayed worker-scoped — i.e. that it was
# NOT folded into loom.objectStoreEnv, which would have changed the rendered env of
# ingest/query-api/engine — is the `without_worker … hasnt LOOM_WAREHOUSE_URI`
# assertion in the "default (no S3)" block above.

echo "ALL ASSERTIONS PASSED"
