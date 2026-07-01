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
echo "$OUT" | hasnt 'LOOM_WAREHOUSE_URI'

echo "== objectStore.s3.enabled: S3 env on all 3 containers, no PVC, relaxed affinity, egress =="
OUT="$(helm template loom "$CHART" \
  --set objectStore.s3.enabled=true \
  --set objectStore.s3.bucket=loomwh \
  --set objectStore.s3.endpoint=http://minio:9000 \
  --set objectStore.s3.port=9000 \
  --set objectStore.s3.credentialsSecret=loom-s3)"
echo "$OUT" | hasnt 'kind: PersistentVolumeClaim'
echo "$OUT" | hasnt 'claimName:'
# three containers (ingest, query-api, engine) each get the warehouse URI
[ "$(echo "$OUT" | grep -c 'name: LOOM_WAREHOUSE_URI')" = "3" ] || fail "expected 3 LOOM_WAREHOUSE_URI"
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

echo "ALL ASSERTIONS PASSED"
