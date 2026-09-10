{{/*
Chart name / fullname helpers (standard Helm idiom).
*/}}
{{- define "loom.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "loom.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "loom.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common + selector labels. selectorLabels are also what the NetworkPolicy
podSelector matches on, so every loom workload pod carries them.
*/}}
{{- define "loom.labels" -}}
helm.sh/chart: {{ include "loom.chart" . }}
{{ include "loom.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "loom.selectorLabels" -}}
app.kubernetes.io/name: {{ include "loom.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: loom
{{- end -}}

{{- define "loom.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "loom.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
CNPG cluster name and the app-secret it generates (<cluster>-app).
*/}}
{{- define "loom.pgClusterName" -}}
{{- /* trunc the base to 60 so "<base>-pg" stays <=63 — it is used as the
       cnpg.io/cluster label VALUE (63-char max) in the NetworkPolicy and by CNPG. */ -}}
{{- printf "%s-pg" (include "loom.fullname" . | trunc 60 | trimSuffix "-") -}}
{{- end -}}

{{/*
Resolve the Postgres connection secret name + per-field keys, whether bundled
(CNPG) or external. Emits a dict-ish via two helpers used by loom.dbEnv.
*/}}
{{- define "loom.dbSecretName" -}}
{{- if .Values.postgres.enabled -}}
{{- printf "%s-app" (include "loom.pgClusterName" .) -}}
{{- else -}}
{{- required "postgres.external.existingSecret is required when postgres.enabled=false" .Values.postgres.external.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
DB env block: maps LOOM_DB_* to secretKeyRefs. CNPG's app secret uses the keys
host/port/username/password/dbname; external secrets are mapped via
postgres.external.keys.
*/}}
{{- define "loom.dbEnv" -}}
{{- $secret := include "loom.dbSecretName" . -}}
{{- $k := dict "host" "host" "port" "port" "user" "username" "password" "password" "dbname" "dbname" -}}
{{- if not .Values.postgres.enabled -}}
{{- $k = .Values.postgres.external.keys -}}
{{- end }}
- name: LOOM_DB_HOST
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $k.host }}
- name: LOOM_DB_PORT
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $k.port }}
- name: LOOM_DB_USER
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $k.user }}
- name: LOOM_DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $k.password }}
- name: LOOM_DB_NAME
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $k.dbname }}
{{- with .Values.postgres.maxConnections }}
- name: LOOM_DB_MAX_CONNECTIONS
  value: {{ . | quote }}
{{- end }}
{{- end -}}

{{/*
Object-store env for the service containers. Default (no S3) emits the local
LOOM_DATA_PATH warehouse. With objectStore.s3.enabled it emits the S3 warehouse
URI + region + optional endpoint + credentials-from-secret; LOOM_DATA_PATH stays
set (Config::from_env requires it) but is overridden by LOOM_WAREHOUSE_URI.
*/}}
{{- define "loom.objectStoreEnv" -}}
- name: LOOM_DATA_PATH
  value: {{ .Values.objectStore.mountPath | quote }}
{{- if .Values.objectStore.s3.enabled }}
{{- $s3 := .Values.objectStore.s3 }}
- name: LOOM_WAREHOUSE_URI
  value: {{ printf "s3://%s%s" (required "objectStore.s3.bucket is required when s3.enabled" $s3.bucket) (empty $s3.prefix | ternary "" (printf "/%s" $s3.prefix)) | quote }}
- name: AWS_REGION
  value: {{ $s3.region | quote }}
{{- with $s3.endpoint }}
- name: AWS_ENDPOINT_URL
  value: {{ . | quote }}
{{- end }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ required "objectStore.s3.credentialsSecret is required when s3.enabled" $s3.credentialsSecret }}
      key: {{ $s3.credentialsKeys.accessKeyId }}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $s3.credentialsSecret }}
      key: {{ $s3.credentialsKeys.secretAccessKey }}
{{- end }}
{{- end -}}

{{/*
Worker-only warehouse env. `loom.objectStoreEnv` emits LOOM_WAREHOUSE_URI only in
the S3 case; the zero-pool worker's `ObjectStoreConfig::parse_from_env` REQUIRES it
(it is postgres-free, so it has no Config/data_path to fall back on — see
src/services/store-config/src/lib.rs), and worker-bin parses it EAGERLY at startup.
So a file://-warehouse worker must be handed it explicitly or the container
crash-loops on a default install. Emitted only when S3 is off — with S3 on,
objectStoreEnv already set it.

Deliberately a separate helper rather than a branch inside loom.objectStoreEnv:
folding it in there would change the rendered env of ingest/query-api/engine.
*/}}
{{- define "loom.workerWarehouseEnv" -}}
{{- if not .Values.objectStore.s3.enabled }}
- name: LOOM_WAREHOUSE_URI
  value: {{ printf "file://%s" .Values.objectStore.mountPath | quote }}
{{- end }}
{{- end -}}

{{/*
On-boot migration env: emitted on the service containers only when
migrations.mode == onBoot. Each pod applies the schema at startup (sqlx advisory
lock serialises concurrent pods).
*/}}
{{- define "loom.migrateOnBootEnv" -}}
{{- if eq .Values.migrations.mode "onBoot" }}
- name: LOOM_DB_MIGRATE_ON_BOOT
  value: "true"
{{- end }}
{{- end -}}

{{/*
Render a service container image ref: digest-pinned when .digest is set
(release builds), else repository:tag.
*/}}
{{- define "loom.image" -}}
{{- if .digest -}}
{{- printf "%s@%s" .repository .digest -}}
{{- else -}}
{{- printf "%s:%s" .repository .tag -}}
{{- end -}}
{{- end -}}
