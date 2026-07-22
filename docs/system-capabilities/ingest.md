# Ingest capabilities

The ingest service (`src/services/ingest/`) is loom's landing edge: it accepts Arrow data over plain HTTP, validates it against an optional or ontology-derived model, and persists it as an Iceberg snapshot with lineage committed atomically in one Postgres transaction. It covers three concerns — the raw dataset landing path (endpoint, IPC decode, materializer, DataFusion multi-file Parquet write), the transactional snapshot-commit + lineage primitive it rides on, and the typed path (dataset→model binding, model inference on ingest, and per-value constraints) that turns landed rows into governed, serveable objects.

_As of 403f7a6a._

## Landing path

Raw landing is `POST /datasets/{schema}/{table}` with an Arrow IPC stream body (`application/vnd.apache.arrow.stream`). The handler (`src/services/ingest/src/http.rs`) decodes the body via the shared `datafusion_io::decode_ipc`, resolves the physical schema, and dispatches the write through the `LandingMaterializer` port — a per-table-format seam chosen at boot; the one shipped backend is `IcebergMaterializer`, a thin forwarder to the postgres-crate `iceberg_landing::land` entrypoint. Two optional headers shape a request: `X-Loom-Model` carries a JSON `ModelShape` for the conformance gate ("this data *is* this model"), and `X-Loom-Run-Id` threads a caller-supplied run id into the lineage event (a fresh UUID otherwise). Malformed IPC or headers are a 400; gate violations are a 422 with a structured violations body; backend faults return an opaque `"internal error"` 500 whose detail is logged server-side through the structural `ApiError::Internal` path — closing the class of unlogged opaque-500 arms (#313). The router also exposes an operator action, `POST /tables/{schema}/{table}/compact`, which enqueues a compaction job for a zero-pool worker to perform asynchronously. It is the one **admin-gated** route in ingest (#434): it is built by a sibling `compact_routes` builder whose inner `route_layer` applies `require_admin` under the shared `protect` wrapper, so the data-plane routes (`/datasets`, `/models`) stay `require_auth` only — a non-admin caller gets a 403, an unauthenticated one a 401. The handler routes through the **same** `maybe_enqueue_compact` guard + `pg_insert_if_absent` dedup the auto-trigger uses (`#road-compaction-auto-trigger`), so operator and automatic jobs dedup against each other and the correctness guards (declared stream tables, changelog tables, shadow-flagged tables — where re-projecting files at a higher `begin_snapshot` could resurrect tombstoned rows under COW merge-on-read) refuse an operator request exactly as they refuse the trigger. There is no force flag: those guards prevent row resurrection, not policy. Operator intent shows up only as an **eager threshold** — `min_small_files: 2` (the worker's own convergence floor) instead of the trigger's `LOOM_COMPACT_TRIGGER_FILES` — so an explicit request compacts any compactable pair without waiting for threshold N. The response is honest about what happened: **404** unknown table, **202** `{"job_id": …}` enqueued, **200** `{"job_id": null}` suppressed (ineligible, nothing to do, or already-pending). v1 deliberately does not distinguish *which* suppression reason applied.

Schema resolution follows the authoritative-schema rule: when a model is supplied the model's columns become the physical schema and inference is not consulted; without one, `datafusion_io::infer_columns` maps the Arrow schema to loom logical types. The inference map is deliberately small — `boolean`, `integer` (Int32), `long` (Int64), `double` (Float64), and `string` (Utf8/LargeUtf8) — and an unmapped Arrow type is a deterministic error, never a guess.

The Iceberg landing entrypoint routes by in-memory batch size: small requests inline as mirror-only typed rows (at/above a live-inline-byte threshold the write also enqueues a `flush_table` job), while large requests write real Parquet. Both branches emit lineage atomically and return the loom mirror snapshot id. Both also fire any data-triggered transforms (`TransformDef.on_input_commit`) reading the landed table, in the same commit transaction — see [control-plane.md](control-plane.md#transforms).

The Parquet write itself is the DataFusion compute path, now hosted in the shared `datafusion-io` crate (`write_dataset`): batches register as an in-memory table in a per-call `SessionContext`, a pure size-estimate function derives the partition count (estimated in-memory bytes × a compression factor against a target file size, clamped to a max), and DataFusion repartitions and writes N Snappy Parquet files directly to object storage — no in-memory buffering, no assumed file names (the prefix is listed and sorted). Per-file `DataFile` stats are extracted from each footer with min/max merged across all row groups, preserving pruning on multi-row-group files, and all N files register in a single `append_files` call inside the atomic commit. Defaults target ~128 MiB files (max 64) and are tunable via `WriteConfig` / `LOOM_WRITE_*` environment overlays. The compute is identity today — the seam plus high-throughput partitioned writes are the deliverable; casts and projection live with the transform work.

## Snapshot commit and lineage

Everything the landing path persists goes through the register-only snapshot-commit primitive (#32): the caller writes the data files; the control-plane `Tx` seam stages `create_table` (idempotent) and `append_files` alongside the existing `emit` (lineage) and `enqueue` (queue) methods, and `commit()` applies them in **one Postgres transaction** — snapshot, lineage event, and any downstream job become visible together or not at all. `commit` returns `Option<SnapshotId>` (`Some` when a catalog op was staged), and ingest surfaces a `None`-with-staged-ops as a hard `NoSnapshot` error rather than ignoring it. The core types (`ColumnSpec`, `DataFile`, `ColumnStat` with typed `StatValue` bounds) are format-neutral; the active table-format adapter — Iceberg, since the migration off the original DuckLake backend the primitive was first proven against — encodes them into its physical catalog, deriving format-specific details like value counts itself.

The failure posture is "no partial catalog state": gate and write failures occur before `begin()` and leave nothing; a failure between `begin()` and `commit()` rolls back. The one accepted imperfection is the write-then-commit ordering — a commit failure after the object-store write orphans the Parquet files (the catalog never references them), the lesser evil versus a catalog row pointing at a missing file; orphaned-file GC is a deferred concern (`#fut-iceberg-gc-orphan-sweep`). Every land emits a `LineageEvent`: the raw path records the landed dataset with source `http-land` and the run id; the model path records **type-named lineage** (source `http-model` plus the type), so typed rows trace to the model they landed as.

## Dataset→model binding, inference, and constraints

Binding is the validated promotion that makes a landed dataset retrievable as a typed object. `bind` (`src/services/ingest/src/bind.rs`) takes a declared `ObjectType`, requires the target table to be live in the catalog, and validates every declared property against the table's physical schema at the current snapshot: column presence, logical→physical type satisfaction via the closed logical-type vocabulary in `control-plane-core` (base scalars plus semantic aliases like `EmailAddress`; an unknown logical type is an error, never a silent pass), and nullability (a required property may not sit on a nullable column). Later hardening extended the matrix to identity validity, reserved `_`-prefixed names, and derived-property checks (the named link must exist on the type, the aggregated column must exist on the link target, and the aggregation must fit the column's type). All violations are collected and reported together; nothing is persisted on rejection, so a bound type is guaranteed-serveable — the read path can never fail on a missing or mistyped column. Extra physical columns are fine (a type is a view over the table), and validation runs at bind time only; re-validation under schema evolution stays deferred.

`POST /models/{type}` collapses the two-step land-then-bind into one governed call. It is the ingest plane's one ACL'd surface: the subject is extracted via `service_runtime::Subject` and gated deny-by-default **before** anything else — a 403 is returned whether or not the type exists, so existence never leaks pre-authorization. The gate admits **either** a `Write` grant on the `PolicyTarget::Type`, **or** a `Write` grant on the conventional landing `PolicyTarget::Table` `main.<type>`. The table alternative resolves the absent-type bootstrap deadlock (#361): a `Type` grant cannot be created before the type exists (the grant API validates type existence), but a `Table` grant is existence-unchecked, so a subject inferring a brand-new type (#260, below) pre-authorizes it by holding `Write` on the table the inferred type will occupy. Soundness is preserved — a subject authorized *only* by the table grant may write solely to `main.<type>`; a pre-existing type bound to a different physical table falls back to the (denied) type decision. With a pre-existing type (#241), the handler derives a `ModelShape` from the `ObjectType` (one column per property, `required` when non-nullable or the declared identity), runs the conformance gate (422 + violations on structural mismatch), and lands into the type's table — a typed write into an already-bound model, with no `define_type`. With an absent type (#260), an authorized subject's batch *infers* the `ObjectType` — one `PropertyDef` per Arrow field via the same landing type map, nullability carried, bound to the conventional `main.<name>` table. Identity is caller-declared via `?identity=<col>` (validated present and forced required; a wrong guess is hard to undo on a governance platform, so loom never picks one), and a `define_type` create-or-conform guard closes the concurrent-first-batch race: the loser re-resolves and conforms, 422ing if its batch differs. Inference runs only on the absent branch — a differing later batch is a conformance failure, not a schema change.

Beyond structure, the model path enforces **per-value constraints**: after the shape gate and before any write, `validate_values` runs each property's declared `PropertyConstraints` (range, length, pattern, one-of) over the decoded batches, rejecting with a 422 naming the failed rule. The same `core` `PropertyValidator` backs query-api's typed-insert action, so both write paths enforce identical rules. Model ingest is append-only throughout — the identity property is checked for presence, but no row-level dedup or upsert runs.

## Stream log tables

A dataset can be declared an **append-only log table** at ingest time via
`POST /datasets/{schema}/{table}?mode=stream&buckets=N` (default `buckets=1`;
`buckets` is ignored without `mode=stream`). The flag threads
`http.rs` → `LandRequest` → `iceberg_land`; the row's presence in the
control-plane `stream.stream_table` registry (`bucket_count` fixed at creation,
`>= 1` enforced by a DB CHECK) is the "this is a log table" marker. The mode is
immutable: `mode=stream` against an existing batch table, or a `buckets` value
that disagrees with the recorded count, is a `400`. A later flagless write
appends using the table's recorded mode. Declaration and the offset allocator
(`road-stream-substrate`, the `BucketOffsets` seam) are the substrate; this is
Fluss-style Log Tables, slice 1.

Every appended row carries three reserved **framing columns** —
`loom_change_kind` (`+I`/`+U`/`-D`, universal; set at the inline write sites),
`loom_bucket`, and `loom_offset`. For a stream table each row is assigned
`bucket = row_index % bucket_count` and a **gapless, per-bucket, 0-indexed
offset** reserved from `stream.bucket_offset` via `pg_allocate_offset` **inside
the write's transaction**, so offsets are assigned iff the write commits (a
rollback frees the run). Ordering is guaranteed per bucket, not across buckets.
Batch tables skip all stamping and are byte-identical to before the feature.

The framing is **durable and invisible**. It is registered as reserved physical
columns in the Iceberg schema for stream tables, so it survives the flush from
the inline tier into Parquet (flush reads the framing via an unfiltered
`physical_columns` read and the mirror registers it automatically from the
Iceberg schema). A single `is_reserved` filter at the one logical-schema
chokepoint (`IcebergCatalog::schema`) excludes every `loom_`-prefixed column
from all logical reads (`GET /objects`, `GET /datasets`, link traversal,
previews), so a stream table's user-facing schema and reads are byte-identical
to a batch table's — the log framing is never exposed. (Reading the log *by
offset* is the subscribe/tail feed, `#road-stream-subscribe`, slice 3 — now
shipped; see the stream capability doc.)

The **direct large-write Parquet path** (writes over the inline byte limit, which
bypass the inline tier and write Parquet straight to object storage) has full
parity: it reconciles stream mode atomically (same `Conflict`/`Validation`
rules), allocates offsets, and stamps the framing into the written Parquet, with
the allocation riding the **same Postgres transaction** as the snapshot's
pointer-CAS commit (via a caller-provided-tx commit seam and a `TxCommitCatalog`
decorator) so offsets commit iff the snapshot commits — gapless with no offset
gap on failure; a lost CAS rolls back (freeing the run), re-allocates, and
re-writes. This is the one path that holds a PG transaction across the
object-store write; the common inline/flush and batch commit paths keep their
short, object-store-free commit transaction. The two framing-unaware legacy write
paths — the transform-commit (`IcebergTx::commit`) and multi-step-action
(`write_steps`) seams — now **refuse** stream/CDC-registered targets outright (a
`stream-table target refused:` `Validation`, surfaced as a deterministic worker
*abandon* on the transform path and HTTP `422` on the action path), with a
define-time UX guard in `define_transform` catching the misconfiguration at admin
time; the guard also covers a CDC table's durable changelog. This closes the
silent-corruption window rather than threading framing through accident-only
paths — `CommitMicroBatch` ([[road-stream-continuous]]) stays the sole sanctioned
stream-output path (#416).

## Known gaps

- `#fut-ingest-overwrite-endpoint` — no dataset-level replace/overwrite ingest endpoint; the land path is append-only and the shipped overwrite primitive is wired only to the action copy-on-write path.
