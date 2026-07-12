# Roadmap register

_As of 37d7a4f2._

Committed and sequenced work — open (`planned`) items only. Shipped work is
documented per subsystem in [`system-capabilities/`](system-capabilities/README.md)
(the slice-by-slice history lives in git). Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## catalog

## acl

- [ ] **Comprehensive auth capability — build-vs-adopt a fuller identity layer** `{#road-auth-comprehensive area:acl status:planned from:2026-06-23-auth-password-session-design pr:- spec:2026-07-01-auth-comprehensive-adopt-design}`
  The committed decision to grow loom's hand-rolled `#road-auth-password-session` foundation into a **fuller identity layer**, taken as one unit rather than a scatter of independent slices. The spec's **first job is the build-vs-adopt fork**: continue extending loom's own `auth` concern, or integrate an established auth library/service — evaluated against loom's constraints (hermetic buck2 build, no external mail infra today, `SubjectId`-centric ACL model, self-hosted deploy). Whichever path wins, its **acceptance surface** is the deferred identity follow-ons this subsumes: TOTP/OTP second factor [[fut-auth-totp-mfa]], passkeys/WebAuthn [[fut-auth-passkeys]], SAML federation [[fut-auth-saml]], per-IP login rate-limiting [[fut-auth-login-rate-limit]], password policy (forced rotation + strength) [[fut-auth-password-policy]], and session refresh / sliding expiry [[fut-auth-session-refresh]] — those `fut-auth-*` ideas stay in FUTURE as the detailed surface this item delivers against. **Out (stay deferred, not part of this item):** email reset ([[fut-auth-email-reset]], hard-blocked on mail infra), service-token scoping ([[fut-auth-token-scoping]]) and the first-class auth-admin capability ([[fut-auth-admin-capability]]) (both gate on a real least-privilege / multi-operator need), and the `Debug`-redaction hygiene fix ([[fut-auth-credential-debug-redact]]). Depends on `#road-auth-password-lifecycle` landing first. Spec to follow — expect it to decompose into slices once the build-vs-adopt call is made.

## query

- [ ] **Stream subscribe on the production wire (+ consistent-snapshot feed read)** `{#road-stream-subscribe-wire area:query status:planned from:2026-07-08-stream-subscribe-design pr:- spec:2026-07-12-stream-subscribe-wire-design}`
  Promoted 2026-07-12 from `#fut-stream-subscribe-wire` (entry removed). Serve the three `ServingEngine` feed methods over engine-wire — a `ChangelogFeedTicket` Flight `do_get` (mirroring `VectorSearchTicket`), a `ChangelogLatest` unary RPC, and an `AwaitChangelog` unary long-poll (mirroring `AwaitJobs`, client deadline = timeout + 2s) — implemented on `EngineServingClient`, so `GET /objects/{type}/changes` stops answering 501 on the production deployment. **Folds in the [[iss-stream-feed-torn-read]] fix** (operator decision 2026-07-12, Part A of the spec): pin the changelog + base snapshots atomically (one-statement dual read) and read the inline tier as-of the pin, so the feed's XOR invariant holds under a concurrent flush; that ISSUES item closes with this one. Prerequisite for a networked subscribe deployment; the log-table feed ([[road-stream-log-table-subscribe]]) rides the same kind-agnostic dispatch.
- [ ] **Log-table (non-CDC) subscribe** `{#road-stream-log-table-subscribe area:query status:planned from:2026-07-08-stream-subscribe-design pr:- spec:2026-07-12-stream-log-table-subscribe-design}`
  Promoted 2026-07-12 from `#fut-stream-log-table-subscribe` (entry removed). Log (append-only) tables carry offsets but no separate changelog — the events *are* the data — so their tail feed reads the base table's offset-framed rows directly (files ∪ inline, `mv_delta_scan`'s union shape, constant `+I` change kind), relaxing the `Cdc`-only positions probe (`changelog_positions_latest`). Cursor + NDJSON contract unchanged; engine-side kind dispatch means the wire ticket from [[road-stream-subscribe-wire]] serves log tables transparently. Independent of the wire item — whichever lands second inherits the other.
- [ ] **Fold the Flight export knobs into the typed config seam** `{#road-flight-export-config-seam area:query status:planned from:2026-06-26-governed-flight-export-design pr:- spec:2026-07-12-flight-export-config-seam-design}`
  Promoted 2026-07-12 from `#fut-flight-export-config-seam` (entry removed). `LOOM_FLIGHT_BIND_ADDR`/`LOOM_EXPORT_MAX_ROWS` move off the raw `env.get`/`parse_var` reads in query-api's `serve()` onto a `FlightExportTuning` section of `QueryApiConfig` — a verbatim `SqlWireTuning` sibling (defaults < file < env, startup validation naming the key), env names unchanged so no deployment breaks. Smallest item in the 2026-07-12 batch.
## transform

- [ ] **MV watermark-aware GC (source-retention floor)** `{#road-mv-watermark-aware-gc area:transform status:planned from:2026-07-09-stream-continuous-design pr:- spec:2026-07-12-mv-watermark-aware-gc-design}`
  Promoted 2026-07-12 from `#fut-mv-watermark-aware-gc` (entry removed). `gc_locked` gains a durable invariant: for a table that is a micro-batch MV **source**, reclaim holds any row/file at or above the per-bucket `min(next_offset)` floor across `stream.mv_watermark` (a registered-but-unrun MV floors at 0), so a lagging MV's unread tail survives GC instead of silently losing events — today's only protection is "retention must exceed the slowest MV's lag". Dropped sources bypass the floor (loud warning naming the stranded MV); holds are observable (`GcSummary.held_by_mv_floor` + logs). Built to union a second floor source when [[fut-stream-consumer-offsets]] lands.

## iceberg

- [ ] **Orphaned-Parquet GC sweep** `{#road-iceberg-gc-orphan-sweep area:iceberg status:planned from:2026-06-30-iceberg-gc-dropped-table-design pr:- spec:2026-07-12-iceberg-gc-orphan-sweep-design}`
  Promoted 2026-07-12 from `#fut-iceberg-gc-orphan-sweep` (entry removed). The third GC source: a warehouse-scoped, schedulable `sweep_orphans` job kind (an immediate consumer of the landed `/admin/schedules` surface) that LISTs the warehouse, diffs **pattern-scoped** objects (`*.parquet` + Puffin only — Iceberg metadata/manifests are excluded by scope, never diffed) against every mirror-referenced path (**all** `data_file` rows regardless of `end_snapshot`, plus `vector_index.puffin_path`), and deletes the unreferenced remainder older than a write-race grace (`LOOM_ORPHAN_SWEEP_GRACE_SECS`, default 24h). LIST-before-read ordering + the grace window make concurrent writers/GC safe; no dry-run in v1 (operator decision 2026-07-12).

## cross-cutting
