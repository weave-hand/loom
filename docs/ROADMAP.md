# Roadmap register

_As of 403f7a6a._

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

- [ ] **Time-travel selector guards (retention horizon + snapshot-id validation)** `{#road-timetravel-selector-guards area:query status:planned from:2026-07-06-timetravel-reads-design pr:- spec:2026-07-06-timetravel-reads-design}`
  Merges the promoted `#fut-timetravel-retention-guard` + `#fut-timetravel-snapshot-id-validation` (entries removed 2026-07-09; one small work item, both guards, spec of record is the landed time-travel design). (1) An `?as_of`/`?as_of_snapshot` selector resolving to a snapshot whose files GC already reclaimed currently **under-reads** (partial/empty result) — add a retention-horizon guard returning 410 (contract-based: reclamation is not recorded post-hoc, so *any* out-of-window selector rejects deterministically via the same shared horizon predicate `gc_table` reclaims under, with a quiet-table exemption for live-equivalent reads). (2) The object-read path gates `as_of_snapshot` on the mirror's open-ended liveness predicate, so an id **above** the current snapshot silently reads live data — validate via a unified exact-history `Catalog::snapshot(table, id)` lookup, 404ing like the dataset path. Relates to [[fut-iceberg-time-travel-schema]]. **Plan:** `docs/superpowers/plans/2026-07-09-timetravel-selector-guards.md` (pre-written at spec time — the implementer starts from it).

## transform

- [ ] **Stream engine — Stream joins / delta-join analog (slice 5)** `{#road-stream-joins area:transform status:planned from:2026-07-06-stream-engine-design pr:- spec:2026-07-09-stream-joins-design}`
  Lookup-join (point-lookup against a PK index, [[fut-stream-pk-index]]) + micro-batch stream-stream join. True stateful incremental join deferred ([[fut-stream-incremental-join]]). **Plan:** `docs/superpowers/plans/2026-07-09-stream-joins.md` (pre-written at spec time — the implementer starts from it).

## cross-cutting

- [ ] **Scheduled maintenance jobs (generalized schedule mechanism)** `{#road-scheduled-maintenance-jobs area:cross-cutting status:planned from:to-be-planned pr:- spec:2026-07-09-scheduled-maintenance-jobs-design}`
  Promoted 2026-07-09 from `#fut-scheduled-jobs` (entry removed). Cron-like scheduling for **non-transform** job kinds (GC, compaction). Decision 2026-07-09: **generalize the landed transform-schedule mechanism** (`#road-transform-schedules`, PR #367 — croner/`next_run_at`/atomic-claim) so a schedule row carries kind + payload (e.g. `gc_table` for table X), managed via an admin HTTP endpoint — per-table granularity, one scheduling mechanism, not global env cron knobs. The spec refines the mechanics: a sibling `queue.schedule` table (not a widened transform table) whose fire folds claim-advance + deduped enqueue into **one tx** (exactly-once per firing). Future consumer: scheduled orphan-sweep GC ([[fut-iceberg-gc-orphan-sweep]]); complement to the event-driven `#road-compaction-auto-trigger` (shipped, PR #419). **Plan:** `docs/superpowers/plans/2026-07-09-scheduled-maintenance-jobs.md` (pre-written at spec time — the implementer starts from it).
