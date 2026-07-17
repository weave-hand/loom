# Roadmap register

_As of 37d7a4f2._

Committed and sequenced work — open (`planned`) items only. Shipped work is
documented per subsystem in [`system-capabilities/`](system-capabilities/README.md)
(the slice-by-slice history lives in git). Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## catalog

## ontology

## acl

- [ ] **Comprehensive auth capability — build-vs-adopt a fuller identity layer** `{#road-auth-comprehensive area:acl status:planned from:2026-06-23-auth-password-session-design pr:- spec:2026-07-01-auth-comprehensive-adopt-design}`
  The committed decision to grow loom's hand-rolled `#road-auth-password-session` foundation into a **fuller identity layer**, taken as one unit rather than a scatter of independent slices. The spec's **first job is the build-vs-adopt fork**: continue extending loom's own `auth` concern, or integrate an established auth library/service — evaluated against loom's constraints (hermetic buck2 build, no external mail infra today, `SubjectId`-centric ACL model, self-hosted deploy). Whichever path wins, its **acceptance surface** is the deferred identity follow-ons this subsumes: TOTP/OTP second factor [[fut-auth-totp-mfa]], passkeys/WebAuthn [[fut-auth-passkeys]], SAML federation [[fut-auth-saml]], per-IP login rate-limiting [[fut-auth-login-rate-limit]], password policy (forced rotation + strength) [[fut-auth-password-policy]], and session refresh / sliding expiry [[fut-auth-session-refresh]] — those `fut-auth-*` ideas stay in FUTURE as the detailed surface this item delivers against. **Out (stay deferred, not part of this item):** email reset ([[fut-auth-email-reset]], hard-blocked on mail infra), service-token scoping ([[fut-auth-token-scoping]]) and the first-class auth-admin capability ([[fut-auth-admin-capability]]) (both gate on a real least-privilege / multi-operator need), and the `Debug`-redaction hygiene fix ([[fut-auth-credential-debug-redact]]). Depends on `#road-auth-password-lifecycle` landing first. Spec to follow — expect it to decompose into slices once the build-vs-adopt call is made.

## query

## transform

## iceberg

## deploy

- [ ] **The worker gets a deployment path — standalone in-process + Helm worker** `{#road-deploy-worker area:deploy status:planned from:2026-07-16-worker-deployment-path-design pr:- spec:2026-07-16-worker-deployment-path-design}`
  **No shipped loom runs the worker.** `worker-bin` is the only thing constructing the job loop outside tests (`control_plane_worker` is depended on by exactly two BUCK files — itself and `src/services/worker`; the sole non-test `Worker::new` is `worker/src/main.rs:81`), and it has **no OCI image** (`deploy/images/` ships ingest/query-api/engine only), **no Helm template**, and **no place in `standalone`'s dep closure** — so neither the chart nor the single `loom` binary drains anything. Every queue-driven job kind (`flush_table`, `gc_table`, `compact_table`, `sweep_orphans`, `transform`, `typed-transform`, `stream_consolidate`, `stream_mv`, `build_vector_index`) is enqueued and accumulates forever; transform, compaction, GC, and micro-batch MVs cannot run in any deployed configuration. **Slice 1 of 2, UDS-only:** compose the `worker` library in-process into `standalone::serve_composite` (spawned after the existing `eng_ready_rx` gate, dialing the internal engine socket, reusing the already-resolved `cfg.object_store` so the zero-pool `parse_from_env` warehouse requirement never arises), and add a worker image + a worker Deployment carrying its own engine sidecar over a shared `engine-sock` emptyDir — plus the `release.yml` push lines and the `deploy/chart/BUCK` digest pin without which the chart would reference an unbuilt image. The gap was anticipated but untracked: `[[fut-worker-lazy-compact-ctx]]` already said "when the worker Helm manifest is authored either set `LOOM_WAREHOUSE_URI` there or make `CompactCtx` lazy" — this item takes the first branch and that idea stays deferred. **Slice 2** (configurable TCP transport, distributed independently-scaled components, and the TLS/auth trust boundary a networked engine forces) is tracked by `[[fut-engine-wire-multi-tls]]` and is out of scope here.

## cross-cutting
