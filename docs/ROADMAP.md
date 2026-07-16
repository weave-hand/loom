# Roadmap register

_As of 37d7a4f2._

Committed and sequenced work — open (`planned`) items only. Shipped work is
documented per subsystem in [`system-capabilities/`](system-capabilities/README.md)
(the slice-by-slice history lives in git). Deferred ideas live in
[`FUTURE.md`](FUTURE.md); known defects in [`ISSUES.md`](ISSUES.md). Grammar:
`docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md`.

## catalog

## ontology

- [ ] **Semantic description fields across the ontology** `{#road-ontology-semantic-descriptions area:ontology status:planned from:2026-07-16-ontology-semantic-descriptions-design pr:- spec:2026-07-16-ontology-semantic-descriptions-design}`
  Optional human-readable `description` on **every declarable ontology entity** — object types, properties, links, actions, action params, derived properties, and vector indexes — carried as a nullable column on each `define_*` row and surfaced on reads. Foundry-style semantic metadata: nothing in the engine consumes it, so it is pure annotation, but it is the substrate for richer generated API docs (the ontology-derived OpenAPI generator already ships), a future ontology-browsing UI, and LLM/semantic search over the schema. A link carries its own description (the relationship's meaning), distinct from its endpoint types'. **Two strictly-ordered PRs:** PR 1 is a pure constructor refactor — the 7 domain structs are built by struct literals in 746 places (611 in tests, 94 in testkit, 41 in 5 production files), so every struct first gains the fluent builder `ObjectType`/`ActionDef` already have and the literals migrate onto it, making this field (and the next) ~free at call sites. PR 2 adds the field (`serde(default, skip_serializing_if)` ⇒ **no engine-wire `.proto` change**, since ontology structs cross that wire as serde-JSON strings), one nullable-add migration over 7 `ontology.*` tables, postgres persistence + `.sqlx` regen, and the read surface: `GET /ontology/types/{name}` and the ontology-driven OpenAPI generator (both already exist and are the reason this is worth building now). Descriptions **clear on redefine** — `define_*` is replace-not-merge. Derived properties and vector indexes are **persist-only**: neither is on any read surface today, and exposing them is a separate item.

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
