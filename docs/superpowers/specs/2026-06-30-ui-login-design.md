# UI login — design

**Date:** 2026-06-30
**Status:** approved (brainstorm), pending implementation plan
**Area:** ui, query-api (devx/serving)
**Branch:** `feature/ui`

## Goal

Add **login** to loom's Yew web UI (`src/ui/`), and wire the backend so the *same*
wasm artifact works in **both** deploy topologies the project wants to support:

- **Tight deploy** — `query-api` serves the UI bundle itself (same origin, no CORS).
- **Detached deploy** — the UI is served standalone (its own origin) and calls
  `query-api` cross-origin (CORS).

loom already has the full login **backend** (`service_runtime/src/auth.rs`, mounted on
both the `query-api` and `ingest` binaries): `POST /auth/login` (`{username,password}`
→ `{token}`), `POST /auth/logout` (Bearer, revokes the session), bearer-token `Subject`
extraction, session TTL, and bootstrap-admin seeding. This work is therefore a **frontend
feature plus two backend serving switches** — it does not touch the auth logic.

## Scope decisions (from brainstorming)

- **Slice size:** the full slice — UI login **and** query-api static serving (same-origin)
  **and** a CORS layer (cross-origin) — so login is live in both topologies.
- **Token persistence:** `sessionStorage` (survives reload within a tab; cleared on tab
  close). Not localStorage (broader XSS/disk exposure), not in-memory (reload logs out).
- **API base:** resolved at **runtime**, not build time, so one bundle serves both
  topologies. Default same-origin; overridable for the detached case.

## Architecture

Three cohesive, independently-testable components.

### A. UI login (`src/ui/`)

Yew app with auth state. No token → `<Login>` form; has token → `<Authenticated>` view
(for now just a "logged in" indicator + logout button — there is no other UI yet). Split
into focused modules so `main.rs` stays small:

- **`config.rs`** — `api_base() -> String` reads `window.LOOM_CONFIG.apiBase` (absent/empty
  → `""` = relative/same-origin). `url(base, path) -> String` joins them (a `""` base
  yields the relative `path`; a non-empty base is trimmed of a trailing `/` and prefixed).
- **`session.rs`** — `load() -> Option<String>`, `store(token: &str)`, `clear()` over
  `sessionStorage['loom_token']`. The only place web-storage is touched.
- **`auth.rs`** — `gloo-net` calls:
  - `login(base, username, password) -> Result<String, AuthError>`: `POST {base}/auth/login`
    with a JSON body; on 200 parse `{token}`; map 401 → `AuthError::BadCredentials`, other
    non-2xx → `AuthError::Server(status)`, transport/JSON failure → `AuthError::Network`.
  - `logout(base, token)`: `POST {base}/auth/logout` with `Authorization: Bearer <token>`;
    best-effort (errors ignored — the local session is cleared regardless).
- **`main.rs`** — `<App>` seeds `Option<token>` from `session::load()`. `<Login>` is a
  controlled form (username, password) that calls `auth::login` (via
  `wasm_bindgen_futures::spawn_local`), on success `session::store` + set state, on error
  renders the mapped message inline. `<Authenticated>` shows the logged-in state + a logout
  button that calls `auth::logout`, then `session::clear` + reset state.

**New third-party deps** (reindeer; the wasm `[platform]` config from the previous slice
gates these correctly): `gloo-net` (fetch), `wasm-bindgen-futures` (spawn async from
callbacks), and `web-sys` features `Window` + `Storage`. Expect a few buildscript fixups.

**Error handling:** every network/JSON failure maps to a typed `AuthError` surfaced as
inline form text. The crate keeps its crate-level `#![allow(clippy::pedantic,
clippy::restriction)]` (for the `html!` macro), but the Rust logic avoids `unwrap`/`panic`
on fetch/JSON results.

### B. Runtime API base (the "both topologies" mechanism)

The bundle ships a small, **overridable** `config.js` that `index.html` loads *before* the
wasm module:

```js
// src/ui/config.js  (shipped default — same-origin)
window.LOOM_CONFIG = { apiBase: "" };
```

- Empty `apiBase` → relative URLs (`/auth/login`) → **same origin**. The **tight deploy**
  works with zero config.
- The **detached deploy**'s static host **overrides `config.js`** to set `apiBase` to the
  query-api origin (e.g. `"https://query-api.example"`). No rebuild; the wasm is untouched.

`config.js` is added to the `:bundle` genrule output alongside `index.html`. Because it is
a separate file (not baked into the wasm), a deployment replaces it without rebuilding.

### C. query-api backend (`src/services/query-api/`)

Two additions localized to router assembly in `main.rs`; existing handlers/routes untouched.

- **Static serving** — if `LOOM_UI_DIR` is set, attach a `tower-http`
  `ServeDir::new($LOOM_UI_DIR)` with an `index.html` SPA fallback as the router's
  `.fallback_service(...)`. Unmatched GETs (`/`, `/app.js`, `/app_bg.wasm`, `/config.js`)
  serve the bundle; the existing `/auth/*` and `/objects/*` routes take precedence. Unset
  ⇒ no static serving (pure-API deploy). Needs tower-http's `fs` feature.
- **CORS** — a `tower-http` `CorsLayer` applied as an outer layer over the whole app
  (wrapping both public `/auth/login` and protected routes). Allowed origins from
  `LOOM_CORS_ALLOWED_ORIGINS` (comma-separated; empty ⇒ no CORS layer, so the tight deploy
  adds nothing). Allow methods `GET/POST/OPTIONS`; allow `Authorization` + `Content-Type`
  request headers, so the detached UI's preflight + Bearer calls pass. Needs tower-http's
  `cors` feature.
- **Deps/features** — add `fs` + `cors` to query-api's `tower-http` dependency; re-buckify.

## Data flow

```
Tight (LOOM_UI_DIR set, no CORS):
  browser → GET / → query-api ServeDir → index.html + app.js + config.js(apiBase:"")
  browser → POST /auth/login (same origin) → {token} → sessionStorage
  browser → API calls with Authorization: Bearer <token>

Detached (UI on its own origin; query-api CORS allows it):
  static host → index.html + app.js + config.js(apiBase:"https://query-api…")
  browser → OPTIONS/POST https://query-api…/auth/login → CorsLayer allows → {token}
  (otherwise identical)
```

## Testing

- **query-api (native `rust_test`, loom's integration-only pattern):**
  - Static: with `LOOM_UI_DIR` → a temp dir holding `index.html`, assert `GET /` returns it,
    `GET /some/spa/route` falls back to `index.html`, and `GET /objects/...` still routes to
    the API (the fallback does not shadow real routes). With `LOOM_UI_DIR` unset, `GET /`
    is a 404 (no static serving).
  - CORS: with an allowed origin configured, an `OPTIONS` preflight returns the
    `Access-Control-Allow-Origin/Methods/Headers`; a disallowed origin does not. Standard
    axum `oneshot` tests.
- **UI (`src/ui/`):** wasm cannot run under buck2, so the **build assertion**
  (`buck2 build //src/ui:bundle`) is the structural test, **plus** the pure logic —
  `config::url(base, path)` join and the `AuthError`-from-status mapping — is extracted into
  a host-compiled **native `rust_test`** so it is actually exercised (these functions take
  plain values, no `web-sys`). The login round-trip is verified manually: detached via
  `buck2 run //src/ui:serve` against a locally-run query-api with `LOOM_CORS_ALLOWED_ORIGINS`
  set, and tight via query-api with `LOOM_UI_DIR` set — confirming login → authed view →
  logout in a browser.

## Out of scope (follow-ups)

- The `deploy//` cell wiring (copy `dist/` into the tight image, set `LOOM_UI_DIR`; publish
  the detached static image). This spec delivers the serving *capability* + env switches,
  not the Helm/apko changes.
- Any UI beyond login + a placeholder authenticated view (no data views, no router, no
  registration/password-reset — the backend has no such endpoints).
- A `/auth/whoami` endpoint / showing the logged-in username (login returns only a token;
  the UI can show the username the user typed if desired, but no new backend endpoint).
- Refresh tokens / silent re-auth (session TTL governs expiry; an expired token → the next
  API call 401s → the UI clears the session and shows `<Login>`).
