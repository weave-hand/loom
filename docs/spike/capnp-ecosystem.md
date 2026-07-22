# Cap'n Proto (Rust) — Ecosystem Spike

> Spike note, June 2026. De-risks the transport decision for the **engine-wire**
> (the typed RPC boundary where one process owns Postgres/the engine and other
> roles — starting with the flush `worker` — become clients over a local unix
> socket). Not a spec; informs the "Cap'n Proto vs tonic vs tarpc" call. Sources
> are linked inline.

## Why we're looking at it

The engine-wire's first slice (design doc in git history: the `…engine-wire…` spec, pending)
needs a wire format + RPC mechanism between loom's own Rust processes on **one
computer**, over a **unix-domain socket**. The user wants Cap'n Proto for its
zero-copy reads and a language-neutral IDL. This spike checks whether the Rust
ecosystem and loom's hermetic buck2 build can actually carry it.

## The crates

[capnproto-rust](https://github.com/capnproto/capnproto-rust) is one repo, four
crates:

| Crate | Role |
|-------|------|
| `capnp` | Runtime: readers/builders over Cap'n Proto messages (zero-copy), packed & unpacked serialization, reflection. `no_std`/`no-alloc` capable. |
| `capnpc` | Code generator. A `build.rs` `CompilerCommand` (or a manual invocation) that turns `.capnp` schema → Rust. |
| `capnp-rpc` | "Level-1" RPC: promise pipelining, capabilities, the `twoparty` two-party network. |
| `capnp-futures` | Async read/write of message *frames* over an `AsyncRead`/`AsyncWrite` (e.g. a tokio `UnixStream`). |

**Maintenance:** actively maintained, primarily by David Renshaw (`dwrensha`).
Steady release cadence — `0.18` (Sep 2023) → `0.22` "async methods" (27 Oct
2025) → `0.23` "`Rc<Self>` in RPC methods" (28 Oct 2025); the `capnp` runtime
crate is at **0.26** as of mid-2026. ~2.5k stars. **Bus factor: essentially one
maintainer** — a real consideration for a long-lived foundation, mitigated by the
format being a stable spec and the runtime crate being small and stable.
[releases / blog](https://dwrensha.github.io/capnproto-rust/index.html)

## Finding 1 — codegen needs the C++ `capnp` binary (build cost)

`capnpc`'s `CompilerCommand` is a wrapper that shells out to the **C++ `capnp`
compiler**: it runs the equivalent of `capnp compile -orust:$OUT_DIR
schema/foo.capnp`. There is **no pure-Rust schema compiler** — the `capnp`
binary (and the `capnpc-rust` plugin it invokes for the `-orust` backend) must
exist at build time.
[capnpc docs](https://docs.rs/capnpc/) ·
[lib.rs/capnpc](https://lib.rs/crates/capnpc)

Implications for loom's hermetic buck2 build:

- We can't lean on `capnpc`'s `build.rs` (reindeer controls third-party
  buildscripts; the host has no `capnp` on PATH). Instead, **invoke codegen as an
  explicit buck2 genrule** — the same shape loom already uses for vendored tool
  binaries (`//tools:jq`, `//tools:rust-code-analysis`, …): a genrule runs the
  vendored `capnp compile` over our `.capnp` files and emits `*_capnp.rs`, which
  the engine/worker crates consume as generated sources.
- Two binaries are needed at codegen time: the **C++ `capnp`** compiler and the
  **`capnpc-rust`** plugin (a Rust binary, buildable from the `capnpc` crate as a
  buck2 `rust_binary`).
- The C++ `capnp` binary itself: Cap'n Proto ships source tarballs, not Linux
  prebuilts. Cleanest fit with loom's established pattern is to **cut a prebuilt
  `capnp` release on a fork and vendor it per-arch via `http_archive`** — exactly
  how loom already sources `btd`/`rust-code-analysis`/`duplo` (see CLAUDE.md "Dev
  tools"). Avoids pulling a full C++ toolchain into the build graph.
- Precedent exists (Bazel:
  [capnp-bazel-example](https://github.com/caphindsight/capnp-bazel-example)), so
  the genrule approach is well-trodden; it's just real one-time work.

**Cost verdict:** non-trivial but **on-pattern and one-time**. This is the single
biggest reason capnp is more setup than tonic-with-`protox` (pure-Rust protobuf,
no external binary) or tarpc (no codegen at all).

## Finding 2 — `capnp-rpc`'s `RpcSystem` is `!Send` (architecture constraint)

The decisive runtime fact. From the crate docs, verbatim:

> "An `RpcSystem` is a non-`Send`able `Future` and needs to be driven by a task
> executor. A common way to accomplish that is to pass the `RpcSystem` to
> `tokio::task::spawn_local()`."

[capnp-rpc lib.rs](https://github.com/capnproto/capnproto-rust/blob/master/capnp-rpc/src/lib.rs)
· making it `Send` is a [known, unresolved
ask](https://github.com/capnproto/capnproto-rust/issues/96).

So the full RPC stack must run on a **single-threaded executor** (`LocalSet` /
current-thread runtime), per connection. In an engine that also does
**multi-threaded sqlx** work, you'd bridge the `!Send` RPC loop and the `Send`
DB/runtime across a channel or a dedicated thread. Workable, but it's a genuine
design wrinkle and a learning curve (promise pipelining, capabilities, vat IDs).

### The consequence: two ways to "use Cap'n Proto"

- **(A) Full `capnp-rpc`** — promise pipelining, capabilities, `twoparty`
  network. Powerful (and the right tool if we ever want server-issued
  capabilities or pipelined calls), but `!Send`, heavier, steeper.
- **(B) `capnp` serialization + a hand-rolled framed request/response** over a
  tokio `UnixStream` (frames via `capnp-futures`, or a length prefix + packed
  reader). **No `RpcSystem`, so no `!Send` constraint — plain multi-threaded
  tokio.** We get capnp's zero-copy wire + `.capnp` IDL without the RPC
  machinery. We give up pipelining and capabilities — **neither of which v0
  (`dequeue`/`complete`/`fail`/`heartbeat`/`flush_table`, simple req→resp)
  needs.**

The buck2 codegen cost is identical for A and B; B simply drops the `capnp-rpc`
dependency and keeps the concurrency model vanilla.

## Recommendation

**Adopt Cap'n Proto via path (B): `capnp` for the wire encoding + a small framed
request/response protocol over the unix socket; defer `capnp-rpc` until a feature
(pipelining/capabilities) actually demands it.**

Rationale:

- Keeps the engine on ordinary multi-threaded tokio + sqlx — no `!Send`
  islanding, no `LocalSet` bridging — so the *architecture* (engine owns PG,
  worker is a zero-pool client) is what we prove first, not capnp-rpc's model.
- Still lands the things the user wants now: the zero-copy `capnp` runtime and a
  language-neutral `.capnp` schema, with schema-evolution rules suited to a
  long-lived foundation.
- Leaves a clean upgrade path: the schema and message types are reusable if we
  later switch the framing to full `capnp-rpc`.

The one cost we accept up front regardless of A/B is the **`//tools:capnp`
vendoring + codegen genrule**. If that yak-shave turns out deeper than expected
when we spike the buck2 wiring, the documented fallbacks are tonic (mature,
`protox` avoids `protoc`, but `!`zero-copy and heavier) or tarpc (fastest, pure
Rust, but Rust-only — no language-neutral IDL).

## Open questions for the engine-wire spec

1. **Framing for path (B):** `capnp-futures` message frames vs. a explicit
   length-prefix + packed message. (Lean: `capnp-futures`, it's purpose-built.)
2. **Schema shape:** how the `Queue` ops + `flush_table` map to request/response
   message structs; the error model over the wire (a result union vs. a status
   field).
3. **buck2 codegen wiring:** confirm the `//tools:capnp` prebuilt source (fork
   release vs. C++-from-source) and the `capnpc-rust` plugin `rust_binary`, then
   the genrule that emits `*_capnp.rs`. This is the part most likely to surprise;
   worth its own small buck2 spike before the spec is final.
4. **reindeer:** pin `capnp` + `capnp-futures` versions; check for `links`/native
   concerns (none expected — these are pure Rust runtime crates; the C++ binary
   is a *tool*, not a crate dep).
