# Engine-Wire Transport — Decision Record

> Spike + decision, June 2026. Resolves the transport for the **engine-wire**
> (the typed boundary where one process owns Postgres/the engine and other roles
> become clients over a local unix socket, starting with the flush `worker`).
> Companion to `docs/spike/capnp-ecosystem.md`. Sources linked inline.

## Decision

**tonic (gRPC) for the control plane + Apache Arrow Flight for the data plane,
both over a unix-domain socket.** Cap'n Proto was explored and rejected.

## How we got here

1. **Cap'n Proto** was the initial favourite (zero-copy, language-neutral IDL).
   The capnp spike (`capnp-ecosystem.md`) found two costs: codegen *requires* the
   C++ `capnp` binary (a vendored tool + genrule), and `capnp-rpc`'s `RpcSystem`
   is `!Send` (single-threaded executor + a channel bridge to `Send` sqlx work).
2. **Zero-copy doesn't actually apply to loom.** Control messages
   (`dequeue`/`flush_table`/`complete`) are tiny — zero-copy is a non-event. And
   loom's bulk data is **Arrow**; putting Arrow into capnp means transcoding
   Arrow→capnp→Arrow, which *destroys* zero-copy. The zero-copy that capnp
   advertised is, for an Arrow-native engine, already delivered by **Arrow IPC**.
3. **The data must cross the wire too** (reads/writes through the engine, not
   just control). For Arrow data, the purpose-built answer is **Arrow Flight**
   (gRPC + Arrow IPC streaming), which keeps the Arrow buffers in their
   zero-copy representation end-to-end. That pulls the whole stack toward
   **tonic/gRPC** for coherence — one wire, one toolchain.

Net: the property that made capnp exciting (zero-copy) is supplied by Arrow, not
capnp; and the data-plane requirement points at Flight, which is tonic-based. So
the stack unifies on tonic.

## Spike findings (tonic / Flight in Rust, for loom's hermetic buck2 build)

### Protoc-less codegen — confirmed (the decisive win)

`prost-build` normally needs the `protoc` binary, **but** the pure-Rust compiler
[`protox`](https://lib.rs/crates/protox) removes it: `protox::compile()` produces
a `FileDescriptorSet`, fed to `tonic_build::configure().build_server(true)
.compile_fds(fds)` (or `prost_build::compile_fds`).
[tonic-prost-build](https://docs.rs/tonic-prost-build) ·
[protox](https://crates.io/crates/protox)

- **No external compiler, no vendored C++ tool** — the entire codegen path is
  crate deps. In buck2: a small first-party codegen `rust_binary` (protox +
  tonic-prost-build) invoked by a genrule emitting the generated `.rs`. Strictly
  simpler than the capnp `//tools:capnp` + plugin wiring.
- (Current crate split: tonic moved prost integration into `tonic-prost-build`;
  the FDS path is what keeps it protoc-free.)

### tonic over a unix socket — confirmed

`tokio::net::UnixListener` (or `UnixListenerStream`) +
`Server::serve_with_incoming(_shutdown)`, with `UdsConnectInfo` implementing
`Connected` for connect-info. There is a documented tonic `uds` example.
[tonic UDS issue/example](https://github.com/hyperium/tonic/issues/826)

### arrow-flight — confirmed, and version-aligned

[`arrow-flight`](https://docs.rs/arrow-flight/) is a first-party arrow-rs crate
on the same release train as the `arrow`/`parquet` crates loom already pins. The
`FlightService` trait exposes `Handshake`/`ListFlights`/`GetFlightInfo`/`DoGet`/
`DoPut`/`DoExchange`/`DoAction`/`ListActions`. `DoGet`/`DoPut` stream
`RecordBatch`es as Arrow IPC; `DoAction` carries opaque app actions.

- **Caveat (data-plane, deferred):** `arrow-flight` pins **one** arrow major;
  loom straddles arrow 57 (postgres/iceberg) and 58 (ingest). The engine will
  pick a lane when the read/write vertical lands. Not a slice-1 concern.

## Architecture that falls out

- **One tonic server, one UDS.** Hosts:
  - **slice 1 — a custom `EngineControl` tonic service** (`dequeue`,
    `flush_table`, `complete`, `fail`, `heartbeat`). Typed, purpose-built —
    *not* shoehorned into Flight's opaque `DoAction`.
  - **slice 2+ — the `arrow-flight` `FlightService`** alongside it, for Arrow
    data (`DoGet`/`DoPut`). Control and data split cleanly across the two
    services on the same socket.
- **Flush vertical (slice 1) is control-plane only** — flush moves no data over
  the wire (the engine does the row/Parquet work internally), so Flight isn't
  exercised until the read vertical. Slice 1's job is to prove the *architecture*
  (engine owns PG, worker is a zero-pool client), on ordinary multi-threaded
  tokio + sqlx — no `!Send` islanding, no channel bridge.

## Status

**Slice 1 — flush vertical: LANDED.**

- `src/services/engine-wire/` — `EngineControl` proto + tonic codegen (pure-Rust
  protox, no protoc), `GrpcQueueClient` (impl `Queue`), UDS client connector.
- `src/services/engine/` — `EngineControlService` (tonic server); `engine-bin`
  binary that boots from env, binds the UDS, and serves.
- `src/services/worker/` — zero-pool `worker-bin`: connects to the engine over
  the UDS via `GrpcQueueClient`, runs `control_plane_worker::Worker`, drains
  `flush_table` jobs via `handle_flush`. No Postgres in the binary dep closure.
  Proven by `//src/services/worker:e2e` (fixture test): inline threshold triggers
  job → client dequeues → `handle_flush` flushes over wire → table file-backed.

Arrow Flight data-plane (slice 2+) remains deferred pending the read/write
vertical design.

## Resolved questions

1. **`EngineControl` schema** — `engine_control.proto`; errors as gRPC `Status`.
2. **buck2 codegen rule** — `//src/services/engine-wire:codegen` (protox +
   tonic-prost-build rust_binary) + `pb-gen` genrule.
3. **Engine process lifecycle** — `LOOM_ENGINE_SOCKET` env; `ctrl_c` shutdown;
   worker as the first zero-pool `EngineControl` client.

## Remaining open questions

4. **Data-plane (slice 2, deferred)** — the arrow-major lane for `arrow-flight`;
   `DoGet`/`DoPut` ticket/descriptor design for reads/writes through the engine.
