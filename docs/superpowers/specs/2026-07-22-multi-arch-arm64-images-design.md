# Multi-arch (arm64) service images Design

> **Status:** built + proven. Enables building loom's four service OCI images for `linux/arm64`
> in addition to `linux/amd64`, via a **native** build (aarch64 toolchain on an arm64 executor —
> no cross-linker/sysroot). All 5 toolchain/platform layers are landed on this branch and
> **proven on real arm64 RE hardware**: a native `aarch64` glibc ELF binary builds end-to-end.
> The only remaining human step is a one-time `workflow_dispatch` to republish the RBE image
> multi-arch and repin its index digest (below).

## Problem

All four service images ship `linux/amd64` only (`deploy/images/*/apko.yaml` → `archs:
[x86_64]`), and every service binary compiled x86_64-only because the toolchain was single-arch.
We want arm64 images **without cross-compilation** — run the aarch64 toolchain on an arm64
executor so it emits and links arm64 natively (target == exec == host = aarch64).

## Established facts (verified 2026-07-22)

- **BuildBuddy routing:** executor selection is the `Arch` platform property (`amd64` / `arm64`);
  arm64 requires **self-hosted executors** (managed pool is amd64-only). weave-hand's arm64
  executors live in the **default pool** (operator-confirmed), so `Arch: arm64` alone routes —
  no `Pool` property. Sources: buildbuddy.io/docs/rbe-platforms, /docs/rbe-pools.
- **RBE base image is already multi-arch:** `gcr.io/flame-public/rbe-ubuntu24-04` at the pinned
  base digest is an OCI index with linux/amd64 + linux/arm64. No arm64-base hunt.
- **Our RBE image is single-arch amd64:** `ghcr.io/weave-hand/loom-rbe-browser@…99cada5b` is a
  plain schema2 manifest → needs a multi-arch rebuild.

## The native-arm64 requirement chain (5 layers)

Building a native arm64 binary over RE requires every host-executed tool in the compile+link
path to be aarch64 AND to run on an arm64 executor. Discovered empirically, one failing action
at a time:

### Layer 1 — aarch64 rustc/std/triple — LANDED & PROVEN (`toolchains/BUCK`)
Added `rustc-aarch64-linux` (upstream sha256 `c530b883…`). `rustc_dist` / `std_dist` /
`rustc_target_triple` now `select` on `prelude//cpu/constraints:arm64`; wasm keeps the host
x86_64 rustc (cross-compile). **Proof:** an arm64 build materialized `__rustc-aarch64-linux__`;
x86_64 builds byte-for-byte unchanged.

### Layer 2 — arm64 execution platform — LANDED & PROVEN (`platforms/defs.bzl`, `platforms/BUCK`)
`_executor_config(arch)` injects `Arch: amd64|arm64`; a new `loom_execution_platforms` rule
registers both the amd64 and arm64 platforms (from the `:linux-x86_64` / `:linux-aarch64`
`platform()` targets) into the single `ExecutionPlatformRegistrationInfo` that `.buckconfig
[build] execution_platforms` points at. One `_RBE_IMAGE` index digest serves both (each worker
pulls its arch).

### Layer 3 — exec-platform resolution — LANDED & PROVEN (`toolchains/BUCK`)
buck2 picks the first registered exec platform compatible with an action's `exec_compatible_with`
(unconstrained → amd64). The rust toolchain now carries
`exec_compatible_with = select({arm64: [arm64], DEFAULT: []})`, which propagates to every rust
compile so arm64 compiles land on the arm64 platform. **Proof (Layers 2+3):**
`prelude//rust/tools:rustc_cfg` — which failed `Exec format error` before — **builds for arm64**,
i.e. the aarch64 rustc runs natively on a default-pool arm64 executor.

### Layer 4 — aarch64 LLVM/clang — LANDED & PROVEN (`toolchains/BUCK`)
The rust prelude drives the final link through the cxx toolchain's `clang++`, which was
x86_64-hardcoded → `Exec format error` on the arm64 worker. Added `llvm-aarch64-linux` (LLVM
22.1.2 ARM64, sha256 `cf2e84d9…`) and made `hermetic_cxx_tools`'s `llvm_dist` select on arm64.
**Proof:** arm64 proc-macro links (`paste`, `seq-macro`) that failed on x86 clang now link.

### Layer 5 — RE-side native linking — LANDED & PROVEN (`toolchains/cxx_dist.bzl`, `toolchains/BUCK`)
The prelude `system_cxx_toolchain` (our `cxx-native`) hardcodes `link_binaries_locally =
link_libraries_locally = archive_objects_locally = True` (`prelude/toolchains/cxx.bzl:163-165`),
**no override attr**. So every rust link runs on the *local* executor. Fine when local host arch
== target arch; for an arm64 link driven from an x86 host it failed:
`Spawning …/cpython…/bin/python failed: Failed to spawn a process` (the link action, forced
local, can't run the arm64 toolchain on x86).

**Chosen: (A) arm64-only RE-linking cxx toolchain.** Added `native_re_cxx_toolchain` in
`cxx_dist.bzl` — the linux/clang/gnu branch of the prelude's `_cxx_toolchain_from_cxx_tools_info`
with the three locality flags `False` (so links run on the arm64 RE worker), mirroring how the
wasm toolchain already links on RE (`cxx_dist.bzl:89-98`). The `:cxx` toolchain_alias select now
routes arm64 → `:cxx-native-re` while x86_64 keeps the unchanged prelude `system_cxx_toolchain`
(local links) — **all risk confined to arm64**. Trade-off: ~140 lines vendoring the prelude cxx
impl, which must be re-checked on a prelude bump. (Rejected (B) tree-wide RE linking — it would
change x86 CI link behavior for no arm64 benefit.)

**Proof:** `//src/testing:emit-seed` built for `--target-platforms=linux-aarch64` is
`ELF 64-bit LSB pie executable, ARM aarch64, dynamically linked, interpreter
/lib/ld-linux-aarch64.so.1` (`readelf` Machine: AArch64) — a genuine native arm64 glibc binary.

## Piece — Multi-arch RBE image (`.github/workflows/rbe-image.yml`) — LANDED
Converted the single-arch `docker build` to `buildx --platform linux/amd64,linux/arm64 --push`
(+ QEMU/buildx setup); amd64-only smoke (arm64 lib closure is identical package names). The
Dockerfile is unchanged (multi-arch base, arch-agnostic apt). Publishing is a `workflow_dispatch`
run; pin the resulting **index digest** in `platforms/defs.bzl`.

## Verification status
- x86_64: all four `//src/services/{ingest,engine,worker,query-api}:*-bin` build green with
  **all** changes applied — the arm64-only selects are inert on the DEFAULT path.
- arm64 (multi-arch base image temporarily pinned for the test): `//src/testing:emit-seed` builds
  end-to-end on RE to a native `aarch64` glibc ELF (Layers 1–5 all exercised).

## Multi-arch RBE image — DONE
`rbe-image.yml` republished `loom-rbe-browser` as a multi-arch index
(`sha256:8fe89f64…`, linux/amd64 + linux/arm64), now pinned in `platforms/defs.bzl` (`_RBE_IMAGE`).
End-to-end proof against the **real** image: `//src/services/ingest:ingest-bin` built
`--target-platforms=linux-aarch64` is a native aarch64 ELF (`ld-linux-aarch64.so.1`); the amd64
build is unaffected. (Publishing needed an org `GHCR_PAT` with `write:packages` — the package
predated the repo so the `GITHUB_TOKEN` lacked write; the login step prefers `GHCR_PAT` and falls
back to `GITHUB_TOKEN`.)

## Multi-arch service images — DONE (follow-up PR)
All four service images (`ingest`, `engine`, `worker`, `query-api`) now build multi-arch
(linux/amd64 + linux/arm64). apko's `archs:` gained `aarch64` (locks regenerated), and a
loom-owned rule `//images:multiarch.bzl` composes them: homelab's `oci_image` is single-arch by
construction (`regctl image import` collapses a multi-arch index to the host manifest), so the
rule instead builds a single-arch apko base per arch (`apko build --arch`, no index to collapse),
stages that arch's binary with `regctl image mod`, and combines the two with `regctl index
create`. Push uses `regctl image copy` (not `crane push`, which only loads docker-archive tars);
`release.yml` is unchanged (same `image.push`/`image.info` target names). Verified: each image's
amd64 manifest carries the x86-64 binary and arm64 the aarch64 binary, entrypoint + nonroot user
preserved, and `regctl image copy` pushes the index to a local registry as both platforms.

- The query-api image's wasm UI (`platforms:wasm` hardcodes x86_64 cpu) is arch-agnostic, so one
  bundle is layered onto both arches (verified present on amd64 + arm64) — no per-arch UI build.
