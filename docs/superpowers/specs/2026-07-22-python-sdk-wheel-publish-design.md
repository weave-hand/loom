# Python SDK Wheel Build + GitHub Package Publish Design

> **Status:** planned (`road-python-sdk-wheel-publish`, minted 2026-07-22).
> Follow-on to the v1 SDK (`2026-07-21-python-sdk-v1-design.md`); distribution
> slice — nothing in the SDK's code surface changes.

## Problem

`loom-sdk` exists only as source in the monorepo. There is no built wheel and
no published artifact: a consumer outside the repo cannot `pip install` it or
fetch it from any registry. loom already publishes its other deployables to
ghcr as OCI artifacts (four service images + the Helm chart, via buck2 `.push`
runnables under `deploy//`); the SDK should join them.

## Decisions (operator, 2026-07-22)

- **The wheel is a buck2 target — no `uv build`, no PEP 517 backend.** The
  build system is buck2; the wheel comes out of `buck2 build`.
- **Use the prelude's `python_wheel` rule** (`prelude/python/python_wheel.bzl`)
  rather than a hand-rolled wheel-writer. It consumes `python_library` deps,
  emits the correctly-named wheel via its bundled writer tool
  (`prelude//python/tools:wheel`), handles PEP 503 name normalization,
  `Requires-Dist` (via `requires`) and extra metadata, and
  `system_demo_toolchains()` already instantiates the
  `toolchains//:python_wheel` toolchain it needs. Both `python_library`
  targets it packages (`:loom-sdk`, `:loom-sdk-pydantic`) already exist.
- **Publish target is GitHub Packages via ghcr as an OCI artifact**
  (`ghcr.io/weave-hand/loom-sdk`) — GitHub Packages has no pip-protocol Python
  registry, so the wheel lands the same way the images and chart do, under the
  repo's Packages tab. Push tooling is the already-pinned
  `homelab//buck2/bin:regctl` (`regctl artifact put` speaks OCI artifacts
  natively; no new vendored tool).
- **Continuous publish runs as a BuildBuddy workflow action**; the versioned
  release leg stays in `release.yml` (it owns the GitHub Release and the
  `workflow_dispatch` version input, which BuildBuddy actions lack).
- **The wheel is additionally attached to the versioned GitHub Release** so a
  plain-pip path exists with zero extra infra:
  `pip install https://github.com/weave-hand/loom/releases/download/vX.Y.Z/loom_sdk-X.Y.Z-py3-none-any.whl`.

## Build: `//src/sdk/python:wheel`

```python
python_wheel(
    name = "wheel",
    dist = "loom-sdk",
    version = "0.1.0",   # must match pyproject.toml [project] version — see drift guard
    libraries = [":loom-sdk", ":loom-sdk-pydantic"],
    requires = [
        "httpx>=0.28",
        "pyarrow>=25",
        'pydantic>=2.11; extra == "pydantic"',
    ],
    extra_metadata = {"Provides-Extra": "pydantic"},
    platform = "any",    # pure-python; overrides the toolchain's linux_x86_64 default
)
```

- Output: `loom_sdk-0.1.0-py3-none-any.whl` (rule normalizes `loom-sdk` per
  PEP 503 for the filename; `METADATA` keeps the display name).
- The `pydantic` subpackage ships **in** the wheel (same distribution); the
  extra only gates the `pydantic` dependency — matching the sdist-less
  `pip install loom-sdk[pydantic]` contract from v1.
- `libraries` is an explicit list, so only first-party sources are packaged —
  third-party deps (`httpx`/`pyarrow` `python_library`s from
  `//third-party/python`) stay out of the wheel and are declared via
  `requires` instead.
- The rule computes `Requires-Python: ==3.*` from the toolchain's `py3` tag —
  permissive and compatible with the pyproject's `>=3.13` (pip enforces the
  installed interpreter; the runtime floor stays documented in pyproject).
- `pyproject.toml` is untouched (`[tool.uv] package = false` stays; muntjac and
  the `muntjac-check` hook see no change). No sdist is built — nothing
  consumes one.
- The target is hermetic (no network), so it joins the normal `//src/...` CI
  sweep — unlike the `deploy//` image targets, which are network-dependent and
  deliberately excluded.

### Version drift guard

The BUCK `version` attr and pyproject's `[project] version` are two copies of
one fact. A unit test in the existing `:units` suite (or a sibling test
target) reads `pyproject.toml` (`tomllib`, stdlib) and asserts it equals the
version injected from BUCK (via the test rule's `env`), so drift fails the
normal test sweep. The release workflow adds a second, end-of-pipe guard (below).

## Publish: `deploy//sdk:wheel.push`

A small runnable in the `image.push` style (lives in `deploy//` beside the
other publish machinery, keeping credentialed/network actions off the `//src`
sweep): takes one or more tags as args and pushes the built wheel to
`ghcr.io/weave-hand/loom-sdk` via `regctl artifact put`, with the wheel file as
the artifact blob (media type: the standard wheel/zip content type; artifactType
set so the Packages UI renders sensibly). Registry login follows the existing
pattern (`regctl registry login` with a token on stdin, mirroring the crane/helm
logins in `release.yml`).

## Cadence

Two legs, split by what each runner can do:

1. **BuildBuddy workflow action `publish-wheel`** (new action in
   `buildbuddy.yaml`, triggered on push to `main`): runs
   `buck2 build //src/sdk/python:wheel` then
   `buck2 run deploy//sdk:wheel.push -- sha-<short> edge` — immutable
   per-commit tag plus a moving `edge` channel, mirroring the image/chart
   scheme. **Credentials:** per the operator (2026-07-22), a GitHub token is
   already available to trusted BuildBuddy runs as the injected `GITHUB_TOKEN`
   env var (BuildBuddy auto-injects org secrets), so no new secret is
   prescribed. The implementation's first step is to **verify** this on a
   trusted run — that `GITHUB_TOKEN` is present and can push to ghcr
   (`packages:write`); if it turns out absent or read-only, fall back to
   adding a `GHCR_PUBLISH_TOKEN` org secret (fine-grained PAT,
   `packages:write`) and flag the operator. Either way the action fails
   loudly when the login fails rather than skipping the push.
2. **`release.yml` `release` job** (existing `workflow_dispatch X.Y.Z`):
   - validates the input version **equals the committed pyproject/BUCK
     version** (refuses to publish otherwise — no build-time version stamping,
     which would break determinism and caching);
   - `buck2 run deploy//sdk:wheel.push -- X.Y.Z latest`;
   - adds the `.whl` to the existing `softprops/action-gh-release` step's
     `files:`, attaching it to the `vX.Y.Z` Release.

No PR dev-wheels (the images needed PR previews for chart testing; nothing
consumes a PR wheel — YAGNI).

## Consumption (README one-liner, part of this slice)

The SDK README gains an install section: release-asset URL for pip, and
`regctl artifact get ghcr.io/weave-hand/loom-sdk:edge` (or `oras pull`) for
the ghcr package.

## Testing / acceptance

- The version-drift unit test (above) in the normal sweep.
- A wheel-shape unit test: build `:wheel` as a test input (`$(location)`),
  open it with `zipfile`, assert the dist-info `METADATA` carries the
  `pydantic` extra (`Provides-Extra` + conditional `Requires-Dist`) and that
  `loom_sdk/` + `loom_sdk/pydantic/` modules are present.
- End-to-end acceptance (manual, on the PR that lands the workflow):
  `pip install <built wheel>` into a scratch venv + `import loom_sdk` /
  `import loom_sdk.pydantic`; the BuildBuddy action goes green on the first
  main push and the package appears under the repo's Packages tab.

## Out of scope

- **PyPI proper** (name claim + trusted publishing via GHA OIDC) — revisit if
  external adoption warrants it.
- **A PEP 503 static index on gh-pages** (pip `--index-url` UX over the
  release assets) — a cheap follow-on if release-asset URLs prove annoying.
- **sdist publishing** — nothing consumes one.
- **PR preview wheels** — YAGNI, see Cadence.
