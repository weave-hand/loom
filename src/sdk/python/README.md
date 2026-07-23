# loom-sdk

The Python SDK for [loom](../../../README.md) — a hand-written sync `Client` +
`AsyncClient` over loom's write path (Arrow-IPC dataset/model landing), the admin
ontology surface, and verification reads. `pydantic` is an optional extra.

## Install

The SDK is published to GitHub Packages (ghcr) as an OCI artifact and attached to
each versioned GitHub Release.

Plain pip, from a release asset:

    pip install https://github.com/weave-hand/loom/releases/download/vX.Y.Z/loom_sdk-X.Y.Z-py3-none-any.whl

With the pydantic ontology layer:

    pip install "loom-sdk[pydantic] @ https://github.com/weave-hand/loom/releases/download/vX.Y.Z/loom_sdk-X.Y.Z-py3-none-any.whl"

From the ghcr OCI package (immutable `sha-<commit>` tags + moving `edge`/`latest`):

    oras pull ghcr.io/weave-hand/loom-sdk:edge   # writes loom_sdk-*.whl into the cwd
    pip install loom_sdk-*.whl

## Building the wheel

The wheel is a buck2 target — no `uv build`:

    buck2 build //src/sdk/python:wheel

Its version is pinned in `pyproject.toml` (`[project] version`) and kept in lockstep
with the BUCK target by `//src/sdk/python:wheel-test`.
