# buck2 — vendored container-image / Helm build rules

Reusable [Buck2](https://buck2.build) rules for building and publishing OCI
container images and Helm charts. **Vendored** from
[`jomcgi/homelab`](https://github.com/jomcgi/homelab/tree/main/buck2) (the
`buck2/` subtree) at commit `ee8669c3245229eeaf813fff98c271b3345c4e26`.

These live under the repo-root cell so the rules' internal `//buck2/…` and
`prelude//…` references resolve unchanged — exactly as they do in the upstream
repo. loom's `deploy/` targets consume them; see `docs/deploy.md`.

## Layout

- `bin/` — pinned host CLI binaries the rules wrap (`apko`, `crane`, `helm`,
  `regctl`, `kubeconform`, `yq`), each fetched + extracted + host-selected.
- `apko/` — Wolfi/apko image rules (`apko_image` builds a reproducible OCI base
  from an apko config + committed lockfile).
- `oci/` — a small rules_oci port: `oci_image` (layer a filesystem tar onto a
  base via `regctl`), `tar_layer` (stage a binary into a layer), `oci_push`
  (`crane push` runnable), `oci_image_info` (digest-pin provider).
- `helm/` — `helm_chart` / `helm_package` / `helm_push` / `helm_images_values`
  (digest-pin images into `values.yaml`) / `argocd_app`.

## Pins (keep aligned with the rest of loom)

- buck2 release: **2026-05-18** (matches `BUCK2_RELEASE` in `.github/workflows/ci.yml`)
- prelude submodule: matches `.gitmodules`

## Migration to a published cell

Upstream intends these rules to be consumed *as an external cell* rather than
vendored. Once `homelab` publishes them (recommended: a small public,
semver-tagged repo that keeps the top-level `buck2/` directory layout, so the
`//buck2/…` internal references keep resolving), loom can switch by:

1. Adding that repo as a git submodule (like `prelude/`).
2. Declaring a cell for it in `.buckconfig` (`[cells] homelab = <submodule-path>`).
3. Changing `deploy/**` loads from `//buck2/…` to `homelab//buck2/…`.
4. Deleting this vendored copy.

Until then this directory is the source of truth. To refresh, re-copy the
`buck2/` subtree from upstream at a known commit and update the commit hash and
pins above.
