"""Multi-arch (linux/amd64 + linux/arm64) OCI image composition for loom services.

homelab's `oci_image` is single-arch by construction: it `regctl image import`s the
apko base, which collapses a multi-arch index to the host manifest. Composing a
per-arch binary layer onto each arch therefore needs a different pipeline, proven
in git history: 2026-07-22-multi-arch-arm64-images-design:

  per arch (amd64, arm64):
    apko build --arch <arch>         # single-arch base (no index → no collapse)
    regctl image import              # arch preserved
    regctl image mod --layer-add     # stage the arch's binary (+ arch-agnostic layers)
  regctl index create --ref <amd> --ref <arm>   # combine into one OCI index

The per-arch build fetches apk packages over the network (apko), so those genrules
are marked local (`uses_undeclared_inputs`); the index-combine step is offline and
RE-eligible. `.push` (crane) and `.info` (digest pin) reuse homelab's rules — a
multi-arch OCI tar pushes as an index and its index.json's first digest is the
manifest-list digest, exactly what you pin.
"""

load("@homelab//buck2/oci:defs.bzl", "oci_image_info")

_APKO = "@homelab//buck2/bin:apko"
_REGCTL = "@homelab//buck2/bin:regctl"

def _multiarch_oci_push_impl(ctx: AnalysisContext) -> list[Provider]:
    # crane push only loads docker-archive tars (needs manifest.json), which a
    # multi-arch OCI image can't be — so push with `regctl image copy` from the
    # exported OCI layout, which handles the index natively. Auth comes from
    # ~/.docker/config.json (the release workflow's `crane auth login` populates it;
    # regctl reads the same file).
    image = ctx.attrs.image[DefaultInfo].default_outputs[0]
    sh = ctx.actions.write(
        "multiarch_push.sh",
        "\n".join([
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'REGCTL="$1"; IMAGE="$2"; REPO="$3"; shift 3',
            'if [ "$#" -eq 0 ]; then echo "usage: buck2 run <target> -- <tag>..." >&2; exit 2; fi',
            'TMP="$(mktemp -d)"; trap \'rm -rf "$TMP"\' EXIT',
            'tar -xf "$IMAGE" -C "$TMP"',
            # The exported layout tags the multi-arch index `multi` (see the index
            # genrule); copy that ref to each runtime-supplied tag.
            'for t in "$@"; do echo "pushing $REPO:$t" >&2; "$REGCTL" image copy "ocidir://$TMP:multi" "$REPO:$t"; done',
        ]),
        is_executable = True,
    )
    return [
        DefaultInfo(),
        RunInfo(args = cmd_args([sh, ctx.attrs._regctl[RunInfo], image, ctx.attrs.repository])),
    ]

# Runnable: `buck2 run <image>.push -- <tag>...` pushes the multi-arch image to its
# repository at each runtime-supplied tag (via regctl). Mirrors homelab's oci_push
# but multi-arch-aware.
multiarch_oci_push = rule(
    impl = _multiarch_oci_push_impl,
    attrs = {
        "image": attrs.dep(),
        "repository": attrs.string(),
        "_regctl": attrs.exec_dep(default = _REGCTL, providers = [RunInfo]),
    },
)

# (oci/regctl arch, apko arch, buck2 target platform for the per-arch binary)
_ARCHES = [
    ("amd64", "x86_64", "root//platforms:linux-x86_64"),
    ("arm64", "aarch64", "root//platforms:linux-aarch64"),
]

def multiarch_oci_image(
        name,
        config,
        lock,
        binary,
        path,
        layers = [],
        repository = None,
        visibility = ["PUBLIC"]):
    """Build a multi-arch OCI image: apko base per arch + `binary` staged at `path`.

    Args:
      name: target name; output `<name>` is a multi-arch OCI image tar.
      config: apko YAML config (its `archs:` must list x86_64 + aarch64).
      lock: multi-arch `apko.lock.json` (`apko lock`, both arches).
      binary: an executable target (e.g. a rust_binary) — built once per arch via a
        configured_alias and staged at `path` in that arch's manifest.
      path: absolute in-image path for the binary (e.g. "/usr/local/bin/server").
      layers: arch-agnostic layer tars added to every arch (e.g. a UI bundle).
      repository: ghcr repo for `.push`/`.info`; omit to skip them.
      visibility: target visibility.
    """
    parent = path.rsplit("/", 1)[0]
    layer_add = " ".join(['--layer-add "tar=$(location {})"'.format(l) for l in layers])

    per_arch = []
    for oci_arch, apko_arch, plat in _ARCHES:
        # Build `binary` for this arch (deploy cell defaults to host = amd64; the
        # alias forces the arm64 configuration — cf. the wasm ui-bundle alias).
        binalias = "{}-bin-{}".format(name, oci_arch)
        native.configured_alias(
            name = binalias,
            actual = binary,
            platform = plat,
            visibility = visibility,
        )

        img = "{}-{}".format(name, oci_arch)
        native.genrule(
            name = img,
            # config + lock are source files → $SRCS positional ($(location) can't
            # name a bare source file in buck2); layers/binary are targets.
            srcs = [config, lock],
            out = "image.tar",
            cmd = " && ".join([
                'set -- $SRCS',
                'CFG="$1"; LOCK="$2"',
                'mkdir -p "$TMP/cache" "$TMP/root{parent}"'.format(parent = parent),
                'cp "$(location :{ba})" "$TMP/root{path}"'.format(ba = binalias, path = path),
                'chmod +x "$TMP/root{path}"'.format(path = path),
                'tar -C "$TMP/root" -cf "$TMP/bin.tar" .',
                '$(exe {apko}) build --arch {aa} --lockfile "$LOCK" --cache-dir "$TMP/cache" --sbom=false "$CFG" {name}:latest "$TMP/base.tar"'.format(
                    apko = _APKO,
                    aa = apko_arch,
                    name = name,
                ),
                '$(exe {rc}) image import "ocidir://$TMP/oci:base" "$TMP/base.tar"'.format(rc = _REGCTL),
                '$(exe {rc}) image mod "ocidir://$TMP/oci:base" --create "ocidir://$TMP/oci:img" --layer-add "tar=$TMP/bin.tar" {extra}'.format(
                    rc = _REGCTL,
                    extra = layer_add,
                ),
                '$(exe {rc}) image export "ocidir://$TMP/oci:img" "$OUT"'.format(rc = _REGCTL),
            ]),
            # apko fetches apk packages over the network → run local, off RE.
            labels = ["uses_undeclared_inputs"],
            visibility = visibility,
        )
        per_arch.append((oci_arch, img))

    # Combine the per-arch single-arch images into one multi-arch OCI index. Offline
    # (RE-eligible): inputs are the per-arch image tars.
    imports = " && ".join([
        '$(exe {rc}) image import "ocidir://$TMP/oci:{oa}" "$(location :{img})"'.format(rc = _REGCTL, oa = oa, img = img)
        for (oa, img) in per_arch
    ])
    refs = " ".join(['--ref "ocidir://$TMP/oci:{oa}"'.format(oa = oa) for (oa, _img) in per_arch])
    native.genrule(
        name = name,
        out = "image.tar",
        cmd = " && ".join([
            imports,
            '$(exe {rc}) index create "ocidir://$TMP/oci:multi" {refs}'.format(rc = _REGCTL, refs = refs),
            '$(exe {rc}) image export "ocidir://$TMP/oci:multi" "$OUT"'.format(rc = _REGCTL),
        ]),
        visibility = visibility,
    )

    if repository:
        oci_image_info(
            name = name + ".info",
            image = ":" + name,
            repository = repository,
            visibility = visibility,
        )
        multiarch_oci_push(
            name = name + ".push",
            image = ":" + name,
            repository = repository,
            visibility = visibility,
        )
