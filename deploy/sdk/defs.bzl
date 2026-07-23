# Push the buck2-built loom-sdk wheel to ghcr as an OCI artifact. Mirrors
# deploy/images/multiarch.bzl's multiarch_oci_push: a run-only rule whose RunInfo
# execs a small bash script shelling `regctl artifact put`. Auth is out of band —
# regctl reads ~/.docker/config.json, which the workflow's `crane auth login`
# populates (same as the image push). `buck2 run <target> -- <tag>...` pushes the
# wheel to `repository` at each runtime-supplied tag.

_REGCTL = "@homelab//buck2/bin:regctl"

# Media/artifact types so the Packages UI renders the artifact sensibly. The wheel
# blob is a zip; PyPI's own simple-index wheel type is used as the artifactType.
_ARTIFACT_TYPE = "application/vnd.pypi.simple.v1+json"
_WHEEL_MEDIA_TYPE = "application/zip"

def _wheel_push_impl(ctx: AnalysisContext) -> list[Provider]:
    wheel = ctx.attrs.wheel[DefaultInfo].default_outputs[0]
    sh = ctx.actions.write(
        "wheel_push.sh",
        "\n".join([
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'REGCTL="$1"; WHEEL="$2"; REPO="$3"; shift 3',
            'if [ "$#" -eq 0 ]; then echo "usage: buck2 run <target> -- <tag>..." >&2; exit 2; fi',
            'for t in "$@"; do',
            '  echo "pushing $REPO:$t" >&2',
            '  "$REGCTL" artifact put \\',
            '    --artifact-type "' + _ARTIFACT_TYPE + '" \\',
            '    -m "' + _WHEEL_MEDIA_TYPE + '" \\',
            '    -f "$WHEEL" \\',
            '    "$REPO:$t"',
            "done",
        ]),
        is_executable = True,
    )
    return [
        DefaultInfo(),
        RunInfo(args = cmd_args([sh, ctx.attrs._regctl[RunInfo], wheel, ctx.attrs.repository])),
    ]

wheel_push = rule(
    impl = _wheel_push_impl,
    attrs = {
        "repository": attrs.string(),
        "wheel": attrs.dep(),
        "_regctl": attrs.exec_dep(default = _REGCTL, providers = [RunInfo]),
    },
)
