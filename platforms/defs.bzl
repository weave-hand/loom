# Multi-arch loom RBE image (a manifest-list / index digest). ONE digest serves
# both execution platforms: each BuildBuddy executor pulls the manifest matching
# its own arch, so only the `Arch` platform property routes an action to an
# amd64-vs-arm64 worker. After (re)publishing the image as a multi-arch manifest
# via .github/workflows/rbe-image.yml, update this single digest.
#
# NOTE: until the image is republished multi-arch, this digest is amd64-only, so
# the amd64 path works today and the arm64 path goes green once the multi-arch
# index digest is pinned here (see docs/superpowers/specs/
# 2026-07-22-multi-arch-arm64-images-design.md).
_RBE_IMAGE = "docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:99cada5b232b5d23d16800c3c85d3f959427151856bcf7d249051cc5169042ce"

def _executor_config(arch):
    if read_root_config("project", "remote_enabled", None):
        remote_only = read_root_config("project", "remote_only", None)
        return CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            use_limited_hybrid = not remote_only,
            remote_execution_properties = {
                # BuildBuddy executor selection. arm64 is served by self-hosted
                # executors in the default pool; amd64 is the managed pool.
                "Arch": arch,
                "OSFamily": "Linux",
                "container-image": _RBE_IMAGE,
                "dockerUser": "buildbuddy",
            },
            remote_execution_use_case = "buck2-default",
            remote_output_paths = "output_paths",
        )
    else:
        return CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = False,
        )

def _loom_execution_platforms_impl(ctx):
    # Register both arches. buck2 picks the first platform in this list whose
    # configuration satisfies the action's exec_compatible_with: unconstrained
    # (x86) actions land on amd64; a target whose toolchain constrains exec to
    # arm64 (see the rust toolchain's exec_compatible_with) lands on arm64. Each
    # platform carries the matching `Arch` RE property.
    platforms = []
    for dep, arch in [(ctx.attrs.amd64, "amd64"), (ctx.attrs.arm64, "arm64")]:
        platforms.append(ExecutionPlatformInfo(
            label = dep.label.raw_target(),
            configuration = dep[PlatformInfo].configuration,
            executor_config = _executor_config(arch),
        ))

    return [
        DefaultInfo(),
        ExecutionPlatformRegistrationInfo(platforms = platforms),
    ]

# Aggregates the per-arch `platform()` targets into the single
# ExecutionPlatformRegistrationInfo that `.buckconfig [build] execution_platforms`
# points at. Each `amd64`/`arm64` dep supplies both its label (for the exec
# platform identity) and its cpu+os ConfigurationInfo (for exec compatibility).
loom_execution_platforms = rule(
    impl = _loom_execution_platforms_impl,
    attrs = {
        "amd64": attrs.dep(providers = [PlatformInfo]),
        "arm64": attrs.dep(providers = [PlatformInfo]),
    },
)
