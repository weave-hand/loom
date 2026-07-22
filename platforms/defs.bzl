# Multi-arch loom RBE image (a manifest-list / index digest). ONE digest serves
# both execution platforms: each BuildBuddy executor pulls the manifest matching
# its own arch, so only the `Arch` platform property routes an action to an
# amd64-vs-arm64 worker. After (re)publishing the image as a multi-arch manifest
# via .github/workflows/rbe-image.yml, update this single digest.
#
# This is a multi-arch index (linux/amd64 + linux/arm64), published by
# .github/workflows/rbe-image.yml. Re-publish and repin this digest to refresh
# the RBE base (see docs/superpowers/specs/
# 2026-07-22-multi-arch-arm64-images-design.md).
_RBE_IMAGE = "docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:8fe89f64f6b27bae9c3f7917eca7b86a7e3baea0abef357f7c2ca265b53b6e10"

# Base BuildBuddy RE worker properties, shared by the execution platform's
# executor config (which adds the per-arch `Arch` key) and by tests that PIN
# their run to RE via the `remote_execution` attr (see RE_TEST_PROPS).
RE_EXECUTION_PROPERTIES = {
    "OSFamily": "Linux",
    "container-image": _RBE_IMAGE,
    "dockerUser": "buildbuddy",
}

# `remote_execution` attr value for tests that must ALWAYS run on RE (the
# prelude turns it into the test's default executor — no
# --unstable-allow-all-tests-on-re needed). Today that is every python_test:
# the inplace par's shebang bakes the interpreter's absolute path from the
# par-build action's sandbox (RE), so local test execution cannot work until
# the prelude bootstrap learns a machine-independent shebang
# (fut-python-par-local-shebang). Escape hatch for RE-less environments:
# -c fbcode.disable_re_tests=True (prelude re_utils honors it). Tests run on
# amd64 (no `Arch` key → BuildBuddy's amd64 default).
RE_TEST_PROPS = {
    "capabilities": RE_EXECUTION_PROPERTIES,
    "use_case": "buck2-default",
}

def _executor_config(arch):
    if read_root_config("project", "remote_enabled", None):
        remote_only = read_root_config("project", "remote_only", None)

        # BuildBuddy executor selection: arm64 is served by self-hosted executors
        # in the default pool; amd64 is the managed pool. Add `Arch` to the shared
        # base props per platform.
        props = dict(RE_EXECUTION_PROPERTIES)
        props["Arch"] = arch

        return CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            use_limited_hybrid = not remote_only,
            remote_execution_properties = props,
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
