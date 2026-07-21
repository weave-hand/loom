load("@prelude//platforms:defs.bzl", "host_configuration")

# The BuildBuddy RE worker properties, shared by the execution platform's
# executor config below and by tests that PIN their run to RE via the
# `remote_execution` attr (see RE_TEST_PROPS).
RE_EXECUTION_PROPERTIES = {
    "OSFamily": "Linux",
    "container-image": "docker://ghcr.io/weave-hand/loom-rbe-browser@sha256:99cada5b232b5d23d16800c3c85d3f959427151856bcf7d249051cc5169042ce",
    "dockerUser": "buildbuddy",
}

# `remote_execution` attr value for tests that must ALWAYS run on RE (the
# prelude turns it into the test's default executor — no
# --unstable-allow-all-tests-on-re needed). Today that is every python_test:
# the inplace par's shebang bakes the interpreter's absolute path from the
# par-build action's sandbox (RE), so local test execution cannot work until
# the prelude bootstrap learns a machine-independent shebang
# (fut-python-par-local-shebang). Escape hatch for RE-less environments:
# -c fbcode.disable_re_tests=True (prelude re_utils honors it).
RE_TEST_PROPS = {
    "capabilities": RE_EXECUTION_PROPERTIES,
    "use_case": "buck2-default",
}

def _executor_config():
    if read_root_config("project", "remote_enabled", None):
        remote_only = read_root_config("project", "remote_only", None)
        return CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = True,
            use_limited_hybrid = not remote_only,
            remote_execution_properties = RE_EXECUTION_PROPERTIES,
            remote_execution_use_case = "buck2-default",
            remote_output_paths = "output_paths",
        )
    else:
        return CommandExecutorConfig(
            local_enabled = True,
            remote_enabled = False,
        )

def _buildbuddy_platforms(ctx):
    constraints = dict()
    constraints.update(ctx.attrs.cpu_configuration[ConfigurationInfo].constraints)
    constraints.update(ctx.attrs.os_configuration[ConfigurationInfo].constraints)
    configuration = ConfigurationInfo(constraints = constraints, values = {})

    platform = ExecutionPlatformInfo(
        label = ctx.label.raw_target(),
        configuration = configuration,
        executor_config = _executor_config(),
    )

    return [
        DefaultInfo(),
        PlatformInfo(label = str(ctx.label.raw_target()), configuration = configuration),
        ExecutionPlatformRegistrationInfo(platforms = [platform]),
    ]

buildbuddy_platforms = rule(
    impl = _buildbuddy_platforms,
    attrs = {
        "cpu_configuration": attrs.dep(providers = [ConfigurationInfo]),
        "os_configuration": attrs.dep(providers = [ConfigurationInfo]),
    },
)
