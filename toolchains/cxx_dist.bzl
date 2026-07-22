"""Hermetic CXX toolchain rule using a downloaded LLVM distribution."""

load(
    "@prelude//cxx:cxx_toolchain_types.bzl",
    "BinaryUtilitiesInfo",
    "CCompilerInfo",
    "CvtresCompilerInfo",
    "CxxCompilerInfo",
    "CxxInternalTools",
    "CxxPlatformInfo",
    "CxxToolchainInfo",
    "DepTrackingMode",
    "LinkerInfo",
    "LinkerType",
    "PicBehavior",
    "RcCompilerInfo",
    "RuntimeDependencyHandling",
    "ShlibInterfacesMode",
)
load("@prelude//cxx:headers.bzl", "HeaderMode")
load("@prelude//cxx:linker.bzl", "is_pdb_generated")
load("@prelude//linking:link_info.bzl", "LinkStyle")
load("@prelude//linking:lto.bzl", "LtoMode")
load("@prelude//toolchains:cxx.bzl", "CxxToolsInfo")

def _hermetic_cxx_tools_impl(ctx: AnalysisContext) -> list[Provider]:
    llvm_dist = ctx.attrs.llvm_dist[DefaultInfo].default_outputs[0]

    return [
        DefaultInfo(),
        CxxToolsInfo(
            compiler = cmd_args(llvm_dist, format = "{}/bin/clang"),
            compiler_type = "clang",
            cxx_compiler = cmd_args(llvm_dist, format = "{}/bin/clang++"),
            asm_compiler = cmd_args(llvm_dist, format = "{}/bin/clang"),
            asm_compiler_type = "clang",
            rc_compiler = None,
            cvtres_compiler = None,
            archiver = cmd_args(llvm_dist, format = "{}/bin/llvm-ar"),
            archiver_type = "gnu",
            linker = cmd_args(llvm_dist, format = "{}/bin/clang++"),
            linker_type = LinkerType("gnu"),
        ),
    ]

hermetic_cxx_tools = rule(
    impl = _hermetic_cxx_tools_impl,
    attrs = {
        "llvm_dist": attrs.dep(),
    },
)

# Hermetic cxx toolchain that targets wasm32. The buck2 rust prelude always
# injects the cxx toolchain's linker as `-Clinker=` for the final rustc link
# (build.bzl), so cross-compiling Rust to wasm under buck needs a cxx toolchain
# that reports LinkerType("wasm") and a wasm-capable linker — `system_cxx_toolchain`
# can't, since it derives the linker type from the host OS (always gnu on linux).
#
# The linker is the rustc dist's bundled `rust-lld` *multiplexer*: rustc's WasmLld
# linker flavor invokes it as `rust-lld -flavor wasm ...`, exactly how rustc natively
# links wasm32-unknown-unknown. rust-lld (not the LLVM dist's `lld`) because the
# prebuilt LLVM `lld` is dynamically linked against libxml2.so.2, which the hermetic
# environment lacks — rust-lld is statically self-contained. The compiler fields are
# wired to the LLVM dist's clang only to satisfy the provider — a pure-Rust wasm
# binary never C-compiles through them. host x86_64 only (the rust-lld path is the
# host triple). Selected into `toolchains//:cxx` via toolchain_alias on wasm32.
def _wasm_cxx_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    llvm = ctx.attrs.llvm_dist[DefaultInfo].default_outputs[0]
    rustc = ctx.attrs.rustc_dist[DefaultInfo].default_outputs[0]
    clang = RunInfo(args = [cmd_args(llvm, format = "{}/bin/clang")])
    clangxx = RunInfo(args = [cmd_args(llvm, format = "{}/bin/clang++")])
    lld = RunInfo(args = [cmd_args(rustc, format = "{}/lib/rustlib/x86_64-unknown-linux-gnu/bin/rust-lld")])
    llvm_ar = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-ar")])

    return [
        DefaultInfo(),
        CxxToolchainInfo(
            internal_tools = ctx.attrs.internal_tools[CxxInternalTools],
            linker_info = LinkerInfo(
                linker = lld,
                linker_flags = [],
                post_linker_flags = [],
                archiver = llvm_ar,
                archiver_type = "gnu",
                archiver_supports_argfiles = True,
                generate_linker_maps = False,
                lto_mode = LtoMode("none"),
                type = LinkerType("wasm"),
                # RE-eligible: unlike the host system_cxx_toolchain (which hardcodes
                # local linking), our wasm linker is rustc's statically self-contained
                # rust-lld, so the link runs in the BuildBuddy RE container. Keeping it
                # off-local avoids materializing the rustc/LLVM dists locally on CI (cf.
                # CLAUDE.md's "prefer remote" cost model; the assemble_sysroot action is
                # RE-eligible for the same reason).
                link_binaries_locally = False,
                link_libraries_locally = False,
                archive_objects_locally = False,
                use_archiver_flags = True,
                static_dep_runtime_ld_flags = [],
                static_pic_dep_runtime_ld_flags = [],
                shared_dep_runtime_ld_flags = [],
                independent_shlib_interface_linker_flags = [],
                shlib_interfaces = ShlibInterfacesMode("disabled"),
                link_style = LinkStyle("static"),
                link_weight = 1,
                binary_extension = "",
                object_file_extension = "o",
                shared_library_name_default_prefix = "",
                shared_library_name_format = "{}.wasm",
                shared_library_versioned_name_format = "{}.wasm",
                static_library_extension = "a",
                force_full_hybrid_if_capable = False,
                is_pdb_generated = is_pdb_generated(LinkerType("wasm"), []),
                link_ordering = None,
            ),
            bolt_enabled = False,
            # Point binutils at the LLVM dist (exec_dep) rather than bare host
            # names: a pure-Rust wasm link never invokes these, but if buck's link
            # path ever calls strip/nm/objcopy (or LTO), the host tools would (a) be
            # non-hermetic on RE and (b) not understand wasm. The llvm-* tools do.
            binary_utilities_info = BinaryUtilitiesInfo(
                nm = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-nm")]),
                objcopy = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-objcopy")]),
                objdump = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-objdump")]),
                ranlib = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-ranlib")]),
                strip = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-strip")]),
                dwp = None,
                bolt_msdk = None,
            ),
            cxx_compiler_info = CxxCompilerInfo(
                compiler = clangxx,
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = "clang",
            ),
            c_compiler_info = CCompilerInfo(
                compiler = clang,
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = "clang",
            ),
            as_compiler_info = CCompilerInfo(
                compiler = clang,
                compiler_type = "clang",
            ),
            asm_compiler_info = CCompilerInfo(
                compiler = clang,
                compiler_type = "clang",
            ),
            cvtres_compiler_info = CvtresCompilerInfo(
                compiler = clang,
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = "clang",
            ),
            rc_compiler_info = RcCompilerInfo(
                compiler = clang,
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = "clang",
            ),
            header_mode = HeaderMode("symlink_tree_only"),
            cpp_dep_tracking_mode = DepTrackingMode("show_headers"),
            pic_behavior = PicBehavior("supported"),
            llvm_link = RunInfo(args = [cmd_args(llvm, format = "{}/bin/llvm-link")]),
            use_dep_files = True,
            runtime_dependency_handling = RuntimeDependencyHandling("no_symlink"),
        ),
        CxxPlatformInfo(name = "wasm32"),
    ]

wasm_cxx_toolchain = rule(
    impl = _wasm_cxx_toolchain_impl,
    is_toolchain_rule = True,
    attrs = {
        # exec_dep (not dep): clang/rust-lld are tools that must materialize and
        # run on the EXEC platform (the host / RE container), not the wasm target
        # platform. With link_binaries_locally = False the link runs on RE, so the
        # rust-lld binary has to be present there — a plain dep would configure it
        # for wasm32 and the link would break on RE.
        "llvm_dist": attrs.exec_dep(),
        "rustc_dist": attrs.exec_dep(),
        "internal_tools": attrs.default_only(attrs.exec_dep(
            providers = [CxxInternalTools],
            default = "prelude//cxx/tools:internal_tools",
        )),
    },
)

# Native (linux/gnu clang) cxx toolchain that runs its LINK actions on RE rather
# than the local executor. The prelude `system_cxx_toolchain` hardcodes
# link_binaries_locally/link_libraries_locally/archive_objects_locally = True
# (prelude/toolchains/cxx.bzl), which is fine when the local host arch == target
# arch but makes a native ARM64 link (driven from an x86 host over RE) impossible:
# the forced-local link tries to run the aarch64 toolchain on x86. This mirrors
# the linux/clang/gnu branch of the prelude's _cxx_toolchain_from_cxx_tools_info
# with those three flags set to False, so an arm64 link lands on the arm64 RE
# worker. Selected only for arm64 via `toolchains//:cxx` — x86_64 keeps the
# unchanged prelude system_cxx_toolchain (local links).
def _native_re_cxx_toolchain_impl(ctx: AnalysisContext) -> list[Provider]:
    tools = ctx.attrs.cxx_tools_info[CxxToolsInfo]
    linker_type = LinkerType("gnu")

    def run(x):
        return None if x == None else RunInfo(args = [x])

    return [
        DefaultInfo(),
        CxxToolchainInfo(
            internal_tools = ctx.attrs.internal_tools[CxxInternalTools],
            linker_info = LinkerInfo(
                linker = run(tools.linker),
                # clang drives ld.lld (bundled in the LLVM dist); matches the
                # prelude's linux/clang linker flags.
                linker_flags = ["-fuse-ld=lld"],
                post_linker_flags = [],
                archiver = run(tools.archiver),
                archiver_type = tools.archiver_type,
                archiver_supports_argfiles = True,
                generate_linker_maps = False,
                lto_mode = LtoMode("none"),
                type = linker_type,
                # The whole point: run link/archive on RE, not local.
                link_binaries_locally = False,
                link_libraries_locally = False,
                archive_objects_locally = False,
                use_archiver_flags = True,
                static_dep_runtime_ld_flags = [],
                static_pic_dep_runtime_ld_flags = [],
                shared_dep_runtime_ld_flags = [],
                independent_shlib_interface_linker_flags = [],
                shlib_interfaces = ShlibInterfacesMode("disabled"),
                link_style = LinkStyle("shared"),
                link_weight = 1,
                binary_extension = "",
                object_file_extension = "o",
                shared_library_name_default_prefix = "lib",
                shared_library_name_format = "{}.so",
                shared_library_versioned_name_format = "{}.so.{}",
                static_library_extension = "a",
                force_full_hybrid_if_capable = False,
                is_pdb_generated = is_pdb_generated(linker_type, []),
                link_ordering = None,
            ),
            bolt_enabled = False,
            # Bare host-tool names, resolved from the RE worker's PATH (the base
            # RBE image ships binutils); a pure-Rust link never invokes them.
            binary_utilities_info = BinaryUtilitiesInfo(
                nm = RunInfo(args = ["nm"]),
                objcopy = RunInfo(args = ["objcopy"]),
                objdump = RunInfo(args = ["objdump"]),
                ranlib = RunInfo(args = ["ranlib"]),
                strip = RunInfo(args = ["strip"]),
                dwp = None,
                bolt_msdk = None,
            ),
            cxx_compiler_info = CxxCompilerInfo(
                compiler = run(tools.cxx_compiler),
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = tools.compiler_type,
            ),
            c_compiler_info = CCompilerInfo(
                compiler = run(tools.compiler),
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = tools.compiler_type,
            ),
            as_compiler_info = CCompilerInfo(
                compiler = run(tools.compiler),
                compiler_type = tools.compiler_type,
            ),
            asm_compiler_info = CCompilerInfo(
                compiler = run(tools.asm_compiler),
                compiler_type = tools.asm_compiler_type,
            ),
            cvtres_compiler_info = CvtresCompilerInfo(
                compiler = run(tools.cvtres_compiler),
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = tools.compiler_type,
            ),
            rc_compiler_info = RcCompilerInfo(
                compiler = run(tools.rc_compiler),
                preprocessor_flags = [],
                compiler_flags = [],
                compiler_type = tools.compiler_type,
            ),
            header_mode = HeaderMode("symlink_tree_only"),
            cpp_dep_tracking_mode = DepTrackingMode("show_headers"),
            pic_behavior = PicBehavior("supported"),
            llvm_link = RunInfo(args = ["llvm-link"]),
            use_dep_files = True,
            runtime_dependency_handling = RuntimeDependencyHandling("no_symlink"),
        ),
        CxxPlatformInfo(name = "aarch64"),
    ]

native_re_cxx_toolchain = rule(
    impl = _native_re_cxx_toolchain_impl,
    is_toolchain_rule = True,
    attrs = {
        # exec_dep: the clang/archiver tools must materialize and run on the EXEC
        # platform (the arm64 RE worker). The wrapped hermetic_cxx_tools' llvm_dist
        # select then resolves in the exec (arm64) configuration → aarch64 clang.
        "cxx_tools_info": attrs.exec_dep(providers = [CxxToolsInfo]),
        "internal_tools": attrs.default_only(attrs.exec_dep(
            providers = [CxxInternalTools],
            default = "prelude//cxx/tools:internal_tools",
        )),
    },
)
