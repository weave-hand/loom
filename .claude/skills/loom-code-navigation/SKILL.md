---
name: loom-code-navigation
description: How to navigate loom's Rust code — finding where a symbol is defined, every place it is used, what implements a trait, a symbol's type/signature, a file's outline, or who calls what. Use when tracing a feature's execution path, answering "where is X defined / what does this resolve to", finding all implementations or callers, or any code-navigation question. NOTE: the rust-analyzer LSP is NOT available in cloud/automated sessions — navigate with grep/Glob/Read.
---

# Navigating loom's Rust code

**The `rust-analyzer` LSP is not available in cloud / automated sessions.** The
hermetic rust-analyzer integration was backed out of the cloud setup: driving it
makes the buck2 rust-project integration run check-on-save builds in a *second*
`rust-analyzer` isolation-dir buck-out, and cloud sessions don't have the disk for
a second buck-out. So `tools/cloud-setup.sh` no longer pre-warms it,
`tools/cloud-session-start.sh` no longer regenerates `rust-project.json`, and the
`rust-analyzer-lsp` plugin is not enabled in the repo's `.claude/settings.json`.

**Do not reach for the `LSP` tool here** — `ToolSearch "select:LSP"` will return
nothing, and any attempt to use it just wastes a turn before falling back. Navigate
with `Grep` / `Glob` / `Read` (and dispatch `Explore` subagents for breadth).

## Navigating with grep/Glob/Read

| Question | How |
|---|---|
| Where is X defined? | `Grep` for `fn X`/`struct X`/`enum X`/`trait X`/`impl X` (scope with `type:rust`); jump to the hit |
| Everywhere X is used? | `Grep "\\bX\\b"`, then `Read` the hits to filter comments/strings/shadows |
| What implements trait T? | `Grep "impl .*\\bT\\b for"` (and `impl T for` for inherent-style) |
| Type / signature of a symbol? | `Grep` its declaration and `Read` the surrounding lines |
| Outline a file / find a symbol repo-wide | `Grep -n "^\\s*(pub )?(fn|struct|enum|trait|impl|mod) "` on the file, or repo-wide for the name |
| Who calls this / what does it call? | `Grep "\\bX\\b"` for call sites, then `Read` each to confirm it's a call, not a definition/import |

Grep matches text, so it over-matches: it hits comments, string literals, shadowed
names, and can't see macro-generated symbols (e.g. the tonic `pb::*` types) or
follow re-exports. Compensate by `Read`ing the hits to confirm, and by searching
the macro inputs (`.proto` files, `#[derive(...)]` targets) when a symbol looks
generated.

For broad sweeps (trace an execution path, find every implementor across crates),
dispatch `Explore` subagents — they search the same way but keep the file dumps out
of the main context.

## Local development

A developer's local machine *can* run rust-analyzer if they wire it up themselves
(see `DEVELOPING.md`: generate `rust-project.json` with
`rust-project develop --prefer-rustup-managed-toolchain root//src/...` and enable
the `rust-analyzer-lsp` plugin in their own user-global `~/.claude/settings.json`).
That is a per-developer choice and out of scope for these automated routines — do
not assume the `LSP` tool exists.
