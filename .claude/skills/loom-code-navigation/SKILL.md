---
name: loom-code-navigation
description: Use rust-analyzer's semantic navigation (the LSP tool) instead of grep when exploring loom's Rust code — finding where a symbol is defined, every place it is used, what implements a trait, a symbol's type/signature, a file's outline, or who calls what. Use when tracing a feature's execution path, answering "where is X defined / what does this resolve to", finding all implementations or callers, or whenever a code-navigation question would otherwise be answered by grepping. The LSP tool is main-session-only — drive it yourself in the main loop; subagents cannot use it.
---

# LSP-first code navigation

loom has the `rust-analyzer` LSP wired in (hermetic, via `//tools:rust-analyzer`).
The `LSP` tool resolves **meaning**, where grep only matches **text**: the real
definition behind a name, every genuine use (not string hits), trait
implementations, inferred types, and call graphs. It sees macro-generated
symbols (e.g. the tonic `pb::*` types) and re-exports that grep cannot. Reach for
it first on any semantic question about Rust code.

## The LSP is main-session-only — keep semantic nav in the main loop

The `LSP` tool is available **only in the main conversation**, never to spawned
subagents (Task/Agent). Subagents inherit internal + MCP tools, not LSP/deferred
tools — a subagent's `ToolSearch "select:LSP"` returns nothing, regardless of its
`tools` frontmatter. So:

- **Do semantic navigation yourself, in the main loop.** Don't delegate "find the
  definition / all references / implementations" to a subagent expecting it to use
  the LSP — it will silently fall back to grep.
- **When you fan out to subagents for breadth**, resolve locations with the LSP
  first and hand the subagent the concrete `file:line` targets to Read/expand. The
  main loop is the LSP driver; subagents are the readers.

## Load the tool first

`LSP` is a **deferred** tool — load its schema with `ToolSearch "select:LSP"`
before calling it.

## Readiness

rust-analyzer needs an indexed `rust-project.json`. Probe with `LSP
documentSymbol` on a known file (`src/services/engine/src/service.rs`); symbols
returned ⇒ ready.

In a **cloud session** (`CLAUDE_CODE_REMOTE=true`) the LSP is **not ready at t=0** —
the SessionStart hook regenerates `rust-project.json` in the background
(`/tmp/loom-rust-project.log`) and rust-analyzer then indexes hundreds of crates,
which takes a few minutes. Don't block on `sleep` (the harness disallows it).
Interleave: do grep/Read orientation first, re-probe after each pass, and upgrade
to semantic navigation once it answers. Locally, the only delay is first-call
indexing; one re-probe usually suffices, and a missing `rust-project.json` is
fixed by `rust-project develop --prefer-rustup-managed-toolchain root//src/...`.

## LSP vs grep

| Question | LSP operation | Why not grep |
|---|---|---|
| Where is X defined? | `goToDefinition` | grep hits comments, shadows, re-exports |
| Everywhere X is used? | `findReferences` | grep misses aliased/re-exported uses, hits strings |
| What implements trait T? | `goToImplementation` | grep can't resolve trait impls |
| Type / signature / docs of a symbol? | `hover` | grep can't infer types |
| Outline a file / find a symbol repo-wide | `documentSymbol` / `workspaceSymbol` | faster, structured |
| Who calls this / what does it call? | `prepareCallHierarchy` → `incomingCalls` / `outgoingCalls` | grep can't build a call graph |

**Still use grep/Glob for:** text/comment/string-literal patterns; configuration
and non-Rust files (`BUCK`, `.toml`, `.sql`, shell); finding files by name; or
when rust-analyzer is not indexed.

## Mechanics

- Positions are **1-based** (line and character) and must point at the
  **identifier**, not a keyword or whitespace.
- `workspaceSymbol` needs a non-empty `query`.
- Find a precise position by running `documentSymbol` on the file first (it gives
  each item's line), then aim the call at the name.
