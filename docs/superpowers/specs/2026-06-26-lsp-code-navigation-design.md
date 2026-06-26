# LSP-first code navigation for loom

**Status:** superseded — LSP backed out of cloud. The hermetic rust-analyzer work
(`2026-06-26-cloud-rust-analyzer-design.md`, PR #197) made the `LSP` tool usable
locally, but in cloud/automated sessions driving rust-analyzer makes the buck2
rust-project integration run check builds in a **second `rust-analyzer`
isolation-dir buck-out**, and cloud sessions don't have the disk for it. So this
PR backs the LSP out of the cloud setup instead of building on it: the
`rust-analyzer-lsp` plugin enablement is removed from `.claude/settings.json`, the
cloud pre-warm of `//tools:rust-analyzer`/`//tools:rust-project` and the
per-session `rust-project.json` regeneration are removed from
`tools/cloud-setup.sh` / `tools/cloud-session-start.sh`, and the
`loom-code-navigation` skill is repurposed to **grep/Glob/Read navigation**,
stating the LSP is not available. Local dev can still wire rust-analyzer up
per-developer (see `DEVELOPING.md`). The LSP-first design below is retained for
history.

---

**Original design (LSP-first, not shipped):**

## Problem

The `rust-analyzer-lsp` plugin gives Claude an `LSP` tool (goToDefinition,
findReferences, hover, documentSymbol, workspaceSymbol, goToImplementation,
call hierarchy) — semantic navigation that is strictly more accurate than grep
for Rust. But it sits idle:

1. **The `LSP` tool is deferred** — it must be loaded with
   `ToolSearch "select:LSP"` before it can be called. Code that never searches
   for it never sees it.
2. **Nothing references it.** No skill, no agent, no prompt mentions LSP,
   `goToDefinition`, etc. — verified by grepping every installed agent
   definition. So the default reflex is grep.

The plugin ships no guidance — its entire content is an `lspServers` declaration
(`command: rust-analyzer`, `.rs → rust`) plus a README.

## Decisive constraint: the LSP tool is main-session-only

The initial design also proposed a loom exploration *agent* with `LSP` in its
tool allowlist, to give exploration **subagents** semantic navigation. Empirical
testing plus the official Claude Code docs proved this impossible:

- A `general-purpose` subagent with `tools: *` could not find the `LSP` tool;
  `ToolSearch "select:LSP"` returned "No matching deferred tools found". LSP is
  absent from the subagent's tool surface entirely.
- The [subagents docs](https://code.claude.com/docs/en/sub-agents.md#available-tools)
  state subagents inherit internal tools and **MCP tools** — LSP/deferred tools
  are not listed and do not cross the subagent boundary. Explicit `tools: LSP`
  in agent frontmatter does **not** surface it.

**Therefore the LSP can only be driven from the main loop.** A subagent that
claims LSP-first navigation would silently fall back to grep — worse than
useless. The proposed `loom-code-explorer` agent was **dropped**.

The recommended pattern (per the docs): the **main loop drives the LSP** and
hands subagents the resolved `file:line` locations to Read/expand. Main loop =
LSP driver; subagents = readers.

## What ships

### 1. Skill — `.claude/skills/loom-code-navigation/SKILL.md`

Auto-triggering + user-invocable. Audience: the **main loop** (the only context
that can call the LSP) and explicit use.

- **Frontmatter `description`** triggers on: tracing code, finding
  definitions/references/implementations/callers, type/signature questions,
  navigating loom's Rust; states the LSP is main-session-only.
- **Body:**
  - **Main-session-only rule:** do semantic nav yourself in the main loop; when
    fanning out to subagents, resolve locations with the LSP first and pass
    `file:line` targets — don't expect subagents to resolve symbols.
  - **Load the tool:** `ToolSearch "select:LSP"` (deferred).
  - **Readiness / cloud:** see §2.
  - **Decision table (LSP vs grep):**

    | Question | LSP op | Why not grep |
    |---|---|---|
    | Where is X defined? | `goToDefinition` | grep hits comments, shadows, re-exports |
    | Everywhere X is used? | `findReferences` | grep misses aliased/re-exported uses, hits strings |
    | What implements trait T? | `goToImplementation` | grep can't resolve trait impls |
    | Type/signature/docs of a symbol? | `hover` | grep can't infer types |
    | Outline a file / find a symbol repo-wide | `documentSymbol` / `workspaceSymbol` | faster, structured |
    | Who calls / what does this call? | `prepareCallHierarchy` → `incomingCalls` / `outgoingCalls` | grep can't build a call graph |

  - **Still use grep/Glob for:** text/comment/string patterns, config & non-Rust
    files (`BUCK`, `.toml`, `.sql`, shell), finding files by name, or when RA
    isn't indexed.
  - **Mechanics:** positions are **1-based** and point at the identifier;
    `workspaceSymbol` needs a non-empty query; find a position via
    `documentSymbol` first.
  - **Selling point:** LSP resolves macro-generated symbols (e.g. the tonic
    `pb::*` types) and re-exports grep cannot see.

### 2. Cloud readiness (`CLAUDE_CODE_REMOTE`) — embedded in the skill

In a **cloud session the LSP is not ready at session start.** The SessionStart
hook (`tools/cloud-session-start.sh`) regenerates `rust-project.json` in the
*background* (env.sh build + a `buck2 bxl` over `root//src/...`), and only then
does rust-analyzer reload and index hundreds of crates. The path can take a few
minutes; a call at t=0 errors/empties — warming up, not broken.

Protocol, gated on `CLAUDE_CODE_REMOTE=true`:

1. **Probe, don't assume.** After loading the tool, one `documentSymbol` probe on
   a known file (`src/services/engine/src/service.rs`). Symbols ⇒ ready.
2. **Diagnose if empty:** `rust-project.json` absent ⇒ background generation still
   running (`/tmp/loom-rust-project.log`); present but empty ⇒ still indexing.
3. **No blocking `sleep`** (harness disallows it). **Interleave:** grep/Read
   orientation first, re-probe after each pass, upgrade to semantic nav once it
   answers.
4. **Disclose** when answering from grep because the LSP wasn't warm.

Locally, the only delay is first-call indexing; one re-probe usually suffices,
and a missing `rust-project.json` is fixed with `rust-project develop
--prefer-rustup-managed-toolchain root//src/...`.

### 3. CLAUDE.md pointer

One short section: prefer rust-analyzer semantic nav over raw grep; the `LSP`
tool is deferred (`ToolSearch "select:LSP"`) and **main-session-only** (drive it
from the main loop, hand subagents `file:line`); points at the skill; notes the
cloud warm-up.

## What this is NOT

- Not an exploration *agent* — proven impossible (LSP is main-session-only).
- Not editing the `feature-dev:code-explorer` plugin agent.
- Not a generic multi-language setup — Rust/loom-specific (rust-project.json,
  hermetic toolchain, the buck2 crate graph).

## Verification

Skills have no unit harness; verify by behavior and by the constraint already
proven:

1. **LSP from the main loop works:** `documentSymbol` on
   `src/services/engine/src/service.rs` resolves first-party + tonic
   proc-macro-generated symbols (confirmed against the regenerated 604-crate
   graph).
2. **Subagent LSP is correctly NOT relied upon:** confirmed empirically + via
   docs that subagents cannot reach the LSP tool — hence no agent ships.
3. **Skill auto-trigger:** a navigation prompt surfaces `loom-code-navigation`.
4. **Cloud readiness:** with `CLAUDE_CODE_REMOTE=true` and the LSP not warm, the
   guidance is to probe, fall back to grep, and not block — then upgrade.
5. **Markdown lint:** both `.md` files end with a single trailing newline and no
   trailing whitespace (the `lint` CI gate).
