# LSP-first code navigation for loom

**Status:** designed. Builds on the hermetic rust-analyzer work
(`2026-06-26-cloud-rust-analyzer-design.md`, PR #197), which makes the `LSP`
tool actually usable in loom (local + cloud).

## Problem

The `rust-analyzer-lsp` plugin gives Claude an `LSP` tool (goToDefinition,
findReferences, hover, documentSymbol, workspaceSymbol, goToImplementation,
call hierarchy) — semantic navigation that is strictly more accurate than grep
for Rust. But it sits idle:

1. **The `LSP` tool is deferred** — it must be loaded with
   `ToolSearch "select:LSP"` before it can be called. An agent that never
   searches for it never sees it.
2. **Nothing references it.** No skill, no agent, no prompt mentions LSP,
   `goToDefinition`, etc. — verified by grepping every installed agent
   definition. So the main loop and subagents default to grep.
3. **The dedicated explorer can't use it.** `feature-dev:code-explorer`'s tool
   allowlist (`Glob, Grep, LS, Read, NotebookRead, WebFetch, TodoWrite,
   WebSearch, KillShell, BashOutput`) excludes both `LSP` and `ToolSearch`. The
   built-in `Explore` agent *allows* LSP but has no prompt nudging it.

The plugin itself ships no guidance — its entire content is an `lspServers`
declaration (`command: rust-analyzer`, `.rs → rust`) plus a README.

## Key constraint shaping the design

**Subagents do not reliably load skills.** The `using-superpowers` skill tells a
dispatched subagent to skip skill-loading (`<SUBAGENT-STOP>`). So a skill alone
cannot make exploration *subagents* use LSP — the reliable lever for a subagent
is its **agent definition** (tool allowlist + system prompt). A skill's real
audience is the **main loop** and explicit user invocation. Hence two artifacts
with distinct jobs.

## Components

### 1. Agent — `.claude/agents/loom-code-explorer.md`

A new read-only exploration agent committed in loom; the primary mechanism for
giving *subagents* LSP access. Dispatched for "trace / map / understand this
code" tasks in place of the LSP-blind explorers.

- **Frontmatter:**
  - `name: loom-code-explorer`
  - `description:` deeply trace/understand loom's Rust via semantic navigation
    (definitions, references, implementations, call graphs, types).
  - `tools: LSP, ToolSearch, Read, Grep, Glob, LS, Bash, NotebookRead, TodoWrite`
    — `LSP` + `ToolSearch` explicitly present so the tool is allowlisted and its
    schema is fetchable; **no Edit/Write** (exploration only).
  - `model: inherit` — runs on whatever model the dispatching session uses.
- **Prompt:** opens by loading the tool (`ToolSearch "select:LSP"`), then leads
  with semantic navigation and falls back to grep; embeds the condensed decision
  table (below) and the cloud-readiness protocol (§3), since the agent cannot
  rely on the skill. Reports findings as `file:line`. Notes the warm-up/indexing
  caveat and that positions are 1-based.

### 2. Skill — `.claude/skills/loom-code-navigation/SKILL.md`

Auto-triggering + user-invocable. Audience: the **main loop** and explicit use.

- **Frontmatter `description`** triggers on: tracing code, finding
  definitions/references/implementations/callers, type/signature questions,
  navigating loom's Rust codebase.
- **Body:**
  - **Prerequisites & gotchas:** `ToolSearch "select:LSP"` first (deferred);
    `rust-project.json` must exist (regenerate via `rust-project develop
    --prefer-rustup-managed-toolchain root//src/...` if stale after target
    changes); the first call may be slow while RA indexes ~636 crates;
    positions are **1-based** and must point at the identifier; `workspaceSymbol`
    needs a non-empty query.
  - **Cloud readiness:** in a cloud session (`CLAUDE_CODE_REMOTE=true`) the LSP
    warms up a few minutes in — follow the readiness protocol (§3): probe,
    interleave with grep, upgrade to semantic nav once it answers.
  - **Decision table (LSP vs grep):**

    | Question | LSP op | Why not grep |
    |---|---|---|
    | Where is X defined? | `goToDefinition` | grep hits comments, shadows, re-exports |
    | Everywhere X is used? | `findReferences` | grep misses aliased/re-exported uses, hits strings |
    | What implements trait T? | `goToImplementation` | grep can't resolve trait impls |
    | Type/signature/docs of a symbol? | `hover` | grep can't infer types |
    | Outline a file / find a symbol repo-wide | `documentSymbol` / `workspaceSymbol` | faster + structured |
    | Who calls / what does this call? | `incomingCalls` / `outgoingCalls` (+ `prepareCallHierarchy`) | grep can't build a call graph |

  - **Still use grep/Glob for:** text/comment/string patterns, config &
    non-Rust files, finding files by name, or when RA isn't indexed/available.
  - **Selling point:** LSP resolves macro-generated symbols (e.g. the tonic
    `pb::*` types) and re-exports that grep cannot see.

### 3. Cloud readiness (`CLAUDE_CODE_REMOTE`)

In a **cloud session the LSP is not ready at session start.** The SessionStart
hook (`tools/cloud-session-start.sh`, PR #197) regenerates `rust-project.json`
in the *background* (env.sh build + a `buck2 bxl` over `root//src/...`), and only
then does rust-analyzer reload and index ~636 crates. The whole path can take a
few minutes. A `documentSymbol`/`hover` call issued at t=0 will error or return
empty — not because the tool is broken, but because it is still warming up.

Both the skill and the agent prompt carry an explicit **readiness protocol**,
gated on `CLAUDE_CODE_REMOTE=true` (set by the platform in every cloud session):

1. **Probe, don't assume.** After `ToolSearch "select:LSP"`, issue one cheap
   probe — `documentSymbol` on a known file
   (`src/services/engine/src/service.rs`). Symbols returned ⇒ ready, proceed.
2. **If the probe is empty/errors in cloud:**
   - `rust-project.json` absent at the repo root ⇒ background generation is still
     running (`/tmp/loom-rust-project.log`).
   - present but probe still empty ⇒ rust-analyzer is still indexing.
3. **Do not block on `sleep`** (the harness disallows foreground sleeps).
   Instead **interleave**: start with grep/Read orientation (always useful),
   then re-probe the LSP after the first pass and **upgrade to semantic
   navigation once it answers.** For accuracy-critical questions, re-probe across
   successive steps rather than waiting idle.
4. **Always disclose** when answering from grep because the LSP was not yet
   available, so a conclusion can be revisited once it warms up.

Locally (`CLAUDE_CODE_REMOTE` unset) the only delay is rust-analyzer's
first-call indexing after the file opens; one re-probe usually suffices, and a
missing `rust-project.json` is fixed by re-running `rust-project develop`.

### 4. CLAUDE.md pointer

One short line under a dev-facing section: prefer the `loom-code-explorer` agent
/ `loom-code-navigation` skill (semantic nav via rust-analyzer) over raw grep
for Rust code navigation. Keeps the main loop aware without bloat.

## What this is NOT

- Not editing the `feature-dev:code-explorer` plugin agent (would drift on
  plugin updates) — a fresh loom-owned agent instead.
- Not rewiring the `dispatch`/`feature-dev` skills to force the new agent — it is
  simply available as a `subagent_type`, and the dispatching loop (guided by the
  skill + CLAUDE.md pointer) prefers it.
- Not a generic multi-language setup — Rust/loom-specific (rust-project.json,
  hermetic toolchain, the 636-crate graph).

## Verification

Skills/agents have no unit harness; verify by behavior:

1. **Skill auto-trigger:** a navigation prompt ("where is `SqlCatalog`
   constructed and everywhere it's used?") should surface the
   `loom-code-navigation` skill.
2. **Agent uses LSP:** dispatch `loom-code-explorer` on a semantic query ("find
   every implementation of a given trait in `//src`"). Confirm it runs
   `ToolSearch "select:LSP"` then calls `goToImplementation` / `findReferences`,
   and returns correct `file:line` results (cross-checked against a known
   answer, e.g. the `EngineControl` trait impl in
   `src/services/engine/src/service.rs`).
3. **Cloud readiness:** with `CLAUDE_CODE_REMOTE=true` and the LSP not yet warm
   (no `rust-project.json`, or RA still indexing), the agent must probe, fall
   back to grep, and *not* hard-fail or block on `sleep` — then upgrade to
   semantic nav once a re-probe succeeds.
4. **Markdown lint:** both new `.md` files end with a single trailing newline and
   no trailing whitespace (the `lint` CI gate).
