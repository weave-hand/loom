---
name: explain-pr-to-joe
description: Teach Joe a pull request — fan out one subagent per changed file and leave detailed, educational inline review comments on GitHub that explain each block of code, the Rust/buck2 idiom used, why it was chosen over alternatives, and the non-obvious considerations. Audience is fluent in Bazel, Python, and Go but NOT Rust or buck2, so comments map concepts to those languages. Use ONLY when explicitly invoked (e.g. /explain-pr-to-joe [PR#]); never run automatically. This is a learning aid, not a correctness gate — it posts a COMMENT review, never approves or requests changes.
---

Explain a pull request to **Joe**, who is fluent in **Bazel, Python, and Go** but
is still learning **Rust** and **buck2**. For every changed file, leave detailed
**inline** GitHub review comments that teach the code: what each small block does,
the language/ecosystem idiom it uses, *why* that idiom is the right call here, what
alternatives existed, and the non-obvious considerations a newcomer would miss.

This is **manually invoked only** and is a **teaching aid, not a review gate** — it
posts a single `COMMENT` review (never `APPROVE`/`REQUEST_CHANGES`) and never edits
code. Run it when asked (`/explain-pr-to-joe`, `/explain-pr-to-joe 152`, "explain
this PR to me", "walk me through PR #152").

## The audience contract (this is the whole point — get it right)

Write every comment FOR Joe. He knows software, just not this stack. So:

- **Map to what he knows.** "Like Bazel's `genrule`, but…", "Go's `error` return is
  this `Result<T, E>` — the `?` is `if err != nil { return err }`", "Python's
  context manager ≈ this `Drop`/RAII guard", "a Go interface ≈ this `trait`".
- **Teach the idiom, then justify it.** Don't just say *what* the code does — say
  why it's written the idiomatic Rust/buck2 way and what the alternatives' trade-offs
  are (e.g. `&str` vs `String`, `?` vs `match`, `Arc` vs clone, `impl Trait` vs
  generics, an owned vs borrowed receiver, a `select()` vs duplicated buck2 targets).
- **Surface the non-obvious.** Lifetimes, ownership/borrow decisions, why a clone is
  cheap or necessary, why an `unwrap`/`expect` is safe here, async cancellation, trait
  bounds, error-conversion via `From`/`?`, `sqlx` compile-time macros, and buck2
  specifics (targets, visibility, `deps` vs `named_deps`, fixups, RE-vs-local labels).
- **Explain rationale, not just mechanics.** When the author chose one design over
  another that a Go/Python dev would reach for, call it out and explain the reasoning.
- **Tone:** generous and concrete, never condescending. Short paragraphs. Real code
  references. It's fine for a single comment to be several sentences when the block
  earns it — Joe asked for *detailed*.
- **Anchor small, and post MANY.** Prefer lots of small inline comments — roughly
  one per coherent block, tricky expression, signature, or buck2 target — over a few
  fat ones. Each is pinned to the exact line it explains (`{path, line, side:
  RIGHT}`), so the explanation sits *directly under that code* in the diff. Err
  toward more comments: if a hunk has three teachable ideas, that's three pins, not
  one paragraph covering all three. Density is the point — Joe is reading to learn,
  so the explanation should always be next to the thing it describes, never collected
  elsewhere. (The single review in step 3 is just the delivery envelope; it still
  renders every comment inline on its own line.)

This codebase's `CLAUDE.md` is rich on buck2/sqlx/arrow-version/test-layout specifics
— lean on it for accurate rationale (e.g. why tests are separate `rust_test` targets,
why the `.sqlx` cache exists, why arrow 57 vs 58 is split across crates).

## Procedure

### 1. Resolve the PR and its diff

```bash
# PR number: use $1 if given, else the PR for the current branch.
PR="${1:-$(gh pr view --json number -q .number)}"
REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
HEAD_SHA=$(gh pr view "$PR" --json headRefOid -q .headRefOid)

# Files changed, with status, skipping ones not worth teaching.
gh pr view "$PR" --json files -q '.files[] | "\(.path)\t\(.additions)+\(.deletions)-"'
```

**Skip** (note them in the summary, don't comment on them): deleted files, lock
files (`Cargo.lock`), generated files (`third-party/BUCK`, anything under a
`.sqlx/` dir), vendored code, and pure binary assets. **Include** Rust (`.rs`),
buck2 (`BUCK`, `*.bzl`, `.buckconfig`), manifests (`Cargo.toml`), SQL migrations,
shell, and docs — Joe wants to understand buck2 too, and it's Bazel-adjacent so the
analogies land well.

### 2. Fan out — one subagent per changed file

Dispatch the per-file agents **in parallel in a single message** (the
`superpowers:dispatching-parallel-agents` pattern). For a large PR (more than ~15
teachable files) prefer the **Workflow tool** to pipeline them under a concurrency
cap; otherwise the Agent tool is simpler. **The subagents RETURN structured comments
— they do NOT post** (the parent posts one consolidated review in step 3, so Joe gets
a single notification and line numbers are validated once).

Give each subagent this brief (substitute the file):

> You are explaining ONE file of a GitHub PR to **Joe**, who is fluent in Bazel,
> Python, and Go but is learning Rust and buck2. Get the file's diff AND its full
> context:
> - **Per-file diff (use this exact command — NOT `gh pr diff <PR> -- <path>`, which
>   errors):**
>   `gh api repos/<owner>/<repo>/pulls/<PR>/files --paginate --jq '.[] | select(.filename=="<path>") | .patch'`
>   This prints only that file's unified patch, with `@@ -a,b +c,d @@` hunk headers.
> - **Full file at the PR head:** `Read <path>` (so your line numbers match the
>   file's real numbering, not the patch's local offsets).
> - Also skim repo `CLAUDE.md` for accurate rationale.
>
> Produce a set of **inline teaching comments**. For each block explain, in Joe's
> terms: (1) what it does, (2) the Rust/buck2 idiom and a Bazel/Python/Go analogy,
> (3) why this idiom/design over the alternatives, (4) any non-obvious consideration
> (ownership, lifetimes, errors, async, sqlx macros, buck2 target wiring). Be detailed
> but tight; one comment per coherent block.
>
> **Anchoring rules (GitHub rejects the whole review if any one is violated):**
> - Anchor only to a line **shown in this file's patch** as an added (`+`) or context
>   (` `) line — i.e. on the RIGHT/new side. Never a removed (`-`) line, never a line
>   outside every `@@` hunk. Each `@@ -a,b +c,d @@` header means the new-side window is
>   lines `c` through `c+d-1`; your `line` (and `start_line`) must fall inside one such
>   window.
> - `line` is the LAST line of the anchor; `start_line` (for a range) must be
>   **strictly less** than `line` (`start_line < line`) and in the SAME hunk window.
>   Never `start_line == line` and never `start_line > line` — if you only mean one
>   line, set `start_line` to `null`.
> - When unsure whether a range is fully in-hunk, prefer a single-line anchor — it's
>   far less likely to be rejected.
>
> Return ONLY JSON matching this schema (no prose):
> `{ "path": "<path>", "summary": "<1-2 sentence what-this-file-does-in-the-PR>",
>   "comments": [ { "line": <int>, "start_line": <int|null>, "body": "<markdown>" } ] }`
> `start_line` is null for a single-line anchor; set it (`< line`) for a range.
> If the file isn't worth teaching, return an empty `comments` array with a `summary`.

**Validate anchors before posting (do this in the parent — don't trust the agents).**
The agents reliably write good *prose* but routinely get *line math* wrong (full-file
vs patch offsets, inverted or equal `start_line`, anchors a line or two outside the
hunk). Cheap insurance that avoids a rejected POST: fetch each changed file's patch
once (`gh api repos/$REPO/pulls/$PR/files --paginate`), parse its `@@ -a,b +c,d @@`
headers into new-side windows `[c, c+d-1]`, then for every returned comment —

- **drop** it if `line` is outside every window for that path,
- **null out** `start_line` if it's `>= line` or in a different window than `line`,
- keep the rest.

Collect the dropped ones; they feed the fallback in step 3 (don't silently lose them).

When using the **Agent tool**, request that exact JSON and parse it from each
result. When using the **Workflow tool**, pass that JSON shape as the `schema` so
each agent returns a validated object, and collect the array.

### 3. Post the inline comments (one review envelope, many inline pins)

Collect every comment from every subagent and post them as **inline diff comments**.
The default is to deliver them in a **single review** — a GitHub review is just an
envelope that holds many inline comments; each still renders on its own line, so you
get the dense inline teaching Joe wants plus a single notification (not 40). Each
comment needs `path`, `line`, `side: "RIGHT"`, `body` (and `start_line` +
`start_side: "RIGHT"` for a range). Write the payload to a temp JSON file and submit:

```bash
# review.json — assembled from the subagents' returned comments:
# {
#   "commit_id": "<HEAD_SHA>",
#   "event": "COMMENT",
#   "body": "<overall teaching summary: what the PR does, file-by-file one-liners,\n            and a note listing any skipped files>",
#   "comments": [
#     { "path": "src/…/foo.rs", "line": 42, "side": "RIGHT", "body": "…" },
#     { "path": "…/BUCK", "start_line": 10, "start_side": "RIGHT", "line": 14, "side": "RIGHT", "body": "…" }
#   ]
# }
gh api "repos/$REPO/pulls/$PR/reviews" --input review.json
```

Robustness:

- **A `502`/`5xx` or timeout does NOT mean the review failed — it often succeeded.**
  Observed in practice: the POST returned `HTTP 502 Server Error` yet the review was
  fully created (body + all 30 inline comments). **Never blind-retry a write.** On any
  non-2xx, first check whether it already landed:
  `gh api repos/$REPO/pulls/$PR/reviews --jq '.[] | select(.user.login=="<you>") | "\(.id) \(.submitted_at)"'`
  (and `…/pulls/$PR/comments --jq length` for the inline count). Only re-POST if
  nothing was created. A naive retry double-posts the entire review.
- **Expect a GitHub *secondary* rate limit on bursty content creation** (`HTTP 403`
  "exceeded a secondary rate limit … temporarily blocked from content creation"). One
  review-with-N-comments is a single creation call and is fine; an immediate retry, or
  the individual-comments fallback below (N separate POSTs), is what trips it. If you
  hit it, wait ~60s and back off — don't hammer.
- **Validation rejection (`422`).** If you skipped the parent-side pre-validation in
  step 2 and GitHub rejects the **whole** review for a line outside the diff hunks, the
  error names the offending path/line — drop those comment(s) and re-submit. Better:
  do the pre-validation so this never happens.
- **Never silently drop an explanation.** Any comment that can't be anchored (failed
  validation, or repeatedly rejected) still has teaching value. Append its text to the
  review `body` under a short "**Couldn't pin inline:**" section, keyed by `path:line`
  (e.g. `` `iceberg_landing.rs:49` — <the explanation> ``), so Joe still gets it next to
  a file/line reference. If there are several, an alternative is a single follow-up
  plain PR comment (`gh api repos/$REPO/pulls/$PR/comments`… is for *inline* review
  comments; use `gh pr comment $PR --body-file …` for a normal top-level comment)
  collecting them — but folding into the review `body` keeps it to one notification.
- **Alternative — individual inline comments.** If Joe prefers each note to post
  independently (or a PR has so many comments that one bad anchor sinking the batch is
  annoying), post each on its own via
  `gh api repos/$REPO/pulls/$PR/comments -f commit_id=$HEAD_SHA -f path=… -F line=… -f side=RIGHT -f body=…`.
  Same inline rendering, each survives or fails alone — but it's N notifications **and**
  N creation calls (the secondary-rate-limit risk above). Default to the single review;
  switch only when asked or when the batch keeps tripping on un-anchorable lines, and
  throttle if you do.
- Keep the review `body` itself useful on its own: a plain-language overview of the
  PR plus a one-line "what changed and why" per file, so Joe gets the narrative even
  before reading the inline pins.
- Use `event: "COMMENT"` always. This skill never approves or requests changes.

### 4. Report back

Tell Joe the review is posted, link it (`gh pr view "$PR" --web` or the review URL
from the API response), and give a 3-5 line plain-English tour of the PR's shape so
he has the map before he clicks in.

## Notes

- **Never automatic.** No hook wires this; it runs only on explicit invocation.
- **Read-only on code.** It posts comments; it never edits files or the branch.
- **Local buck2 builds may not work on every host** (e.g. macOS has no cpython
  toolchain entry) — this skill needs none. It only reads the diff and talks to
  GitHub via `gh`, so it works anywhere `gh` is authenticated.
- If the PR has no GitHub remote (a local-only branch), say so — inline comments
  require the PR to exist on GitHub.
