---
paths:
  - "**/*.md"
---

# Markdown files must pass the lint hooks

The `lint` CI job (`buck2 run //tools:prek -- run --all-files`) runs the
pre-commit hooks from `prek.toml` against **every file in the tree, not just
Rust** — `end-of-file-fixer` and `trim trailing whitespace` police `.md` files
too. A docs-only PR with a green build/test will still fail `lint` (which runs
on all events) if a markdown file trips a hook. This is exactly what happened on
PR #12 (the auto-generated `docs/stpa/STPA.md`).

When writing or editing any `.md` file — including **generated** docs like
`docs/stpa/STPA.md` that a scheduled routine renders:

- **End the file with exactly one trailing newline.** No extra blank line(s) at
  EOF — a templated trailing `\n\n` is the usual culprit. `end-of-file-fixer`
  rewrites this and fails CI when it has to.
- **No trailing whitespace** on any line — `trim trailing whitespace` enforces
  this.

**Before pushing, run the hooks and commit whatever they change:**

```
buck2 run //tools:prek -- run --all-files
```

The hooks fix files in place; if CI shows a diff under "All changes made by
hooks," this step was skipped. If you can't run prek locally (e.g. a routine
that pushes straight to a branch), follow up on the PR until the `lint` check is
green rather than leaving it red.
