---
name: loom-stpa
description: Generate or refresh the STPA (System-Theoretic Process Analysis, STPA-Sec) safety model for this repo at docs/stpa/STPA.md, landing any change as a PR against main that merges on green CI. Use when asked to update/regenerate loom's STPA safety analysis, run the STPA routine, audit unsafe control actions, or when invoked on a schedule. The analysis is deterministic — findings are extracted as JSON (judgment), the markdown is rendered by an embedded jq renderer (mechanism), so scheduled runs produce small reviewable diffs.
---

Generate/update the STPA safety model for this repository as `docs/stpa/STPA.md`, and land any change as a PR against `main` that you merge once CI is green.

You are a systems-safety analyst applying STPA (System-Theoretic Process Analysis, Leveson) in the STPA-Sec tradition: "losses" are security and data-integrity violations, not just physical harm. You will EXTRACT findings as JSON (judgment), RENDER the document by running BLOCK A verbatim (mechanism), and — only if it changed — open/refresh a PR via BLOCK B. Determinism — and therefore small, reviewable diffs on every scheduled run — depends on you producing the markdown ONLY via the renderer. Do NOT hand-write or reformat the markdown.

────────────────────────────────────────────────────────
## Target invariants — EDIT THIS BLOCK per repository
- Mission: an open-source Palantir Foundry — a self-hosted, typed-object data platform: (a) an ontology (typed objects/links) over raw tables, (b) governance that follows the data (row/column ACLs, lineage, audit) enforced in the query path, (c) one substrate shared by pipelines, queries, apps.
- Reason to exist = GOVERNED CORRECTNESS of data access and provenance. Unsafe states violate that — not mere crashes.
Everything else (services, schemas, traits, which components exist) you MUST derive from the repo as it is now. Do not assume an architecture from prior knowledge — read it.
────────────────────────────────────────────────────────

## Steps
1. **Read the prior, if any.** If `docs/stpa/STPA.md` exists, read it — it is your prior analysis. Its tables carry the stable semantic keys (first column), conditions, and evidence. You will reuse them to minimize drift.
2. **Ground yourself in the code.** List the repo tree; read the READMEs, architecture/design docs, AND any "future work / deferred / roadmap" doc (discover by listing — don't assume filenames; it's the authoritative ledger of designed-but-not-built). Read the code units that issue or receive commands (traits, services, queues, transactions, worker loops). Trust CODE over docs on conflict; note the conflict in `scope`. Get the commit: `git rev-parse --short HEAD`.
3. **Analyze (STPA steps 1-3).** Hazards = system states that, worst-case, lead to a loss (map to loss keys). Control structure = controllers, controlled processes (nodes), control actions (commands, node→node), feedback (signals, node→node), each tagged `built`|`designed`. UCAs = for each control action test four guidewords and keep ONLY genuinely-unsafe ones: `providing`, `not-providing`, `wrong-timing`, `wrong-duration`. A bounded nuisance recovered by a fallback is NOT a UCA — put it in `non_ucas`.
4. **Write your findings** as a single JSON object (schema below) to `/tmp/stpa.json`.
5. **Render** by running BLOCK A VERBATIM. It writes `docs/stpa/STPA.md` from your JSON deterministically and prints either `STPA_RESULT=nochange` or `STPA_RESULT=changed`.
6. **If `nochange`:** report "no change — STPA.md already current" and STOP. Do not open a PR.
7. **If `changed`:** prepare the PR text. Run `git --no-pager diff -- docs/stpa/STPA.md` to see exactly what moved (or note this is the first version). Then:
   - Write a Conventional-Commits title to `/tmp/stpa-title.txt` — ONE line, ≤72 chars, form `docs(stpa): <what changed>` (e.g. `docs(stpa): add queue.fail abandon UCA; refresh 2 evidence refs`; first run: `docs(stpa): add STPA control-plane safety model`).
   - Write a concise markdown body to `/tmp/stpa-body.md` — 2–6 bullets naming the UCAs / hazards / control-actions **added, removed, or materially changed** (by their semantic keys), plus any scope/maturity shift. Summarize; don't paste the diff.
8. **Open/refresh the PR and merge on green** by running BLOCK B VERBATIM. It commits on a stable branch, force-pushes, opens or updates a PR against `main`, watches CI, and squash-merges once all checks pass (no repo auto-merge setting needed). Report the PR URL and whether it merged or was left open for review.

## Drift minimization (this is an UPDATE, not a rewrite)
The rendered doc is committed and reviewed as a diff — keep churn minimal:
- REUSE each prior finding's semantic key VERBATIM when the code it cites still exists and still means the same thing.
- KEEP prior `condition`/`statement`/`label`/`scope` wording UNCHANGED unless the underlying code changed. Do not reword for style.
- Update an `evidence` `path:line` only if the referenced code actually moved.
- But RE-VERIFY, never parrot: open each prior finding's cited code and confirm it still holds. Logic changed → update it. Cited code deleted → drop it. New unsafe control action → add it with a new key.
Net: unchanged code ⇒ identical finding; changed code ⇒ localized change; added/removed code ⇒ added/removed finding. Nothing else moves.

## Semantic keys (what makes diffs stable — the renderer sorts by them)
Derive each `key` from WHAT IT IS, never its position. Lowercase, no spaces.
- nodes: short component slug, NO dots (mermaid id): `acl`, `query-api`, `worker`, `postgres`, `ducklake`.
- losses: `L.unauthorized-access`, `L.integrity-loss`, `L.silent-incorrectness`, `L.provenance-loss`, `L.liveness-loss`.
- control_actions: `<component>.<operation>`: `acl.check`, `ontology.resolve`, `queue.dequeue`, `tx.commit`.
- hazards: condition slug: `stale-policy`, `partial-atomic-unit`, `premature-reclaim`, `dangling-target`.
- ucas: `<control_action-key>.<guideword>`: `queue.dequeue.wrong-timing`.
`from`/`to`/`control_action`/`hazards`/`losses` reference other objects BY KEY.

## Grounding rules (hard)
- Every node, control_action, uca has `evidence`: `path:line` (or `path`); for a doc/comment, also a ≤8-word verbatim quote. No evidence → omit it.
- NEVER mark a designed-only element `built`.
- No invention. Uncertainty → `open_questions`, not a guessed UCA.
- Prioritize; don't pad. Keep `condition`/`statement` to ONE line, no `|`, no newlines.

## JSON schema (write to /tmp/stpa.json; arrays in ANY order — the renderer sorts)
{
  "repo": "string", "commit": "short-sha", "scope": "1-3 sentences: built vs designed, what you read, any code-vs-docs conflict.",
  "losses": [ {"key":"L.unauthorized-access","title":"..."}, ...all five... ],
  "nodes": [ {"key":"acl","label":"Acl trait","layer":"control-plane","maturity":"built"}, ... ],   // layer: enforcement|control-plane|store
  "control_actions": [ {"key":"acl.check","label":"...","from":"query-api","to":"acl","maturity":"built","evidence":"acl.rs:126"}, ... ],
  "feedback": [ {"from":"postgres","to":"worker","signal":"NOTIFY wakeup hint"}, ... ],
  "hazards": [ {"key":"stale-policy","statement":"...","losses":["L.unauthorized-access"],"maturity":"built"}, ... ],
  "ucas": [ {"key":"queue.dequeue.wrong-timing","control_action":"queue.dequeue","guideword":"wrong-timing","condition":"...","severity":"high","hazards":["premature-reclaim"],"evidence":"postgres/src/lib.rs:153"}, ... ],  // severity: high|medium|low
  "non_ucas": [ {"item":"await_jobs missed NOTIFY","reason":"bounded by 5s poll fallback (worker/src/lib.rs:16)"}, ... ],
  "open_questions": [ "string", ... ]
}

## BLOCK A — render + detect change (run verbatim; never hand-write the .md)
```bash
set -euo pipefail
mkdir -p docs/stpa
cat > /tmp/stpa-render.jq <<'JQ'
def esc: (. // "") | gsub("\\|"; "\\|") | gsub("\n"; " ");
def mlabel: (. // "") | gsub("\n"; " ") | gsub("\""; "'") | gsub("<"; "(") | gsub(">"; ")");
def layer_idx: {"enforcement":0,"control-plane":1,"store":2};
def layer_title: {"enforcement":"Enforcement layer","control-plane":"Control plane (built)","store":"Stateful processes"};
"# STPA Control Analysis — \(.repo) @ \(.commit)\n\n"
+ "_Auto-generated STPA safety model: the unsafe states this system can reach and the control actions that get it there. Semantic IDs keep regenerations diff-stable._\n\n"
+ "<details>\n<summary><b>How to read this</b> — STPA primer and diagram legend</summary>\n\n"
+ "**STPA** (System-Theoretic Process Analysis) treats the system as *controllers* issuing *control actions* to *controlled processes*, with *feedback* flowing back up. Instead of \"what component can fail,\" it asks \"what control action, given or withheld at the wrong time, drives the system into an unsafe state?\" \"Unsafe\" here means a violation of the platform's reason to exist — governed correctness of data access and provenance — not merely a crash.\n\n"
+ "Read top-down: **Losses** are outcomes we must never cause; **Hazards** are system states that lead to a loss; the **control-structure diagram** shows who commands whom (solid arrows = control actions, dashed = feedback, a node tagged `(designed)` is in the architecture but **not yet built**); the **Unsafe Control Actions** table is the core. Every claim cites `path:line`; unbuilt elements are marked. Semantic, stable IDs mean regenerating changes only the findings that changed.\n</details>\n\n"
+ "**Scope & maturity:** \(.scope)\n\n"
+ "## Control structure\n\n```mermaid\nflowchart TD\n"
+ ( .nodes | map(. + {i:(layer_idx[.layer]//9)}) | sort_by(.i,.key) | group_by(.i)
    | map( (.[0].layer) as $L | "  subgraph \($L)[\"\(layer_title[$L]//$L)\"]\n"
        + (map("    \(.key)[\"\(.label|mlabel)\(if .maturity=="designed" then " (designed)" else "" end)\"]")|join("\n")) + "\n  end\n")
    | join("") )
+ ( .control_actions | sort_by(.key) | map("  \(.from) -- \"\(.key)\" --> \(.to)")|join("\n") ) + "\n"
+ ( (.feedback//[]) | sort_by("\(.from)|\(.to)|\(.signal)") | map("  \(.from) -. \"\(.signal|mlabel)\" .-> \(.to)")|join("\n") )
+ "\n```\n\n"
+ "## Losses\n\n| ID | Loss |\n|----|------|\n"
+ ( .losses | sort_by(.key) | map("| `\(.key)` | \(.title|esc) |")|join("\n") ) + "\n\n"
+ "## Hazards\n\n| ID | Hazard (unsafe state) | → Losses | Maturity |\n|----|----|----|----|\n"
+ ( .hazards | sort_by(.key) | map("| `\(.key)` | \(.statement|esc) | \((.losses//[])|join(", ")) | \(.maturity|esc) |")|join("\n") ) + "\n\n"
+ "## Control actions\n\n| ID | Control action | Controller → Process | Maturity | Evidence |\n|----|----|----|----|----|\n"
+ ( .control_actions | sort_by(.key) | map("| `\(.key)` | \(.label|esc) | `\(.from)` → `\(.to)` | \(.maturity|esc) | \(.evidence|esc) |")|join("\n") ) + "\n\n"
+ "## Unsafe control actions\n\n*Each row: a control action made unsafe via one guideword, the hazard/loss it causes, and where in the code it lives.*\n\n"
+ "| ID | Control action | Guideword | Unsafe condition | Severity | → Hazards | Evidence |\n|----|----|----|----|----|----|----|\n"
+ ( .ucas | sort_by(.key) | map("| `\(.key)` | `\(.control_action)` | \(.guideword|esc) | \(.condition|esc) | \(.severity|esc) | \((.hazards//[])|join(", ")) | \(.evidence|esc) |")|join("\n") ) + "\n\n"
+ ( if (.non_ucas//[])|length>0 then "**Not UCAs (examined and rejected):** " + (.non_ucas|sort_by("\(.item)|\(.reason)")|map("\(.item|esc) — \(.reason|esc)")|join("; ")) + "\n\n" else "" end )
+ "## Open questions\n\n" + ((.open_questions//[])|sort|map("- \(esc)")|join("\n")) + "\n"
JQ
LC_ALL=C jq -rf /tmp/stpa-render.jq /tmp/stpa.json > /tmp/STPA.candidate.md
if [ -f docs/stpa/STPA.md ] && diff -q docs/stpa/STPA.md /tmp/STPA.candidate.md >/dev/null; then
  echo "STPA_RESULT=nochange"
else
  mv /tmp/STPA.candidate.md docs/stpa/STPA.md
  echo "STPA_RESULT=changed"
fi
```

## BLOCK B — commit, PR, watch CI, merge on green (run verbatim; ONLY when STPA_RESULT=changed)
```bash
set -euo pipefail
BRANCH=bot/stpa-update
git config --get user.email >/dev/null 2>&1 || git config user.email "stpa-bot@users.noreply.github.com"
git config --get user.name  >/dev/null 2>&1 || git config user.name  "stpa-bot"
git switch -C "$BRANCH"
git add docs/stpa/STPA.md
# --no-verify: skip loom's local commit-msg/pre-push hooks (buck2-build/test would
# stall the routine); conventional style is carried by the PR title -> squash commit.
git commit --no-verify -m "$(cat /tmp/stpa-title.txt)" -m "$(cat /tmp/stpa-body.md)"
git push --no-verify -f -u origin "$BRANCH"
if ! gh pr view "$BRANCH" >/dev/null 2>&1; then
  gh pr create --base main --head "$BRANCH" --title "$(cat /tmp/stpa-title.txt)" --body-file /tmp/stpa-body.md
fi
URL="$(gh pr view "$BRANCH" --json url -q .url)"
echo "PR: $URL"
# Poll CI ourselves, then merge on green — no repo-level auto-merge setting required.
if gh pr checks "$BRANCH" --watch --fail-fast; then
  gh pr merge "$BRANCH" --squash --delete-branch && echo "merged (CI green): $URL"
else
  echo "NOTE: CI not green — PR left open for review: $URL"
fi
```
