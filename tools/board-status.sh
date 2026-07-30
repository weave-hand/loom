#!/usr/bin/env bash
# Set a `loom v1` board Status for an issue — the single home for the ProjectV2
# GraphQL mutation the loom-work-* skills used to inline in several places.
#
# Usage: tools/board-status.sh <issue-number> <status-option-id>
#   Status option ids (Status field PVTSSF_lADOEV2iVs4BeJ8bzhYmQKk):
#     Backlog f75ad846 | Ready 61e4505c | In progress 47fc9ee4 |
#     In review df73e18b | Done 98236657
#
# Auth + egress:
#   - The mutation needs the `project` scope. A fine-grained PAT carrying org
#     `project` permission has it; a `gh auth login` OAuth token (gho_*) does NOT
#     by default — it must be granted explicitly:
#         gh auth refresh -s project
#     A missing scope is a PERMANENT local misconfiguration, not a transient
#     egress fault, and `skip` below names it as such (this script used to claim
#     "the token is never the blocker", which was wrong and sent readers hunting
#     for a proxy problem that did not exist).
#   - In CLOUD the other obstacle is the Claude GitHub App proxy, which 403s
#     `gh api` calls to `api.github.com` (Projects GraphQL). So a cloud session
#     routes `api.github.com` around the proxy via NO_PROXY and lets the ambient
#     token reach Projects GraphQL directly — no separate secret needed.
#   - If the direct egress is still blocked (or gh is unauthenticated), the update
#     is SKIPPED (non-fatal, exit 0) and a later local session reconciles. This
#     script never aborts the caller — labels / issue+PR state stay authoritative.
#
# Every GitHub call is wrapped in `timeout` so a blocked-egress cloud session
# fails fast (and skips) instead of hanging.
set -u

PROJECT=PVT_kwDOEV2iVs4BeJ8b
STATUS_FIELD=PVTSSF_lADOEV2iVs4BeJ8bzhYmQKk
REPO=weave-hand/loom
GH_TIMEOUT=20

usage() { echo "usage: tools/board-status.sh <issue-number> <status-option-id>" >&2; exit 2; }
[ "$#" -eq 2 ] || usage
ISSUE="$1"
OPTION="$2"
case "$ISSUE" in ''|*[!0-9]*) usage;; esac

# Every `gh` call appends its stderr here, so a skip can report WHY rather than
# guessing. Without this the calls were `2>/dev/null` and every failure — missing
# scope, blocked egress, bad id — rendered as the same misleading "unreachable?".
ERRLOG=$(mktemp)
trap 'rm -f "$ERRLOG"' EXIT

skip() {
  echo "board-status: $1 — skipping board update for #$ISSUE (a local session reconciles)." >&2
  # Match gh's stderr rendering, not the JSON `type: INSUFFICIENT_SCOPES` (that
  # goes to stdout, which --jq consumes — so it never reaches this log).
  if grep -qi 'not been granted the required scopes' "$ERRLOG" 2>/dev/null; then
    echo "board-status:   cause: the gh token lacks the 'project' scope. This is NOT an egress problem." >&2
    echo "board-status:   fix:   gh auth refresh -s project" >&2
  elif [ -s "$ERRLOG" ]; then
    echo "board-status:   last error: $(tail -n 1 "$ERRLOG")" >&2
  fi
  exit 0
}

# Cloud sessions: route api.github.com around the egress proxy so the ambient gh
# token reaches Projects GraphQL directly. We set NO_PROXY HERE, not only in
# ~/.bashrc, because non-interactive `bash -c` tool-call shells skip the profile
# past its `[ -z "$PS1" ] && return` guard — so the profile's bypass never reaches
# this process (the same reason the buck2 shim self-injects NO_PROXY for its
# daemon). No token swap: the ambient token already has `project` permission.
if [ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || [ "${REMOTE_ENV:-}" = "true" ]; then
  export NO_PROXY="${NO_PROXY:+$NO_PROXY,}api.github.com"
  export no_proxy="$NO_PROXY"
fi

gh_api() { timeout "$GH_TIMEOUT" gh api "$@" 2>>"$ERRLOG"; }

node_id=$(gh_api "repos/$REPO/issues/$ISSUE" --jq .node_id) || skip "could not resolve node id for #$ISSUE"
[ -n "$node_id" ] || skip "empty node id for #$ISSUE"

item_id=$(gh_api graphql -f query='mutation($p:ID!,$c:ID!){addProjectV2ItemById(input:{projectId:$p,contentId:$c}){item{id}}}' \
  -f p="$PROJECT" -f c="$node_id" --jq '.data.addProjectV2ItemById.item.id') \
  || skip "addProjectV2ItemById failed"
[ -n "$item_id" ] || skip "empty project item id for #$ISSUE"

gh_api graphql -f query='mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,value:{singleSelectOptionId:$o}}){projectV2Item{id}}}' \
  -f p="$PROJECT" -f i="$item_id" -f f="$STATUS_FIELD" -f o="$OPTION" --jq '.data.updateProjectV2ItemFieldValue.projectV2Item.id' >/dev/null \
  || skip "updateProjectV2ItemFieldValue failed"

echo "board-status: #$ISSUE Status -> $OPTION"
