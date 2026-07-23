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
#   - LOCAL sessions use your own `gh` auth, which carries `project` scope — the
#     mutation just works.
#   - CLOUD sessions cannot reach Projects GraphQL through the Claude GitHub App
#     proxy (it brokers only the repo git lane + the issues/PRs MCP lane; the
#     platform token has no `project` scope and api.github.com is proxied). So in
#     a cloud session this uses $LOOM_PROJECT_TOKEN — a PAT with `project` scope —
#     and relies on `api.github.com` being in NO_PROXY (set by
#     tools/cloud-session-start.sh) to egress directly around the proxy.
#   - If $LOOM_PROJECT_TOKEN is unset in a cloud session, or the direct egress is
#     blocked, the update is SKIPPED (non-fatal, exit 0) and a later local session
#     reconciles the board. This script never aborts the caller — the `ready`
#     label / issue+PR state stay authoritative regardless.
#
# Every GitHub call is wrapped in `timeout` so a blocked-egress cloud session
# fails fast instead of hanging.
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

skip() { echo "board-status: $1 — skipping board update for #$ISSUE (a local session reconciles)." >&2; exit 0; }

# Cloud sessions: swap in the project-scoped PAT (the platform token can't touch
# Projects GraphQL) and route api.github.com around the egress proxy. We set
# NO_PROXY HERE, not only in ~/.bashrc, because non-interactive `bash -c`
# tool-call shells skip the profile past its `[ -z "$PS1" ] && return` guard — so
# the profile's bypass never reaches this process (the same reason the buck2 shim
# self-injects NO_PROXY for its daemon). Empty PAT => skip softly.
if [ "${CLAUDE_CODE_REMOTE:-}" = "true" ] || [ "${REMOTE_ENV:-}" = "true" ]; then
  [ -n "${LOOM_PROJECT_TOKEN:-}" ] || skip "LOOM_PROJECT_TOKEN unset in cloud session"
  export GH_TOKEN="$LOOM_PROJECT_TOKEN"
  export NO_PROXY="${NO_PROXY:+$NO_PROXY,}api.github.com"
  export no_proxy="$NO_PROXY"
fi

gh_api() { timeout "$GH_TIMEOUT" gh api "$@"; }

node_id=$(gh_api "repos/$REPO/issues/$ISSUE" --jq .node_id 2>/dev/null) || skip "could not resolve node id for #$ISSUE (board unreachable?)"
[ -n "$node_id" ] || skip "empty node id for #$ISSUE"

item_id=$(gh_api graphql -f query='mutation($p:ID!,$c:ID!){addProjectV2ItemById(input:{projectId:$p,contentId:$c}){item{id}}}' \
  -f p="$PROJECT" -f c="$node_id" --jq '.data.addProjectV2ItemById.item.id' 2>/dev/null) \
  || skip "addProjectV2ItemById failed (Projects GraphQL unreachable?)"
[ -n "$item_id" ] || skip "empty project item id for #$ISSUE"

gh_api graphql -f query='mutation($p:ID!,$i:ID!,$f:ID!,$o:String!){updateProjectV2ItemFieldValue(input:{projectId:$p,itemId:$i,fieldId:$f,value:{singleSelectOptionId:$o}}){projectV2Item{id}}}' \
  -f p="$PROJECT" -f i="$item_id" -f f="$STATUS_FIELD" -f o="$OPTION" --jq '.data.updateProjectV2ItemFieldValue.projectV2Item.id' >/dev/null 2>&1 \
  || skip "updateProjectV2ItemFieldValue failed"

echo "board-status: #$ISSUE Status -> $OPTION"
