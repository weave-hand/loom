#!/usr/bin/env bash
# loom documentation-register tool. Parses the tagged-item grammar in
# docs/{ROADMAP,FUTURE,ISSUES}.md and provides validate / query / shipped-open.
# See docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md.
set -euo pipefail

AREAS=" lineage catalog ontology acl ingest query transform iceberg ui ux test quality devx build deploy cross-cutting "
REGISTERS=(docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md)

usage(){ echo "usage: docs.sh {validate|query|shipped-open|claim|release|claims} [args]" >&2; exit 2; }

# Emit a TSV of every well-formed item across the given files:
#   file  lineno  cb  regtype  id  area  status  from  pr  spec  title
_extract(){
  awk '
  function reg(f){ f=tolower(f);
    if (f ~ /roadmap/) return "roadmap";
    if (f ~ /future/)  return "future";
    if (f ~ /issues/)  return "issues";
    return "unknown" }
  /^- \[[ xX]\] / && index($0, "`{#") && index($0, "}`") {
    line=$0
    cb=tolower(substr(line,4,1))
    s=index(line,"**"); rest=substr(line,s+2); e=index(rest,"**"); title=substr(rest,1,e-1)
    b1=index(line,"`{"); b2=index(line,"}`"); block=substr(line,b1+2,b2-(b1+2))
    id=""; area=""; status=""; from="-"; pr="-"; spec="-"
    n=split(block,t," ")
    for (i=1;i<=n;i++){ x=t[i];
      if (x ~ /^#/) { id=substr(x,2) }
      else { c=index(x,":"); if (c>0){ k=substr(x,1,c-1); v=substr(x,c+1);
        if (k=="area") area=v; else if (k=="status") status=v;
        else if (k=="from") from=v; else if (k=="pr") pr=v; else if (k=="spec") spec=v } }
    }
    if (id=="") id="-"; if (area=="") area="-"; if (status=="") status="-"
    printf "%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
      FILENAME, FNR, cb, reg(FILENAME), id, area, status, from, pr, spec, title
  }' "$@"
}

SPECDIR="docs/superpowers/specs"

_claim_ref(){ printf 'refs/claim/%s' "$1"; }

# True if $1 is a well-formed register id (prefixed + ref-safe).
_valid_id(){
  case "$1" in road-*|fut-*|iss-*) : ;; *) return 1 ;; esac
  printf '%s' "$1" | grep -qE '^[a-z0-9-]+$'
}

# Print the TSV row(s) for item id $1 from the on-disk registers (empty if none).
_item_row(){
  local id="$1" present=() f tsv
  for f in "${REGISTERS[@]}"; do [ -f "$f" ] && present+=("$f"); done
  [ ${#present[@]} -gt 0 ] || return 0
  tsv="$(_extract "${present[@]}")"
  printf '%s\n' "$tsv" | awk -F'\t' -v id="$id" '$5==id'
}

# Read claimant/since from the remote claim ref and report who holds id $1.
_print_holder(){
  local id="$1" ref body who since
  ref="$(_claim_ref "$id")"
  git fetch -q origin "$ref" 2>/dev/null || true
  body="$(git show -s --format=%B FETCH_HEAD 2>/dev/null || true)"
  who="$(printf '%s\n' "$body" | awk -F': ' '/^claimant:/{print $2}')"
  since="$(printf '%s\n' "$body" | awk -F': ' '/^since:/{print $2}')"
  echo "claim: '$id' is held by ${who:-?} since ${since:-?}" >&2
}

# ---- GitHub API fallback for ref writes -------------------------------------
# The claim mutex pushes/deletes refs under refs/claim/*. Some git remotes
# forbid that — notably the Claude Code on the web cloud session's git proxy,
# which 403s any push to a non-refs/heads namespace and any ref deletion. When
# the proxy push fails and a $GITHUB_TOKEN is present, we fall back to GitHub's
# git-database REST API, which authenticates with the token directly (bypassing
# the proxy). Atomicity is preserved: POST /git/refs is create-only (422 if the
# ref already exists), the same compare-and-swap the --force-with-lease push gives.

# The well-known empty-tree object id (present in every git repo). GitHub rejects
# creating an empty tree via the API, but accepts referencing this sha directly.
EMPTY_TREE=4b825dc642cb6eb9a060e54bf8d69288fbee4904

# True when the REST fallback is usable.
_have_api(){ [ -n "${GITHUB_TOKEN:-}" ] && command -v curl >/dev/null 2>&1; }

# owner/repo parsed from the origin remote URL (handles http(s) and scp forms).
_repo_slug(){
  local url; url="$(git config --get remote.origin.url 2>/dev/null || true)"
  url="${url%.git}"
  printf '%s' "$url" | sed -E 's#.*[/:]([^/]+/[^/]+)$#\1#'
}

# Extract the first (document-order) JSON string field named $1 from stdin.
# grep -o yields each "key":"value" hit on its own line so head -1 picks the
# first — robust whether the JSON is pretty-printed or single-line.
_json_field(){
  grep -oE "\"$1\"[[:space:]]*:[[:space:]]*\"[^\"]*\"" | head -1 \
    | sed -E 's/.*:[[:space:]]*"([^"]*)"/\1/'
}

# _api METHOD PATH [BODY]; sets _API_CODE (HTTP status) and _API_OUT (body).
_API_CODE=""
_API_OUT=""
_api(){
  local method="$1" path="$2" body="${3:-}" slug out a
  slug="$(_repo_slug)"
  out="$(mktemp)"
  a=(-s -o "$out" -w '%{http_code}' -X "$method"
     -H "Authorization: Bearer ${GITHUB_TOKEN:-}"
     -H "Accept: application/vnd.github+json"
     "https://api.github.com/repos/$slug$path")
  [ -n "$body" ] && a+=(-d "$body")
  _API_CODE="$(curl "${a[@]}" || true)"
  _API_OUT="$(cat "$out")"; rm -f "$out"
}

# JSON-escaped one-line claim commit message (literal \n separators, matching the
# multi-line body git commit-tree produces on the native path).
_claim_message(){
  local id="$1" who="$2" ts="$3" reg="$4" spec="$5"
  who="${who//\\/\\\\}"; who="${who//\"/\\\"}"   # escape \ and " for JSON
  printf 'claim: %s\\n\\nid: %s\\nclaimant: %s\\nsince: %s\\nregister: %s\\nspec: %s\\nbranch: work/%s' \
    "$id" "$id" "$who" "$ts" "$reg" "$spec" "$id"
}

# Delete a remote ref ($1), preferring a proxy push and falling back to the API.
# 0 = ref gone (deleted or already absent); 1 = could not delete.
_delete_ref_remote(){
  local ref="$1"
  git push origin ":$ref" >/dev/null 2>&1 && return 0
  _have_api || return 1
  _api DELETE "/git/refs/${ref#refs/}"
  case "$_API_CODE" in 204|422) return 0 ;; *) return 1 ;; esac   # 422 = already gone
}

CLAIM_GRACE_MIN="${LOOM_CLAIM_GRACE_MIN:-60}"

# Epoch seconds for an ISO-8601 UTC timestamp (0 on parse failure).
_epoch(){ date -u -d "$1" +%s 2>/dev/null || echo 0; }

# Echo open|gone|none|unknown for the work branch PR of id $1 (gone = merged/closed).
# Overridable for tests via LOOM_CLAIM_PR_PROBE (a command receiving the id and
# echoing the state).
_pr_state(){
  local id="$1" open_n all_n
  if [ -n "${LOOM_CLAIM_PR_PROBE:-}" ]; then "$LOOM_CLAIM_PR_PROBE" "$id"; return; fi
  if command -v gh >/dev/null 2>&1; then
    open_n="$(gh pr list --head "work/$id" --state open --json number -q 'length' 2>/dev/null || echo 0)"
    if [ "${open_n:-0}" -gt 0 ]; then echo open; return; fi
    # No open PR: was there ever one (merged/closed)? If so the work is done -> gone.
    all_n="$(gh pr list --head "work/$id" --state all --json number -q 'length' 2>/dev/null || echo 0)"
    [ "${all_n:-0}" -gt 0 ] && echo gone || echo none
  else
    echo unknown
  fi
}

cmd_validate(){
  # spec-file existence is enforced only when validating the real registers (no
  # file args) — fixture-based grammar checks pass synthetic spec slugs.
  local files=("$@"); local check_specs=0
  [ ${#files[@]} -gt 0 ] || { files=("${REGISTERS[@]}"); check_specs=1; }
  local ERR; ERR="$(mktemp)"
  local present=() f
  for f in "${files[@]}"; do
    if [ ! -f "$f" ]; then echo "$f: not found" >>"$ERR"; continue; fi
    present+=("$f")
    # Any item bullet lacking a well-formed `{# … }` tag block is malformed.
    # index() string-matching (not a brace regex) — portable across awk/mawk
    # builds, where `{`/`}` in an ERE are interval metacharacters parsed
    # inconsistently (the cause of a CI-only false "unresolved link").
    awk '/^- \[[ xX]\] / { if (index($0, "`{#") == 0 || index($0, "}`") == 0)
      printf "%s:%d: malformed item tag block\n", FILENAME, FNR }' "$f" >>"$ERR"
  done
  if [ ${#present[@]} -eq 0 ]; then sort -u "$ERR"; rm -f "$ERR"; return 1; fi

  local TSV; TSV="$(mktemp)"; _extract "${present[@]}" >"$TSV"
  local file ln cb R id area status from pr spec title want allowed term
  while IFS=$'\t' read -r file ln cb R id area status from pr spec title; do
    case "$R" in
      roadmap) want="road-"; allowed=" planned done ";  term=" done " ;;
      future)  want="fut-";  allowed=" deferred promoted dropped "; term=" promoted dropped " ;;
      issues)  want="iss-";  allowed=" open fixed wontfix ";        term=" fixed wontfix " ;;
      *) echo "$file:$ln: unknown register type (filename must contain ROADMAP/FUTURE/ISSUES)" >>"$ERR"; continue ;;
    esac
    if [ "$id" = "-" ]; then echo "$file:$ln: missing #id" >>"$ERR"
    else case "$id" in "$want"*) : ;; *) echo "$file:$ln: id '$id' must start with '$want'" >>"$ERR" ;; esac; fi
    if [ "$area" = "-" ]; then echo "$file:$ln: missing area" >>"$ERR"
    else case "$AREAS" in *" $area "*) : ;; *) echo "$file:$ln: bad area '$area'" >>"$ERR" ;; esac; fi
    if [ "$status" = "-" ]; then echo "$file:$ln: missing status" >>"$ERR"
    else case "$allowed" in *" $status "*) : ;; *) echo "$file:$ln: bad status '$status' for $R register" >>"$ERR" ;; esac; fi
    if [ "$cb" = "x" ]; then
      case "$term" in *" $status "*) : ;; *) echo "$file:$ln: checked [x] item must have a terminal status" >>"$ERR" ;; esac
    else
      case "$term" in *" $status "*) echo "$file:$ln: terminal status '$status' must be checked [x]" >>"$ERR" ;; *) : ;; esac
    fi
    if [ "$pr" != "-" ]; then
      printf '%s' "$pr" | grep -qE '^#[0-9]+(,#[0-9]+)*$' || echo "$file:$ln: bad pr '$pr' (want '-' or '#N[,#N...]')" >>"$ERR"
    fi
    if [ "$check_specs" = 1 ] && [ "$spec" != "-" ] && [ ! -f "$SPECDIR/$spec.md" ]; then
      echo "$file:$ln: spec file '$SPECDIR/$spec.md' missing" >>"$ERR"
    fi
  done <"$TSV"

  # Duplicate ids across all present files.
  cut -f5 "$TSV" | sort | uniq -d | while read -r d; do
    [ -n "$d" ] && echo "duplicate id: $d" >>"$ERR"; done

  # Every [[link]] must resolve to a known id.
  local IDS; IDS="$(cut -f5 "$TSV" | sort -u)"
  for f in "${present[@]}"; do
    # `|| true` so a register with zero links doesn't fail the pipe under pipefail.
    { grep -oE '\[\[[a-z0-9-]+\]\]' "$f" 2>/dev/null || true; } | sed 's/\[\[//; s/\]\]//' | while read -r lid; do
      printf '%s\n' "$IDS" | grep -qx "$lid" || echo "$f: unresolved link [[$lid]]" >>"$ERR"
    done
  done

  rm -f "$TSV"
  if [ -s "$ERR" ]; then sort -u "$ERR"; rm -f "$ERR"; return 1; fi
  rm -f "$ERR"; echo "docs.sh validate: OK (${#present[@]} files)"
}

cmd_query(){
  local kind="${1:-}"; shift || true
  if [ "$kind" = links ]; then
    local id="${1:-}"; [ -n "$id" ] || { echo "usage: docs.sh query links <id>" >&2; return 2; }
    grep -nE "\[\[$id\]\]" "${REGISTERS[@]}" 2>/dev/null || echo "no references to [[$id]]"
    return 0
  fi
  local area="" status=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --area)   area="${2:-}";   shift 2 ;;
      --status) status="${2:-}"; shift 2 ;;
      *) shift ;;
    esac
  done
  local present=() f
  for f in "${REGISTERS[@]}"; do [ -f "$f" ] && present+=("$f"); done
  [ ${#present[@]} -gt 0 ] || { echo "no registers found" >&2; return 1; }
  local TSV; TSV="$(mktemp)"; _extract "${present[@]}" >"$TSV"
  case "$kind" in
    open|done)
      local cb; [ "$kind" = open ] && cb=" " || cb="x"
      awk -F'\t' -v cb="$cb" -v a="$area" -v s="$status" '
        $3==cb && (a==""||$6==a) && (s==""||$7==s){ printf "%-9s %-26s %s\n", $4, $5, $11 }' "$TSV" ;;
    by-area)
      cut -f6 "$TSV" | sort | uniq -c | sort -rn ;;
    *) echo "unknown query kind: $kind (want open|done|by-area|links)" >&2; rm -f "$TSV"; return 2 ;;
  esac
  rm -f "$TSV"
}

cmd_shipped_open(){
  local stale=0
  [ "${1:-}" = "--stale" ] && stale=1
  local present=() f
  for f in "${REGISTERS[@]}"; do [ -f "$f" ] && present+=("$f"); done
  [ ${#present[@]} -gt 0 ] || { echo "no registers found" >&2; return 1; }
  local TSV; TSV="$(mktemp)"; _extract "${present[@]}" >"$TSV"
  # if-form, not `A && have_gh=1`, so a missing gh doesn't trip set -e.
  local have_gh=0; if command -v gh >/dev/null 2>&1; then have_gh=1; fi
  local file ln cb R id area status from pr spec title
  while IFS=$'\t' read -r file ln cb R id area status from pr spec title; do
    [ "$cb" = " " ] || continue                    # open items only
    if [ "$stale" = 1 ]; then
      [ "$pr" = "-" ] || continue                  # --stale: open with NO pr
      printf '%s\t%s\t(no PR linked)\n' "$id" "$title"
    else
      [ "$pr" != "-" ] || continue                 # default: open WITH a pr
      local note="$pr"
      if [ "$have_gh" = 1 ]; then
        local n="${pr%%,*}"; n="${n#\#}"
        local st; st="$(gh pr view "$n" --json state -q .state 2>/dev/null || echo '?')"
        note="$pr (#$n: $st)"
      fi
      printf '%s\t%s\t%s\n' "$id" "$title" "$note"
    fi
  done <"$TSV"
  rm -f "$TSV"
}

cmd_claim(){
  local id="${1:-}"
  [ -n "$id" ] || { echo "usage: docs.sh claim <id>" >&2; return 2; }
  _valid_id "$id" || { echo "claim: invalid id '$id' (want road-/fut-/iss- + [a-z0-9-])" >&2; return 2; }
  local row cb reg status spec
  row="$(_item_row "$id")"
  row="${row%%$'\n'*}"   # first matching line only (ids are unique); no pipe → no SIGPIPE/pipefail
  [ -n "$row" ] || { echo "claim: unknown id '$id' (not in any register)" >&2; return 1; }
  cb="$(printf '%s' "$row" | cut -f3)"
  reg="$(printf '%s' "$row" | cut -f4)"
  status="$(printf '%s' "$row" | cut -f7)"
  spec="$(printf '%s' "$row" | cut -f10)"
  [ "$cb" = " " ] || { echo "claim: '$id' is closed ([x]); only open items are claimable" >&2; return 1; }
  case "$reg" in
    roadmap) [ "$status" = planned ]  || { echo "claim: '$id' status '$status' not actionable (want planned)"  >&2; return 1; } ;;
    future)  [ "$status" = deferred ] || { echo "claim: '$id' status '$status' not actionable (want deferred)" >&2; return 1; } ;;
    issues)  [ "$status" = open ]     || { echo "claim: '$id' status '$status' not actionable (want open)"     >&2; return 1; } ;;
    *) echo "claim: '$id' has unknown register '$reg'" >&2; return 1 ;;
  esac
  [ "$spec" != "-" ] || { echo "claim: '$id' has no spec (direction not set); brainstorm a spec first" >&2; return 1; }
  [ -f "$SPECDIR/$spec.md" ] || { echo "claim: spec file '$SPECDIR/$spec.md' missing for '$id'" >&2; return 1; }
  local ref; ref="$(_claim_ref "$id")"
  if [ -n "$(git ls-remote origin "$ref" 2>/dev/null)" ]; then
    echo "claim: '$id' is already claimed" >&2; _print_holder "$id"; return 1
  fi
  local who ts tree commit
  who="$(git config user.email 2>/dev/null || git config user.name 2>/dev/null || echo unknown)"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  tree="$(git mktree </dev/null)"
  commit="$(printf 'claim: %s\n\nid: %s\nclaimant: %s\nsince: %s\nregister: %s\nspec: %s\nbranch: work/%s\n' \
    "$id" "$id" "$who" "$ts" "$reg" "$spec" "$id" | git commit-tree "$tree")"
  if git push --force-with-lease="$ref:" origin "$commit:$ref" >/dev/null 2>&1; then
    _claim_success "$id" "$who" "$ts"; return 0
  fi
  # Native push failed (e.g. the cloud git proxy forbids custom-ref writes).
  # Fall back to the GitHub API if a token is available; the create is atomic.
  if _have_api; then
    local body sha
    body="$(_claim_message "$id" "$who" "$ts" "$reg" "$spec")"
    _api POST /git/commits "$(printf '{"message":"%s","tree":"%s","parents":[]}' "$body" "$EMPTY_TREE")"
    [ "$_API_CODE" = 201 ] || { echo "claim: API commit-create failed (HTTP $_API_CODE)" >&2; return 1; }
    sha="$(printf '%s' "$_API_OUT" | _json_field sha || true)"
    [ -n "$sha" ] || { echo "claim: could not parse commit sha from API response" >&2; return 1; }
    _api POST /git/refs "$(printf '{"ref":"%s","sha":"%s"}' "$ref" "$sha")"
    case "$_API_CODE" in
      201) _claim_success "$id" "$who" "$ts"; return 0 ;;
      422) echo "claim: lost race for '$id'" >&2; _print_holder "$id"; return 1 ;;   # ref already exists
      *)   echo "claim: API ref-create failed (HTTP $_API_CODE)" >&2; return 1 ;;
    esac
  fi
  echo "claim: lost race for '$id'" >&2; _print_holder "$id"; return 1
}

# Print the success banner for a freshly minted claim (id who ts).
_claim_success(){
  echo "claimed $1 by $2 at $3"
  echo "next: git switch -c work/$1  →  implement  →  open a PR with head work/$1"
}

cmd_release(){
  local id="${1:-}"
  [ -n "$id" ] || { echo "usage: docs.sh release <id>" >&2; return 2; }
  _valid_id "$id" || { echo "release: invalid id '$id'" >&2; return 2; }
  local ref; ref="$(_claim_ref "$id")"
  if [ -z "$(git ls-remote origin "$ref" 2>/dev/null)" ]; then
    echo "release: no live claim for '$id' (nothing to do)"; return 0
  fi
  if _delete_ref_remote "$ref"; then
    git update-ref -d "$ref" 2>/dev/null || true
    echo "released $id"
  else
    echo "release: failed to delete $ref" >&2; return 1
  fi
}

cmd_claims(){
  local reap=0; [ "${1:-}" = "--reap" ] && reap=1
  local now grace_s lines
  now="$(date -u +%s)"
  grace_s=$(( CLAIM_GRACE_MIN * 60 ))
  lines="$(git ls-remote origin 'refs/claim/*' 2>/dev/null || true)"
  [ -n "$lines" ] || { echo "no live claims"; return 0; }
  local sha ref id body who since since_s age pr state left
  while read -r sha ref; do
    [ -n "$ref" ] || continue
    id="${ref#refs/claim/}"
    git fetch -q origin "$ref" 2>/dev/null || true
    body="$(git show -s --format=%B FETCH_HEAD 2>/dev/null || true)"
    who="$(printf '%s\n' "$body" | awk -F': ' '/^claimant:/{print $2}')"
    since="$(printf '%s\n' "$body" | awk -F': ' '/^since:/{print $2}')"
    since_s="$(_epoch "$since")"
    age=$(( now - since_s ))
    pr="$(_pr_state "$id")"
    if [ "$pr" = open ]; then
      state="PR open"
    elif [ "$pr" = gone ]; then
      state="merged/closed"                  # PR done -> reapable now, ignore grace
    elif [ "$pr" = unknown ]; then
      state="PR unknown (gh unavailable)"   # never reaped — can't confirm no PR
    elif [ "$age" -gt "$grace_s" ]; then
      state="stale"
    else
      left=$(( (grace_s - age + 59) / 60 ))
      state="PR pending (${left}m left)"
    fi
    if [ "$reap" = 1 ] && { [ "$state" = stale ] || [ "$state" = "merged/closed" ]; }; then
      _delete_ref_remote "$ref" && echo "reaped $id ($state)"
    else
      printf '%s\t%s\t%dm\t%s\n' "$id" "${who:-?}" "$(( age / 60 ))" "$state"
    fi
  done <<EOF
$lines
EOF
}

main(){
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    validate) cmd_validate "$@" ;;
    query) cmd_query "$@" ;;
    shipped-open) cmd_shipped_open "$@" ;;
    claim) cmd_claim "$@" ;;
    release) cmd_release "$@" ;;
    claims) cmd_claims "$@" ;;
    *) usage ;;
  esac
}
# Run only when executed directly; `source`-ing exposes helpers for unit tests.
# Use an if-block (not `&&`) so a sourced script ends with status 0 under set -e.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then main "$@"; fi
