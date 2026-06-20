#!/usr/bin/env bash
# loom documentation-register tool. Parses the tagged-item grammar in
# docs/{ROADMAP,FUTURE,ISSUES}.md and provides validate / query / shipped-open.
# See docs/superpowers/specs/2026-06-20-docs-registers-consolidation-design.md.
set -euo pipefail

AREAS=" lineage catalog ontology acl ingest query transform iceberg ui ux test quality devx build deploy cross-cutting "
REGISTERS=(docs/ROADMAP.md docs/FUTURE.md docs/ISSUES.md)

usage(){ echo "usage: docs.sh {validate|query|shipped-open} [args]" >&2; exit 2; }

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

cmd_validate(){
  local files=("$@"); [ ${#files[@]} -gt 0 ] || files=("${REGISTERS[@]}")
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
      roadmap) want="road-"; allowed=" planned in-progress done ";  term=" done " ;;
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

main(){
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    validate) cmd_validate "$@" ;;
    query) cmd_query "$@" ;;
    shipped-open) cmd_shipped_open "$@" ;;
    *) usage ;;
  esac
}
main "$@"
