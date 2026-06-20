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
  /^- \[[ x]\] \*\*.*\*\* `\{#.*\}`[ \t]*$/ {
    line=$0
    cb=substr(line,4,1)
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
    # Any item bullet that does NOT match the strict grammar is malformed.
    awk '/^- \[[ x]\] / && $0 !~ /^- \[[ x]\] \*\*.*\*\* `\{#.*\}`[ \t]*$/ {
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
    [ -n "$id" ] || echo "$file:$ln: missing #id" >>"$ERR"
    case "$id" in "$want"*) : ;; *) echo "$file:$ln: id '$id' must start with '$want'" >>"$ERR" ;; esac
    case "$AREAS"   in *" $area "*)   : ;; *) echo "$file:$ln: bad area '$area'" >>"$ERR" ;; esac
    case "$allowed" in *" $status "*) : ;; *) echo "$file:$ln: bad status '$status' for $R register" >>"$ERR" ;; esac
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

main(){
  local cmd="${1:-}"; shift || true
  case "$cmd" in
    validate) cmd_validate "$@" ;;
    *) usage ;;
  esac
}
main "$@"
