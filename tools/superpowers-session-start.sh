#!/usr/bin/env bash
# SessionStart hook: inject the vendored superpowers "using-superpowers" skill
# as session context, mirroring the upstream superpowers plugin's session-start
# hook. The skills are vendored under .claude/skills/ by
# tools/vendor-superpowers.sh, so this reads the skill from there rather than
# from a plugin root. Wired in .claude/settings.json.
set -euo pipefail

ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
skill_file="$ROOT/.claude/skills/using-superpowers/SKILL.md"

content="$(cat "$skill_file" 2>/dev/null || echo "Error reading using-superpowers skill")"

# Escape for JSON embedding (single-pass parameter substitutions).
escape_for_json() {
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

escaped="$(escape_for_json "$content")"
ctx="<EXTREMELY_IMPORTANT>\nYou have superpowers.\n\n**Below is the full content of your 'using-superpowers' skill - your introduction to using skills. For all other skills, use the 'Skill' tool:**\n\n${escaped}\n</EXTREMELY_IMPORTANT>"

printf '{\n  "hookSpecificOutput": {\n    "hookEventName": "SessionStart",\n    "additionalContext": "%s"\n  }\n}\n' "$ctx"
exit 0
