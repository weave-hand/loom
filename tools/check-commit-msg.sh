#!/usr/bin/env bash
# Enforce Conventional Commits (https://www.conventionalcommits.org/) on the
# commit message. prek runs this on the `commit-msg` stage and passes the path
# to the prepared commit message file as $1.
#
# Accepted header:  <type>(<scope>)?(!)?: <subject>
#   type    one of the allowed kinds below
#   scope   optional, any non-")" text in parens
#   !       optional, marks a breaking change
#   subject required, non-empty
#
# Merge, revert, and fixup/squash commits are skipped (git generates those and
# they don't follow the grammar).
#
# Usage: tools/check-commit-msg.sh <commit-msg-file>
set -euo pipefail

msg_file="${1:?commit message file path required}"

# First non-comment, non-blank line is the header.
header="$(grep -vE '^\s*(#|$)' "$msg_file" | head -n1 || true)"

# Skip machine-generated commits.
case "$header" in
    "Merge "*|"Revert "*|"fixup! "*|"squash! "*|"amend! "*)
        exit 0
        ;;
esac

types='feat|fix|docs|style|refactor|perf|test|build|ci|chore|revert'
pattern="^(${types})(\([^)]+\))?!?: .+"

if [[ "$header" =~ $pattern ]]; then
    exit 0
fi

cat >&2 <<EOF
✗ Commit message does not follow Conventional Commits.

  Got:      ${header:-<empty>}
  Expected: <type>(<scope>)?: <subject>

  type must be one of: ${types//|/, }
  examples:
    feat(ingest): add snapshot commit path
    fix: handle empty changes file in btd
    docs(architecture)!: drop the Ballista section

  See https://www.conventionalcommits.org/
EOF
exit 1
