#!/usr/bin/env bash
# Check that a PR description carries the "Reviewer loop" block the qiita-review skill
# prints (CLAUDE.md, "Reviewing a branch"). Environment:
#   PR_BODY   the PR description
#   LABELS    comma-joined PR labels
#   HEAD_SHA  the PR's head commit (default: HEAD)
#   BASE_SHA  the base branch tip (default: origin/main)
# Run from a checkout whose history holds both.
#
# Passes when the 'no-agent-review' label is present, or when the description has a
# "## Reviewer loop" section whose "Reviewed at:" line names one of the PR's own
# commits and whose four ledger headings are present. It checks that the block is
# there and points at this branch; it cannot check that the loop was run.
set -euo pipefail

fail() {
    {
        echo "ERROR: $1"
        echo "Run the qiita-review skill on the branch and replace the '## Reviewer loop'"
        echo "section of the PR description with the block it prints. If the loop cannot be"
        echo "run for this PR, add the 'no-agent-review' label and say why under that heading."
    } >&2
    exit 1
}

case ",${LABELS:-}," in *,no-agent-review,*)
    echo "Skipped: 'no-agent-review' label present."; exit 0;;
esac

# The section runs from its heading to the next level-2 heading or the end. CRs are
# dropped because GitHub stores descriptions with CRLF line endings.
# Here-strings, not pipes: under pipefail a `grep -q` that exits on its first match
# can leave the writer killed by SIGPIPE, which would read as "no match".
body=${PR_BODY:-}
body=${body//$'\r'/}
grep -q '^## Reviewer loop[[:space:]]*$' <<<"$body" \
    || fail "the PR description has no '## Reviewer loop' section."
section=$(awk '/^## Reviewer loop[[:space:]]*$/ {on=1; next} /^## / {on=0} on' <<<"$body")

sha=$(sed -n 's/^- Reviewed at:[[:space:]]*`\{0,1\}\([0-9a-f]\{7,40\}\)`\{0,1\}.*/\1/p' \
    <<<"$section")
sha=${sha%%$'\n'*}
[ -n "$sha" ] || fail "'- Reviewed at:' does not name a commit sha."

git cat-file -e "${sha}^{commit}" 2>/dev/null \
    || fail "'Reviewed at: ${sha}' is not a commit in this repository."
head="${HEAD_SHA:-HEAD}"
base="${BASE_SHA:-origin/main}"
# `merge-base --is-ancestor` exits 128 on a ref it cannot resolve, which an `if`
# reads the same as "not an ancestor". Resolve both first.
for ref in "$head" "$base"; do
    git rev-parse --verify --quiet "${ref}^{commit}" >/dev/null \
        || { echo "ERROR: cannot resolve '${ref}' in this checkout." >&2; exit 2; }
done
git merge-base --is-ancestor "$sha" "$head" \
    || fail "'Reviewed at: ${sha}' is not on this branch."
if git merge-base --is-ancestor "$sha" "$base"; then
    fail "'Reviewed at: ${sha}' is a commit of the base branch, not one of this PR's."
fi

for heading in "Fixed:" "Declined:" "Deferred:" "Not probed:"; do
    grep -q "^- ${heading}" <<<"$section" \
        || fail "the Reviewer loop section has no '- ${heading}' line."
done

echo "Reviewer loop block present; reviewed at ${sha}."
