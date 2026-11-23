#!/bin/sh
# Release notes from bd: the work closed and the requirements added between two commits, read from the bd snapshot
# (.bd/issues.jsonl) committed at each.
#
#   scripts/release-notes.sh v0.2.0                 # since the previous release tag
#   scripts/release-notes.sh --since v0.1.0 HEAD    # between two revisions
#   scripts/release-notes.sh --brief v0.3.0         # requirements counted, not listed
#
# The previous release is the latest v* tag before REV; for a final release (no -suffix) pre-release tags are skipped,
# so v0.2.0 covers everything since v0.1.0, its release candidates included. Without one, the notes say it is the first
# release. Work counts when closed as done and not superseded or a duplicate; epics, decisions, specs, requirements and
# non-goals are left out of it. Requirements in force at REV and not at the previous release are listed under their
# spec, folded; with --brief only counted, pointing to the full notes among the release's files (GitHub refuses a release
# body over 125000 characters). A full changelog link to the repository in Cargo.toml ends the notes. Needs git and jq.
set -eu

die() {
    printf 'release-notes: %s\n' "$*" >&2
    exit 1
}

since=
brief=false
while [ $# -gt 0 ]; do
    case "$1" in
    --since) since=$2; shift 2 ;;
    --brief) brief=true; shift ;;
    -h | --help) sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) die "unknown option: $1" ;;
    *) break ;;
    esac
done
[ $# -eq 1 ] || die "usage: release-notes.sh [--since REV] [--brief] REV"
rev=$1
command -v jq >/dev/null 2>&1 || die "jq not found"
git rev-parse --verify --quiet "$rev^{commit}" >/dev/null || die "no commit $rev"

if [ -z "$since" ]; then
    case "$rev" in
    v*-*) since=$(git describe --tags --abbrev=0 --match 'v*' "$rev^" 2>/dev/null) || since= ;;
    *) since=$(git describe --tags --abbrev=0 --match 'v*' --exclude 'v*-*' "$rev^" 2>/dev/null) || since= ;;
    esac
fi

repo=$(git show "$rev:Cargo.toml" | sed -n 's/^repository = "\(.*\)"$/\1/p' | head -n 1)

if [ -z "$since" ]; then
    echo "First release."
    exit 0
fi
git rev-parse --verify --quiet "$since^{commit}" >/dev/null || die "no commit $since"

snapshot() {
    git show "$1:.bd/issues.jsonl" 2>/dev/null || true
}
before=$(mktemp)
trap 'rm -f "$before"' EXIT
snapshot "$since" >"$before"

# shellcheck disable=SC2016 # jq program
snapshot "$rev" | jq -n -r --slurpfile before "$before" --argjson brief "$brief" '
    def issues: map(select(._type == "issue"));
    def labelled($l): (.labels // []) | index($l) != null;
    def requirement: labelled("requirement") or labelled("non-goal");
    def done: .status == "closed" and .close_outcome == "done"
        and ((.close_reason // "") | test("^(superseded by|duplicate of) ") | not);
    def work: (requirement or labelled("spec") or (.issue_type | IN("epic", "decision"))) | not;
    def bullets: map("- \(.title) (\(.id))") | join("\n");
    def section($heading; $items):
        if ($items | length) > 0 then "## \($heading)\n\n\($items | bullets)\n" else empty end;
    def folded($heading; $items):
        if ($items | length) > 0 and $brief then
            "## \($heading)\n\n\($items | length) added or changed, listed in `release-notes.md` among the release'"'"'s files.\n"
        elif ($items | length) > 0 then
            "## \($heading)\n\n<details><summary>\($items | length) added or changed</summary>\n\n"
            + "\($items | bullets)\n\n</details>\n"
        else empty end;

    [inputs] | issues as $now
    | ($before | issues) as $then
    | ($then | map(select(done) | {key: .id, value: true}) | from_entries) as $closed
    | ($then | map(select(requirement and .status == "pinned") | {key: .id, value: true}) | from_entries) as $required
    | ($now | map(select(labelled("spec")) | {key: .id, value: .title}) | from_entries) as $specs
    | ($now | map(select(work and done and ($closed[.id] | not))) | sort_by(.closed_at)) as $closed_now
    | ($now | map(select(requirement and .status == "pinned" and ($required[.id] | not)))
        | map(.title = ((if labelled("non-goal") then "Non-goal: " else "" end)
            + "\($specs[.id | sub("\\.[0-9]+$"; "")] // "Other"): \(.title)"))
        | sort_by(.title)) as $required_now
    | [section("Features"; $closed_now | map(select(.issue_type == "feature"))),
       section("Fixes"; $closed_now | map(select(.issue_type == "bug"))),
       section("Other changes"; $closed_now | map(select(.issue_type | IN("feature", "bug") | not))),
       folded("Requirements"; $required_now)]
    | if length > 0 then join("\n") else "No recorded changes.\n" end
'
[ -z "$repo" ] || printf '**Full changelog:** %s/compare/%s...%s\n' "$repo" "$since" "$rev"
