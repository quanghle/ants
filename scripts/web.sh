#!/bin/sh
# Put the page of the web client pinned in crates/ants/web.sha256 where a build with the `web` feature takes it
# (target/web, or the directory ANTS_WEB_BUNDLE names), for `cargo build --release --features web`. The page is built
# by the repo ants-web, which is local for now: from its checkout beside this repo (ANTS_WEB_CHECKOUT, default
# ../ants-web), with its scripts/build.sh against this checkout's ants-client. The bundle has to have the pinned
# SHA-256 digest and be built for this ants version, the workspace's; the build checks both again (crates/ants/build.rs).
#
#   scripts/web.sh          # build the pinned page and put it in place
#   scripts/web.sh --pin    # build the page of the ants-web checkout, committed, put it in place and pin it
#
# Needs what ants-web's build does: its pinned Node and npm, and the Rust toolchain with the wasm32-unknown-unknown
# target. A checkout of ants without ants-web beside it, as CI's, fails: until ants-web publishes its releases, the
# page is built only where its checkout is.
set -eu

die() {
    printf 'web.sh: %s\n' "$*" >&2
    exit 1
}

root=$(cd "$(dirname "$0")/.." && pwd)
pinned=$root/crates/ants/web.sha256
bundle=${ANTS_WEB_BUNDLE:-$root/target/web}
checkout=${ANTS_WEB_CHECKOUT:-$root/../ants-web}
version=$(sed -n 's/^version = "\(.*\)"$/\1/p' "$root/Cargo.toml" | head -n 1)
[ -n "$version" ] || die "no version in $root/Cargo.toml"

pin=false
case ${1-} in
--pin) pin=true ;;
'') ;;
*) die "usage: scripts/web.sh [--pin]" ;;
esac

line=$(grep -v -e '^#' -e '^[[:space:]]*$' "$pinned" || true)
# shellcheck disable=SC2086 # the pin's words: digest, "ants-web", commit, "for", "ants", version
set -- $line
if [ $# -eq 6 ] && [ "$2 $4 $5" = "ants-web for ants" ]; then
    pinned_digest=$1 pinned_commit=$3 pinned_version=$6
elif ! $pin; then
    die "$pinned pins no page as \`SHA-256  ants-web COMMIT for ants VERSION\`"
fi

[ -x "$checkout/scripts/build.sh" ] || die "the page is built from a checkout of ants-web, which $checkout is not: \
clone it beside this repo or set ANTS_WEB_CHECKOUT; until ants-web publishes its releases, \
a release build needs that checkout"
commit=$(git -C "$checkout" rev-parse --short=7 HEAD)
if $pin; then
    [ -z "$(git -C "$checkout" status --porcelain)" ] ||
        die "$checkout has changes not committed: commit them, so that the pin names the commit built"
elif [ "$pinned_version" != "$version" ]; then
    die "the pinned page is built for ants $pinned_version, not $version: pin a page built for it (scripts/web.sh --pin)"
fi

printf 'building the page of ants-web %s (%s)\n' "$commit" "$checkout" >&2
digest=$(ANTS_DIR=$root "$checkout/scripts/build.sh")
built_for=$(cat "$checkout/dist/ants-version")
[ "$built_for" = "$version" ] || die "ants-web $commit built a page for ants $built_for, not $version"
if ! $pin && [ "$digest" != "$pinned_digest" ] && [ "$commit" = "$pinned_commit" ]; then
    die "ants-web $commit built a page of digest $digest, not the pinned $pinned_digest: the page is built against \
ants-client of this checkout, which changed since it was pinned; pin it again (scripts/web.sh --pin)"
elif ! $pin && [ "$digest" != "$pinned_digest" ]; then
    die "ants-web $commit built a page of digest $digest, not the pinned $pinned_digest of ants-web $pinned_commit: \
check out ants-web $pinned_commit, or pin this one (scripts/web.sh --pin)"
fi

rm -rf "$bundle"
mkdir -p "$(dirname "$bundle")"
cp -R "$checkout/dist" "$bundle"
if $pin; then
    {
        grep -e '^#' "$pinned"
        printf '%s  ants-web %s for ants %s\n' "$digest" "$commit" "$version"
    } >"$pinned.new"
    mv "$pinned.new" "$pinned"
    printf 'pinned the page of ants-web %s for ants %s (%s) in %s\n' "$commit" "$version" "$digest" "$pinned" >&2
fi
printf 'the page of ants-web %s for ants %s is in %s: cargo build --release --features web\n' "$commit" "$version" \
    "$bundle" >&2
