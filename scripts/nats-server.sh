#!/bin/sh
# Fetch the nats-server release pinned in crates/ants/nats-server.sha256 for one platform: download its archive from
# GitHub, check it against the pinned SHA-256 digest, and put nats-server (nats-server.exe for Windows) and its
# license, as LICENSE-nats-server, into a directory. CI runs its tests with it; the release workflow packs it beside
# ants.
#
#   scripts/nats-server.sh PLATFORM DIR   # PLATFORM as nats-server releases name it: linux-amd64, linux-arm64,
#                                         # darwin-arm64, darwin-amd64 or windows-amd64
#   scripts/nats-server.sh --version      # print the pinned version
set -eu

die() {
    printf 'nats-server.sh: %s\n' "$*" >&2
    exit 1
}

pinned=$(cd "$(dirname "$0")/.." && pwd)/crates/ants/nats-server.sha256

if [ "${1-}" = --version ]; then
    sed -n 's/^[0-9a-f]\{64\}  nats-server-\(v.*\)-linux-amd64\.tar\.gz$/\1/p' "$pinned"
    exit 0
fi
[ $# -eq 2 ] || die "usage: scripts/nats-server.sh PLATFORM DIR | --version"
platform=$1
dir=$2

line=$(grep -E "^[0-9a-f]{64}  nats-server-v[^ ]*-$platform\.(tar\.gz|zip)\$" "$pinned") ||
    die "no nats-server pinned for $platform in $pinned"
digest=${line%% *}
archive=${line##* }
version=${archive#nats-server-}
version=${version%-"$platform".*}
stem=${archive%.tar.gz}
stem=${stem%.zip}
exe=nats-server
case $platform in windows-*) exe=nats-server.exe ;; esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -fsSL --retry 3 -o "$tmp/$archive" "https://github.com/nats-io/nats-server/releases/download/$version/$archive"
if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$tmp/$archive" | cut -d ' ' -f 1)
else
    actual=$(shasum -a 256 "$tmp/$archive" | cut -d ' ' -f 1)
fi
[ "$actual" = "$digest" ] || die "$archive has SHA-256 digest $actual, not the pinned $digest"
case $archive in
*.zip) if command -v 7z >/dev/null 2>&1; then 7z x -bso0 -o"$tmp" "$tmp/$archive"; else unzip -q "$tmp/$archive" -d "$tmp"; fi ;;
*) tar -xzf "$tmp/$archive" -C "$tmp" ;;
esac
mkdir -p "$dir"
cp "$tmp/$stem/$exe" "$dir/$exe"
cp "$tmp/$stem/LICENSE" "$dir/LICENSE-nats-server"
chmod +x "$dir/$exe"
printf 'nats-server %s (%s) in %s\n' "$version" "$platform" "$dir"
