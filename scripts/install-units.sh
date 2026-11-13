#!/bin/sh
# Install a machine's user services: its nats-server (the leaf node from `ants leaf config`, or the hub on the hub
# machine) and the persona daemon (`ants daemon --config`). launchd agents on macOS, systemd user units on Linux and
# WSL; templates in contrib/.
#
#   scripts/install-units.sh                     # install both and (re)start them
#   scripts/install-units.sh --out DIR           # only write the filled-in units into DIR, for review
#
# Options:  --ants PATH          ants binary (default: ants on PATH)
#           --nats-server PATH   nats-server binary (default: nats-server on PATH)
#           --nats-conf FILE     nats-server configuration (default: ~/.config/ants/leaf.conf)
#           --config FILE        daemon config file (default: ~/.ants/config.toml)
#           --no-daemon          only the nats-server service, on a machine without personas
#           --out DIR            write the units into DIR; install and start nothing
#
# The daemon gets the PATH of this shell, so it finds the harness (copilot) and bd. Logs: ~/Library/Logs/ants on
# macOS, the journal on Linux (journalctl --user -u ants-daemon).
set -eu

die() {
    printf 'install-units: %s\n' "$*" >&2
    exit 1
}

# $1 as an absolute path.
absolute() {
    case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$(pwd)" "$1" ;;
    esac
}

root=$(cd "$(dirname "$0")/.." && pwd)
ants=
nats_server=
nats_conf=$HOME/.config/ants/leaf.conf
config=$HOME/.ants/config.toml
daemon=1
out=

while [ $# -gt 0 ]; do
    case "$1" in
    --ants) ants=$2; shift 2 ;;
    --nats-server) nats_server=$2; shift 2 ;;
    --nats-conf) nats_conf=$2; shift 2 ;;
    --config) config=$2; shift 2 ;;
    --no-daemon) daemon=; shift ;;
    --out) out=$2; shift 2 ;;
    -h | --help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
    esac
done

[ -n "$nats_server" ] || nats_server=$(command -v nats-server) || die "nats-server not found (give --nats-server)"
nats_server=$(absolute "$nats_server")
[ -x "$nats_server" ] || die "no nats-server binary at $nats_server"
nats_conf=$(absolute "$nats_conf")
if [ -n "$daemon" ]; then
    [ -n "$ants" ] || ants=$(command -v ants) || die "ants not found (give --ants, or cargo install --path crates/ants)"
    ants=$(absolute "$ants")
    [ -x "$ants" ] || die "no ants binary at $ants"
    config=$(absolute "$config")
fi
if [ -z "$out" ]; then
    [ -f "$nats_conf" ] || die "no nats-server configuration at $nats_conf (write one with \`ants leaf config\`)"
    [ -z "$daemon" ] || [ -f "$config" ] || die "no daemon config file at $config (see README.md, Config file)"
fi

case "$(uname -s)" in
Darwin)
    kind=launchd
    units="ants.nats.plist${daemon:+ ants.daemon.plist}"
    dest=${out:-$HOME/Library/LaunchAgents}
    logs=$HOME/Library/Logs/ants
    ;;
Linux)
    kind=systemd
    units="ants-nats.service${daemon:+ ants-daemon.service}"
    dest=${out:-$HOME/.config/systemd/user}
    logs=
    ;;
*) die "unsupported system: $(uname -s)" ;;
esac

# Escapes $1 for a sed replacement, and for XML in plists or specifiers in systemd units.
escape() {
    v=$1
    if [ "$kind" = launchd ]; then
        v=$(printf '%s' "$v" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')
    else
        v=$(printf '%s' "$v" | sed -e 's/%/%%/g')
    fi
    printf '%s' "$v" | sed -e 's/[\\|&]/\\&/g'
}

mkdir -p "$dest"
for unit in $units; do
    sed -e "s|@ANTS@|$(escape "$ants")|g" \
        -e "s|@NATS_SERVER@|$(escape "$nats_server")|g" \
        -e "s|@NATS_CONF@|$(escape "$nats_conf")|g" \
        -e "s|@CONFIG@|$(escape "$config")|g" \
        -e "s|@PATH@|$(escape "$PATH")|g" \
        -e "s|@LOGS@|$(escape "$logs")|g" \
        "$root/contrib/$kind/$unit" >"$dest/$unit"
    if [ "$kind" = launchd ] && command -v plutil >/dev/null 2>&1; then
        plutil -lint -s "$dest/$unit" || die "$dest/$unit is not a valid property list"
    fi
    printf '%s\n' "$dest/$unit"
done
[ -z "$out" ] || exit 0

if [ "$kind" = launchd ]; then
    mkdir -p "$logs"
    domain=gui/$(id -u)
    for unit in $units; do
        label=${unit%.plist}
        if launchctl bootout "$domain/$label" 2>/dev/null; then
            i=0
            while launchctl print "$domain/$label" >/dev/null 2>&1 && [ $i -lt 150 ]; do
                sleep 1
                i=$((i + 1))
            done
        fi
        launchctl bootstrap "$domain" "$dest/$unit"
    done
    printf 'started; logs in %s\n' "$logs"
else
    names=$(printf '%s\n' $units | tr '\n' ' ')
    systemctl --user daemon-reload
    # shellcheck disable=SC2086
    systemctl --user enable $names
    # shellcheck disable=SC2086
    systemctl --user restart $names
    printf 'started; logs: journalctl --user -u ants-nats -u ants-daemon\n'
    if [ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)" != yes ]; then
        printf 'to keep them running without a login session: loginctl enable-linger %s\n' "$(id -un)"
    fi
fi
