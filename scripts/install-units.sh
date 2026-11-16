#!/bin/sh
# Install a machine's user services: its nats-server (the leaf node from `ants leaf config`, or the hub on the hub
# machine), the ant daemon (`ants daemon --config`), on the hub the channel manager (`ants channel serve`) and the
# webhook ingress (`ants webhook serve`), on the machine holding the team directory the signer (`ants signer`), for
# an uplink through a private Dev Tunnel `devtunnel connect` and, on the hub machine, `devtunnel host` of that tunnel,
# and for an uplink through the HTTPS fallback bridge `ants bridge connect` and, on the hub machine,
# `ants bridge serve`. launchd agents on macOS, systemd user units on Linux and WSL; templates in contrib/.
#
#   scripts/install-units.sh                         # install nats-server and the daemon and (re)start them
#   scripts/install-units.sh --devtunnel ants-hub    # and `devtunnel connect ants-hub` for nats-leaf://127.0.0.1:7422
#   scripts/install-units.sh --no-daemon --channel-manager ~/.config/ants/auth/channel-manager.creds   # on the hub
#   scripts/install-units.sh --webhook               # and `ants webhook serve` for ~/.ants/webhooks.toml
#   scripts/install-units.sh --signer                # with the team directory, and `ants signer` for --config
#   scripts/install-units.sh --devtunnel-host ants-hub   # on the hub, and `devtunnel host ants-hub`
#   scripts/install-units.sh --bridge https://hub.example/bridge --bridge-token FILE   # and `ants bridge connect`
#   scripts/install-units.sh --no-daemon --bridge-serve --bridge-token FILE           # on the hub, `ants bridge serve`
#   scripts/install-units.sh --out DIR               # only write the filled-in units into DIR, for review
#
# Options:  --ants PATH              ants binary (default: ants on PATH)
#           --nats-server PATH       nats-server binary (default: nats-server on PATH)
#           --nats-conf FILE         nats-server configuration (default: ~/.config/ants/leaf.conf)
#           --config FILE            daemon config file (default: ~/.ants/config.toml)
#           --no-daemon              no daemon service, on a machine without ants
#           --channel-manager CREDS  also run the channel manager with CREDS, from `ants auth channel-manager`
#           --signer                 also keep `ants signer --config FILE` running, on the machine holding the team
#                                    directory (see docs/config.md, Manager ants)
#           --devtunnel TUNNEL       also keep `devtunnel connect TUNNEL` running, for an uplink through that tunnel
#           --devtunnel-host TUNNEL  also keep `devtunnel host TUNNEL` running, on the hub machine
#           --devtunnel-bin PATH     devtunnel binary (default: devtunnel on PATH)
#           --devtunnel-token FILE   connect with the access token in FILE, read at each start, instead of the CLI
#                                    login (see docs/transport.md, Dev Tunnels)
#           --bridge URL             also keep `ants bridge connect URL` running, for an uplink through the HTTPS
#                                    fallback bridge (see docs/transport.md, HTTPS fallback bridge)
#           --bridge-serve           also keep `ants bridge serve` running, on the hub machine
#           --bridge-token FILE      the bridge's bearer token in FILE, read at each start, for either end
#           --bridge-proxy URL       connect through this HTTP CONNECT proxy, http://[user:password@]host:port
#           --bridge-ca FILE         connect trusting the PEM CA certificates in FILE (SSL_CERT_FILE)
#           --webhook                also keep `ants webhook serve` running (see docs/webhooks.md)
#           --webhook-config FILE    its config file (default: ~/.ants/webhooks.toml)
#           --out DIR                write the units into DIR; install and start nothing
#
# The daemon gets the PATH of this shell, so it finds the harness and configured tools. Logs: ~/Library/Logs/ants on
# macOS, rotated by the agent ants.logrotate (over 10 MiB, 5 gzipped copies kept), the journal on Linux
# (journalctl --user -u ants-daemon). With ants, also installs the completions for the login shell
# (`ants completions --install`).
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
devtunnel=
tunnel=
host_tunnel=
token_file=
channel_creds=
signer=
bridge_url=
bridge_serve=
bridge_token=
bridge_proxy=
bridge_ca=
webhook=
webhook_config=$HOME/.ants/webhooks.toml
out=

while [ $# -gt 0 ]; do
    case "$1" in
    --ants) ants=$2; shift 2 ;;
    --nats-server) nats_server=$2; shift 2 ;;
    --nats-conf) nats_conf=$2; shift 2 ;;
    --config) config=$2; shift 2 ;;
    --no-daemon) daemon=; shift ;;
    --channel-manager) channel_creds=$2; shift 2 ;;
    --signer) signer=1; shift ;;
    --devtunnel) tunnel=$2; shift 2 ;;
    --devtunnel-host) host_tunnel=$2; shift 2 ;;
    --devtunnel-bin) devtunnel=$2; shift 2 ;;
    --devtunnel-token) token_file=$2; shift 2 ;;
    --bridge) bridge_url=$2; shift 2 ;;
    --bridge-serve) bridge_serve=1; shift ;;
    --bridge-token) bridge_token=$2; shift 2 ;;
    --bridge-proxy) bridge_proxy=$2; shift 2 ;;
    --bridge-ca) bridge_ca=$2; shift 2 ;;
    --webhook) webhook=1; shift ;;
    --webhook-config) webhook_config=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    -h | --help) sed -n '2,45p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
    esac
done

[ -n "$nats_server" ] || nats_server=$(command -v nats-server) || die "nats-server not found (give --nats-server)"
nats_server=$(absolute "$nats_server")
[ -x "$nats_server" ] || die "no nats-server binary at $nats_server"
nats_conf=$(absolute "$nats_conf")
if [ -n "$daemon$channel_creds$signer$webhook$bridge_url$bridge_serve" ]; then
    [ -n "$ants" ] || ants=$(command -v ants) || die "ants not found (give --ants, or cargo install --path crates/ants)"
    ants=$(absolute "$ants")
    [ -x "$ants" ] || die "no ants binary at $ants"
    config=$(absolute "$config")
    webhook_config=$(absolute "$webhook_config")
fi
[ -z "$channel_creds" ] || channel_creds=$(absolute "$channel_creds")
if [ -n "$tunnel$host_tunnel" ]; then
    [ -n "$devtunnel" ] || devtunnel=$(command -v devtunnel) || die "devtunnel not found (give --devtunnel-bin)"
    devtunnel=$(absolute "$devtunnel")
    [ -x "$devtunnel" ] || die "no devtunnel binary at $devtunnel"
else
    [ -z "$devtunnel" ] || die "--devtunnel-bin needs --devtunnel TUNNEL or --devtunnel-host TUNNEL"
fi
if [ -n "$tunnel" ]; then
    [ -z "$token_file" ] || token_file=$(absolute "$token_file")
else
    [ -z "$token_file" ] || die "--devtunnel-token needs --devtunnel TUNNEL"
fi
[ -z "$tunnel" ] || [ -z "$bridge_url" ] || die "--devtunnel and --bridge both listen on 127.0.0.1:7422; give one"
if [ -n "$bridge_url$bridge_serve" ]; then
    [ -z "$bridge_token" ] || bridge_token=$(absolute "$bridge_token")
else
    [ -z "$bridge_token" ] || die "--bridge-token needs --bridge URL or --bridge-serve"
fi
if [ -n "$bridge_url" ]; then
    [ -z "$bridge_ca" ] || bridge_ca=$(absolute "$bridge_ca")
else
    [ -z "$bridge_proxy" ] || die "--bridge-proxy needs --bridge URL"
    [ -z "$bridge_ca" ] || die "--bridge-ca needs --bridge URL"
fi
if [ -z "$out" ]; then
    [ -f "$nats_conf" ] || die "no nats-server configuration at $nats_conf (write one with \`ants leaf config\`)"
    [ -z "$daemon" ] || [ -f "$config" ] || die "no daemon config file at $config (see docs/config.md)"
    [ -z "$signer" ] || [ -f "$config" ] || die "no config file at $config for the signer's templates"
    [ -z "$token_file" ] || [ -r "$token_file" ] || die "no devtunnel token file at $token_file"
    [ -z "$channel_creds" ] || [ -r "$channel_creds" ] || die "no channel manager credentials at $channel_creds"
    [ -z "$webhook" ] || [ -f "$webhook_config" ] || die "no webhook config file at $webhook_config (see docs/webhooks.md)"
    [ -z "$bridge_token" ] || [ -r "$bridge_token" ] || die "no bridge token file at $bridge_token"
    [ -z "$bridge_ca" ] || [ -r "$bridge_ca" ] || die "no CA certificates at $bridge_ca"
fi
if [ -z "$tunnel$bridge_url" ] && [ -f "$nats_conf" ] &&
    grep -Eq 'nats-leaf://(127\.0\.0\.1|localhost|\[::1\]):' "$nats_conf"; then
    printf 'install-units: the uplink in %s is on this machine; for one through a Dev Tunnel or the HTTPS bridge, add %s\n' \
        "$nats_conf" '--devtunnel TUNNEL or --bridge URL' >&2
fi

case "$(uname -s)" in
Darwin)
    kind=launchd
    units="${tunnel:+ants.devtunnel.plist }${bridge_url:+ants.bridge.plist }ants.nats.plist${daemon:+ ants.daemon.plist}${channel_creds:+ ants.channels.plist}${signer:+ ants.signer.plist}${webhook:+ ants.webhook.plist}${host_tunnel:+ ants.devtunnel-host.plist}${bridge_serve:+ ants.bridge-serve.plist} ants.logrotate.plist"
    dest=${out:-$HOME/Library/LaunchAgents}
    logs=$HOME/Library/Logs/ants
    ;;
Linux)
    kind=systemd
    units="${tunnel:+ants-devtunnel.service }${bridge_url:+ants-bridge.service }ants-nats.service${daemon:+ ants-daemon.service}${channel_creds:+ ants-channels.service}${signer:+ ants-signer.service}${webhook:+ ants-webhook.service}${host_tunnel:+ ants-devtunnel-host.service}${bridge_serve:+ ants-bridge-serve.service}"
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

# Lines marked @IF_X@ stay only when X is given, those marked @IF_NO_X@ only when not; the marker goes. X: TOKEN (a
# devtunnel token file), BRIDGE_TOKEN, BRIDGE_PROXY, BRIDGE_CA.
drop=
for flag in "TOKEN=$token_file" "BRIDGE_TOKEN=$bridge_token" "BRIDGE_PROXY=$bridge_proxy" "BRIDGE_CA=$bridge_ca"; do
    if [ -n "${flag#*=}" ]; then
        drop="$drop/@IF_NO_${flag%%=*}@/d;"
    else
        drop="$drop/@IF_${flag%%=*}@/d;"
    fi
done

mkdir -p "$dest"
for unit in $units; do
    sed -e "$drop" \
        -e 's/[[:space:]]*<!-- @IF_[A-Z_]*@ -->//' \
        -e 's/^@IF_[A-Z_]*@//' \
        -e "s|@ANTS@|$(escape "$ants")|g" \
        -e "s|@NATS_SERVER@|$(escape "$nats_server")|g" \
        -e "s|@NATS_CONF@|$(escape "$nats_conf")|g" \
        -e "s|@CONFIG@|$(escape "$config")|g" \
        -e "s|@WEBHOOK_CONFIG@|$(escape "$webhook_config")|g" \
        -e "s|@DEVTUNNEL@|$(escape "$devtunnel")|g" \
        -e "s|@TUNNEL@|$(escape "$tunnel")|g" \
        -e "s|@HOST_TUNNEL@|$(escape "$host_tunnel")|g" \
        -e "s|@TOKEN_FILE@|$(escape "$token_file")|g" \
        -e "s|@CHANNEL_CREDS@|$(escape "$channel_creds")|g" \
        -e "s|@BRIDGE_URL@|$(escape "$bridge_url")|g" \
        -e "s|@BRIDGE_TOKEN@|$(escape "$bridge_token")|g" \
        -e "s|@BRIDGE_PROXY@|$(escape "$bridge_proxy")|g" \
        -e "s|@BRIDGE_CA@|$(escape "$bridge_ca")|g" \
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
    printf 'started; logs: journalctl --user%s%s -u ants-nats%s%s%s%s%s%s\n' "${tunnel:+ -u ants-devtunnel}" \
        "${bridge_url:+ -u ants-bridge}" "${daemon:+ -u ants-daemon}" "${channel_creds:+ -u ants-channels}" \
        "${signer:+ -u ants-signer}" \
        "${webhook:+ -u ants-webhook}" "${host_tunnel:+ -u ants-devtunnel-host}" "${bridge_serve:+ -u ants-bridge-serve}"
    if [ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)" != yes ]; then
        printf 'to keep them running without a login session: loginctl enable-linger %s\n' "$(id -un)"
    fi
fi
[ -z "$ants" ] || "$ants" completions --install || true
