#!/bin/bash
# A local NATS hub, two leaves and three Copilot-backed ants in Apple containers.
set -euo pipefail
umask 077

usage() {
    cat <<'EOF'
Usage: scripts/demo-apple-containers.sh COMMAND [ARGS...]

  build                 Build ants, NATS, Copilot and bd into a Linux image
  up                    Start the hub and two leaves (build first if needed)
  login NODE            Sign in to Copilot on hub, leaf-a or leaf-b
  ants NODE ARGS...      Run ants on a node, with its demo human credentials
  status                Show the demo colony
  smoke                 Check three live ants and messages across both leaves
  logs NODE             Show the last 80 lines of a node's logs
  down                  Remove demo containers and network; keep all state

Environment:
  ANTS_DEMO_NAME             Resource prefix (default: ants-demo)
  ANTS_DEMO_STATE            Dedicated state directory
                            (default: ~/.local/share/ants/apple-container-demo)
  ANTS_DEMO_COPILOT_VERSION  Copilot npm version for build (default: 1.0.112)
  ANTS_DEMO_BD_VERSION       bd release tag for build (default: v0.8.0)
EOF
}

die() { printf 'ants-demo: %s\n' "$*" >&2; exit 1; }
command=${1:---help}
shift "$(($# > 0 ? 1 : 0))"
case "$command" in
-h | --help | help) usage; exit ;;
build | up | status | smoke | down) [ "$#" -eq 0 ] || die "unexpected arguments for $command" ;;
login | logs) [ "$#" -eq 1 ] || die "$command needs a node: hub, leaf-a or leaf-b" ;;
ants) [ "$#" -ge 2 ] || die "ants needs a node and command arguments" ;;
*) usage >&2; die "unknown command: $command" ;;
esac
if [ "$#" -gt 0 ]; then
    case "$1" in hub | leaf-a | leaf-b) ;; *) die "unknown node: $1" ;; esac
fi
for tool in container jq; do
    command -v "$tool" >/dev/null || die "$tool not found"
done
container system status >/dev/null
root=$(cd "$(dirname "$0")/.." && pwd)
name=${ANTS_DEMO_NAME:-ants-demo}
case "$name" in "" | -* | *[!a-z0-9-]*) die "ANTS_DEMO_NAME must contain lowercase letters, digits and hyphens" ;; esac
state=${ANTS_DEMO_STATE:-$HOME/.local/share/ants/apple-container-demo}
case "$state" in /*) ;; *) state=$PWD/$state ;; esac
case "$state" in *,*) die "ANTS_DEMO_STATE must not contain a comma" ;; esac
mkdir -p "$state"
state=$(cd "$state" && pwd -P)
if [ -f "$state/.ants-demo" ]; then
    [ "$(cat "$state/.ants-demo")" = "$name" ] || die "state belongs to another demo"
else
    [ -z "$(ls -A "$state")" ] || die "state directory is not empty: $state"
    printf '%s\n' "$name" > "$state/.ants-demo"
fi
image=$name:local
label=net.quangle.ants.demo

owned() {
    local json
    json=$(container inspect "$name-$1" 2>/dev/null) || return 1
    jq --exit-status --arg label "$label" --arg state "$state" \
        '.[0].configuration.labels[$label] == $state' <<<"$json" >/dev/null \
        || die "refusing to change a foreign container: $name-$1"
}

run_ants() {
    local node=$1
    shift
    owned "$node" >/dev/null || die "node not found: $node (run up)"
    container exec "$name-$node" ants --creds /demo/auth/human.creds "$@"
}

build() (
    local context
    context=$(mktemp -d "$state/build.XXXXXX")
    trap 'rm -rf "$context"' EXIT
    cp "$root/Cargo.toml" "$root/Cargo.lock" "$root/rust-toolchain.toml" "$context/"
    cp -R "$root/.cargo" "$root/crates" "$context/"
    mkdir -p "$context/contrib"
    cp -R "$root/contrib/apple-container" "$root/contrib/launchd" "$root/contrib/systemd" "$context/contrib/"
    container build --cpus 4 --memory 8g --tag "$image" --file "$context/contrib/apple-container/Containerfile" \
        --build-arg "COPILOT_VERSION=${ANTS_DEMO_COPILOT_VERSION:-1.0.112}" \
        --build-arg "BD_VERSION=${ANTS_DEMO_BD_VERSION:-v0.8.0}" "$context"
)

ready() {
    local node=$1 attempt
    for ((attempt = 0; attempt < 90; attempt++)); do
        if container exec "$name-$node" curl --fail --silent --max-time 2 \
            http://127.0.0.1:9464/healthz >/dev/null 2>&1; then
            return
        fi
        if ! container inspect "$name-$node" | jq --exit-status '.[0].status.state == "running"' >/dev/null; then
            break
        fi
        sleep 1
    done
    container logs -n 80 "$name-$node" >&2 || true
    die "node did not become ready: $node"
}

up() {
    local node hub_ip network volume
    for node in hub leaf-a leaf-b init; do
        if owned "$node"; then
            die "container already exists: $name-$node (run down before up)"
        fi
    done
    container image inspect "$image" >/dev/null 2>&1 || build
    if network=$(container network inspect "$name" 2>/dev/null); then
        jq --exit-status --arg label "$label" --arg state "$state" \
            '.[0].configuration.labels[$label] == $state' <<<"$network" >/dev/null \
            || die "refusing to use a foreign network: $name"
    else
        container network create --label "$label=$state" "$name"
    fi
    for node in hub leaf-a leaf-b; do
        if volume=$(container volume inspect "$name-$node" 2>/dev/null); then
            jq --exit-status --arg label "$label" --arg state "$state" \
                '.[0].configuration.labels[$label] == $state' <<<"$volume" >/dev/null \
                || die "refusing to use a foreign volume: $name-$node"
        else
            container volume create --label "$label=$state" -s 4g "$name-$node"
        fi
    done
    container run --rm --name "$name-init" --label "$label=$state" \
        --workdir / --mount "type=volume,source=$name-hub,target=/demo/hub" \
        --mount "type=volume,source=$name-leaf-a,target=/demo/leaf-a" \
        --mount "type=volume,source=$name-leaf-b,target=/demo/leaf-b" "$image" prepare
    for node in hub leaf-a leaf-b; do
        container run --detach --name "$name-$node" --network "$name" --label "$label=$state" \
            --cpus 2 --memory 2g --mount "type=volume,source=$name-$node,target=/demo" \
            --env "ANTS_DEMO_HUB=${hub_ip:-}" "$image" "$node"
        ready "$node"
        if [ "$node" = hub ]; then
            hub_ip=$(container inspect "$name-hub" | jq --raw-output \
                '.[0].status.networks[0].ipv4Address | split("/")[0]')
        fi
    done
    printf 'Demo ready. Copilot sign-in: %s login NODE\n' "$0"
    run_ants hub colony
}

down() {
    local node network
    # Validate every owner before changing any resource.
    for node in hub leaf-a leaf-b init; do owned "$node" || true; done
    if network=$(container network inspect "$name" 2>/dev/null); then
        jq --exit-status --arg label "$label" --arg state "$state" \
            '.[0].configuration.labels[$label] == $state' <<<"$network" >/dev/null \
            || die "refusing to remove a foreign network: $name"
    fi
    for node in leaf-b leaf-a hub init; do
        if owned "$node"; then
            if container inspect "$name-$node" | jq --exit-status '.[0].status.state == "running"' >/dev/null; then
                container stop --time 30 "$name-$node"
            fi
            container delete "$name-$node"
        fi
    done
    if [ -n "$network" ]; then container network delete "$name"; fi
    printf 'Demo stopped; node volumes and state at %s kept\n' "$state"
}

smoke() {
    local node thread text source
    for node in hub leaf-a leaf-b; do
        owned "$node" >/dev/null || die "node not found: $node"
        ready "$node"
        container exec "$name-$node" test -S /demo/ant/mcp.sock
        run_ants "$node" colony --json | jq --slurp --exit-status \
            'map(select(.state == "live" and (.harness | startswith("copilot"))) | .ant) | sort
             == ["hub-ant", "human-assistant", "leaf-a-ant", "leaf-b-ant"]' >/dev/null
    done
    run_ants hub assistant show >/dev/null
    for node in hub leaf-a leaf-b; do
        run_ants hub ant pause "$node-ant" >/dev/null
        run_ants hub ant resume "$node-ant" >/dev/null
    done
    run_ants hub ant memories hub-ant --json \
        | jq --exit-status '.bd.db == "/demo/work/.bd/bd.db" and .bd.error == null' >/dev/null
    thread=$(uuidgen | tr '[:upper:]' '[:lower:]')
    for node in hub leaf-a leaf-b; do
        text="demo smoke $thread from $node"
        run_ants "$node" notes "$thread" "$text" >/dev/null
    done
    for node in hub leaf-a leaf-b; do
        run_ants "$node" notes "$thread" > "$state/smoke-$node.log"
        for source in hub leaf-a leaf-b; do
            grep --fixed-strings "demo smoke $thread from $source" "$state/smoke-$node.log" >/dev/null
        done
    done
    printf 'Four Copilot-backed ants live (human-assistant granted) and obeyed admin orders; hub-ant has bd; all three nodes exchanged notes. No model calls made.\n'
}

case "$command" in
build) build ;;
up) up ;;
down) down ;;
status) run_ants hub colony ;;
smoke) smoke ;;
ants) node=$1; shift; run_ants "$node" "$@" ;;
login) owned "$1" || die "node not found: $1"; container exec --interactive --tty "$name-$1" copilot login --device-code ;;
logs) owned "$1" || die "node not found: $1"; container logs -n 80 "$name-$1" ;;
esac
