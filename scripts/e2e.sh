#!/bin/bash
# The end-to-end scenarios of docs/e2e-scenarios.md, run against a hub, a worker leaf and a client in Apple container.
set -euo pipefail
umask 077

usage() {
    cat <<'EOF'
Usage: scripts/e2e.sh COMMAND [ARGS...]

  build                 Build the e2e image: ants, NATS and Claude Code
  up                    Create or start the fixture, wire its outboxes, wait until every ant is live (idempotent)
  run [-j N] [SELECTOR...]
                        Run scenarios: ids (M1), groups (M), priorities (P0, P1, P2) or all
                        (default: P0 and P1); prints PASS, FAIL or SKIP for each. With -j N, the scenarios
                        sharing the fixture run N at a time; those changing it run one by one after them.
                        The slow class (SLOW: group D, about 35 minutes each) runs only when named by id,
                        by group or by `slow`, never by all, a priority or the default
  list                  List the scenarios with priority (or class) and title
  status                Show the colony, as alice on the client
  ants NODE ARGS...     Run ants on hub, leaf or client (the client as alice, the hub as bob)
  sh NODE               Open a shell on a node
  logs NODE [SERVICE]   Show the node's log, or the last lines of one of its services' logs
  down                  Remove the containers and network; keep the volumes
  reset                 Remove the containers, network and volumes: the next up starts afresh

Environment:
  ANTS_E2E_CLAUDE_ENV     Env file holding CLAUDE_CODE_OAUTH_TOKEN (default: ~/.config/ants-e2e/claude.env)
  ANTS_E2E_STATE          Run logs (default: ~/.local/share/ants/e2e)
  ANTS_E2E_CLAUDE_VERSION Claude Code npm version for build (default: 2.1.330)
  ANTS_E2E_CLAUDE_ACP_VERSION
                          claude-agent-acp npm version for build (default: 0.107.0)
  ANTS_E2E_JOBS           Default of run's -j (default: 1)
  ANTS_E2E_IMAGE          Image that build tags and up creates missing nodes from (default: ants-e2e:local)

build, up, run, down and reset take a lock on the state directory: a second one waits for the first to end.
EOF
}

die() { printf 'ants-e2e: %s\n' "$*" >&2; exit 1; }
say() { printf 'ants-e2e: %s\n' "$*" >&2; }

command=${1:---help}
shift "$(($# > 0 ? 1 : 0))"
case "$command" in
-h | --help | help) usage; exit ;;
build | up | list | status | down | reset) [ "$#" -eq 0 ] || die "unexpected arguments for $command" ;;
run) ;;
sh) [ "$#" -eq 1 ] || die "sh needs a node: hub, leaf or client" ;;
logs) [ "$#" -ge 1 ] || die "logs needs a node: hub, leaf or client" ;;
ants) [ "$#" -ge 2 ] || die "ants needs a node and command arguments" ;;
*) usage >&2; die "unknown command: $command" ;;
esac
case "$command" in
sh | logs | ants) case "$1" in hub | leaf | client) ;; *) die "unknown node: $1" ;; esac ;;
esac

command -v container >/dev/null || die "Apple container not found"
command -v jq >/dev/null || die "jq not found"
container system status >/dev/null || die "the container service is not running: container system start"

root=$(cd "$(dirname "$0")/.." && pwd)
name=ants-e2e
image=${ANTS_E2E_IMAGE:-$name:local}
label=net.quangle.ants.e2e
claude_env=${ANTS_E2E_CLAUDE_ENV:-$HOME/.config/ants-e2e/claude.env}
state=${ANTS_E2E_STATE:-$HOME/.local/share/ants/e2e}
mkdir -p "$state"
# Two of these at once would undo each other's fixture: the second waits, re-run under the lock.
case "$command" in
build | up | run | down | reset)
    if [ -z "${ANTS_E2E_LOCKED:-}" ]; then
        lockf -s -t 0 "$state/fixture.lock" true || say "waiting for another scripts/e2e.sh $command or run to end"
        ANTS_E2E_LOCKED=1 exec lockf -k "$state/fixture.lock" "$0" "$command" "$@"
    fi ;;
esac
auth=/demo/home/.config/ants/auth
nodes=(hub leaf client)

exists() { container inspect "$name-$1" >/dev/null 2>&1; }
running() { container inspect "$name-$1" 2>/dev/null | jq --exit-status '.[0].status.state == "running"' >/dev/null; }
owned() { # owned KIND NAME: the resource exists and carries the e2e label
    local json
    json=$(container "$1" inspect "$2" 2>/dev/null) || return 1
    jq --exit-status --arg label "$label" \
        '.[0] | (.configuration.labels // .labels // .config.labels // {})[$label] == "1"' <<<"$json" >/dev/null \
        || die "refusing to touch a foreign $1: $2"
}

hub_ip() {
    container inspect "$name-hub" | jq --raw-output '.[0].status.networks[0].ipv4Address // empty' | cut -d/ -f1
}

node_exec() { local node=$1; shift; container exec "$name-$node" "$@"; }

# Copies a file of the hub's team directory into another node's (a scratch node's too), once; with --force, again.
copy_creds() {
    local force= node file
    if [ "$1" = --force ]; then force=1; shift; fi
    node=$1
    shift
    for file in "$@"; do
        if [ -z "$force" ] && node_exec "$node" test -s "$auth/$file"; then continue; fi
        node_exec hub cat "$auth/$file" | container exec --interactive "$name-$node" sh -c \
            "umask 077 && mkdir -p '$auth' && cat >'$auth/$file.tmp' && mv '$auth/$file.tmp' '$auth/$file'"
    done
}

ready() { # ready NODE: the daemon's health endpoint answers, or for the client, its leaf node listens
    local node=$1 attempt
    for ((attempt = 0; attempt < 120; attempt++)); do
        if [ "$node" = client ]; then
            node_exec client bash -c '(: >/dev/tcp/127.0.0.1/4222) 2>/dev/null' && return 0
        elif node_exec "$node" curl --fail --silent --max-time 2 http://127.0.0.1:9464/healthz >/dev/null 2>&1; then
            return 0
        fi
        running "$node" || break
        sleep 1
    done
    container logs -n 40 "$name-$node" >&2 || true
    die "node did not become ready: $node"
}

# Points a node's leaf uplink at the hub's address (up) or at a dead one (down), as the doc's uplink helper does.
uplink() {
    local node=$1 url="nats-leaf://127.0.0.1:9"
    [ "$2" = down ] || url="nats-leaf://$(hub_ip):7422"
    node_exec "$node" sh -c "ants leaf config $node --uplink $url >/dev/null && kill -HUP \$(pidof nats-server)"
}

# After the hub (re)started: tells the leaf and the client its address, re-pointing their uplinks when it moved.
sync_hub_ip() {
    local ip node old
    ip=$(hub_ip)
    [ -n "$ip" ] || die "no address for the hub"
    for node in leaf client; do
        running "$node" || continue
        old=$(node_exec "$node" cat /demo/hub-ip 2>/dev/null || true)
        [ "$old" = "$ip" ] && continue
        printf '%s\n' "$ip" | container exec --interactive "$name-$node" sh -c 'cat >/demo/hub-ip'
        if node_exec "$node" test -s /demo/home/.config/ants/leaf.conf && node_exec "$node" pidof nats-server >/dev/null; then
            say "hub moved to $ip: re-pointing the $node uplink"
            uplink "$node" up
        fi
    done
}

check_claude_env() {
    [ -f "$claude_env" ] || die "no Claude token: write CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-... to $claude_env (mode 600), see docs/e2e-scenarios.md, Claude sign-in"
    grep -q '^CLAUDE_CODE_OAUTH_TOKEN=.' "$claude_env" || die "$claude_env sets no CLAUDE_CODE_OAUTH_TOKEN"
    ! grep -q '^ANTHROPIC_API_KEY=' "$claude_env" || die "$claude_env sets ANTHROPIC_API_KEY, which bills the API instead"
}

build() (
    local context rust_version nats_version
    # The base images follow the pins: the toolchain of rust-toolchain.toml, the nats-server of nats-server.sha256.
    rust_version=$(sed -n 's/^channel = "\(.*\)"$/\1/p' "$root/rust-toolchain.toml")
    nats_version=$("$root/scripts/nats-server.sh" --version)
    nats_version=${nats_version#v}
    [ -n "$rust_version" ] && [ -n "$nats_version" ] || die "no pinned Rust toolchain or nats-server version"
    context=$(mktemp -d "$state/build.XXXXXX")
    trap 'rm -rf "$context"' EXIT
    cp "$root/Cargo.toml" "$root/Cargo.lock" "$root/rust-toolchain.toml" "$context/"
    cp -R "$root/.cargo" "$root/crates" "$context/"
    mkdir -p "$context/contrib"
    cp -R "$root/contrib/e2e" "$root/contrib/launchd" "$root/contrib/systemd" "$context/contrib/"
    container build --cpus 4 --memory 8g --tag "$image" --file "$context/contrib/e2e/Containerfile" \
        --build-arg "RUST_VERSION=$rust_version" --build-arg "NATS_VERSION=$nats_version" \
        --build-arg "CLAUDE_VERSION=${ANTS_E2E_CLAUDE_VERSION:-2.1.330}" \
        --build-arg "CLAUDE_ACP_VERSION=${ANTS_E2E_CLAUDE_ACP_VERSION:-0.107.0}" "$context"
)

# Creates a node's container, or starts it when stopped. ENV_FILE overrides the Claude env file (H1 uses it).
start_node() {
    local node=$1 env_args=() memory=4g
    if exists "$node"; then
        running "$node" || container start "$name-$node" >/dev/null
        return
    fi
    if [ "$node" != client ]; then
        check_claude_env
        env_args=(--env-file "${ENV_FILE:-$claude_env}")
    fi
    case "$node" in client) memory=1g ;; leaf) memory=6g ;; esac
    if ! container volume inspect "$name-$node" >/dev/null 2>&1; then
        container volume create --label "$label=1" -s 8g "$name-$node" >/dev/null
    fi
    container run --detach --name "$name-$node" --network "$name" --label "$label=1" \
        --cpus 2 --memory "$memory" \
        --mount "type=volume,source=$name-$node,target=/demo" ${env_args[@]+"${env_args[@]}"} \
        "$image" "$node" >/dev/null
}

# Removes a node's container, keeping its volume.
remove_node() {
    exists "$1" || return 0
    running "$1" && container stop --time 30 "$name-$1" >/dev/null
    container delete "$name-$1" >/dev/null
}

# Creates a scratch node of group B: a container of the fixture's image on its network, with no volume (its /demo goes
# with it), idle until its scenario sets it up. The harness ants setup picks stays on Haiku by ANTHROPIC_MODEL.
scratch_node() {
    local node=$1
    case "$node" in hub | leaf | client) die "not a scratch node: $node" ;; esac
    remove_scratch "$node"
    check_claude_env
    container run --detach --name "$name-$node" --network "$name" --label "$label=1" --label "$label.scratch=1" \
        --cpus 2 --memory 3g --env-file "$claude_env" --env IS_SANDBOX=1 --env ANTHROPIC_MODEL=claude-haiku-4-5 \
        "$image" scratch >/dev/null
}

remove_scratch() {
    case "$1" in hub | leaf | client) die "not a scratch node: $1" ;; esac
    exists "$1" || return 0
    container inspect "$name-$1" | jq --exit-status --arg label "$label.scratch" \
        '.[0].configuration.labels[$label] == "1"' >/dev/null || die "refusing to remove a node not scratch: $1"
    container delete --force "$name-$1" >/dev/null
}

# The scratch nodes left behind, by name without the prefix.
scratch_nodes() {
    container list --all --format json \
        | jq --raw-output --arg label "$label.scratch" --arg prefix "$name-" \
            '.[] | select((.configuration.labels // {})[$label] == "1") | .configuration.id | ltrimstr($prefix)'
}

wire() {
    # The outboxes, wired from the hub as a human; run again, it changes nothing.
    local out
    out=$(node_exec hub ants --creds "$auth/alice.creds" leaf outbox leaf --ant leaf-ant --ant gate-ant 2>&1) \
        || { printf '%s\n' "$out" >&2; die "wiring the leaf's outbox failed"; }
    out=$(node_exec hub ants --creds "$auth/alice.creds" leaf outbox client --human alice 2>&1) \
        || { printf '%s\n' "$out" >&2; die "wiring the client's outbox failed"; }
}

wait_live() { # wait_live ANT...: each is live in the colony, as alice reads it
    local attempt json missing ant
    for ((attempt = 0; attempt < 90; attempt++)); do
        json=$(node_exec client ants colony --json 2>/dev/null || true)
        missing=
        for ant in "$@"; do
            jq --slurp --exit-status --arg ant "$ant" 'any(.[]; .ant == $ant and .state == "live")' \
                <<<"$json" >/dev/null 2>&1 || missing="$missing $ant"
        done
        [ -z "$missing" ] && return 0
        sleep 2
    done
    die "not live in the colony:$missing"
}

up() {
    container image inspect "$image" >/dev/null 2>&1 || build
    if ! container network inspect "$name" >/dev/null 2>&1; then
        container network create --label "$label=1" "$name" >/dev/null
    fi
    start_node hub
    ready hub
    start_node leaf
    start_node client
    sync_hub_ip
    copy_creds leaf server.conf leaf-ant.creds gate-ant.creds host-leaf.creds leaf-leaf.creds leaf-leaf.system.creds \
        leaf-leaf.store.creds
    copy_creds client server.conf alice.creds leaf-client.creds leaf-client.system.creds \
        leaf-client.store.creds
    ready leaf
    ready client
    wire
    wait_live hub-ant leaf-ant gate-ant system-messenger sage-ant
    say "fixture up; hub at $(hub_ip)"
}

down() {
    local node
    for node in $(scratch_nodes); do remove_scratch "$node"; done
    for node in client leaf hub; do remove_node "$node"; done
    if container network inspect "$name" >/dev/null 2>&1; then container network delete "$name" >/dev/null; fi
    say "containers and network removed; volumes kept"
}

reset() {
    local node
    down
    for node in "${nodes[@]}"; do
        if container volume inspect "$name-$node" >/dev/null 2>&1; then container volume delete "$name-$node" >/dev/null; fi
    done
    say "volumes removed"
}

case "$command" in
build) build ;;
up) up ;;
down) down ;;
reset) reset ;;
status) node_exec client ants colony ;;
sh) container exec --interactive --tty "$name-$1" bash ;;
logs)
    if [ "$#" -ge 2 ]; then node_exec "$1" tail -n 80 "/demo/log/$2.log"; else container logs -n 80 "$name-$1"; fi ;;
ants)
    node=$1
    shift
    node_exec "$node" ants "$@" ;;
list | run)
    # shellcheck source=e2e/scenarios.sh
    . "$root/scripts/e2e/scenarios.sh"
    if [ "$command" = list ]; then list_scenarios; else run_scenarios "$@"; fi ;;
esac
