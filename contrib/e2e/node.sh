#!/bin/bash
# Entrypoint of the e2e nodes (docs/e2e-scenarios.md, Fixture): hub, leaf or client, or a scratch node of group B,
# idle until its scenario sets it up as a user would; `ants-e2e-node config MACHINE` prints the nodes' shared settings.
# Every step is safe to repeat: keys and credentials are issued once and kept on the node's volume, config files are
# written anew at each start.
set -euo pipefail
umask 077

node=${1:?expected hub, leaf, client, scratch or config MACHINE}
auth=$HOME/.config/ants/auth
conf=$HOME/.ants/config.toml
case "$node" in
config | scratch) mkdir -p /demo/log ;;
*) mkdir -p "$auth" "$HOME/.ants" /demo/log ;;
esac

log() { printf 'ants-e2e-node %s: %s\n' "$node" "$*" >&2; }

# Runs a command again whenever it exits, as a service manager would.
keep() {
    local name=$1
    shift
    (
        while :; do
            "$@" >>"/demo/log/$name.log" 2>&1 || true
            log "$name exited; starting it again in 2 s"
            sleep 2
        done
    ) &
}

wait_files() {
    local file attempt missing
    for ((attempt = 0; attempt < 600; attempt++)); do
        missing=
        for file in "$@"; do
            [ -s "$file" ] || missing=1
        done
        [ -n "$missing" ] || return 0
        sleep 1
    done
    log "timed out waiting for $*"
    exit 1
}

wait_port() {
    local attempt
    for ((attempt = 0; attempt < 60; attempt++)); do
        if (: >/dev/tcp/127.0.0.1/4222) 2>/dev/null; then return; fi
        sleep 1
    done
    log "nats-server did not listen on 127.0.0.1:4222"
    exit 1
}

issue() { # issue FILE ants auth ARGS...: issue credentials once
    local file=$auth/$1
    shift
    [ -s "$file" ] || "$@" >/dev/null
}

shared_config() { # shared_config MACHINE
    cat <<EOF
machine = "$1"
harness = "claude"
human = "alice"
sage = "none"
js_domain = "hub"
metrics = "127.0.0.1:9464"
nats = "nats://127.0.0.1:4222"

# Claude Code on its native protocol. The nodes run as root, where Claude Code refuses bypassPermissions unless
# IS_SANDBOX=1 says the machine is a sandbox.
[harnesses.claude]
command = "claude"
protocol = "claude"
mode = "bypassPermissions"
ask = { mode = "default" }
review = { mode = "plan" }
sign_in = "claude login"
env = { IS_SANDBOX = "1" }
EOF
}

stop_all() {
    trap - TERM INT
    log "stopping"
    kill -TERM 0 2>/dev/null || true
    wait || true
    exit 0
}

leaf_conf() { # leaf_conf MACHINE: the leaf node's configuration, pointing at the hub address /demo/hub-ip holds
    wait_files /demo/hub-ip "$auth/server.conf" "$auth/leaf-$1.creds" "$auth/leaf-$1.system.creds" "$auth/leaf-$1.store.creds"
    if [ ! -s "$HOME/.config/ants/leaf.conf" ]; then
        ants leaf config "$1" --uplink "nats-leaf://$(cat /demo/hub-ip):7422" >/dev/null
    fi
}

hub() {
    [ -s "$auth/server.conf" ] || ants auth init >/dev/null
    issue hub-ant.creds ants auth user hub-ant
    issue leaf-ant.creds ants auth user leaf-ant
    issue gate-ant.creds ants auth user gate-ant
    issue system-messenger.creds ants auth builtin messenger
    issue host-hub.creds ants auth host hub
    issue host-leaf.creds ants auth host leaf
    issue channel-manager.creds ants auth channel-manager
    issue bob.creds ants auth user bob --human
    issue alice.creds ants auth user alice --human --admin --label client
    issue leaf-leaf.creds ants auth leaf leaf --ant leaf-ant --ant gate-ant
    issue leaf-client.creds ants auth leaf client --human alice
    issue bootstrap.creds ants auth bootstrap
    mkdir -p /demo/jetstream /demo/jwt /demo/work
    if [ ! -d /demo/work/.git ]; then
        git init --quiet --initial-branch main /demo/work
        printf 'node=hub\n' >/demo/work/node.txt
        git -C /demo/work add node.txt
        git -C /demo/work -c user.name=e2e -c user.email=e2e@localhost commit --quiet -m 'node.txt'
    fi
    git config --global user.name hub-ant
    git config --global user.email hub-ant@localhost
    cat >/demo/nats.conf <<EOF
server_name: e2e-hub
listen: 127.0.0.1:4222
http: 127.0.0.1:8222
jetstream {
  domain: hub
  store_dir: "/demo/jetstream"
  max_memory_store: 256MB
  max_file_store: 2GB
}
leafnodes {
  listen: 0.0.0.0:7422
  no_advertise: true
}
feature_flags { js_api_reply_v2: true }
include "home/.config/ants/auth/server.conf"
resolver { type: full, dir: "/demo/jwt" }
EOF
    {
        shared_config hub
        cat <<'EOF'

[cli]
creds = "~/.config/ants/auth/bob.creds"

# Human registration for `ants login` (B6 to B9), against the stub GitHub on the hub's loopback.
[registration]
client_id = "e2e-stub"
github_url = "http://127.0.0.1:8765"
api_url = "http://127.0.0.1:8765"
email_domains = ["e2e.example"]

[[ant]]
name = "hub-ant"
creds = "~/.config/ants/auth/hub-ant.creds"
workdir = "/demo/work"
capabilities = ["e2e", "hub"]
model = "claude-haiku-4-5"
max_harnesses = 3
EOF
    } >"$conf"
    keep nats-server nats-server --config /demo/nats.conf
    wait_port
    until ants init --creds "$auth/alice.creds" >>/demo/log/init.log 2>&1; do sleep 2; done
    keep github-stub python3 /usr/local/bin/ants-e2e-github-stub 8765
    keep signer ants signer --upgrade-check 0
    keep channel ants channel serve --creds "$auth/channel-manager.creds" --upgrade-check 0
    keep daemon ants daemon --upgrade-check 0
}

leaf() {
    wait_files "$auth/leaf-ant.creds" "$auth/gate-ant.creds" "$auth/host-leaf.creds"
    leaf_conf leaf
    mkdir -p /demo/work /demo/gate
    printf 'node=leaf\n' >/demo/work/node.txt
    printf 'node=gate\n' >/demo/gate/node.txt
    {
        shared_config leaf
        cat <<'EOF'

[nats_server]
config = "~/.config/ants/leaf.conf"

[[ant]]
name = "leaf-ant"
creds = "~/.config/ants/auth/leaf-ant.creds"
workdir = "/demo/work"
capabilities = ["e2e", "leaf"]
model = "claude-haiku-4-5"
max_harnesses = 3
env = { E2E_SECRET_TOKEN = "e2e-s3cr3t-0123456789" }

[[ant]]
name = "gate-ant"
creds = "~/.config/ants/auth/gate-ant.creds"
workdir = "/demo/gate"
model = "claude-haiku-4-5"
tools = "ask"
allow = ["read", "search", "execute:ls*"]
max_harnesses = 2
EOF
    } >"$conf"
    keep daemon ants daemon --upgrade-check 0
}

client() {
    wait_files "$auth/alice.creds"
    leaf_conf client
    cat >"$conf" <<'EOF'
js_domain = "hub"
nats = "nats://127.0.0.1:4222"

[cli]
creds = "~/.config/ants/auth/alice.creds"
EOF
    keep nats-server nats-server --config "$HOME/.config/ants/leaf.conf"
}

# A machine as a new user finds it: a working directory holding node.txt, nothing issued, configured or started.
scratch() {
    mkdir -p /demo/work
    printf 'node=scratch\n' >/demo/work/node.txt
    log "up"
    exec sleep infinity
}

if [ "$node" = config ]; then
    shared_config "${2:?config needs a machine name}"
    exit
fi
trap stop_all TERM INT
case "$node" in
hub) hub ;;
leaf) leaf ;;
client) client ;;
scratch) scratch ;;
*) log "invalid node"; exit 1 ;;
esac
log "up"
wait
