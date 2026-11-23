#!/bin/bash
set -euo pipefail
umask 077

prepare() {
    local team=/demo/hub/auth node
    unset ANTS_CONFIG
    for node in hub leaf-a leaf-b; do
        mkdir -p "/demo/$node"/{auth,home,work,jetstream,jwt,ant}
    done
    if [ ! -f "$team/server.conf" ]; then
        ants auth init --dir "$team"
    fi
    for node in hub leaf-a leaf-b; do
        if [ ! -f "$team/$node-ant.creds" ]; then
            ants auth user "$node-ant" --dir "$team"
        fi
        if [ ! -f "$team/host-$node.creds" ]; then
            ants auth host "$node" --dir "$team"
        fi
    done
    if [ ! -f "$team/human.creds" ]; then
        ants auth user human --admin --dir "$team"
    fi
    for node in leaf-a leaf-b; do
        if [ ! -f "$team/leaf-$node.creds" ]; then
            ants auth leaf "$node" --ant "$node-ant" --human human --dir "$team"
        fi
        cp "$team/server.conf" "$team/human.creds" "$team/$node-ant.creds" "$team/host-$node.creds" \
            "$team/leaf-$node.creds" "$team/leaf-$node.store.creds" "$team/leaf-$node.system.creds" \
            "/demo/$node/auth/"
    done
}

if [ "${1:-}" = prepare ]; then
    prepare
    exit
fi

node=${1:?expected hub, leaf-a or leaf-b}
case "$node" in
hub | leaf-a | leaf-b) ;;
*) printf 'ants-demo-node: invalid node: %s\n' "$node" >&2; exit 1 ;;
esac

cat > /demo/config.toml <<EOF
harness = "copilot"
machine = "$node"
messenger = false
nats = "nats://127.0.0.1:4222"
js_domain = "hub"
metrics = "127.0.0.1:9464"

[cli]
creds = "/demo/auth/human.creds"

[host]
creds = "/demo/auth/host-$node.creds"

[storage]
max_memory_store = "64MB"
max_file_store = "1GB"

# Copilot CLI's native JSON-RPC server through the Copilot SDK, in place of ACP: it runs its tools unasked.
[harnesses.copilot]
command = "/usr/local/bin/copilot"
protocol = "copilot-sdk"
sign_in = "copilot login"
env = { COPILOT_AUTO_UPDATE = "false" }

[harnesses.copilot.context]
default = { args = ["--context", "default"] }
long_context = { args = ["--context", "long_context"] }

[[ant]]
name = "$node-ant"
creds = "/demo/auth/$node-ant.creds"
home = "/demo/ant"
workdir = "/demo/work"
whoami = "Demo ant on $node. Identify the node when asked. Use ask_ant to contact the other demo ants."
capabilities = ["demo", "$node"]
allow = ["*"]
max_harnesses = 1
worktrees = false
EOF

if [ "$node" = hub ]; then
    cat >> /demo/config.toml <<'EOF'

# The assistant acts with the human's privileges, and a native harness runs its tools unasked, so the model gets
# only Copilot's file reading and search tools and the ants MCP tools: no shell, edits or web search. mcp:* also
# admits the built-in GitHub MCP server, which the CLI's server starts regardless of --disable-builtin-mcps, so its
# tools are excluded by name.
[harnesses.copilot-assistant]
command = "/usr/local/bin/copilot"
protocol = "copilot-sdk"
sign_in = "copilot login"
args = [
    "--available-tools", "view", "grep", "glob", "mcp:*",
    "--excluded-tools", "web_search",
    "github-mcp-server-actions_get", "github-mcp-server-actions_list", "github-mcp-server-get_commit",
    "github-mcp-server-get_copilot_space", "github-mcp-server-get_file_contents", "github-mcp-server-get_job_logs",
    "github-mcp-server-issue_read", "github-mcp-server-list_branches", "github-mcp-server-list_commits",
    "github-mcp-server-list_copilot_spaces", "github-mcp-server-list_issues", "github-mcp-server-list_pull_requests",
    "github-mcp-server-pull_request_read", "github-mcp-server-search_code", "github-mcp-server-search_issues",
    "github-mcp-server-search_pull_requests", "github-mcp-server-search_repositories",
    "github-mcp-server-search_users",
]
env = { COPILOT_AUTO_UPDATE = "false" }
EOF
    cat > /demo/nats.conf <<'EOF'
server_name: demo-hub
listen: 127.0.0.1:4222
http: 127.0.0.1:8222
jetstream {
  domain: hub
  store_dir: "/demo/jetstream"
  max_memory_store: 64MB
  max_file_store: 1GB
}
leafnodes {
  listen: 0.0.0.0:7422
  no_advertise: true
}
feature_flags { js_api_reply_v2: true }
include "auth/server.conf"
resolver { type: full, dir: "/demo/jwt" }
EOF
else
    ants leaf config "$node" --uplink "nats-leaf://${ANTS_DEMO_HUB:?missing hub address}:7422" \
        --dir /demo/auth --out /demo/nats.conf
fi

nats_pid=
daemon_pid=
cleanup() {
    trap - EXIT INT TERM
    if [ -n "$daemon_pid" ]; then
        kill -TERM "$daemon_pid" 2>/dev/null || true
        wait "$daemon_pid" 2>/dev/null || true
    fi
    if [ -n "$nats_pid" ]; then
        kill -TERM "$nats_pid" 2>/dev/null || true
        wait "$nats_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT
trap 'exit 0' INT TERM
nats-server --config /demo/nats.conf &
nats_pid=$!
for ((attempt = 0; attempt < 60; attempt++)); do
    kill -0 "$nats_pid"
    if (: > /dev/tcp/127.0.0.1/4222) 2>/dev/null; then
        break
    fi
    sleep 1
done
(: > /dev/tcp/127.0.0.1/4222)
if [ "$node" = hub ]; then
    ants init --creds /demo/auth/human.creds
    ants assistant enable --dir /demo/auth --creds /demo/auth/human.creds --config /demo/config.toml \
        --home /demo/assistant --harness copilot-assistant
fi
ants daemon --config /demo/config.toml --upgrade-check 0 &
daemon_pid=$!
wait -n "$nats_pid" "$daemon_pid"
exit 1
