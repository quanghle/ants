# The scenarios of docs/e2e-scenarios.md, sourced by scripts/e2e.sh (bash 3.2: no associative arrays).
# Each scenario is a function named by its id, run in a subshell with errexit: any failing command or check fails it,
# exit status 77 skips it. A scenario leaves the fixture as it found it, its traps undoing what it changed.

registry=
scenario() { registry="$registry$1 $2 $3"$'\n'; }
# Scenarios that change what the others rely on (a node, an uplink, an ant's settings or pause, the chat's last
# thread), run one at a time after the rest; the others share the fixture and run side by side with `run -j N`.
alone=" B2 B3 B4 B5 R1 R2 R3 R4 R5 R6 H1 H2 H3 O1 O2 O3 O6 O7 S4 T4 U4 U5 U7 U8 X1 X4 X6 X7 K1 E2 E3 E6 "
is_alone() { case "$alone" in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# --- Helpers on the Mac, as the doc's Conventions give them -----------------------------------------------------------

# The CLI's own logs stay out of what the checks read.
hub() { container exec --env ANTS_LOG=warn "$name-hub" "$@"; }
leaf() { container exec --env ANTS_LOG=warn "$name-leaf" "$@"; }
alice() { container exec --env ANTS_LOG=warn "$name-client" ants "$@"; }
bob() { container exec --env ANTS_LOG=warn "$name-hub" ants --creds "$auth/bob.creds" "$@"; }
tok() { uuidgen | tr '[:upper:]' '[:lower:]' | cut -c1-8; }
# Both read all of stdin: stopping early would end the command piped in on a broken pipe.
thread() { awk '$1 == "thread" && !found { print $2; found = 1 }'; }
message() { awk '$1 == "message" && !found { print $2; found = 1 }'; }
# slow SECONDS: a shell command for an ant to run that takes SECONDS. Claude Code refuses a bare `sleep N`.
# Prompts using it say what they are for and ask for the foreground: a model balks at an aimless wait, and Claude Code
# may otherwise run it in the background, ending the prompt at once.
e2e="This is an end-to-end test of ants, which needs a prompt that takes a while."
slow() { printf 'python3 -c "import time; time.sleep(%s)"' "$1"; }

fail() { printf 'FAILED: %s\n' "$*"; exit 1; }
skip() { printf 'SKIPPED: %s\n' "$*"; exit 77; }
note() { printf -- '-- %s\n' "$*"; }
# known ISSUE TEXT: a check fails on a bug filed in bd; the scenario goes on and the runner reports it beside PASS.
known() { printf 'KNOWN %s: %s\n' "$1" "$2"; }

# has TEXT [WHAT]: stdin holds TEXT; lacks TEXT [WHAT]: it does not.
has() {
    local input
    input=$(cat)
    grep -qF -- "$1" <<<"$input" || { printf '%s\n' "$input" | tail -n 40; fail "expected ${2:-output} to hold: $1"; }
}
lacks() {
    local input
    input=$(cat)
    ! grep -qF -- "$1" <<<"$input" || { printf '%s\n' "$input" | tail -n 40; fail "expected ${2:-output} not to hold: $1"; }
}
count() { grep -cF -- "$1" || true; } # count TEXT: lines of stdin holding TEXT

# eventually SECONDS CMD...: runs CMD every 5 s until it succeeds; fails after SECONDS.
eventually() {
    local limit=$1 start=$SECONDS
    shift
    until "$@" >/dev/null 2>&1; do
        ((SECONDS - start < limit)) || { "$@" || true; fail "not within ${limit}s: $*"; }
        sleep 5
    done
}

# The texts of the thread's messages that are not alice's, one JSON string a line.
ant_texts() {
    alice thread show "$1" --json | jq --raw-output 'select(.from != "alice") | .text // empty' 2>/dev/null \
        || alice thread show "$1"
}
replied() { ant_texts "$1" | grep -qF -- "$2"; } # replied THREAD TEXT: an ant's message on the thread holds TEXT

colony_state() { # colony_state ANT: the ant's state in the colony, or "absent"
    alice colony --json | jq --slurp --raw-output --arg ant "$1" \
        'map(select(.ant == $ant)) | if length == 0 then "absent" else .[0].state end'
}
colony_field() { # colony_field ANT JQ
    alice colony --json | jq --slurp --raw-output --arg ant "$1" "map(select(.ant == \$ant)) | .[0] | $2"
}
is_live() { [ "$(colony_state "$1")" = live ]; }
is_paused() { [ "$(colony_field "$1" '.paused != null')" = true ]; }
not_live() { [ "$(colony_state "$1")" != live ]; }
all_live() { wait_live hub-ant leaf-ant gate-ant system-messenger sage-ant; }

# Starts a node again after a scenario stopped it, and waits until the fixture is whole.
restore_node() {
    local node=$1
    running "$node" || start_node "$node"
    ready "$node"
    [ "$node" != hub ] || sync_hub_ip
    all_live
}

# Recreates the leaf's container with another Claude env file (its volume kept), as H1 does.
recreate_leaf() {
    remove_node leaf
    ENV_FILE=${1:-} start_node leaf
    ready leaf
}

pending_tool() { # pending_tool ANT THREAD: the id ANT/ID of a pending tool request on the thread
    alice tools list --json | jq --slurp --raw-output --arg ant "$1" --arg thread "$2" \
        'map(select(.ant == $ant and .thread_id == $thread)) | .[0] | if . == null then empty else "\(.ant)/\(.id)" end'
}

queued() { alice queue "$1" | has "$2" "ants queue"; } # queued ANT TEXT: `ants queue ANT` lists TEXT

ask_bg() { # ask_bg FILE ARGS...: alice asks in the background, writing the output to FILE
    local out=$1
    shift
    alice ask "$@" >"$out" 2>&1 &
}
thread_of() { # thread_of FILE: waits for the thread line of a background ask's output
    local attempt
    for ((attempt = 0; attempt < 60; attempt++)); do
        if grep -q '^thread ' "$1" 2>/dev/null; then thread <"$1"; return; fi
        sleep 1
    done
    cat "$1"
    fail "no thread printed"
}

# --- F. Preflight ---------------------------------------------------------------------------------------------------

scenario F1 P0 "The colony is up across the three containers"
F1() {
    hub curl --fail --silent http://127.0.0.1:9464/healthz >/dev/null
    leaf curl --fail --silent http://127.0.0.1:9464/healthz >/dev/null
    local colony ant
    colony=$(alice colony --json)
    for ant in hub-ant leaf-ant gate-ant system-messenger sage-ant; do
        jq --slurp --exit-status --arg ant "$ant" \
            'any(.[]; .ant == $ant and .state == "live" and .harness == "claude")' <<<"$colony" >/dev/null \
            || { printf '%s\n' "$colony"; fail "$ant is not live on claude"; }
    done
    for ant in hub-ant system-messenger sage-ant; do
        [ "$(colony_field "$ant" .machine)" = hub ] || fail "$ant is not on machine hub"
    done
    for ant in leaf-ant gate-ant; do
        [ "$(colony_field "$ant" .machine)" = leaf ] || fail "$ant is not on machine leaf"
    done
    local machines
    machines=$(alice machines)
    has hub <<<"$machines"
    has leaf <<<"$machines"
    has "account key is on machine hub" <<<"$machines"
    local node out
    # The leaf has no human: its doctor reads with leaf-ant's credentials, which skip the outbox check.
    for node in hub leaf client; do
        out=$(container exec --env ANTS_LOG=warn "$name-$node" ants $([ "$node" != leaf ] || echo --creds "$auth/leaf-ant.creds") \
            doctor --no-harness 2>&1) || true
        note "doctor on $node"
        printf '%s\n' "$out"
        ! grep -qE '^\s*(✗|fail)' <<<"$out" || fail "ants doctor on $node reports a failure"
        [ "$node" = hub ] || grep -E '^ok +leaf uplink' <<<"$out" >/dev/null || fail "leaf uplink not ok on $node"
    done
    grep -E '^ok +outbox' <<<"$out" >/dev/null || fail "outbox not ok on the client"
}

scenario F2 P0 "Claude Code is installed and signed in on both nodes"
F2() {
    local node out
    for node in hub leaf; do
        out=$(container exec --env ANTS_LOG=warn "$name-$node" ants harness check 2>&1)
        printf '%s\n' "$out"
        has "2.1." <<<"$out"
        lacks auth_required <<<"$out"
    done
}

scenario F3 P0 "The bus carries notes between all three nodes"
F3() {
    local t
    t=$(uuidgen | tr '[:upper:]' '[:lower:]')
    alice notes "$t" "from client $t" >/dev/null
    bob notes "$t" "from hub $t" >/dev/null
    alice notes "$t" | has "from hub $t"
    alice notes "$t" | has "from client $t"
    bob notes "$t" | has "from client $t"
    ! leaf ants --creds "$auth/leaf-ant.creds" inbox >/dev/null 2>&1 || fail "ant credentials read a human inbox"
}

# --- B. Bringing up machines and people -----------------------------------------------------------------------------

# on NODE CMD...: runs CMD on a node, scratch nodes too, the CLI's own logs left out.
on() { local node=$1; shift; container exec --env ANTS_LOG=warn "$name-$node" "$@"; }
# detached NODE LOG CMD: runs the shell command CMD on a node in the background, its output to /demo/log/LOG.log and
# its exit status, once it ends, to /demo/log/LOG.status.
detached() {
    container exec "$name-$1" sh -c "rm -f /demo/log/$2.status; setsid -f sh -c '($3) >>/demo/log/$2.log 2>&1; echo \$? >/demo/log/$2.status' </dev/null >/dev/null 2>&1"
}
ended() { on "$1" test -s "/demo/log/$2.status"; } # ended NODE LOG: the command of `detached NODE LOG` ended
# listens NODE: something listens on the node's NATS client port.
listens() { on "$1" bash -c '(: >/dev/tcp/127.0.0.1/4222) 2>/dev/null'; }
no_fail() { ! grep -qE '^\s*(✗|fail)' || return 1; } # no_fail <DOCTOR: ants doctor printed no failure

# join_node MACHINE [ANT...]: a scratch node MACHINE set up as the README's worker machine, up to its leaf node: the
# hub issues its uplink credentials (carrying ANT...), the node gets server.conf and the uplink credentials, the
# fixture's shared settings with machine = MACHINE, a leaf.conf pointing at the hub, and its nats-server started.
join_node() {
    local machine=$1 ant args=()
    shift
    for ant in "$@"; do args+=(--ant "$ant"); done
    scratch_node "$machine"
    hub ants auth leaf "$machine" ${args[@]+"${args[@]}"} --js-domain hub >/dev/null
    copy_creds "$machine" server.conf "leaf-$machine.creds" "leaf-$machine.system.creds" "leaf-$machine.store.creds"
    container exec "$name-$machine" sh -c "mkdir -p /demo/home/.ants && ants-e2e-node config $machine >/demo/home/.ants/config.toml"
    on "$machine" ants leaf config "$machine" --uplink "nats-leaf://$(hub_ip):7422" >/dev/null
    detached "$machine" nats-server "nats-server -c /demo/home/.config/ants/leaf.conf"
    eventually 60 listens "$machine"
}

# join_down MACHINE [ANT...]: undoes join_node and what the hub issued for it: the outbox sourcing of the ants, the
# node, the ants' credentials and enrollment credentials, the machine's host and uplink credentials. Also run first, for a run cut short.
join_down() {
    local machine=$1 ant
    shift
    for ant in "$@"; do
        hub ants --creds "$auth/alice.creds" leaf outbox "$machine" --remove-ant "$ant" >/dev/null 2>&1 || true
    done
    remove_scratch "$machine"
    for ant in "$@"; do
        hub ants auth revoke "$ant" >/dev/null 2>&1 || true
        hub ants auth revoke "enroll-$ant" >/dev/null 2>&1 || true
    done
    for ant in "host-$machine" "leaf-$machine"; do
        if hub ants auth issued --json | jq --exit-status --slurp --arg name "$ant" \
            'any(.[]; .name == $name and .revoked == false)' >/dev/null 2>&1; then
            hub ants auth revoke "$ant" >/dev/null
        fi
    done
}

scenario B1 P0 "ants setup brings an empty machine up, as the quick start does"
B1() {
    local t out sums th files="/demo/home/.config/ants/nats.conf /demo/home/.ants/config.toml"
    t=$(tok)
    # The image's ANTS_JS_DOMAIN=hub is the fixture's: a machine of its own has no JetStream domain.
    solo() { container exec --env ANTS_LOG=warn "$name-solo" env -u ANTS_JS_DOMAIN "$@"; }
    trap 'remove_scratch solo' EXIT
    scratch_node solo
    note "ants setup"
    out=$(solo ants setup --ant dev-a --workdir /demo/work --managed-nats --no-services 2>&1) \
        || { printf '%s\n' "$out"; fail "ants setup failed"; }
    printf '%s\n' "$out"
    has "done   harness choice: claude" <<<"$out"
    has "ants daemon --config /demo/home/.ants/config.toml" "what setup leaves to the user" <<<"$out"
    has "--no-services leaves it to start" "the server check skipped" <<<"$out"
    solo test -s /demo/home/.config/ants/nats.conf || fail "no nats.conf"
    out=$(solo cat /demo/home/.ants/config.toml)
    printf '%s\n' "$out"
    has "[nats_server]" <<<"$out"
    has 'name = "dev-a"' <<<"$out"
    has "messenger = true" <<<"$out"
    # What setup leaves to the user under --no-services: start the daemon, which runs nats-server, then ants init.
    detached solo daemon "env -u ANTS_JS_DOMAIN ants daemon --config /demo/home/.ants/config.toml --upgrade-check 0"
    eventually 60 listens solo
    solo ants init >/dev/null
    live_on_solo() {
        solo ants colony --json | jq --slurp --exit-status --arg ant "$1" 'any(.[]; .ant == $ant and .state == "live")'
    }
    eventually 120 live_on_solo dev-a
    eventually 60 live_on_solo system-messenger
    out=$(solo ants ask --wait --timeout 3m @dev-a "Read node.txt in the working directory and reply with its content and SETUP-$t.")
    printf '%s\n' "$out"
    has node=scratch <<<"$out"
    has "SETUP-$t" <<<"$out"
    th=$(thread <<<"$out")
    solo ants inbox
    out=$(solo ants thread show "$th")
    printf '%s\n' "$out"
    has "human -> dev-a" <<<"$out"
    has '$' "the prompt's cost" <<<"$out"
    solo ants colony
    # No model in the config: ANTHROPIC_MODEL of the node keeps the adapter's Claude Code on Haiku.
    solo sh -c 'cat /demo/home/.claude/projects/*/*.jsonl' | grep -oE '"model":"claude-[a-z0-9.-]+"' | sort -u \
        | { out=$(cat); printf '%s\n' "$out"; has claude-haiku-4-5 "the session's model" <<<"$out"; lacks sonnet <<<"$out"; lacks opus <<<"$out"; }
    solo ants channel create ops --member dev-a
    out=$(solo ants post --wait --timeout 3m '#ops' "@dev-a reply with exactly OPS-$t")
    printf '%s\n' "$out"
    has "OPS-$t" <<<"$out"
    out=$(solo ants doctor 2>&1) || true
    note "doctor"
    printf '%s\n' "$out"
    no_fail <<<"$out" || fail "ants doctor reports a failure"
    sums=$(solo sha256sum $files)
    note "ants setup, again"
    if ! out=$(solo ants setup --ant dev-a --workdir /demo/work --managed-nats --no-services 2>&1); then
        printf '%s\n' "$out"
        fail "ants setup failed the second time"
    fi
    printf '%s\n' "$out"
    has "exists config" <<<"$out"
    [ "$(solo sha256sum $files)" = "$sums" ] || fail "the second ants setup changed a file"
}

# enroll_heard ANT: `ants enroll list` on the hub lists a request for ANT.
enroll_heard() { hub ants enroll list 2>&1 | grep -qF -- "$1"; }

# enroll_bg MACHINE ANT LOG [ARGS...]: on the node, `ants add ANT --enroll` in the background with the enrollment
# credentials the hub issued for ANT, copied there.
enroll_bg() {
    local machine=$1 ant=$2 log=$3
    shift 3
    hub ants auth enroll "$ant" >/dev/null
    copy_creds "$machine" "enroll-$ant.creds"
    detached "$machine" "$log" "ants add $ant --workdir /demo/work --enroll $auth/enroll-$ant.creds $*"
}

scenario B2 P0 "A worker machine joins the hub by enrollment"
B2() {
    local t m=join-b2 ant out th
    t=$(tok)
    ant=b2-$t
    join_down "$m"
    trap "join_down $m $ant" EXIT
    join_node "$m" "$ant"
    enroll_bg "$m" "$ant" add --model claude-haiku-4-5
    eventually 60 enroll_heard "$ant"
    out=$(hub ants enroll list 2>&1)
    printf '%s\n' "$out"
    has "$ant" <<<"$out"
    has "$m" "the machine whose host credentials are asked for" <<<"$out"
    hub ants enroll approve "$ant" --js-domain hub
    eventually 30 ended "$m" add
    out=$(on "$m" cat /demo/log/add.log)
    printf '%s\n' "$out"
    [ "$(on "$m" cat /demo/log/add.status)" = 0 ] || fail "ants add failed"
    on "$m" test -s "$auth/$ant.creds" || fail "no credentials of $ant on the node"
    has "host credentials of $m" <<<"$out"
    # No service runs the leaf node: ants add prints the reload that connects its uplink with the renewed credentials.
    has "send the running leaf node SIGHUP" <<<"$out"
    on "$m" sh -c 'kill -HUP $(pidof nats-server)'
    hub ants --creds "$auth/alice.creds" leaf outbox "$m" --ant "$ant" --js-domain hub
    detached "$m" daemon "ants daemon --upgrade-check 0"
    eventually 120 is_live "$ant"
    [ "$(colony_field "$ant" .machine)" = "$m" ] || fail "$ant is not on machine $m"
    out=$(alice ask --wait --timeout 3m "@$ant" "Read node.txt in the working directory and reply with its content and JOIN-$t.")
    printf '%s\n' "$out"
    has node=scratch <<<"$out"
    has "JOIN-$t" <<<"$out"
    alice machines | has "$m"
    out=$(hub ants auth issued)
    has "$ant" <<<"$out"
    has "host-$m" <<<"$out"
    out=$(on "$m" ants --creds "$auth/$ant.creds" doctor --no-harness 2>&1) || true
    note "doctor on $m"
    printf '%s\n' "$out"
    no_fail <<<"$out" || fail "ants doctor on $m reports a failure"
    grep -E '^ok +leaf uplink' <<<"$out" >/dev/null || fail "leaf uplink not ok on $m"
    grep -E '^ok +host credentials' <<<"$out" >/dev/null || fail "host credentials not ok on $m"
}

scenario B3 P1 "A rejected enrollment stops ants add"
B3() {
    local t m=join-b3 ant out start
    t=$(tok)
    ant=b3-$t
    join_down "$m"
    trap "join_down $m $ant" EXIT
    join_node "$m" "$ant"
    enroll_bg "$m" "$ant" add
    eventually 60 enroll_heard "$ant"
    start=$SECONDS
    hub ants enroll reject "$ant" --reason "e2e $t"
    eventually 30 ended "$m" add
    note "ants add ended $((SECONDS - start)) s after the rejection"
    out=$(on "$m" cat /demo/log/add.log)
    printf '%s\n' "$out"
    [ "$(on "$m" cat /demo/log/add.status)" != 0 ] || fail "ants add succeeded"
    has "e2e $t" "the rejection's reason" <<<"$out"
    ! on "$m" test -e "$auth/$ant.creds" || fail "credentials of $ant on the node"
    not_heard() { ! enroll_heard "$ant"; }
    eventually 60 not_heard
    [ "$(colony_state "$ant")" = absent ] || fail "$ant is in the colony"
}

scenario B4 P1 "An ant added out of band with a signed JWT"
B4() {
    local t m=join-b4 ant out key jwt
    t=$(tok)
    ant=b4-$t
    join_down "$m"
    trap "join_down $m $ant" EXIT
    # The uplink carries the ant from the start: `ants auth leaf` issues none without a member.
    join_node "$m" "$ant"
    out=$(on "$m" ants add "$ant" --workdir /demo/work --model claude-haiku-4-5 2>&1)
    printf '%s\n' "$out"
    key=$(grep -oE '\bU[A-Z2-7]{55}\b' <<<"$out" | head -n 1)
    [ -n "$key" ] || fail "ants add printed no public key"
    jwt=$(hub ants auth user "$ant" --public-key "$key" --js-domain hub)
    on "$m" ants add "$ant" --jwt "$jwt"
    on "$m" test -s "$auth/$ant.creds" || fail "no credentials of $ant on the node"
    hub ants --creds "$auth/alice.creds" leaf outbox "$m" --ant "$ant" --js-domain hub
    detached "$m" daemon "ants daemon --upgrade-check 0"
    eventually 120 is_live "$ant"
    out=$(alice ask --wait --timeout 3m "@$ant" "Reply with exactly: JWT-$t")
    printf '%s\n' "$out"
    has "JWT-$t" <<<"$out"
}

scenario B5 P2 "Enrollment requests seen from ants chat"
B5() {
    local t ant home
    t=$(tok)
    ant=b5-$t
    home=/demo/b5-$t
    b5_down() { # b5_down ANT HOME
        hub cat "$2/add.log" 2>/dev/null || true
        hub pkill -f "^ants add $1 " || true
        hub ants auth revoke "enroll-$1" >/dev/null 2>&1 || true
        hub rm -rf "$2" || true
    }
    trap "b5_down $ant $home" EXIT
    # A request heard on the hub, from an ants add under a home of its own there.
    hub ants auth enroll "$ant" >/dev/null
    container exec "$name-hub" sh -c "mkdir -p $home/.ants && printf 'harness = \"claude\"\n' >$home/.ants/config.toml && setsid -f env HOME=$home ants add $ant --workdir /tmp \
        --enroll $auth/enroll-$ant.creds >$home/add.log 2>&1 </dev/null"
    eventually 60 enroll_heard "$ant"
    # The client holds no account key: home and the credentials page name the machine that does.
    chat "wait:connected" "wait:answered on machine hub" "type:c" "wait:account key is not on this machine" \
        "wait:machine hub"
    # The hub holds it: the chat there lists the request, and rejects it from its page.
    container exec --interactive "$name-hub" sh -c 'cat >/tmp/ants-e2e-chat.py' <"$root/contrib/e2e/chat.py"
    container exec "$name-hub" python3 /tmp/ants-e2e-chat.py "wait:connected" "wait:$ant (enroll)" "type:c" \
        "wait:$ant asks to enroll" "type:r" "wait:y confirms the order" "type:y" "gone:$ant asks to enroll:30"
    eventually 30 hub grep -q reject "$home/add.log"
    hub cat "$home/add.log"
}

# ants login runs on the hub, whose client port listens on its loopback only, each person under a HOME of its own,
# against the stub GitHub of contrib/e2e/github_stub.py there.
github=https://github.e2e.example
stub=http://127.0.0.1:8765

# login_bg HOME LOG NAME [ARGS...]: `ants login` of NAME in the background on the hub, as a person under HOME.
login_bg() {
    local home=$1 log=$2 human=$3
    shift 3
    hub mkdir -p "$home"
    detached hub "$log" "HOME=$home ants login --hub nats://127.0.0.1:4222 --name $human \
        --bootstrap $auth/bootstrap.creds --github-url $github $*"
}
login_said() { hub cat "/demo/log/$1.log" 2>/dev/null | grep -qF -- "$2"; } # login_said LOG TEXT
login_status() { hub cat "/demo/log/$1.status"; }

# signs_in LOG LOGIN ID EMAIL...: plays the person signing in to GitHub with the user code `ants login` showed: the
# stub GitHub then answers for that user.
signs_in() {
    local log=$1 login=$2 id=$3 code body
    shift 3
    eventually 60 login_said "$log" "enter the code"
    code=$(hub cat "/demo/log/$log.log" | sed -nE 's/.*enter the code ([A-Z0-9]+).*/\1/p' | head -n 1)
    [ -n "$code" ] || fail "no user code shown"
    body=$(jq --null-input --compact-output --arg code "$code" --arg login "$login" --argjson id "$id" \
        '{user_code: $code, login: $login, id: $id, emails: $ARGS.positional}' --args "$@")
    hub curl --fail --silent --show-error -X POST --data "$body" "$stub/e2e/authorize" >/dev/null
}

# signer_request NAME: the id signer/ID of the signer's pending request to register NAME.
signer_request() {
    alice tools list --json | jq --slurp --raw-output --arg name "$1" \
        'map(select(.ant == "signer" and (tostring | contains($name)))) | .[0] | if . == null then empty else "\(.ant)/\(.id)" end'
}
has_signer_request() { [ -n "$(signer_request "$1")" ]; }

# signer_decides approve|reject NAME: alice, on the client, decides the signer's request to register NAME.
signer_decides() {
    local id
    id=$(signer_request "$2")
    [ -n "$id" ] || fail "no request of the signer for $2"
    alice tools "$1" "$id" || fail "alice could not $1 $id"
}

issued_of() { hub ants auth issued --json | jq --slurp --compact-output --arg name "$1" '.[] | select(.name == $name)'; }
github_id() { printf '%d' "$((16#$1))"; } # github_id TOKEN: a numeric GitHub user id of the token's own

scenario B6 P1 "A person the registration policy admits is signed at once"
B6() {
    local t th human home log out
    t=$(tok)
    human=dana-$t
    home=/demo/login-$t
    log=login-$t
    trap "hub ants auth revoke $human >/dev/null 2>&1 || true; hub pkill -f \"^ants login .* --name $human \" || true; hub rm -rf $home /demo/log/$log.log /demo/log/$log.status || true" EXIT
    login_bg "$home" "$log" "$human" --label e2e
    signs_in "$log" "$human" "$(github_id "$t")" "$human@e2e.example"
    eventually 60 hub test -s "/demo/log/$log.status"
    out=$(hub cat "/demo/log/$log.log")
    printf '%s\n' "$out"
    [ "$(login_status "$log")" = 0 ] || fail "ants login failed"
    has "sign in to GitHub at $github/login/device and enter the code" <<<"$out"
    [ "$(hub stat -c %a "$home/.config/ants/auth/$human.creds")" = 600 ] || fail "the credentials are not mode 600"
    hub cat "$home/.ants/config.toml" | has "$human.creds" "[cli] of the new config"
    hub env HOME="$home" ants inbox
    th=$(uuidgen | tr '[:upper:]' '[:lower:]')
    hub env HOME="$home" ants notes "$th" "from dana $t" >/dev/null
    alice notes "$th" | has "from dana $t"
    out=$(issued_of "$human")
    printf '%s\n' "$out"
    has '"e2e"' "the device's label" <<<"$out"
    has "email-domain" "the grant" <<<"$out"
    has "\"$human\"" <<<"$out"
    hub ants auth issued | has "$human"
    hub ants auth devices "$human" | has e2e
    hub cat /demo/log/github-stub.log | has "POST /login/device/code"
}

scenario B7 P1 "A person outside the registration policy waits for a human"
B7() {
    local t human id out
    t=$(tok)
    human=erin-$t
    id=$(github_id "$t")
    trap "hub ants auth revoke $human >/dev/null 2>&1 || true; hub pkill -f \"^ants login .* --name $human \" || true; hub rm -rf /demo/login-$t /demo/login-$t-2 /demo/log/login-$t.log /demo/log/login-$t.status /demo/log/login-$t-2.log /demo/log/login-$t-2.status || true" EXIT
    login_bg "/demo/login-$t" "login-$t" "$human"
    signs_in "login-$t" "$human" "$id" "$human@elsewhere.example"
    eventually 60 login_said "login-$t" "waiting for a human of the team to approve"
    eventually 60 has_signer_request "$human"
    out=$(alice tools list)
    printf '%s\n' "$out"
    has "register human $human" <<<"$out"
    signer_decides approve "$human"
    eventually 60 hub test -s "/demo/log/login-$t.status"
    hub cat "/demo/log/login-$t.log"
    [ "$(login_status "login-$t")" = 0 ] || fail "ants login failed"
    out=$(issued_of "$human")
    printf '%s\n' "$out"
    has approval "the grant" <<<"$out"
    grep -qE '"(alice|bob)"' <<<"$out" || fail "the human approving is not recorded"
    note "a second device of $human"
    login_bg "/demo/login-$t-2" "login-$t-2" "$human"
    signs_in "login-$t-2" "$human" "$id" "$human@elsewhere.example"
    eventually 60 hub test -s "/demo/log/login-$t-2.status"
    out=$(hub cat "/demo/log/login-$t-2.log")
    printf '%s\n' "$out"
    [ "$(login_status "login-$t-2")" = 0 ] || fail "the second device's ants login failed"
    lacks "waiting for a human" <<<"$out"
    [ "$(issued_of "$human" | count '"revoked":false')" -ge 2 ] || fail "not two devices of $human issued"
}

scenario B8 P2 "A rejected registration leaves nothing behind"
B8() {
    local t human home log out
    t=$(tok)
    human=frank-$t
    home=/demo/login-$t
    log=login-$t
    trap "hub pkill -f \"^ants login .* --name $human \" || true; hub rm -rf $home /demo/log/$log.log /demo/log/$log.status || true" EXIT
    login_bg "$home" "$log" "$human"
    signs_in "$log" "$human" "$(github_id "$t")" "$human@elsewhere.example"
    eventually 60 has_signer_request "$human"
    signer_decides reject "$human"
    eventually 60 hub test -s "/demo/log/$log.status"
    out=$(hub cat "/demo/log/$log.log")
    printf '%s\n' "$out"
    [ "$(login_status "$log")" != 0 ] || fail "ants login succeeded"
    has "was refused" <<<"$out"
    [ -z "$(hub find "$home" -type f \( -name '*.creds' -o -name '*.nk' \))" ] || fail "a key or credentials file was left"
    [ -z "$(issued_of "$human")" ] || fail "$human was issued credentials"
}

scenario B9 P2 "A registered device renews"
B9() {
    local t human home log out before
    t=$(tok)
    human=gina-$t
    home=/demo/login-$t
    log=login-$t
    trap "hub ants auth revoke $human >/dev/null 2>&1 || true; hub ants auth revoke hank-$t >/dev/null 2>&1 || true; hub pkill -f \"^ants login .* --name (gina|hank)-$t \" || true; hub rm -rf $home $home-2 /demo/log/$log.log /demo/log/$log.status /demo/log/$log-2.log /demo/log/$log-2.status || true" EXIT
    # An approval grant renews while the approval stands.
    login_bg "$home" "$log" "$human"
    signs_in "$log" "$human" "$(github_id "$t")" "$human@elsewhere.example"
    eventually 60 has_signer_request "$human"
    signer_decides approve "$human"
    eventually 60 hub test -s "/demo/log/$log.status"
    [ "$(login_status "$log")" = 0 ] || { hub cat "/demo/log/$log.log"; fail "ants login failed"; }
    before=$(hub sha256sum "$home/.config/ants/auth/$human.creds")
    out=$(hub env HOME="$home" ants renew 2>&1)
    printf '%s\n' "$out"
    [ "$(hub sha256sum "$home/.config/ants/auth/$human.creds")" != "$before" ] || fail "the credentials file was not renewed"
    hub env HOME="$home" ants inbox >/dev/null
    out=$(issued_of "$human")
    printf '%s\n' "$out"
    has renewed_from "the renewal" <<<"$out"
    # An email-domain grant is refused, naming ants login: only a new sign-in shows the verified addresses.
    login_bg "$home-2" "$log-2" "hank-$t"
    signs_in "$log-2" "hank-$t" "$(github_id "${t}1")" "hank-$t@e2e.example"
    eventually 60 hub test -s "/demo/log/$log-2.status"
    [ "$(login_status "$log-2")" = 0 ] || { hub cat "/demo/log/$log-2.log"; fail "ants login of hank-$t failed"; }
    ! out=$(hub env HOME="$home-2" ants renew 2>&1) || { printf '%s\n' "$out"; fail "an email-domain grant renewed"; }
    printf '%s\n' "$out"
    has "ants login" <<<"$out"
}

# --- M. Messages between a human and an ant -------------------------------------------------------------------------

scenario M1 P0 "Ask the hub's ant from the client"
M1() {
    local t out th
    t=$(tok)
    out=$(alice ask --wait --timeout 3m @hub-ant "Reply with exactly: PONG-$t")
    printf '%s\n' "$out"
    has "PONG-$t" <<<"$out"
    th=$(thread <<<"$out")
    [ -n "$(message <<<"$out")" ] || fail "no message id printed"
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    has "alice -> hub-ant" <<<"$out"
    has "hub-ant -> alice" <<<"$out"
    has "ended: replied" <<<"$out"
    has '$' <<<"$out"
    alice spend hub-ant --thread "$th" | has "$th"
}

scenario M2 P0 "Ask the leaf's ant; its tools run on the leaf"
M2() {
    local out th
    out=$(alice ask --wait --timeout 3m @leaf-ant "Read the file node.txt in the working directory and reply with its content only.")
    printf '%s\n' "$out"
    has node=leaf <<<"$out"
    th=$(thread <<<"$out")
    alice thread show "$th" | has "Read("
}

scenario M3 P0 "A follow-up resumes the thread's session"
M3() {
    local t th out
    t=$(tok)
    th=$(alice ask --wait --timeout 3m @leaf-ant "Remember the word $t for this conversation. Reply OK." | thread)
    out=$(alice ask --wait --timeout 3m --thread "$th" @leaf-ant \
        "Which word did the first message of this conversation give? Reply with it only.")
    printf '%s\n' "$out"
    has "$t" <<<"$out"
    out=$(alice thread show "$th")
    lacks "could not be resumed" <<<"$out"
    lacks "session rebuilt" <<<"$out"
}

scenario M4 P0 "Inbox, read markers and marking unread"
M4() {
    local t th
    t=$(tok)
    th=$(alice ask @hub-ant "Reply with exactly: INBOX-$t" | thread)
    eventually 180 replied "$th" "INBOX-$t"
    alice inbox --peek --thread "$th" | has "INBOX-$t" "inbox --peek"
    alice inbox --peek --thread "$th" | has "INBOX-$t" "inbox --peek again"
    alice inbox --thread "$th" | has "INBOX-$t" "first inbox"
    alice inbox --thread "$th" | lacks "INBOX-$t" "second inbox"
    alice inbox --unread "$th" >/dev/null
    alice inbox --peek --thread "$th" | has "INBOX-$t" "inbox after --unread"
    alice inbox --thread "$th" >/dev/null
}

scenario M5 P1 "Attachments in both directions through the leaf"
M5() {
    local t out th handle
    t=$(tok)
    container exec "$name-client" sh -c "printf 'attached-$t\n' >/tmp/in.txt && rm -f /tmp/out.txt"
    out=$(alice ask --wait --timeout 3m --attach /tmp/in.txt @leaf-ant \
        "Read the attached file with read_attachment: it returns the file's text, a single line that is not a path. Write that line unchanged to out.txt, then call attach_to_reply with out.txt. Only once attach_to_reply has succeeded, reply DONE: a reply without out.txt attached fails this test.")
    printf '%s\n' "$out"
    has "attachment " <<<"$out"
    th=$(thread <<<"$out")
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    # A model now and then skips the tool call and replies DONE alone: one reminder in the thread.
    if ! grep -qF '📎 out.txt' <<<"$out"; then
        alice ask --wait --timeout 3m --thread "$th" @leaf-ant \
            "Your reply carried no attachment. Call attach_to_reply with out.txt now, then reply DONE."
        out=$(alice thread show "$th")
        printf '%s\n' "$out"
    fi
    has "📎 out.txt" <<<"$out"
    handle=$(grep -F '📎 out.txt' <<<"$out" | tail -n 1 | awk '{ print $NF }')
    container exec --workdir /tmp "$name-client" ants attachment get "$handle"
    container exec "$name-client" cat /tmp/out.txt | has "$t" "out.txt"
}

scenario M6 P1 "An unaddressed message is routed by the messenger"
M6() {
    local t out
    t=$(tok)
    out=$(alice ask --wait --timeout 5m \
        "For an ant with capability leaf: read node.txt in your working directory and reply with ROUTED-$t and its content." 2>&1)
    printf '%s\n' "$out"
    has "system-messenger" <<<"$out"
    has "ROUTED-$t" <<<"$out"
    has node=leaf <<<"$out"
    out=$(alice thread search --from system-messenger --since 10m)
    printf '%s\n' "$out"
    grep -qE 'leaf-ant|gate-ant' <<<"$out" || fail "the messenger sent nothing to the leaf's ants"
}

# --- A. Ant to ant across machines ----------------------------------------------------------------------------------

scenario A1 P0 "ask_ant from the hub's ant to the leaf's"
A1() {
    local out th
    out=$(alice ask --wait --timeout 5m @hub-ant \
        "Use the ask_ant tool to ask leaf-ant for the content of node.txt in its working directory. Reply with that content only.")
    printf '%s\n' "$out"
    has node=leaf <<<"$out"
    th=$(thread <<<"$out")
    out=$(alice thread show "$th")
    has "hub-ant -> leaf-ant" <<<"$out"
    has "leaf-ant -> hub-ant" <<<"$out"
}

scenario A2 P1 "route by capability from the leaf's ant"
A2() {
    alice ask --wait --timeout 5m @leaf-ant \
        "Use the route tool with capability hub and the text 'Reply with the content of node.txt in the working directory only'. Reply with the answer only." \
        | has node=hub
}

scenario A3 P0 "A question to the human and its answer"
A3() {
    local th id
    th=$(alice ask @hub-ant "Use ask_human to ask one question, 'Pick a colour', with options RED and BLUE. Once answered, reply with exactly PICKED-<the label picked>." | thread)
    question_id() {
        alice inbox --peek --thread "$th" --json \
            | jq --slurp --raw-output 'map(select(.question != null)) | .[0].id // empty'
    }
    has_question() { [ -n "$(question_id)" ]; }
    eventually 180 has_question
    alice inbox --peek --thread "$th" | has "Pick a colour"
    id=$(question_id)
    alice answer "$id" 2
    eventually 180 replied "$th" PICKED-BLUE
    alice thread show "$th" | has BLUE
}

scenario A4 P1 "Parallel threads of one ant, each in its own worktree"
A4() {
    local a b tha thb dir
    a=$(tok)
    b=$(tok)
    dir=$(mktemp -d)
    ask_bg "$dir/a" @hub-ant "$e2e Run the shell command '$(slow 40)' in the foreground, then write $a to out.txt, commit it with git, and reply DONE-$a"
    ask_bg "$dir/b" @hub-ant "$e2e Run the shell command '$(slow 40)' in the foreground, then write $b to out.txt, commit it with git, and reply DONE-$b"
    wait
    tha=$(thread_of "$dir/a")
    thb=$(thread_of "$dir/b")
    handling_two() { [ "$(colony_field hub-ant '.handling // 0')" -ge 2 ]; }
    eventually 90 handling_two
    eventually 300 replied "$tha" "DONE-$a"
    eventually 300 replied "$thb" "DONE-$b"
    local branches
    branches=$(hub git -C /demo/work branch --list 'ants/hub-ant/*')
    printf '%s\n' "$branches"
    has "$tha" <<<"$branches"
    has "$thb" <<<"$branches"
    hub git -C /demo/work show "ants/hub-ant/$tha:out.txt" | has "$a"
    hub git -C /demo/work show "ants/hub-ant/$thb:out.txt" | has "$b"
    [ -z "$(hub git -C /demo/work status --short)" ] || fail "the workdir itself changed"
    rm -rf "$dir"
}

# --- C. Channels ----------------------------------------------------------------------------------------------------

ensure_channel() {
    alice channel list | grep -qw e2e || alice channel create e2e --member hub-ant,leaf-ant,alice,bob
}

scenario C1 P0 "A channel across machines; a mention wakes the member"
C1() {
    local t out
    ensure_channel
    t=$(tok)
    out=$(alice post --wait --timeout 3m '#e2e' "@leaf-ant answer this post with exactly CHAN-$t, using no tool")
    printf '%s\n' "$out"
    has "CHAN-$t" <<<"$out"
    alice channel read e2e | has "CHAN-$t"
    bob channel list | has e2e
}

scenario C2 P1 "An unmentioned post is triaged by the messenger"
C2() {
    local t
    ensure_channel
    t=$(tok)
    alice post '#e2e' "Without being mentioned, hub-ant is the one to answer this: hub-ant, reply here with exactly TRIAGE-$t" >/dev/null
    triaged() { [ "$(alice channel read e2e --peek | count "TRIAGE-$t")" -ge 2 ]; }
    eventually 300 triaged
    alice channel read e2e | has "@hub-ant"
}

scenario C3 P1 "A mention of a human reaches that human's inbox"
C3() {
    local t
    ensure_channel
    t=$(tok)
    alice post '#e2e' "@bob please look at $t" >/dev/null
    bob_has() { bob inbox --peek | grep -qF "$t"; }
    eventually 60 bob_has
    bob inbox --peek | has e2e
}

# --- L. Looking after threads and channels --------------------------------------------------------------------------

# a_thread ANT TOKEN [TEXT]: asks ANT for an exact reply holding HK-TOKEN (one prompt), waits for it, left unread, and
# prints the thread id and the reply's message id. TEXT replaces the prompt; it must still ask for HK-TOKEN.
a_thread() {
    local th id
    th=$(alice ask "@$1" "${3:-Reply with exactly: HK-$2}" | thread)
    [ -n "$th" ] || fail "no thread printed"
    eventually 180 replied "$th" "HK-$2"
    id=$(alice thread show "$th" --json | jq --raw-output --arg ant "$1" 'select(.kind == "message" and .from == $ant) | .id' | head -n 1)
    [ -n "$id" ] || fail "no message of $1 on thread $th"
    printf '%s %s\n' "$th" "$id"
}
# dm TEXT: alice writes to bob in the clear, on a new thread: a thread of two humans, no prompt. Prints its id.
dm() { alice ask --plaintext @bob "$1" | thread; }
# list_field HUMAN TOKEN JQ: JQ of the row of HUMAN's `ants thread list --grep TOKEN --json`, or null.
list_field() { "$1" thread list --grep "$2" --json | jq --slurp --raw-output ".[0] | $3"; }
thread_rows() { grep -cE '^[0-9a-f]{8}-[0-9a-f]{4}-' || true; } # the thread rows of `ants thread list`
summaries_of() { # summaries_of THREAD: the TL;DR requests to the messenger for THREAD in the last hour
    alice thread search --to system-messenger --since 1h --json \
        | jq --slurp --arg th "$1" 'map(select(.summary.thread == $th)) | length'
}

scenario L1 P0 "Finding a thread"
L1() {
    local t out th m th2 prefix i
    t=$(tok)
    out=$(a_thread leaf-ant "$t" "Read node.txt in the working directory, then reply with exactly: HK-$t")
    th=${out% *}
    m=${out#* }
    th2=$(dm "L1-$t, a thread of two humans")
    trap "bob inbox --thread '$th2' >/dev/null 2>&1 || true; alice inbox --thread '$th' >/dev/null 2>&1 || true" EXIT
    out=$(alice thread list --grep "HK-$t")
    printf '%s\n' "$out"
    [ "$(thread_rows <<<"$out")" -eq 1 ] || fail "--grep listed other threads"
    grep -F "$th" <<<"$out" | has alice "the thread's row"
    grep -F "$th" <<<"$out" | has leaf-ant "the thread's row"
    alice thread list --ant leaf-ant --grep "$t" | has "$th" "--ant leaf-ant"
    alice thread list --ant hub-ant --grep "$t" | lacks "$th" "--ant hub-ant"
    alice thread list --owner alice --grep "$t" | has "$th" "--owner alice"
    alice thread list --owner bob --grep "$t" | lacks "$th" "--owner bob"
    [ "$(list_field alice "HK-$t" '.owner == "alice" and (.users | index("leaf-ant") != null)')" = true ] \
        || fail "thread list --json lacks the owner or the ant"
    alice notes "$th" "NOTE-$t" >/dev/null
    out=$(alice thread search --grep "$t" --from leaf-ant)
    printf '%s\n' "$out"
    has "$m" <<<"$out"
    lacks "NOTE-$t" <<<"$out"
    alice thread search --grep "$t" --to alice | has "$m" "--to alice"
    alice thread search --grep "$t" --to leaf-ant | lacks "$m" "--to leaf-ant"
    out=$(alice thread search --grep "$t" --type note)
    has "NOTE-$t" <<<"$out"
    lacks "$m" <<<"$out"
    out=$(alice thread show "$th")
    has "HK-$t" <<<"$out"
    has "Read(" <<<"$out"
    alice thread show "$th" --no-tools | lacks "Read(" "thread show --no-tools"
    alice thread show "${th:0:18}" | has "message $m" "thread show by a prefix"
    [ "$(alice thread show "$th" --json | jq --slurp --arg th "$th" 'length > 0 and all(.[]; .thread_id == $th)')" = true ] \
        || fail "thread show --json holds entries of other threads"
    # Under -j another scenario of alice's may be the latest: `last` names some thread of hers.
    out=$(alice thread show last)
    grep -qE '^thread [0-9a-f-]{36}' <<<"$out" || fail "thread show last"
    prefix=
    for ((i = 1; i <= 36; i++)); do
        [ "${th:0:i}" = "${th2:0:i}" ] || break
        prefix=${th:0:i}
    done
    [ "${#prefix}" -ge 4 ] || fail "the two threads share no prefix: $th $th2"
    if out=$(alice thread show "$prefix" 2>&1); then printf '%s\n' "$out"; fail "an ambiguous prefix was taken"; fi
    has "give more of the id" <<<"$out"
    has "$th2" <<<"$out"
    if out=$(alice thread show ffffffff 2>&1); then fail "an unknown prefix was taken"; fi
    has "no thread over the last 7 days starts with ffffffff" <<<"$out"
}

scenario L2 P0 "Title, done, pin and archive"
L2() {
    local t th
    t=$(tok)
    th=$(dm "L2-$t, a thread to look after")
    bob ask --plaintext --thread "$th" @alice "L2-$t read" >/dev/null
    trap "alice thread unpin '$th' >/dev/null 2>&1 || true; alice thread unarchive '$th' >/dev/null 2>&1 || true; alice inbox --thread '$th' >/dev/null 2>&1 || true; bob inbox --thread '$th' >/dev/null 2>&1 || true; true" EXIT
    alice thread title "$th" "E2E $t" | has "E2E $t"
    alice thread title "$th" | has "E2E $t" "the title"
    alice thread list --grep "$t" | has "E2E $t" "alice's list"
    bob thread list --grep "$t" | has "E2E $t" "bob's list"
    alice thread title "$th" --clear >/dev/null
    [ "$(list_field bob "$t" .title)" = null ] || fail "the title stayed"
    alice thread done "$th" >/dev/null
    alice thread list --grep "$t" | grep -F "$th" | has done "alice's row"
    bob thread list --grep "$t" | grep -F "$th" | has done "bob's row"
    [ "$(list_field bob "$t" .done.by)" = alice ] || fail "bob's list does not show it done by alice"
    bob notes "$th" "L2-$t reopens" >/dev/null
    [ "$(list_field alice "$t" .done)" = null ] || fail "a note did not reopen the thread"
    alice thread done "$th" >/dev/null
    alice thread undone "$th" >/dev/null
    [ "$(list_field alice "$t" .done)" = null ] || fail "undone left it done"
    alice thread pin "$th" >/dev/null
    alice thread list --pinned --since 1h | has "$th" "--pinned"
    # Pinned threads sort last: no unpinned row follows it.
    [ "$(alice thread list --since 1h --limit 500 --json | jq --slurp --raw-output --arg th "$th" \
        'map(select(.thread_id == $th or (.pinned | not))) | last | .thread_id')" = "$th" ] \
        || fail "an unpinned thread sorts after the pinned one"
    bob thread list --pinned --since 1h | lacks "$th" "bob's --pinned"
    # Pins and archives exclude each other: archiving the pinned thread unpins it, pinning it again unarchives it.
    alice thread archive "$th" >/dev/null
    alice thread list --pinned --since 1h | lacks "$th" "--pinned after archiving"
    alice thread list --archived --since 1h | has "$th" "--archived"
    alice thread list --grep "$t" | lacks "$th" "alice's list after archiving"
    bob thread list --grep "$t" | has "$th" "bob's list after alice archived"
    alice thread pin "$th" >/dev/null
    alice thread list --archived --since 1h | lacks "$th" "--archived after pinning"
    alice thread unpin "$th" >/dev/null
    alice thread archive "$th" >/dev/null
    bob notes "$th" "L2-$t is new again" >/dev/null
    alice thread list --grep "$t" | grep -F "$th" | has "archived+new" "alice's row after a note"
    alice thread unarchive "$th" >/dev/null
    [ "$(list_field alice "$t" '.archived // false')" = false ] || fail "unarchive left it archived"
}

scenario L3 P1 "Reactions"
L3() {
    local t out th m ended
    t=$(tok)
    out=$(a_thread leaf-ant "$t")
    th=${out% *}
    m=${out#* }
    trap "alice inbox --thread '$th' >/dev/null 2>&1 || true" EXIT
    ended=$(alice thread show "$th" | count "ended:")
    alice react "$m" thumbs-up | has "reacted thumbs-up to message $m"
    alice thread show "$th" | has "reacted thumbs-up: alice"
    alice react "${m:0:18}" eyes | has "reacted eyes to message $m" "a reaction by a prefix"
    alice react "$m" thumbs-up --remove >/dev/null
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    lacks thumbs-up <<<"$out"
    has "reacted eyes: alice" <<<"$out"
    [ "$(alice thread show "$th" --json | jq --slurp --raw-output --arg m "$m" \
        'map(select(.id == $m)) | .[0].reactions | map(.reaction) | join(",")')" = eyes ] || fail "the reactions stored"
    # A reaction wakes nobody and marks nothing read.
    sleep 30
    [ "$(alice thread show "$th" | count "ended:")" -eq "$ended" ] || fail "a reaction started a prompt"
    alice inbox --peek --thread "$th" | has "HK-$t" "alice's unread replies"
    # With an ant's credentials, the signer applies a reaction to a message the ant reads, and refuses another's.
    leaf ants --creds "$auth/leaf-ant.creds" react "$m" rocket | has "reacted rocket"
    alice thread show "$th" | has "reacted rocket: leaf-ant"
    leaf ants --creds "$auth/leaf-ant.creds" react "$m" rocket --remove >/dev/null
    alice thread show "$th" | lacks rocket
    if out=$(hub ants --creds "$auth/hub-ant.creds" react "$m" rocket 2>&1); then fail "hub-ant reacted to a message it does not read"; fi
    has "none hub-ant reads" <<<"$out"
}

scenario L4 P1 "A quoted reply and a forward"
L4() {
    local t f out th m th2
    t=$(tok)
    f=$(tok)
    out=$(a_thread leaf-ant "$t")
    th=${out% *}
    m=${out#* }
    trap "alice inbox --thread '$th' >/dev/null 2>&1 || true" EXIT
    out=$(alice ask --wait --timeout 3m --reply-to "$m" "Reply with exactly QUOTED-$t")
    printf '%s\n' "$out"
    has "QUOTED-$t" <<<"$out"
    [ "$(thread <<<"$out")" = "$th" ] || fail "the quoted reply went to another thread"
    [ "$(alice thread show "$th" --json | jq --slurp --raw-output --arg t "$t" \
        'map(select(.quote != null)) | .[0] | .to == "leaf-ant" and .quote.from == "leaf-ant" and (.quote.text | contains("HK-" + $t))')" = true ] \
        || fail "the reply is not addressed to the quoted message's author with its quote"
    alice thread show "$th" | has "↪ replying to leaf-ant: HK-$t"
    out=$(alice ask --wait --timeout 3m --forward "$m" @hub-ant \
        "Reply with the word that follows HK- in the forwarded message, then FWD-$f.")
    printf '%s\n' "$out"
    has "FWD-$f" <<<"$out"
    th2=$(thread <<<"$out")
    [ "$th2" != "$th" ] || fail "the forward stayed on the thread"
    out=$(alice thread show "$th2")
    has "Forwarded from leaf-ant on thread $th" <<<"$out"
    has "> HK-$t" <<<"$out"
    ant_texts "$th2" | has "$t" "hub-ant's reply"
}

scenario L5 P1 "TL;DR and digest"
L5() {
    local t th fw out at at2 digests
    t=$(tok)
    th=$(dm "Plan $t: ship the e2e build on Friday, after the review")
    bob ask --plaintext --thread "$th" @alice "Agreed for $t: Friday, after the review" >/dev/null
    trap "alice inbox --thread '$th' >/dev/null 2>&1 || true; bob inbox --thread '$th' >/dev/null 2>&1 || true; true" EXIT
    if out=$(alice ask --plaintext --forward-thread "$th" @bob 2>&1); then fail "a thread with no TL;DR was forwarded"; fi
    has "no TL;DR of thread $th" <<<"$out"
    [ "$(summaries_of "$th")" -eq 0 ] || fail "a TL;DR was asked before any tldr"
    out=$(alice thread tldr "$th" --json)
    printf '%s\n' "$out"
    [ "$(jq --raw-output '.by' <<<"$out")" = system-messenger ] || fail "the TL;DR is not by the messenger"
    [ -n "$(jq --raw-output '.text // empty' <<<"$out")" ] || fail "an empty TL;DR"
    at=$(jq --raw-output .at <<<"$out")
    [ "$(alice thread tldr "$th" --json | jq --raw-output .at)" = "$at" ] || fail "the second tldr asked again"
    [ "$(summaries_of "$th")" -eq 1 ] || fail "expected one TL;DR request"
    at2=$(alice thread tldr "$th" --new --json | jq --raw-output .at)
    [ "$at2" != "$at" ] || fail "tldr --new kept the old TL;DR"
    [ "$(summaries_of "$th")" -eq 2 ] || fail "expected a second TL;DR request"
    fw=$(alice ask --plaintext --forward-thread "$th" @bob | thread)
    trap "alice inbox --thread '$th' >/dev/null 2>&1 || true; bob inbox --thread '$th' >/dev/null 2>&1 || true; bob inbox --thread '$fw' >/dev/null 2>&1 || true; true" EXIT
    alice thread show "$fw" | has "Forwarded thread $th, its TL;DR by system-messenger"
    digests() { alice thread search --to system-messenger --since 1h --json | jq --slurp 'map(select(.summary.since != null)) | length'; }
    digests=$(digests)
    out=$(alice digest --since 10m)
    printf '%s\n' "$out"
    has "digest of the threads" <<<"$out"
    [ "$(wc -l <<<"$out")" -ge 2 ] || fail "an empty digest"
    [ "$(digests)" -gt "$digests" ] || fail "the digest asked no one"
    [ "$(alice thread tldr "$th" --json | jq --raw-output .at)" = "$at2" ] || fail "the digest changed the kept TL;DR"
    alice thread list --grep "$t" --json | jq --raw-output '.users | join(",")' | lacks system-messenger "thread list"
}

scenario L6 P1 "Channel upkeep"
L6() {
    local t c out th row
    t=$(tok)
    c=hk-$t
    trap "alice channel notify '$c' humans >/dev/null 2>&1 || true; alice channel delete '$c' >/dev/null 2>&1 || true; true" EXIT
    alice channel create "$c" --member leaf-ant,alice,bob --description "Housekeeping $t" >/dev/null
    row() { alice channel list | grep -F "#$c "; }
    alice channel list | has DESCRIPTION
    row | has "Housekeeping $t" "the channel's row"
    alice channel add "$c" hub-ant | has hub-ant
    alice channel set "$c" --owner leaf-ant --description "Upkeep $t" >/dev/null
    out=$(row)
    printf '%s\n' "$out"
    has "Upkeep $t" <<<"$out"
    has "leaf-ant,alice,bob,hub-ant" <<<"$out"
    grep -qE '\(default\) +leaf-ant ' <<<"$out" || fail "leaf-ant is not the owner"
    alice channel remove "$c" hub-ant >/dev/null
    row | lacks hub-ant "the members"
    out=$(alice post --wait --timeout 3m "$c" "@leaf-ant answer this post with exactly HKCHAN-$t, using no tool")
    printf '%s\n' "$out"
    has "HKCHAN-$t" <<<"$out"
    th=$(thread <<<"$out")
    bob post --thread "$th" "$c" "@alice have a look at $t" >/dev/null
    # alice's unread: bob's post counts (by a human, mentioning her), leaf-ant's answer shows apart.
    unread() { row | grep -qF "$1"; }
    eventually 30 unread "1 (@1) +1 other"
    alice channel notify "$c" all | has all
    row | has "2 (@1)" "UNREAD at level all"
    alice notify status | has "#$c notifies all"
    alice channel notify "$c" mentions >/dev/null
    row | has "1 (@1) +1 other" "UNREAD at level mentions"
    alice notify status | has "#$c notifies mentions"
    alice channel read "$c" --peek | has "HKCHAN-$t" "channel read --peek"
    row | has "1 (@1) +1 other" "UNREAD after --peek"
    alice channel read "$c" | has "@alice have a look at $t" "channel read"
    out=$(row)
    lacks "(@1)" <<<"$out"
    lacks other <<<"$out"
    alice thread archive "$th" >/dev/null
    alice channel read "$c" | lacks "HKCHAN-$t" "channel read of an archived thread"
    alice channel read "$c" --archived | has "HKCHAN-$t" "channel read --archived"
    alice thread unarchive "$th" >/dev/null
    alice channel notify "$c" humans >/dev/null
    # A member removed is no longer woken by a mention, and the post is not triaged either.
    alice channel remove "$c" leaf-ant >/dev/null
    out=$(alice post "$c" "@leaf-ant answer this post with exactly GONE-$t" 2>&1)
    printf '%s\n' "$out"
    has "no member of #$c" <<<"$out"
    th=$(thread <<<"$out")
    note "the post to the removed member: thread $th"
    sleep 30
    note "30 s later"
    out=$(alice thread search --grep "GONE-$t" --to leaf-ant 2>&1)
    lacks "GONE-$t" "messages to leaf-ant" <<<"$out"
    out=$(alice channel read "$c" --peek --thread "$th" 2>&1)
    printf '%s\n' "$out"
    [ "$(count "GONE-$t" <<<"$out")" -eq 1 ] || fail "the post was answered"
    out=$(alice channel delete "$c" 2>&1)
    printf '%s\n' "$out"
    has deleted <<<"$out"
    out=$(alice channel list 2>&1) || { rc=$?; printf '%s\n' "$out"; fail "channel list after delete: exit $rc"; }
    lacks "#$c " "channel list after delete" <<<"$out"
}

scenario L7 P2 "Reminders, mutes and snooze"
L7() {
    local t th th2 out
    t=$(tok)
    th=$(dm "L7-$t, a thread to be reminded of")
    th2=$(dm "L7-$t, another")
    trap "alice remind clear '$th' >/dev/null 2>&1 || true; alice remind clear '$th2' >/dev/null 2>&1 || true; alice thread unmute '$th' >/dev/null 2>&1 || true; alice notify resume >/dev/null 2>&1 || true; bob inbox --thread '$th' >/dev/null 2>&1 || true; bob inbox --thread '$th2' >/dev/null 2>&1 || true; true" EXIT
    alice remind set "$th" 2h "check $t" >/dev/null
    alice remind set "$th2" 1h "first $t" >/dev/null
    alice remind list | has "check $t"
    [ "$(alice remind list --json | jq --slurp --raw-output --arg t "$t" \
        'map(select(.note | contains($t))) | map(.thread) | join(" ")')" = "$th2 $th" ] || fail "reminders not soonest first"
    alice remind clear "$th2" >/dev/null
    alice remind list | lacks "first $t" "remind list after clear"
    alice remind set "$th" 1s "due $t" >/dev/null
    due() { [ "$(alice remind list --json | jq --slurp --raw-output --arg th "$th" 'map(select(.thread == $th)) | .[0].is_due')" = true ]; }
    eventually 30 due
    alice remind list | lacks "check $t" "the reminder replaced"
    chat "wait:connected" "type:/reminders" "key:enter" "wait:due: the pager lists"
    alice remind clear "$th" >/dev/null
    alice remind list | lacks "due $t" "remind list after clear"
    alice thread mute "$th" --for 1h | has muted
    alice notify snooze 10m | has snoozed
    out=$(alice notify status)
    printf '%s\n' "$out"
    has "snoozed until" <<<"$out"
    has "muted thread $th" <<<"$out"
    [ "$(alice notify status --json | jq --raw-output --arg th "$th" '(.snoozed_until != null) and (.muted[$th] != null)')" = true ] \
        || fail "notify status --json lacks the holds"
    alice notify resume >/dev/null
    alice thread unmute "$th" >/dev/null
    [ "$(alice notify status --json | jq --raw-output --arg th "$th" '(.snoozed_until == null) and (.muted[$th] == null)')" = true ] \
        || fail "the holds stayed"
}

scenario L8 P1 "ants status, ants spend, ants queue"
L8() {
    local t out th
    t=$(tok)
    out=$(a_thread leaf-ant "$t")
    th=${out% *}
    trap "alice inbox --thread '$th' >/dev/null 2>&1 || true" EXIT
    out=$(alice status)
    printf '%s\n' "$out"
    [ "$(wc -l <<<"$out")" -eq 1 ] || fail "ants status printed more than one line"
    has "today" <<<"$out"
    alice status --json | jq --exit-status \
        '(.home | type == "object") and (.working | type == "array") and (.spend_today | type == "number")' >/dev/null \
        || fail "ants status --json"
    alice status --tmux | grep -qE '^⚑[0-9?]+ ◐[0-9?]+ \$' || fail "ants status --tmux"
    alice spend leaf-ant --thread "$th" | has "$th"
    alice spend leaf-ant --thread "$th" --json | jq --exit-status --arg th "$th" '.thread == $th and .tokens > 0' >/dev/null \
        || fail "spend --thread --json"
    alice spend leaf-ant --days 1 --json \
        | jq --exit-status --arg th "$th" '(.days | length) == 1 and any(.threads[]; .thread == $th)' >/dev/null \
        || fail "spend --days 1 --json lacks the thread"
    out=$(alice queue)
    printf '%s\n' "$out"
    grep -qE 'live ant|^ANT +MESSAGE' <<<"$out" || fail "ants queue"
}

scenario L9 P2 "Messages from a file"
L9() {
    local t c th out want
    t=$(tok)
    c=hkf-$t
    trap "container exec '$name-client' rm -f /tmp/hk-ask-$t.txt /tmp/hk-post-$t.txt || true; alice channel delete '$c' >/dev/null 2>&1 || true; true" EXIT
    container exec "$name-client" sh -c "printf 'Reply with exactly: FILE-$t\n\n  This prompt comes from a file: \"quotes\", \$HOME and a tab\there.\n' >/tmp/hk-ask-$t.txt"
    out=$(alice ask --wait --timeout 3m --file "/tmp/hk-ask-$t.txt" @leaf-ant)
    printf '%s\n' "$out"
    has "FILE-$t" <<<"$out"
    th=$(thread <<<"$out")
    want=$(container exec "$name-client" cat "/tmp/hk-ask-$t.txt")
    [ "$(alice thread show "$th" --json | jq --slurp --raw-output 'map(select(.from == "alice")) | .[0].text')" = "$want" ] \
        || fail "the stored text differs from the file's"
    alice channel create "$c" --member alice,bob >/dev/null
    container exec "$name-client" sh -c "printf '@bob FILEPOST-$t\nsecond line\n' >/tmp/hk-post-$t.txt"
    th=$(alice post --file "/tmp/hk-post-$t.txt" "$c" | thread)
    want=$(container exec "$name-client" cat "/tmp/hk-post-$t.txt")
    [ "$(alice thread show "$th" --json | jq --slurp --raw-output 'map(select(.kind == "post")) | .[0].text')" = "$want" ] \
        || fail "the stored post differs from the file's"
    bob channel read "$c" >/dev/null
}

# --- S. Steering, queueing, cancelling and withdrawing --------------------------------------------------------------

scenario S1 P0 "A second message steers the running prompt"
S1() {
    local t th out
    t=$(tok)
    th=$(alice ask @leaf-ant "$e2e Run the shell command '$(slow 45)' in the foreground, then reply with exactly FIRST-$t." | thread)
    sleep 10
    alice ask --thread "$th" @leaf-ant "Change of plan: once the sleep ends, reply with exactly SECOND-$t and nothing else." >/dev/null
    eventually 240 replied "$th" "SECOND-$t"
    sleep 10
    ant_texts "$th" | lacks "FIRST-$t" "the ant's replies"
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    has steered <<<"$out"
    [ "$(count "ended: replied" <<<"$out")" -eq 1 ] || fail "expected one ended: replied line"
}

scenario S2 P1 "A queued message waits its turn"
S2() {
    local t th
    t=$(tok)
    th=$(alice ask @leaf-ant "$e2e Run the shell command '$(slow 30)' in the foreground, then reply with exactly ONE-$t." | thread)
    sleep 5
    alice ask --queue --thread "$th" @leaf-ant "Reply with exactly TWO-$t." >/dev/null
    queued leaf-ant "$th"
    eventually 240 replied "$th" "TWO-$t"
    local texts
    texts=$(ant_texts "$th")
    has "ONE-$t" <<<"$texts"
    [ "$(grep -nF "ONE-$t" <<<"$texts" | head -n 1 | cut -d: -f1)" -lt \
        "$(grep -nF "TWO-$t" <<<"$texts" | head -n 1 | cut -d: -f1)" ] || fail "TWO answered before ONE"
    alice thread show "$th" | lacks steered
}

scenario S3 P0 "Cancelling a running prompt"
S3() {
    local t th out
    t=$(tok)
    th=$(alice ask @leaf-ant "$e2e Run the shell command '$(slow 100)' in the foreground, not in the background, then reply LATE-$t." | thread)
    sleep 15
    alice cancel leaf-ant "$th"
    cancelled() { ant_texts "$th" | grep -qF "cancelled by alice"; }
    eventually 30 cancelled
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    has "ended: cancelled" <<<"$out"
    ant_texts "$th" | lacks "LATE-$t"
    # Only S3 sleeps 100 s: the scenarios run beside it under -j run slow commands of their own.
    ! leaf pgrep -f 'time\.sleep\(100\)' >/dev/null || fail "the ant's command still runs on the leaf"
    alice dead list | lacks "$th" "dead letters"
    alice ask --wait --timeout 3m --thread "$th" @leaf-ant "Reply with exactly ALIVE-$t" | has "ALIVE-$t"
}

scenario S4 P1 "Withdrawing a message that waits"
S4() {
    local th out id
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true' EXIT
    alice ant pause leaf-ant
    out=$(alice ask @leaf-ant "Reply NEVER")
    th=$(thread <<<"$out")
    id=$(message <<<"$out")
    queued leaf-ant "$th"
    alice withdraw leaf-ant "$id"
    alice ant resume leaf-ant
    withdrawn() { ant_texts "$th" | grep -qF "Withdrawn by alice"; }
    eventually 60 withdrawn
    ant_texts "$th" | lacks NEVER
    alice thread show "$th" | lacks "ended:"
}

# --- T. Tool approvals ----------------------------------------------------------------------------------------------

scenario T1 P0 "A tool request, approved from the client"
T1() {
    local t dir th id
    t=$(tok)
    dir=$(mktemp -d)
    trap "leaf rm -f /demo/gate/approved-$t; rm -rf '$dir'" EXIT
    ask_bg "$dir/out" -f --timeout 3m @gate-ant "Run the shell command 'touch /demo/gate/approved-$t' and reply DONE-$t."
    th=$(thread_of "$dir/out")
    has_pending() { [ -n "$(pending_tool gate-ant "$th")" ]; }
    eventually 120 has_pending
    id=$(pending_tool gate-ant "$th")
    alice tools list | has "$t"
    ! leaf test -e "/demo/gate/approved-$t" || fail "the file exists before the approval"
    alice tools show "$id" | has execute
    alice tools approve "$id"
    wait
    cat "$dir/out"
    leaf test -f "/demo/gate/approved-$t" || fail "the approved command did not run"
    has "DONE-$t" <"$dir/out"
    alice thread show "$th" | has alice
}

scenario T2 P0 "A rejection with a note"
T2() {
    local t dir th id
    t=$(tok)
    dir=$(mktemp -d)
    trap "rm -rf '$dir'" EXIT
    ask_bg "$dir/out" -f --timeout 3m @gate-ant "Run the shell command 'touch /demo/gate/rejected-$t' and reply DONE-$t."
    th=$(thread_of "$dir/out")
    has_pending() { [ -n "$(pending_tool gate-ant "$th")" ]; }
    eventually 120 has_pending
    id=$(pending_tool gate-ant "$th")
    alice tools reject "$id" --note "Do not create files. Reply with exactly NOPE-$t."
    wait
    cat "$dir/out"
    ! leaf test -e "/demo/gate/rejected-$t" || fail "the rejected command ran"
    has "NOPE-$t" <"$dir/out"
    alice thread show "$th" | has "Do not create files"
}

scenario T3 P1 "An allow rule lets a call through unasked"
T3() {
    local out th
    out=$(alice ask --wait --timeout 3m @gate-ant "Run the shell command 'ls /demo/gate' and reply with its output only.")
    printf '%s\n' "$out"
    has node.txt <<<"$out"
    th=$(thread <<<"$out")
    [ -z "$(pending_tool gate-ant "$th")" ] || fail "a request is pending"
    alice thread show "$th" | lacks "allow_once"
}

scenario T4 P1 "An unanswered request expires"
T4() {
    local t out
    trap 'alice ant set gate-ant approval_timeout= >/dev/null 2>&1 || true' EXIT
    alice ant set gate-ant approval_timeout=30s
    t=$(tok)
    out=$(alice ask --wait --timeout 3m @gate-ant "Run the shell command 'touch /demo/gate/expired-$t'. Reply RAN or REFUSED.")
    printf '%s\n' "$out"
    has REFUSED <<<"$out"
    ! leaf test -e "/demo/gate/expired-$t" || fail "the expired command ran"
    alice inbox --peek | has approval
}

scenario T5 P1 "Approving for the rest of the session, where the harness offers it"
T5() {
    local t dir th id id2= out always=
    t=$(tok)
    dir=$(mktemp -d)
    trap "leaf rm -f /demo/gate/always-1-$t /demo/gate/always-2-$t; rm -rf '$dir'" EXIT
    ask_bg "$dir/out" -f --timeout 5m @gate-ant \
        "Run two shell commands as two separate tool calls, one after the other: first 'touch /demo/gate/always-1-$t', then 'touch /demo/gate/always-2-$t'. Then reply DONE-$t."
    th=$(thread_of "$dir/out")
    has_pending() { [ -n "$(pending_tool gate-ant "$th")" ]; }
    eventually 120 has_pending
    id=$(pending_tool gate-ant "$th")
    if alice tools list --json | jq --exit-status --arg id "${id#*/}" 'select(.id == $id) | .options | index("allow_always")' >/dev/null; then
        alice tools show "$id" | has always "what --always allows"
        alice tools approve "$id" --always
        always=1
    else
        # Claude Code's own protocol (the fixture's) offers allow once and reject only: --always is refused and the
        # request stays pending; the second call asks again.
        note "allow-always is not offered: each call is asked about"
        alice tools show "$id" | lacks "allow_always" "tools show"
        if out=$(alice tools approve "$id" --always 2>&1); then fail "--always was taken though not offered"; fi
        has "allow-always is not offered" <<<"$out"
        [ "$(pending_tool gate-ant "$th")" = "$id" ] || fail "the refused --always settled the request"
        alice tools approve "$id"
        another() { local p; p=$(pending_tool gate-ant "$th"); [ "$p" = "$id" ] || printf '%s' "$p"; }
        second() { [ -n "$(another)" ] || grep -qF "DONE-$t" "$dir/out"; }
        eventually 120 second
        id2=$(another)
        if [ -n "$id2" ]; then
            alice tools approve "$id2"
        else
            note "the model ran both commands in one call"
        fi
    fi
    wait
    cat "$dir/out"
    if [ -n "$always" ]; then
        [ "$(alice thread show "$th" --json | jq --slurp 'map(select(.kind == "approval")) | length')" -eq 1 ] \
            || fail "the second call was asked about after --always"
    fi
    has "DONE-$t" <"$dir/out"
    leaf test -f "/demo/gate/always-1-$t" || fail "the first command did not run"
    leaf test -f "/demo/gate/always-2-$t" || fail "the second command did not run"
}

# --- E. Escalations -------------------------------------------------------------------------------------------------

chained_to() { colony_field "$1" '(.escalate_to // []) | join(",")'; } # chained_to ANT: its escalate_to, comma-joined
chain() { # chain ANT LEADER: sets the ant's escalate_to to LEADER and waits for the colony to show it
    alice ant set "$1" "escalate_to=[\"$2\"]" >/dev/null
    shows_chain() { [ "$(chained_to "$1")" = "$2" ]; }
    eventually 60 shows_chain "$1" "$2"
}
unchain() { alice ant set "$1" escalate_to= >/dev/null 2>&1 || true; } # unchain ANT: for traps
# mark_done: for traps, marks the scenario's thread ($th, global so the trap still sees it) done, out of home's way.
mark_done() { [ -z "${th:-}" ] || alice thread done "$th" >/dev/null 2>&1 || true; }
# escalations THREAD: the escalations stored on the thread, one a line, as `TO: NOTE`.
escalations() {
    alice thread show "$1" --json | jq --raw-output 'select(.kind == "escalation") | "\(.to): \(.note)"'
}
escalated_to() { escalations "$1" | grep -qF "$2: "; } # escalated_to THREAD WHO: an escalation went to WHO
# listed_escalated THREAD: `ants thread list --escalated` lists the thread, with its escalation under it.
listed_escalated() { alice thread list --escalated --since 1h | grep -A1 -F "$1"; }
leaf_texts() { # leaf_texts THREAD: the texts of leaf-ant's messages on the thread
    alice thread show "$1" --json | jq --raw-output 'select(.from == "leaf-ant") | .text // empty'
}
# settled THREAD: waits, 3 minutes at most, until no message was stored on the thread for 30 s.
settled() {
    local last now same=0 start=$SECONDS
    last=$(alice thread show "$1" --json | wc -l)
    while ((same < 30 && SECONDS - start < 180)); do
        sleep 10
        now=$(alice thread show "$1" --json | wc -l)
        if [ "$now" = "$last" ]; then same=$((same + 10)); else same=0; last=$now; fi
    done
}
from_to() { # from_to THREAD FROM TO: how many messages FROM sent TO on the thread
    alice thread show "$1" --json | jq --slurp --arg from "$2" --arg to "$3" \
        'map(select(.kind == "message" and .from == $from and .to == $to)) | length'
}

scenario E1 P0 "An ant escalates to the thread's owner"
E1() {
    local t out
    trap mark_done EXIT
    t=$(tok)
    th=$(alice ask @hub-ant "Use the escalate tool with the issue 'E2E-ESC-$t', then end the turn." | thread)
    in_inbox() { alice inbox --peek --thread "$th" | grep -qF "E2E-ESC-$t"; }
    eventually 180 in_inbox
    escalated_to "$th" alice || fail "no escalation to alice stored on the thread"
    listed_escalated "$th" | has asked "thread list --escalated"
    alice thread show "$th" | has "escalation level 0, asked"
    # A message sent while the escalating turn still runs would steer it rather than start a prompt of its own.
    turn_ended() { alice thread show "$th" | grep -qF "ended:"; }
    eventually 180 turn_ended
    # Home lists the thread as stuck, but below the screen once enough threads are: the check opens it instead.
    chat "wait:connected" "type:/goto $th" "key:enter" "wait:hub-ant asks for help: E2E-ESC-$t:30"
    out=$(alice ask --wait --timeout 3m --thread "$th" @hub-ant "Reply with exactly ANSWERED-$t")
    printf '%s\n' "$out"
    has "ANSWERED-$t" <<<"$out"
    not_listed() { ! listed_escalated "$th" >/dev/null; }
    eventually 30 not_listed
    alice inbox --thread "$th" >/dev/null
}

scenario E2 P0 "An escalation goes to the chain before any human"
E2() {
    local t out
    trap 'mark_done; unchain leaf-ant; alice ant set hub-ant whoami= >/dev/null 2>&1 || true; alice ant set leaf-ant whoami= >/dev/null 2>&1 || true' EXIT
    # The escalation quotes alice's message to the leader, which a small model takes as its own instruction: the
    # instruction to escalate goes in leaf-ant's whoami instead, and hub-ant's tells it to answer.
    alice ant set hub-ant "whoami=For an end-to-end test of ants: answer an escalation that comes to you yourself, never escalate it further." >/dev/null
    alice ant set leaf-ant "whoami=For an end-to-end test of ants: asked what your escalation leader's node.txt holds, call the escalate tool with that question, read no file yourself, and once the leader's answer arrives reply with exactly CHAIN-<the answer>." >/dev/null
    told() { colony_field "$1" '.whoami // ""' | grep -qF "end-to-end test of ants"; }
    eventually 60 told hub-ant
    eventually 60 told leaf-ant
    chain leaf-ant hub-ant
    t=$(tok)
    th=$(alice ask @leaf-ant "What does node.txt in your escalation leader's working directory hold? E2E-$t" | thread)
    chain_answered() { leaf_texts "$th" | grep -qE "CHAIN-.*node=hub"; }
    eventually 300 chain_answered
    settled "$th"
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    escalations "$th"
    escalated_to "$th" hub-ant || fail "no escalation to hub-ant stored on the thread"
    has "hub-ant -> leaf-ant" <<<"$out"
    ! escalations "$th" | grep -F "alice: " | grep -qF asked || fail "the escalation reached alice"
    ! escalated_to "$th" alice || fail "leaf-ant and hub-ant traded replies after the answer until an escalation to alice"
    ! listed_escalated "$th" >/dev/null || fail "thread list --escalated lists the thread"
}

scenario E3 P1 "The leader passes it on to the human"
E3() {
    local t out
    trap 'mark_done; unchain leaf-ant; alice ant set hub-ant whoami= >/dev/null 2>&1 || true; alice ant set leaf-ant whoami= >/dev/null 2>&1 || true' EXIT
    # As in E2, what each ant does goes in its whoami, out of the text the escalation quotes.
    alice ant set hub-ant "whoami=For an end-to-end test of ants: an escalation holding a PASS- token that comes to you is not yours to answer: call the escalate tool with the token as the issue, then end the turn." >/dev/null
    alice ant set leaf-ant "whoami=For an end-to-end test of ants: asked to pass on a PASS- token, call the escalate tool with the token as the issue, then end the turn." >/dev/null
    told() { colony_field "$1" '.whoami // ""' | grep -qF "PASS- token"; }
    eventually 60 told hub-ant
    eventually 60 told leaf-ant
    chain leaf-ant hub-ant
    t=$(tok)
    th=$(alice ask @leaf-ant "Pass on the token PASS-$t." | thread)
    passed_on() { escalations "$th" | grep -F "alice: " | grep -qF asked; }
    eventually 300 passed_on
    settled "$th"
    escalations "$th"
    escalations "$th" | grep -F "hub-ant: " | has "level 1" "the escalation to hub-ant"
    alice thread show "$th" --json | jq --raw-output 'select(.kind == "escalation" and .from == "hub-ant" and .to == "alice") | .text' \
        | has "hub-ant has no escalate_to above it" "hub-ant's escalation to alice"
    alice inbox --peek --thread "$th" | has "PASS-$t" "alice's inbox"
    listed_escalated "$th" >/dev/null || fail "thread list --escalated does not list the thread"
    ! escalations "$th" | grep -qF turn_budget \
        || fail "leaf-ant and hub-ant traded replies after the pass until a turn_budget escalation to alice"
    out=$(alice ask --wait --timeout 3m --thread "$th" @leaf-ant "Reply with exactly ENDED-$t")
    printf '%s\n' "$out"
    has "ENDED-$t" <<<"$out"
    not_listed() { ! listed_escalated "$th" >/dev/null; }
    eventually 30 not_listed
    alice inbox --thread "$th" >/dev/null
}

scenario E4 P1 "A turn budget runs out between two ants"
E4() {
    local t th out sent replies
    t=$(tok)
    th=$(alice ask @hub-ant "For an end-to-end test of ants' turn budget, play a counting game with leaf-ant, one number per prompt. Now: send leaf-ant one message with the MCP tool mcp__ants__send_message (never ask_ant, nor Claude Code's own SendMessage), the text 'Counting game COUNT-$t: reply with only the number after 1.', then end this turn at once: do not wait, poll or check the inbox. Each reply of leaf-ant comes to you later as a prompt of its own; in it, when the reply is a number N, send leaf-ant one message with mcp__ants__send_message, 'Counting game COUNT-$t: reply with only the number after N.', and end that turn at once too. When mcp__ants__send_message is refused, end the turn." | thread)
    budget_spent() { escalations "$th" | grep -F "alice: " | grep -qF turn_budget; }
    # A model looping in its first turn sends from alice's message, with her fresh turns each time: stop early then.
    spent_or_runaway() { budget_spent || [ "$(from_to "$th" hub-ant leaf-ant)" -ge 12 ]; }
    eventually 600 spent_or_runaway
    escalations "$th"
    sent=$(from_to "$th" hub-ant leaf-ant)
    replies=$(from_to "$th" leaf-ant hub-ant)
    note "hub-ant sent leaf-ant $sent messages, leaf-ant replied $replies times"
    [ "$sent" -lt 12 ] && [ "$replies" -lt 12 ] || fail "twelve exchanges or more before the budget ran out"
    alice inbox --peek --thread "$th" | has "COUNT-$t" "alice's inbox"
    listed_escalated "$th" | has turn_budget "thread list --escalated"
    out=$(alice ask --wait --timeout 5m --thread "$th" @hub-ant "Use ask_ant to ask leaf-ant for one more number of the counting game, then reply with exactly RENEWED-$t.")
    printf '%s\n' "$out"
    has "RENEWED-$t" <<<"$out"
    [ "$(from_to "$th" hub-ant leaf-ant)" -gt "$sent" ] || fail "no exchange after alice's message"
    alice inbox --thread "$th" >/dev/null
}

scenario E5 P1 "handoff starts work on a thread of its own"
E5() {
    local t th out handed
    t=$(tok)
    out=$(alice ask --wait --timeout 3m @hub-ant "Use the handoff tool with the text 'Reply with exactly HANDED-$t' on a new thread. Then reply with exactly FIRST-$t.")
    printf '%s\n' "$out"
    has "FIRST-$t" <<<"$out"
    th=$(thread <<<"$out")
    handed_thread() { alice thread search --grep "HANDED-$t" --since 1h | awk -v th="$th" '$3 != th { print $3 }' | head -n 1; }
    handed_answered() { handed=$(handed_thread) && [ -n "$handed" ] && alice inbox --peek --thread "$handed" | grep -qF "HANDED-$t"; }
    eventually 180 handed_answered
    handed=$(handed_thread)
    out=$(alice thread show "$handed")
    printf '%s\n' "$out"
    has "owned by alice" <<<"$out"
    has "hub-ant -> hub-ant" <<<"$out"
    has "hub-ant -> alice" <<<"$out"
    alice inbox --thread "$handed" >/dev/null
}

scenario E6 P2 "A chain member decides a tool request"
E6() {
    local t dir out
    t=$(tok)
    dir=$(mktemp -d)
    trap "mark_done; unchain gate-ant; alice ant set hub-ant whoami= >/dev/null 2>&1 || true; leaf rm -f /demo/gate/chain-$t; rm -rf '$dir'" EXIT
    alice ant set hub-ant "whoami=For an end-to-end test of ants: when a tool request to run 'touch /demo/gate/chain-$t' comes up your chain, decide it at once with decide_approval allow_once." >/dev/null
    chain gate-ant hub-ant
    ask_bg "$dir/out" -f --timeout 5m @gate-ant "Run the shell command 'touch /demo/gate/chain-$t' and reply DONE-$t."
    th=$(thread_of "$dir/out")
    eventually 240 leaf test -f "/demo/gate/chain-$t"
    wait
    cat "$dir/out"
    has "DONE-$t" <"$dir/out"
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    escalated_to "$th" hub-ant || fail "no approval escalation to hub-ant stored on the thread"
    [ -z "$(pending_tool gate-ant "$th")" ] || fail "the request is still pending"
    settled "$th"
    ! escalations "$th" | grep -qF turn_budget \
        || fail "gate-ant and hub-ant traded replies after the decision until a turn_budget escalation to alice"
}

# --- O. Admin orders from the client --------------------------------------------------------------------------------

scenario O1 P0 "Pause and resume"
O1() {
    local t th
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true' EXIT
    alice ant pause leaf-ant
    paused() { is_paused leaf-ant; }
    eventually 30 paused
    t=$(tok)
    th=$(alice ask @leaf-ant "Reply with exactly RESUMED-$t" | thread)
    sleep 30
    ant_texts "$th" | lacks "RESUMED-$t" "the replies while paused"
    queued leaf-ant "$th"
    alice ant resume leaf-ant
    eventually 180 replied "$th" "RESUMED-$t"
    local orders
    orders=$(alice ant orders leaf-ant)
    printf '%s\n' "$orders" | grep -F "$run" || true
    has pause <<<"$orders"
    has resume <<<"$orders"
    has applied <<<"$orders"
}

scenario O2 P1 "Changing a shared setting"
O2() {
    local t
    trap 'alice ant set leaf-ant whoami= >/dev/null 2>&1 || true' EXIT
    t=$(tok)
    alice ant set leaf-ant "whoami=Role marker $t. Tell the marker when asked for the role."
    alice ant show leaf-ant | has "$t"
    marker_in_colony() { alice colony --json | grep -qF "$t"; }
    eventually 60 marker_in_colony
    alice ask --wait --timeout 3m @leaf-ant "What is the role marker? Reply with it only." | has "$t"
}

scenario O3 P1 "Restarting an ant's harnesses keeps its sessions"
O3() {
    local t th out
    t=$(tok)
    th=$(alice ask --wait --timeout 3m @leaf-ant "Remember the word $t. Reply OK." | thread)
    out=$(alice ant restart leaf-ant)
    printf '%s\n' "$out"
    has "restarting leaf-ant on leaf" <<<"$out"
    alice ask --wait --timeout 3m --thread "$th" @leaf-ant "Which word was to be remembered? Reply with it only." | has "$t"
}

scenario O4 P1 "Reading an ant's memories"
O4() {
    local out
    out=$(alice ant memories leaf-ant)
    printf '%s\n' "$out" | head -n 40
    has whoami <<<"$out"
    has sense <<<"$out"
    lacks e2e-s3cr3t <<<"$out"
    lacks sk-ant-oat <<<"$out"
}

scenario O5 P1 "The harness's MCP servers"
O5() {
    local out
    out=$(alice ant mcp leaf-ant)
    printf '%s\n' "$out"
    grep -qE '^ +ants +connected +[1-9][0-9]* tools' <<<"$out" || fail "the ants server is not connected with its tools"
}

scenario O6 P1 "Adding and retiring an ant on the leaf by order"
O6() {
    local t ant out
    t=$(tok)
    ant=temp-$t
    trap "alice ant retire $ant --wait 6m >/dev/null 2>&1 || true" EXIT
    out=$(alice ant add "$ant" --on leaf --capability temp --model claude-haiku-4-5 --wait 2m)
    printf '%s\n' "$out"
    has "added on leaf" <<<"$out"
    eventually 60 is_live "$ant"
    [ "$(colony_field "$ant" .machine)" = leaf ] || fail "$ant is not on machine leaf"
    alice ask --wait --timeout 3m "@$ant" "Reply with exactly TEMP-$t" | has "TEMP-$t"
    out=$(alice ant retire "$ant" --wait 6m)
    printf '%s\n' "$out"
    has "retired on leaf" <<<"$out"
    trap - EXIT
    gone() { [ "$(colony_state "$ant")" = absent ]; }
    eventually 120 gone
    hub ants auth issued | grep -F "$ant" | has revoked
    leaf test -d "/demo/home/.ants/agents/$ant" || fail "the retired ant's home is gone"
    alice ask --wait --timeout 3m @leaf-ant "Reply with exactly STILL-$t" | has "STILL-$t"
}

scenario O7 P1 "An order from a human without the admin role is refused"
O7() {
    local out
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true' EXIT
    if out=$(bob ant pause leaf-ant 2>&1); then
        printf '%s\n' "$out"
        has refused <<<"$out"
    else
        printf '%s\n' "$out"
        has admin <<<"$out"
    fi
    sleep 5
    ! is_paused leaf-ant || fail "leaf-ant was paused"
}

# --- R. Restarts, outages and store and forward ---------------------------------------------------------------------

scenario R1 P0 "The leaf container restarts; threads carry on"
R1() {
    local t th
    trap 'restore_node leaf' EXIT
    t=$(tok)
    th=$(alice ask --wait --timeout 3m @leaf-ant "Remember the word $t. Reply OK." | thread)
    container stop "$name-leaf" >/dev/null
    eventually 90 not_live leaf-ant
    eventually 90 not_live gate-ant
    alice machines leaf-ant | has leaf
    container start "$name-leaf" >/dev/null
    eventually 120 is_live leaf-ant
    eventually 120 is_live gate-ant
    alice ask --wait --timeout 3m --thread "$th" @leaf-ant "Which word was to be remembered? Reply with it only." | has "$t"
}

scenario R2 P0 "A message to an ant whose machine is down waits and is delivered once"
R2() {
    local t th out
    trap 'restore_node leaf' EXIT
    container stop "$name-leaf" >/dev/null
    eventually 90 not_live leaf-ant
    t=$(tok)
    out=$(alice ask @leaf-ant "Reply with exactly LATER-$t" 2>&1)
    printf '%s\n' "$out"
    th=$(thread <<<"$out")
    [ -n "$th" ] || fail "the message was not stored"
    container start "$name-leaf" >/dev/null
    eventually 240 replied "$th" "LATER-$t"
    sleep 20
    [ "$(ant_texts "$th" | count "LATER-$t")" -eq 1 ] || fail "LATER-$t was not delivered exactly once"
}

scenario R3 P0 "The worker's uplink drops during a prompt; the reply is stored once"
R3() {
    local t th out
    trap 'uplink leaf up' EXIT
    t=$(tok)
    th=$(alice ask @leaf-ant "$e2e Run the shell command '$(slow 40)' in the foreground, then reply with exactly OUTBOX-$t." | thread)
    sleep 10
    uplink leaf down
    sleep 60
    ant_texts "$th" | lacks "OUTBOX-$t" "the hub's view while the uplink is down"
    not_live leaf-ant || fail "leaf-ant stayed live without its uplink"
    uplink leaf up
    eventually 120 replied "$th" "OUTBOX-$t"
    eventually 60 is_live leaf-ant
    sleep 10
    [ "$(ant_texts "$th" | count "OUTBOX-$t")" -eq 1 ] || fail "OUTBOX-$t stored more than once"
    out=$(alice thread show "$th")
    [ "$(count "ended: replied" <<<"$out")" -eq 1 ] || fail "the prompt ran again"
}

scenario R4 P1 "The client sends while its uplink is down"
R4() {
    local t out th
    trap 'uplink client up; alice ant resume leaf-ant >/dev/null 2>&1 || true' EXIT
    uplink client down
    sleep 5
    t=$(tok)
    out=$(alice ask @leaf-ant "Reply with exactly OFFLINE-$t" 2>&1)
    printf '%s\n' "$out"
    th=$(thread <<<"$out")
    [ -n "$th" ] || fail "ants ask did not keep the message"
    ! alice inbox >/dev/null 2>&1 || fail "ants inbox worked without the hub"
    ! alice ant pause leaf-ant >/dev/null 2>&1 || fail "an admin order was sent without the hub"
    uplink client up
    eventually 240 replied "$th" "OFFLINE-$t"
    ! is_paused leaf-ant || fail "leaf-ant was paused"
}

scenario R5 P1 "The hub restarts"
R5() {
    local t th
    trap 'restore_node hub' EXIT
    t=$(tok)
    th=$(alice ask --wait --timeout 3m @hub-ant "Remember the word $t. Reply OK." | thread)
    container stop "$name-hub" >/dev/null
    sleep 20
    container start "$name-hub" >/dev/null
    ready hub
    sync_hub_ip
    all_live
    alice thread show "$th" | has "$t"
    alice ask --wait --timeout 3m --thread "$th" @hub-ant "Which word was to be remembered? Reply with it only." | has "$t"
    alice ask --wait --timeout 3m @leaf-ant "Reply with exactly AFTER-$t" | has "AFTER-$t"

    outbox_line() { container exec --env ANTS_LOG=warn "$name-client" ants doctor --no-harness 2>&1 | grep -E '^[a-z]+ +outbox'; }
    outbox_ok() { local line; line=$(outbox_line); printf '%s\n' "$line"; grep -q '^ok' <<<"$line"; }
    local since=$SECONDS
    eventually 240 outbox_ok
    note "the client's outbox was ok $((SECONDS - since))s after the hub came back"
}

scenario R6 P1 "The leaf is killed during a prompt whose tool ran"
R6() {
    local t th out
    trap 'restore_node leaf' EXIT
    t=$(tok)
    th=$(alice ask @leaf-ant "$e2e Append the line $t to crash.txt, then run the shell command '$(slow 60)' in the foreground, then reply with exactly CRASH-$t." | thread)
    sleep 20
    container kill "$name-leaf" >/dev/null
    sleep 2
    container start "$name-leaf" >/dev/null
    eventually 300 replied "$th" "CRASH-$t"
    sleep 20
    [ "$(ant_texts "$th" | count "CRASH-$t")" -eq 1 ] || fail "CRASH-$t not answered exactly once"
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    alice dead list | lacks "$th" "dead letters"
}

# --- H. Harness holds, limits and redaction -------------------------------------------------------------------------

scenario H1 P0 "A signed-out harness holds its messages, then recovers"
H1() {
    local t th bad
    bad=$(mktemp)
    printf 'CLAUDE_CODE_OAUTH_TOKEN=invalid\n' >"$bad"
    trap "recreate_leaf; all_live; rm -f '$bad'" EXIT
    recreate_leaf "$bad"
    t=$(tok)
    th=$(alice ask @leaf-ant "Reply with exactly SIGNED-$t" 2>&1 | thread)
    signed_out() { [ "$(colony_field leaf-ant '.signed_out != null')" = true ]; }
    eventually 180 signed_out
    notice() { ant_texts "$th" | grep -qF "claude login"; }
    eventually 60 notice
    ant_texts "$th" | has leaf "the sign-in notice"
    alice dead list | lacks "$th" "dead letters"
    alice ask @leaf-ant "Reply with exactly WARNED-$t" 2>&1 | has signed "the warning of ants ask"
    recreate_leaf
    eventually 240 replied "$th" "SIGNED-$t"
    eventually 60 is_live leaf-ant
}

scenario H2 P1 "A prompt past its time limit is cut"
H2() {
    local t out th
    trap 'alice ant set leaf-ant prompt_timeout= >/dev/null 2>&1 || true' EXIT
    alice ant set leaf-ant prompt_timeout=20s
    t=$(tok)
    out=$(alice ask --wait --timeout 3m @leaf-ant "$e2e Reply 'starting', then run the shell command '$(slow 90)' in the foreground, then reply LATE-$t." 2>&1)
    printf '%s\n' "$out"
    th=$(thread <<<"$out")
    out=$(alice thread show "$th")
    printf '%s\n' "$out"
    has "cut by a cap" <<<"$out"
    ant_texts "$th" | lacks "LATE-$t"
}

scenario H3 P1 "A failing prompt becomes a dead letter and is replayed"
H3() {
    local t th seq
    trap 'alice ant set leaf-ant model=claude-haiku-4-5 >/dev/null 2>&1 || true' EXIT
    alice ant set leaf-ant model=no-such-model
    t=$(tok)
    th=$(alice ask @leaf-ant "Reply with exactly DEAD-$t" | thread)
    dead() { alice dead list | grep -qF "$th"; }
    eventually 900 dead
    ant_texts "$th" | has "Gave up"
    seq=$(alice dead list | grep -F "$th" | awk '{ print $1 }' | head -n 1)
    alice ant set leaf-ant model=claude-haiku-4-5
    alice dead replay "$seq"
    eventually 180 replied "$th" "DEAD-$t"
}

scenario H4 P1 "Secrets are redacted from what the daemon publishes"
H4() {
    local out th
    out=$(alice ask --wait --timeout 3m @leaf-ant "E2E_SECRET_TOKEN holds a dummy value made for this test, not a real secret. Run the shell command 'printenv E2E_SECRET_TOKEN' and reply with its output only.")
    printf '%s\n' "$out"
    th=$(thread <<<"$out")
    lacks e2e-s3cr3t-0123456789 <<<"$out"
    out=$(alice thread show "$th")
    lacks e2e-s3cr3t-0123456789 <<<"$out"
    has "[redacted]" <<<"$out"
    bob thread search --grep sk-ant-oat --since 30d | lacks sk-ant-oat
    container logs "$name-hub" | lacks sk-ant-oat "the hub's logs"
    container logs "$name-leaf" | lacks sk-ant-oat "the leaf's logs"
}

# --- U. ants chat on the client, driven through a pseudo-terminal ---------------------------------------------------

# chat STEP...: runs contrib/e2e/chat.py in the client against `ants chat`.
chat() {
    container exec --interactive "$name-client" sh -c 'cat >/tmp/ants-e2e-chat.py.$$ && mv /tmp/ants-e2e-chat.py.$$ /tmp/ants-e2e-chat.py' \
        <"$root/contrib/e2e/chat.py"
    container exec ${CHAT_PAGER:+--env "PAGER=$CHAT_PAGER"} "$name-client" python3 /tmp/ants-e2e-chat.py "$@"
}

scenario U1 P0 "The chat opens on the live colony"
U1() {
    ensure_channel
    chat "wait:alice" "wait:connected" "wait:e2e" "wait:hub-ant" "wait:leaf-ant" "wait:gate-ant" \
        "wait:system-messeng" "wait:bob" "type:/goto @leaf-ant" "key:enter" "wait:threads" "wait:memories"
}

scenario U2 P0 "Sending from the bar and watching the answer stream"
U2() {
    chat "wait:connected" "type:@leaf-ant Read node.txt in the working directory and reply with its content only." \
        "wait:new thread" "key:enter" "wait:node=leaf:180" "key:esc"
}

scenario U3 P0 "A tool request decided on its card"
U3() {
    local t
    t=$(tok)
    trap "leaf rm -f /demo/gate/chat-1-$t /demo/gate/chat-2-$t" EXIT
    chat "wait:connected" "type:@gate-ant Run the shell command 'touch /demo/gate/chat-1-$t' and reply DONE." \
        "key:enter" "wait:chat-1-$t" "key:esc" "key:esc" "wait:A always:120" "type:a" "sleep:5"
    eventually 120 leaf test -f "/demo/gate/chat-1-$t"
    chat "wait:connected" "type:@gate-ant Run the shell command 'touch /demo/gate/chat-2-$t' and reply DONE." \
        "key:enter" "wait:chat-2-$t" "key:esc" "key:esc" "wait:A always:120" "type:n" \
        "type:Do not create files. Reply NOPE." "key:enter" "sleep:5"
    nope() { alice thread search --grep NOPE --since 10m | grep -qF NOPE; }
    eventually 180 nope
    ! leaf test -e "/demo/gate/chat-2-$t" || fail "the rejected command ran"
}

scenario U4 P0 "A question answered on its card"
U4() {
    local t dir
    t=$(tok)
    dir=$(mktemp -d)
    trap "rm -rf '$dir'" EXIT
    chat "wait:connected" \
        "type:@hub-ant Use ask_human to ask 'Pick a colour $t' with options RED and BLUE, then reply PICKED-<label>." \
        "key:enter" "wait:sent to hub-ant"
    chat "wait:connected" "key:esc" "key:esc" "wait:1 RED  2 BLUE:180" "type:2" "gone:1 RED  2 BLUE:30"
    picked() { alice thread search --grep "Pick a colour $t" --since 1h | awk '{ print $3 }' | head -n 1 >"$dir/th" && replied "$(cat "$dir/th")" PICKED-BLUE; }
    eventually 180 picked
}

scenario U6 P1 "A channel in the chat"
U6() {
    local t
    ensure_channel
    t=$(tok)
    chat "wait:connected" "type:/goto #e2e" "key:enter" "wait:members" \
        "type:@hub-ant reply with exactly CHATCHAN-$t" "key:enter" "wait:CHATCHAN-$t:180"
}

scenario U7 P1 "Pausing an ant from its view"
U7() {
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true' EXIT
    chat "wait:connected" "type:/goto @leaf-ant" "key:enter" "wait:memories" "type:p" "wait:y pause the ant" "type:y" \
        "wait:live, paused:60" "type:p" "wait:y resume the ant" "type:y" "sleep:10"
    local orders
    orders=$(alice ant orders leaf-ant)
    has pause <<<"$orders"
    has resume <<<"$orders"
    ! is_paused leaf-ant || fail "leaf-ant stayed paused"
}

# thread_with TEXT: the thread of the first stored message holding TEXT in the last hour, or nothing.
thread_with() { alice thread search --grep "$1" --since 1h | awk '{ print $3 }' | head -n 1; }

scenario U5 P1 "Replies steer, /later queues, x cancels"
U5() {
    local t th
    t=$(tok)
    chat "wait:connected" \
        "type:@leaf-ant $e2e Run the shell command '$(slow 45)' in the foreground, then reply with exactly FIRST-$t." \
        "key:enter" "wait:sent to leaf-ant" "sleep:15" \
        "type:Change of plan: once the sleep ends, reply with exactly SECOND-$t and nothing else." "key:enter" "sleep:5"
    chat "wait:connected" \
        "type:@leaf-ant $e2e Run the shell command '$(slow 40)' in the foreground, then reply with exactly ONE-$t." \
        "key:enter" "wait:sent to leaf-ant" "sleep:10" "type:/later Reply with exactly TWO-$t." "key:enter" \
        "wait:queued · 1 ahead:30"
    chat "wait:connected" \
        "type:@leaf-ant $e2e Run the shell command '$(slow 100)' in the foreground, not in the background, then reply LATE-$t." \
        "key:enter" "wait:sent to leaf-ant" "sleep:15" "key:esc" "key:up" "type:x" "wait:y cancels the prompt" "type:y" \
        "wait:cancelled by alice:60"
    th=$(thread_with "FIRST-$t")
    [ -n "$th" ] || fail "no thread holds FIRST-$t"
    eventually 240 replied "$th" "SECOND-$t"
    ant_texts "$th" | lacks "FIRST-$t" "the steered thread's replies"
    alice thread show "$th" | has steered
    th=$(thread_with "ONE-$t")
    eventually 240 replied "$th" "TWO-$t"
    local texts
    texts=$(ant_texts "$th")
    [ "$(grep -nF "ONE-$t" <<<"$texts" | head -n 1 | cut -d: -f1)" -lt \
        "$(grep -nF "TWO-$t" <<<"$texts" | head -n 1 | cut -d: -f1)" ] || fail "TWO answered before ONE"
    th=$(thread_with "LATE-$t")
    alice thread show "$th" | has "ended: cancelled"
    ant_texts "$th" | lacks "LATE-$t"
}

scenario U8 P1 "The hub goes away and comes back while the chat is open"
U8() {
    local t out th
    trap 'uplink client up' EXIT
    t=$(tok)
    # The chat runs through the drop and the return: the uplink goes down 10 s after it opens, back up 60 s later.
    (sleep 10; uplink client down; sleep 60; uplink client up) &
    out=$(chat "wait:connected" "sleep:20" "snap" \
        "type:@leaf-ant Read node.txt in the working directory and reply with its content and U8-$t." "key:enter" \
        "sleep:20" "snap" "wait:node=leaf:300")
    printf '%s\n' "$out"
    wait
    th=$(thread_with "U8-$t")
    [ -n "$th" ] || fail "the message was lost"
    replied "$th" "node=leaf" || fail "no reply on the thread"
    [ "$(ant_texts "$th" | count "node=leaf")" -eq 1 ] || fail "the reply was stored more than once"
    # While the uplink is down, the chat's own server (the client's leaf node) stays up: the header tells the hub is
    # gone from the reads it does not answer, and a bucket it cannot read is not taken for missing.
    sed -n '/^-- snap/,/^-- /p' <<<"$out" | has "hub out of reach" "the header while the uplink was down"
    lacks "creating bucket" "the chat" <<<"$out"
    lacks "JetStream unavailable" "the chat" <<<"$out"
}

scenario U9 P1 "A sealed direct message to another human"
U9() {
    local t watch
    t=$(tok)
    watch=$(mktemp)
    trap "rm -f '$watch'" EXIT
    chat "wait:connected" "type:/goto @bob" "key:enter" "type:sealed hello $t" \
        "wait:to bob on a new thread, sealed" "key:enter" "wait:🔒"
    bob inbox | has "sealed hello $t" "bob's inbox"
    bob ask @alice "sealed reply $t" >/dev/null
    chat "wait:connected" "type:/goto @bob" "key:enter" "wait:sealed reply $t:60" "wait:🔒"
    # An ant's credentials watching bob's inbox during a second send: refused, or the text stays sealed.
    (hub timeout 30 ants --creds "$auth/hub-ant.creds" watch 'ants.inbox.bob.>' || true) >"$watch" 2>&1 &
    sleep 5
    alice ask @bob "sealed second $t" >/dev/null
    wait
    cat "$watch"
    lacks "sealed second $t" "the ant's watch" <"$watch"
    grep -qE 'Permissions Violation for Subscription|\[sealed' "$watch" || fail "the ant's watch was neither refused nor sealed"
}

scenario U10 P1 "Marks and replies in the chat"
U10() {
    local t out th m fw
    t=$(tok)
    out=$(a_thread leaf-ant "$t")
    th=${out% *}
    m=${out#* }
    trap "alice thread unpin '$th' >/dev/null 2>&1 || true; alice inbox --thread '$th' >/dev/null 2>&1 || true; true" EXIT
    chat "wait:connected" "type:/goto $th" "key:enter" "wait:HK-$t" \
        "type:/title Chat $t" "key:enter" "wait:titled thread $th" \
        "type:/done" "key:enter" "wait:thread $th done" \
        "type:/pin" "key:enter" "wait:thread $th pinned" \
        "type:/react rocket" "key:enter" "wait:reacted rocket to message $m"
    alice thread title "$th" | has "Chat $t" "the title"
    [ "$(list_field alice "HK-$t" .done.by)" = alice ] || fail "/done did not mark the thread done"
    alice thread list --pinned --since 1h | has "$th" "--pinned"
    alice thread show "$th" | has "reacted rocket: alice"
    # A quoted reply (> on the selected reply), a forward of it to bob (f), a TL;DR and a search.
    chat "wait:connected" "type:/goto $th" "key:enter" "wait:HK-$t" \
        "key:up" "type:>" "wait:replying to leaf-ant: HK-$t" "type:Reply with exactly CQUOTE-$t" "key:enter" \
        "wait:sent to leaf-ant"
    eventually 180 replied "$th" "CQUOTE-$t"
    alice thread show "$th" | has "↪ replying to leaf-ant: HK-$t"
    [ "$(list_field alice "HK-$t" .done)" = null ] || fail "the reply did not reopen the thread"
    chat "wait:connected" "type:/goto $th" "key:enter" "wait:CQUOTE-$t" \
        "key:up" "type:f" "wait:/forward @" "type:bob " "key:esc" "type:FWD-$t" "key:enter" "wait:sent to bob on thread" \
        "type:/goto $th" "key:enter" "wait:CQUOTE-$t" "type:/tldr" "key:enter" "wait:TL;DR (system-messenger:180" \
        "type:/search CQUOTE-$t" "key:enter" "wait:search CQUOTE-$t" "wait:Chat $t"
    fwd() { bob inbox --peek | grep -qF "FWD-$t"; }
    eventually 60 fwd
    fw=$(bob inbox --peek --json | jq --slurp --raw-output --arg t "FWD-$t" 'map(select(.text | contains($t))) | .[0].thread_id')
    trap "alice thread unpin '$th' >/dev/null 2>&1 || true; alice inbox --thread '$th' >/dev/null 2>&1 || true; bob inbox --thread '$fw' >/dev/null 2>&1 || true; true" EXIT
    bob inbox --peek | has "Forwarded from leaf-ant" "bob's inbox"
    [ "$(summaries_of "$th")" -ge 1 ] || fail "/tldr asked no one"
    alice thread tldr "$th" --json | jq --exit-status '.by == "system-messenger" and (.text | length > 0)' >/dev/null \
        || fail "no TL;DR kept"
    [ "$(summaries_of "$th")" -eq 1 ] || fail "the TL;DR /tldr asked for was not kept"
}

# --- X. Sense and trails --------------------------------------------------------------------------------------------

scenario X1 P1 "A memory kept by sense is recalled on another thread and after a restart"
X1() {
    local t
    trap 'restore_node leaf' EXIT
    t=$(tok)
    alice ask --wait --timeout 3m @leaf-ant "Use sense_remember to keep this memory: the e2e codeword is $t. Reply KEPT." | has KEPT
    alice ask --wait --timeout 3m @leaf-ant "Use sense_recall to find the e2e codeword. Reply with the codeword only." | has "$t"
    alice ant memories leaf-ant | has "$t"
    container stop "$name-leaf" >/dev/null
    container start "$name-leaf" >/dev/null
    eventually 120 is_live leaf-ant
    alice ask --wait --timeout 3m @leaf-ant "Use sense_recall to find the e2e codeword. Reply with the codeword only." | has "$t"
    [ "$(colony_field leaf-ant '.sense_unkept // null')" = null ] || fail "leaf-ant keeps no sense data"
}

scenario X2 P1 "A reminder wakes the ant"
X2() {
    local t th
    t=$(tok)
    th=$(alice ask --wait --timeout 3m @leaf-ant "Use sense_remind to set a reminder 2 minutes from now with the note 'send WAKE-$t to alice'. Reply SET." | thread)
    woke() { alice thread search --grep "WAKE-$t" --from leaf-ant --since 1h | grep -qF "WAKE-$t"; }
    eventually 360 woke
    alice thread show "$th" | has "wake from sense"
}

scenario X3 P1 "A trail whose steps run on both machines"
X3() {
    local out run
    container exec --interactive "$name-client" sh -c 'cat >/tmp/nodes.py' <<'EOF'
meta = {"name": "nodes", "description": "Reads node.txt on both machines", "phases": ["read"]}

phase("read")
hub, leaf = await parallel(
    agent("Reply with the content of node.txt in the working directory only.", ant="hub-ant", label="hub"),
    agent("Reply with the content of node.txt in the working directory only.", ant="leaf-ant", label="leaf"),
)
{"hub": hub, "leaf": leaf}
EOF
    alice trail save /tmp/nodes.py
    alice trail list | has nodes
    out=$(alice trail run nodes --ant hub-ant --yes --follow)
    printf '%s\n' "$out"
    has "done:" <<<"$out"
    # The model may fence the content: the step's answer holds it.
    grep -qE '"hub": ".*node=hub' <<<"$out" || fail "the hub step did not answer node=hub"
    grep -qE '"leaf": ".*node=leaf' <<<"$out" || fail "the leaf step did not answer node=leaf"
    run=$(alice trail runs --json | jq --slurp --raw-output 'map(select(any(.route[]?; .trail == "nodes"))) | last | .run // empty')
    [ -n "$run" ] || { alice trail runs; fail "no run of nodes listed"; }
    alice trail show "$run" | has "2"
}

# The trail of X4-X8: a step on hub-ant, then one on leaf-ant, to stop between them. Its args: t, the token both
# answers hold, and pre, what the second step's prompt asks before its reply. Saved anew by each, a save replacing it.
save_stoppable() {
    container exec --interactive "$name-client" sh -c 'cat >/tmp/stoppable.py.$$ && mv /tmp/stoppable.py.$$ /tmp/stoppable.py' <<'EOF'
meta = {"name": "stoppable", "description": "Two steps one after the other, to stop between them", "phases": ["first", "second"]}

t = args["t"]
pre = args.get("pre", "A step of an end-to-end test of ants:")
phase("first")
first = await agent("A step of an end-to-end test of ants: reply with exactly FIRST-" + t + " and nothing else.", ant="hub-ant", label="first")
phase("second")
second = await agent(pre + " reply with exactly SECOND-" + t + " and nothing else.", ant="leaf-ant", label="second")
{"first": first, "second": second}
EOF
    alice trail save /tmp/stoppable.py >/dev/null
}

# start_stoppable T [FLAGS...]: starts a run of stoppable conducted by hub-ant with the token T; prints its id.
start_stoppable() {
    local t=$1
    shift
    alice trail run stoppable --ant hub-ant --args "$(jq -cn --arg t "$t" '{t: $t}')" --yes --json "$@" | jq -r .run
}
run_field() { local run=$1; shift; alice trail show "$run" --json | jq --raw-output "$@"; } # run_field RUN [JQ ARGS] JQ
# second_waits RUN: the first step answered, the second sent to leaf-ant and waited on.
second_waits() {
    [ "$(run_field "$1" '[.pending[]? | select(.to == "leaf-ant" and .sent)] | length')" = 1 ] &&
        [ "$(run_field "$1" '[.journal[]? | select(.answer.ok != null)] | length')" = 1 ]
}
second_message() { run_field "$1" '.pending[] | select(.to == "leaf-ant") | .message'; }
second_thread() { run_field "$1" '.pending[] | select(.to == "leaf-ant") | .thread'; }
run_state() { run_field "$1" .run.state; }
# stop_unended RUN: a trap's: stops the run unless it ended or stopped.
stop_unended() {
    [ -n "$1" ] || return 0
    case "$(run_state "$1" 2>/dev/null)" in running | waiting) alice trail stop "$1" >/dev/null 2>&1 || true ;; esac
}
# first_answers T: hub-ant's answers holding FIRST-T, the prompts and the run's end left out.
first_answers() { alice thread search --grep "FIRST-$1" --from hub-ant --since 1h | grep -cE "\): \**FIRST-$1" || true; }
# done_with RUN T: the run is done, its result holding both answers.
done_with() {
    [ "$(run_field "$1" --arg t "$2" '.run.state == "done" and (.run.result.first | contains("FIRST-" + $t))
        and (.run.result.second | contains("SECOND-" + $t))')" = true ] || { alice trail show "$1"; fail "run $1 is not done with both answers"; }
}

scenario X4 P0 "A run stopped while a step waits, the step withdrawn, the run resumed"
X4() {
    local t out message step_thread
    t=$(tok)
    run=
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true; stop_unended "$run"' EXIT
    save_stoppable
    alice ant pause leaf-ant
    run=$(start_stoppable "$t")
    [ -n "$run" ] || fail "no run started"
    eventually 180 second_waits "$run"
    message=$(second_message "$run")
    step_thread=$(second_thread "$run")
    queued leaf-ant "$message"
    out=$(alice trail stop "$run")
    printf '%s\n' "$out"
    has "stopped run $run" <<<"$out"
    has "ants withdraw leaf-ant $message" <<<"$out"
    alice withdraw leaf-ant "$message"
    [ "$(alice trail runs --json | jq --slurp --raw-output --arg run "$run" 'map(select(.run == $run)) | .[0].state')" = stopped ] \
        || fail "the run is not listed stopped"
    # A stopped run stopped again lists the steps whose messages went out, withdrawn or not (docs/trails.md).
    out=$(alice trail stop "$run")
    printf '%s\n' "$out"
    has "stopped run $run" <<<"$out"
    alice ant resume leaf-ant
    withdrawn() { ant_texts "$step_thread" | grep -qF "Withdrawn by alice"; }
    eventually 60 withdrawn
    sleep 20
    alice thread show "$step_thread" --json | jq --raw-output 'select(.from == "leaf-ant") | .text // empty' \
        | lacks "SECOND-$t" "leaf-ant's messages on the withdrawn step's thread"
    out=$(alice trail resume "$run" --follow)
    printf '%s\n' "$out"
    has "done:" <<<"$out"
    done_with "$run" "$t"
    out=$(alice trail show "$run")
    printf '%s\n' "$out"
    has "state: done" <<<"$out"
    ! grep -q "^pending calls:" <<<"$out" || fail "trail show lists the answered calls of a done run as pending"
    [ "$(run_field "$run" .run.steps)" = 2 ] || fail "the run counts $(run_field "$run" .run.steps) steps, not 2"
    [ "$(first_answers "$t")" = 1 ] || fail "hub-ant answered FIRST-$t $(first_answers "$t") times"
    if out=$(alice trail resume "$run" 2>&1); then printf '%s\n' "$out"; fail "a resume of a done run was taken"; fi
    printf '%s\n' "$out"
    has "has ended" <<<"$out"
}

scenario X5 P1 "A run failed on its step limit, resumed with the limit raised"
X5() {
    local t out
    t=$(tok)
    run=
    trap 'stop_unended "$run"' EXIT
    save_stoppable
    if out=$(alice trail run stoppable --ant hub-ant --args "$(jq -cn --arg t "$t" '{t: $t}')" --steps 1 --yes --follow 2>&1); then
        printf '%s\n' "$out"
        fail "the run past its step limit ended well"
    fi
    printf '%s\n' "$out"
    has "limit of 1 steps" <<<"$out"
    run=$(alice trail runs --json | jq --slurp --raw-output --arg t "$t" 'map(select(.route[0].args.t? == $t)) | .[0].run // empty')
    [ -n "$run" ] || { alice trail runs; fail "no run of stoppable with $t listed"; }
    [ "$(run_state "$run")" = failed ] || fail "the run is $(run_state "$run"), not failed"
    alice trail show "$run" | has "failed on its steps limit"
    out=$(alice doctor --no-harness 2>&1) || true
    grep -F "$run" <<<"$out" || { printf '%s\n' "$out"; fail "ants doctor names no run $run"; }
    has "ants trail resume $run --steps" <<<"$out"
    out=$(alice trail resume "$run" --steps 5 --follow)
    printf '%s\n' "$out"
    has "done:" <<<"$out"
    done_with "$run" "$t"
}

scenario X6 P1 "Stopping and resuming a run by admin order"
X6() {
    local t out orders
    t=$(tok)
    run=
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true; stop_unended "$run"' EXIT
    save_stoppable
    alice ant pause leaf-ant
    run=$(start_stoppable "$t")
    [ -n "$run" ] || fail "no run started"
    eventually 180 second_waits "$run"
    if out=$(bob ant stop-trail hub-ant "$run" 2>&1); then
        printf '%s\n' "$out"
        has refused <<<"$out"
    else
        printf '%s\n' "$out"
        has admin <<<"$out"
    fi
    sleep 5
    [ "$(run_state "$run")" = running ] || fail "the run is $(run_state "$run") after bob's order"
    out=$(alice ant stop-trail hub-ant "$run" --wait 30s)
    printf '%s\n' "$out"
    has "ants withdraw leaf-ant $(second_message "$run")" <<<"$out"
    [ "$(run_state "$run")" = stopped ] || fail "the run is $(run_state "$run"), not stopped"
    alice ant resume leaf-ant
    out=$(alice ant resume-trail hub-ant "$run" --wait 30s)
    printf '%s\n' "$out"
    out=$(alice trail show "$run" --follow)
    printf '%s\n' "$out"
    has "done:" <<<"$out"
    done_with "$run" "$t"
    orders=$(alice ant orders hub-ant)
    printf '%s\n' "$orders" | grep -F "$run" || true
    grep -F "stop trail run $run" <<<"$orders" | grep alice | has applied "the stop_trail order"
    grep -F "resume trail run $run" <<<"$orders" | grep alice | has applied "the resume_trail order"
}

scenario X7 P1 "Stopping and resuming a run from ants chat"
X7() {
    local t short out message
    t=$(tok)
    run=
    trap 'alice ant resume leaf-ant >/dev/null 2>&1 || true; stop_unended "$run"' EXIT
    save_stoppable
    alice ant pause leaf-ant
    run=$(start_stoppable "$t")
    [ -n "$run" ] || fail "no run started"
    eventually 180 second_waits "$run"
    short=${run:0:13}
    message=$(second_message "$run")
    # The chat shows /trail runs, and the steps a stop leaves waiting, in $PAGER: one that appends what it is given.
    container exec "$name-client" rm -f /tmp/ants-e2e-x7-runs.txt
    out=$(CHAT_PAGER='cat "$1" >>/tmp/ants-e2e-x7-runs.txt; true' chat "wait:connected" "type:/trail runs" "key:enter" \
        "wait:the pager lists them" "type:/trail stop $short" "key:enter" "wait:stopped run $run on hub-ant" "sleep:2")
    printf '%s\n' "$out"
    has "1 step still waiting in other ants' queues" "the stop's status line" <<<"$out"
    out=$(container exec "$name-client" cat /tmp/ants-e2e-x7-runs.txt)
    grep -F "$run" <<<"$out" | has running "the run in /trail runs"
    has "ants withdraw leaf-ant $message" "the stop's page" <<<"$out"
    [ "$(run_state "$run")" = stopped ] || fail "the run is $(run_state "$run"), not stopped"
    alice ant resume leaf-ant
    chat "wait:connected" "type:/trail resume $short" "key:enter" "wait:resumed run" "snap"
    is_done() { [ "$(run_state "$run")" = done ]; }
    eventually 180 is_done
    done_with "$run" "$t"
}

scenario X8 P2 "A run stopped while its step runs"
X8() {
    local t out step_thread
    t=$(tok)
    run=
    trap 'stop_unended "$run"' EXIT
    save_stoppable
    run=$(alice trail run stoppable --ant hub-ant --yes --json \
        --args "$(jq -cn --arg t "$t" --arg pre "A step of a trail alice runs. $e2e Without asking back, run the shell command '$(slow 63)' in the foreground (it only waits, and alice expects it), then" '{t: $t, pre: $pre}')" | jq -r .run)
    [ -n "$run" ] || fail "no run started"
    eventually 180 second_waits "$run"
    step_thread=$(second_thread "$run")
    sleeping() { leaf pgrep -f 'time\.sleep\(63\)' >/dev/null; }
    eventually 120 sleeping
    out=$(alice trail stop "$run")
    printf '%s\n' "$out"
    has "stopped run $run" <<<"$out"
    eventually 180 replied "$step_thread" "SECOND-$t"
    sleep 5
    [ "$(run_state "$run")" = stopped ] || fail "the run is $(run_state "$run") after the step's reply"
    [ "$(run_field "$run" '[.journal[]? | select(.call == 3)] | length')" = 0 ] || fail "the stopped run took the reply"
    out=$(alice trail resume "$run" --follow)
    printf '%s\n' "$out"
    has "done:" <<<"$out"
    done_with "$run" "$t"
    # The step sent anew went on a thread of its own: leaf-ant answered SECOND on two threads.
    [ "$(alice thread search --grep "SECOND-$t" --from leaf-ant --since 1h | awk '{ print $3 }' | sort -u | wc -l | tr -d ' ')" = 2 ] \
        || fail "leaf-ant did not answer on two threads"
}

# --- K. Credentials and sealing -------------------------------------------------------------------------------------

scenario K1 P1 "Revoked credentials are cut off at the hub and behind a leaf"
K1() {
    local t human
    t=$(tok)
    human=carol-$t
    trap "hub ants auth leaf client --remove-human $human >/dev/null 2>&1 && copy_creds --force client leaf-client.creds && uplink client up" EXIT
    hub ants auth user "$human" --human --label throwaway >/dev/null
    hub ants auth leaf client --add-human "$human" >/dev/null
    copy_creds client "$human.creds"
    copy_creds --force client leaf-client.creds
    uplink client up
    sleep 5
    alice --creds "$auth/$human.creds" notes "$(uuidgen | tr '[:upper:]' '[:lower:]')" "before" >/dev/null
    hub ants auth revoke "$human"
    sleep 15
    ! alice --creds "$auth/$human.creds" colony >/dev/null 2>&1 || fail "revoked credentials still work behind the leaf"
    alice colony >/dev/null
    hub ants auth revoked | has "$human"
}

scenario K2 P1 "Sealed messages stay closed to the hub and to ants"
K2() {
    local t th out
    t=$(tok)
    out=$(alice ask @bob "sealed $t")
    th=$(thread <<<"$out")
    bob inbox --thread "$th" | has "sealed $t"
    out=$(hub ants --creds "$auth/hub-ant.creds" thread show "$th" 2>&1 || true)
    printf '%s\n' "$out"
    lacks "sealed $t" "an ant's view" <<<"$out"
}

# --- D. Dreams and sage reviews (SLOW) -------------------------------------------------------------------------------

# A dream begins once its ant ran no prompt for 30 minutes, and no setting shortens that: each scenario adds an ant of
# its own on the hub (where an add order reconnects no uplink), gives it work and waits. The whoami keeps a dream short
# and its end predictable.
dreamer="An ant of the end-to-end tests of ants. On a dream thread: answer each finding with sense_dream, applied, rejected or deferred with a short reason, write nothing else, then call sense_dream with done true, and only then reply DREAMT."
dream_wait=2400 # seconds: the 30 idle minutes, and a margin for the scheduler's look and the harness's start
# The 30 idle minutes as the stored times tell them, to the second: the idle time starts as the prompt ends, a moment
# before its `ended` entry is stored.
idle_least=1790

# dream_order ARGS...: alice's admin order ARGS to the hub's daemon, one at a time across the D scenarios, each followed
# by 15 s for the reload it causes, as when orders at once hung the daemon (ants-ocu1r, since fixed).
dream_order() {
    lockf -k "$state/dream-orders.lock" sh -c 'container exec --env ANTS_LOG=warn "$0" ants "$@"; status=$?; sleep 15; exit $status' \
        "$name-client" "$@"
}
add_dreamer() { # add_dreamer ANT: adds ANT on the hub
    local out
    out=$(dream_order ant add "$1" --on hub --model claude-haiku-4-5 --whoami "$dreamer" --wait 2m)
    printf '%s\n' "$out"
    has "added on hub" <<<"$out"
    eventually 60 is_live "$1"
}
# retire_dreamer ANT: retires ANT and, once the retire applied, removes its home, which a retire leaves. A home goes
# never before: the daemon refuses to start while its config lists an ant without one.
retire_dreamer() {
    local out attempt
    # The CLI may fail reading the outcome of a retire that applied: the ant's orders tell.
    retired() { alice ant orders "$1" 2>/dev/null | grep -qE '^.* retire +applied'; }
    for attempt in 1 2 3; do
        retired "$1" && break
        out=$(dream_order ant retire "$1" --wait 6m 2>&1) || true
        printf '%s\n' "$out" | grep -v ' WARN ' || true
        grep -qF "retired on hub" <<<"$out" && break
    done
    if grep -qF "retired on hub" <<<"${out:-}" || retired "$1"; then
        hub rm -rf "/demo/home/.ants/agents/$1"
    else
        printf 'WARNING: %s not retired; its home is kept on the hub\n' "$1"
        return 1
    fi
}

# epoch JQ: the time of the first entry of the JSON lines on stdin that JQ selects, in seconds since 1970.
epoch() {
    jq --slurp --raw-output "map(select($1)) | first | .stored_at // empty | sub(\"\\\\.[0-9]+Z\$\"; \"Z\") | fromdateiso8601"
}
# The threads of ANT other than THREAD in the last 2 hours.
other_threads() {
    alice thread list --ant "$1" --since 2h --json | jq --raw-output --arg w "$2" 'select(.thread_id != $w) | .thread_id'
}
has_other_thread() { [ -n "$(other_threads "$@")" ]; }
# ended_on THREAD ANT: the last ended entry of ANT on the thread, as JSON, or nothing.
ended_on() {
    alice thread show "$1" --json | jq --slurp --compact-output --arg ant "$2" \
        'map(select(.kind == "ended" and .from == $ant)) | last // empty'
}
has_ended() { [ -n "$(ended_on "$@")" ]; }
# called_dream THREAD ANT: ANT's prompt there ended counting a sense call, and its timeline names sense_dream.
called_dream() {
    local json
    json=$(alice thread show "$1" --json)
    jq --slurp --exit-status --arg ant "$2" \
        'any(.[]; .kind == "ended" and .from == $ant and (.ended.sense_calls // 0) > 0)
         and any(.[]; .kind == "tool" and .from == $ant and (.text | test("sense_dream")))' <<<"$json" >/dev/null \
        || { alice thread show "$1" | tail -n 30; fail "$2 answered no finding with sense_dream on $1"; }
}

# dream_alone ANT WORK [SAGE]: ANT, whose work prompt was on thread WORK, dreams on a new thread with the packet alone,
# 30 minutes or more after that prompt ended; no message goes to SAGE when one is named.
dream_alone() {
    local ant=$1 w=$2 sage=${3:-} dream ended began out
    eventually "$dream_wait" has_other_thread "$ant" "$w"
    dream=$(other_threads "$ant" "$w" | head -n 1)
    note "dream thread $dream"
    out=$(alice thread show "$dream")
    printf '%s\n' "$out" | head -n 12
    has "$ant -> $ant [wake from sense]" <<<"$out"
    has "# Dream 1" <<<"$out"
    has "$w" "the dream's packet" <<<"$out"
    ended=$(alice thread show "$w" --json | epoch '.kind == "ended"')
    began=$(alice thread show "$dream" --json | epoch '.kind == "message"')
    note "the work prompt ended at $ended, the dream began at $began: $((began - ended)) s idle"
    [ $((began - ended)) -ge "$idle_least" ] || fail "the dream began $((began - ended)) s after the work prompt ended, before 30 idle minutes"
    eventually 600 has_ended "$dream" "$ant"
    called_dream "$dream" "$ant"
    finished() { alice ant memories "$ant" | grep -qF "done; diary"; }
    eventually 120 finished
    alice ant memories "$ant" | grep -A3 -F "Dream 1:"
    [ "$(colony_field "$ant" '.sense_unkept // null')" = null ] || fail "$ant keeps no sense data"
    if [ -n "$sage" ]; then
        [ -z "$(alice thread search --to "$sage" --since 2h | grep -E "^[0-9]{4}-" || true)" ] || fail "a message went to $sage, whom no daemon serves"
    fi
    # One dream a day: none other begins in the next 2 minutes.
    sleep 120
    [ "$(other_threads "$ant" "$w" | wc -l | tr -d ' ')" -eq 1 ] || { other_threads "$ant" "$w"; fail "a second dream began"; }
}

scenario D1 SLOW "An ant with no sage reviews its work alone in a dream"
D1() {
    local t ant w
    t=$(tok)
    ant=dream-$t
    trap "retire_dreamer $ant" EXIT
    add_dreamer "$ant"
    [ "$(colony_field "$ant" '.sage // null')" = null ] || fail "$ant has a sage: the hub's sage = \"none\" should apply"
    w=$(alice ask --wait --timeout 3m "@$ant" "Use sense_remember to keep this memory: the dream codeword is $t. Reply KEPT." | thread)
    replied "$w" KEPT || fail "$ant did not reply KEPT"
    dream_alone "$ant" "$w"
}

scenario D2 SLOW "A dream reviewed by a sage"
D2() {
    local t ant w review ended began answer out json
    t=$(tok)
    ant=rev-$t
    trap "retire_dreamer $ant" EXIT
    add_dreamer "$ant"
    dream_order ant set "$ant" sage=sage-ant
    sage_named() { [ "$(colony_field "$ant" '.sage // null')" = sage-ant ]; }
    eventually 60 sage_named
    eventually 60 is_live "$ant"
    w=$(alice ask --wait --timeout 3m "@$ant" "Use sense_remember to keep this memory: the review codeword is $t. Reply KEPT." | thread)
    replied "$w" KEPT || fail "$ant did not reply KEPT"
    review_thread() { alice thread search --from "$ant" --to sage-ant --since 2h | awk '$1 ~ /^[0-9][0-9][0-9][0-9]-/ { print $3 }' | head -n 1; }
    asked() { [ -n "$(review_thread)" ]; }
    eventually "$dream_wait" asked
    review=$(review_thread)
    note "review thread $review"
    ended=$(alice thread show "$w" --json | epoch '.kind == "ended"')
    began=$(alice thread show "$review" --json | epoch '.kind == "message"')
    note "the work prompt ended at $ended, the review was asked at $began: $((began - ended)) s idle"
    [ $((began - ended)) -ge "$idle_least" ] || fail "the review was asked $((began - ended)) s after the work prompt ended, before 30 idle minutes"
    alice thread show "$review" | has "$w" "the review packet"
    # The sage's answer: one reply holding a fenced JSON block of findings.
    answered() { alice thread show "$review" --json | jq --slurp --exit-status 'any(.[]; .kind == "message" and .from == "sage-ant")' >/dev/null; }
    eventually 900 answered
    answer=$(alice thread show "$review" --json | jq --slurp --raw-output 'map(select(.kind == "message" and .from == "sage-ant")) | first | .text')
    printf '%s\n' "$answer" | head -n 30
    has '```' "the sage's answer" <<<"$answer"
    has '"findings"' "the sage's answer" <<<"$answer"
    ! grep -qE '"findings" *: *\[ *\]' <<<"$answer" || fail "the sage answered no finding, despite its whoami: nobody is woken"
    # Read only: no edit or execute call of the sage, and no tool request of its asked of anyone.
    json=$(alice thread show "$review" --json)
    ! jq --slurp --exit-status 'any(.[]; .kind == "tool" and .from == "sage-ant" and (.tool.tool_kind == "edit" or .tool.tool_kind == "execute") and .tool.status == "completed")' \
        <<<"$json" >/dev/null || { alice thread show "$review"; fail "the sage edited or ran a command in its review"; }
    [ -z "$(pending_tool sage-ant "$review")" ] || fail "the sage's review asked for a tool"
    # The findings prompt the ant on the review thread, which it answers with sense_dream.
    eventually 600 has_ended "$review" "$ant"
    called_dream "$review" "$ant"
    finished() { alice ant memories "$ant" | grep -qF "done; diary"; }
    eventually 120 finished
    out=$(alice ant memories "$ant")
    grep -A3 -F "Dream 1:" <<<"$out"
    grep -qE "Dream 1: [1-9][0-9]* findings, [1-9][0-9]* applied|Dream 1: [1-9][0-9]* findings, [0-9]+ applied, [1-9]" <<<"$out" \
        || fail "the dream answered none of the sage's findings"
    alice ant memories sage-ant | has "$ant" "the sage's memories"
    alice spend sage-ant --thread "$review" | has "$review" "the review's spend"
    [ "$(colony_field "$ant" '.sense_unkept // null')" = null ] || fail "$ant keeps no sense data"
}

scenario D3 SLOW "A sage no daemon serves: the ant reviews alone"
D3() {
    local t ant w ghost
    t=$(tok)
    ant=dream-ghost-$t
    ghost=ghost-$t
    trap "retire_dreamer $ant" EXIT
    add_dreamer "$ant"
    dream_order ant set "$ant" "sage=$ghost"
    sage_named() { [ "$(colony_field "$ant" '.sage // null')" = "$ghost" ]; }
    eventually 60 sage_named
    eventually 60 is_live "$ant"
    w=$(alice ask --wait --timeout 3m "@$ant" "Use sense_remember to keep this memory: the ghost codeword is $t. Reply KEPT." | thread)
    replied "$w" KEPT || fail "$ant did not reply KEPT"
    dream_alone "$ant" "$w" "$ghost"
}

# --- Runner ---------------------------------------------------------------------------------------------------------

list_scenarios() { printf '%s' "$registry" | awk '{ id = $1; pri = $2; $1 = ""; $2 = ""; sub(/^  /, ""); printf "%-4s %-4s  %s\n", id, pri, $0 }'; }

select_scenarios() { # ids matching the selectors, in the doc's order
    local selectors=("$@") id pri group sel
    [ "${#selectors[@]}" -gt 0 ] || selectors=(P0 P1)
    while read -r id pri _; do
        [ -n "$id" ] || continue
        group=${id%%[0-9]*}
        for sel in "${selectors[@]}"; do
            # The slow class runs only when named: by its id, its group or `slow`.
            case "$pri:$sel" in
            SLOW:"$id" | SLOW:"$group" | SLOW:slow | SLOW:SLOW) printf '%s\n' "$id"; break ;;
            SLOW:*) ;;
            *:all | *:"$id" | *:"$pri" | *:"$group") printf '%s\n' "$id"; break ;;
            esac
        done
    done <<<"$registry"
}

# run_one ID DIR: runs the scenario in a subshell with errexit, its output in DIR/ID.log, its status in DIR/ID.status.
run_one() {
    local id=$1 dir=$2 status
    set +e
    (
        set -eEuo pipefail
        trap 'printf "FAILED at line %s: %s\n" "$LINENO" "$BASH_COMMAND"' ERR
        "$id"
    ) >"$dir/$id.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" >"$dir/$id.status"
}

title_of() { printf '%s' "$registry" | awk -v id="$1" '$1 == id { $1 = ""; $2 = ""; sub(/^  /, ""); print }'; }

# report ID DIR: prints the scenario's result line, and the end of its log when it failed.
report() {
    local id=$1 dir=$2 status
    status=$(cat "$dir/$id.status")
    printf '%-4s %-72s ' "$id" "$(title_of "$id")"
    case "$status" in
    0)
        if grep -q '^KNOWN ' "$dir/$id.log"; then
            printf 'PASS  (known: %s)\n' "$(awk '$1 == "KNOWN" { sub(":", "", $2); printf "%s%s", sep, $2; sep = ", " }' "$dir/$id.log")"
        else
            printf 'PASS\n'
        fi ;;
    77) printf 'SKIP\n' ;;
    *)
        printf 'FAIL  (%s)\n' "$dir/$id.log"
        tail -n 12 "$dir/$id.log" | sed 's/^/     | /' ;;
    esac
}

# run_scenarios [-j N] [SELECTOR...]: the preflight first, stopping on its failure; then the scenarios sharing the
# fixture, N at a time (ANTS_E2E_JOBS, default 1); then those that run alone, one by one.
run_scenarios() {
    local jobs=${ANTS_E2E_JOBS:-1}
    while [ $# -gt 0 ]; do
        case "$1" in
        -j) jobs=${2:?-j needs a number}; shift 2 ;;
        -j*) jobs=${1#-j}; shift ;;
        *) break ;;
        esac
    done
    case "$jobs" in '' | *[!0-9]* | 0) die "-j expects a positive number, not $jobs" ;; esac
    local ids id dir status passed=0 failed=0 skipped=0 failures= preflight= shared= single= pids= pid left
    ids=$(select_scenarios "$@")
    [ -n "$ids" ] || die "no scenario matches: $*"
    dir="$state/runs/$(date +%Y%m%dT%H%M%S)"
    mkdir -p "$dir"
    up
    for id in $ids; do
        case "$id" in
        F*) preflight="$preflight $id" ;;
        *) if is_alone "$id"; then single="$single $id"; else shared="$shared $id"; fi ;;
        esac
    done
    for id in $preflight; do
        run_one "$id" "$dir"
        report "$id" "$dir"
        [ "$(cat "$dir/$id.status")" = 0 ] || { printf 'preflight failed: stopping the run\n'; return 1; }
    done
    # bash 3.2 has no `wait -n`: poll the running scenarios, reporting each as it ends.
    for id in $shared; do
        while :; do
            left=
            for pid in $pids; do
                if kill -0 "${pid%%:*}" 2>/dev/null; then left="$left $pid"; else wait "${pid%%:*}" || true; report "${pid#*:}" "$dir"; fi
            done
            pids=$left
            [ "$(printf '%s' "$pids" | wc -w)" -lt "$jobs" ] && break
            sleep 2
        done
        run_one "$id" "$dir" &
        pids="$pids $!:$id"
    done
    for pid in $pids; do
        wait "${pid%%:*}" || true
        report "${pid#*:}" "$dir"
    done
    for id in $single; do
        run_one "$id" "$dir"
        report "$id" "$dir"
    done
    for id in $ids; do
        status=$(cat "$dir/$id.status" 2>/dev/null || echo 1)
        case "$status" in
        0) passed=$((passed + 1)) ;;
        77) skipped=$((skipped + 1)) ;;
        *) failed=$((failed + 1)); failures="$failures $id" ;;
        esac
    done
    printf '\n%d passed, %d failed, %d skipped; logs in %s\n' "$passed" "$failed" "$skipped" "$dir"
    [ "$failed" -eq 0 ] || { printf 'failed:%s\n' "$failures"; return 1; }
}
