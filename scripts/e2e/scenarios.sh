# The scenarios of docs/e2e-scenarios.md, sourced by scripts/e2e.sh (bash 3.2: no associative arrays).
# Each scenario is a function named by its id, run in a subshell with errexit: any failing command or check fails it,
# exit status 77 skips it. A scenario leaves the fixture as it found it, its traps undoing what it changed.

registry=
scenario() { registry="$registry$1 $2 $3"$'\n'; }
# Scenarios that change what the others rely on (a node, an uplink, an ant's settings or pause, the chat's last
# thread), run one at a time after the rest; the others share the fixture and run side by side with `run -j N`.
alone=" R1 R2 R3 R4 R5 R6 H1 H2 H3 O1 O2 O3 O6 O7 S4 T4 U4 U5 U7 U8 X1 K1 E2 E3 E6 "
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
all_live() { wait_live hub-ant leaf-ant gate-ant system-messenger; }

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
    for ant in hub-ant leaf-ant gate-ant system-messenger; do
        jq --slurp --exit-status --arg ant "$ant" \
            'any(.[]; .ant == $ant and .state == "live" and .harness == "claude")' <<<"$colony" >/dev/null \
            || { printf '%s\n' "$colony"; fail "$ant is not live on claude"; }
    done
    for ant in hub-ant system-messenger; do
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
        "Read the attached file with read_attachment. Write its content unchanged to out.txt, then call attach_to_reply with out.txt. Only once attach_to_reply has succeeded, reply DONE: a reply without out.txt attached fails this test.")
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
    alice post '#e2e' "The ant on the hub machine: reply with exactly TRIAGE-$t" >/dev/null
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

# --- E. Escalations -------------------------------------------------------------------------------------------------

chained_to() { colony_field "$1" '(.escalate_to // []) | join(",")'; } # chained_to ANT: its escalate_to, comma-joined
chain() { # chain ANT LEADER: sets the ant's escalate_to to LEADER and waits for the colony to show it
    alice ant set "$1" "escalate_to=[\"$2\"]" >/dev/null
    shows_chain() { [ "$(chained_to "$1")" = "$2" ]; }
    eventually 60 shows_chain "$1" "$2"
}
unchain() { alice ant set "$1" escalate_to= >/dev/null 2>&1 || true; } # unchain ANT: for traps
# done_with: for traps, marks the scenario's thread ($th, global so the trap still sees it) done, out of home's way.
done_with() { [ -z "${th:-}" ] || alice thread done "$th" >/dev/null 2>&1 || true; }
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
    trap done_with EXIT
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
    trap 'done_with; unchain leaf-ant; alice ant set hub-ant whoami= >/dev/null 2>&1 || true; alice ant set leaf-ant whoami= >/dev/null 2>&1 || true' EXIT
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
    if escalated_to "$th" alice; then
        known ants-iwqd6 "leaf-ant and hub-ant traded replies after the answer until a turn_budget escalation to alice"
    else
        ! listed_escalated "$th" >/dev/null || fail "thread list --escalated lists the thread"
    fi
}

scenario E3 P1 "The leader passes it on to the human"
E3() {
    local t out
    trap 'done_with; unchain leaf-ant; alice ant set hub-ant whoami= >/dev/null 2>&1 || true; alice ant set leaf-ant whoami= >/dev/null 2>&1 || true' EXIT
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
        || known ants-iwqd6 "leaf-ant and hub-ant traded replies after the pass until a turn_budget escalation to alice"
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
    trap "done_with; unchain gate-ant; alice ant set hub-ant whoami= >/dev/null 2>&1 || true; leaf rm -f /demo/gate/chain-$t; rm -rf '$dir'" EXIT
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
        || known ants-iwqd6 "gate-ant and hub-ant traded replies after the decision until a turn_budget escalation to alice"
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
    printf '%s\n' "$orders" | head -n 5
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
    container exec "$name-client" python3 /tmp/ants-e2e-chat.py "$@"
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

# --- Runner ---------------------------------------------------------------------------------------------------------

list_scenarios() { printf '%s' "$registry" | awk '{ id = $1; pri = $2; $1 = ""; $2 = ""; sub(/^  /, ""); printf "%-4s %s  %s\n", id, pri, $0 }'; }

select_scenarios() { # ids matching the selectors, in the doc's order
    local selectors=("$@") id pri group sel
    [ "${#selectors[@]}" -gt 0 ] || selectors=(P0 P1)
    while read -r id pri _; do
        [ -n "$id" ] || continue
        group=${id%%[0-9]*}
        for sel in "${selectors[@]}"; do
            case "$sel" in
            all | "$id" | "$pri" | "$group") printf '%s\n' "$id"; break ;;
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
