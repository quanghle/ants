# The scenarios of docs/e2e-scenarios.md, sourced by scripts/e2e.sh (bash 3.2: no associative arrays).
# Each scenario is a function named by its id, run in a subshell with errexit: any failing command or check fails it,
# exit status 77 skips it. A scenario leaves the fixture as it found it, its traps undoing what it changed.

registry=
scenario() { registry="$registry$1 $2 $3"$'\n'; }
# Scenarios that change what the others rely on (a node, an uplink, an ant's settings or pause, the chat's last
# thread), run one at a time after the rest; the others share the fixture and run side by side with `run -j N`.
alone=" R1 R2 R3 R4 R5 R6 H1 H2 H3 O1 O2 O3 O6 O7 S4 T4 U4 U5 U7 U8 X1 K1 "
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
