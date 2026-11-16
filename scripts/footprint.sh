#!/bin/sh
# Resident memory of an idle `ants daemon` serving one ant, against a throwaway nats-server.
#
#   scripts/footprint.sh                          # idle RSS; fails at or above 20 MB
#   scripts/footprint.sh --ask "reply with ok"    # also one harness session (runs the real harness)
#   scripts/footprint.sh --metrics 127.0.0.1:9464 # idle RSS with health and metrics served and scraped
#
# Options:  --ants PATH     ants binary (default: $ANTS_BIN, else target/release/ants, built when missing)
#           --port N        nats-server port (default: $FOOTPRINT_PORT or 14722)
#           --max-mb N      idle RSS limit in MB (default: $FOOTPRINT_MAX_MB or 20)
#           --harness NAME  harness of the ant (default: $FOOTPRINT_HARNESS or copilot)
#           --ask TEXT      send TEXT to the ant and measure again with its harness session live
#           --metrics ADDR  serve health and metrics on ADDR, and scrape both before each idle sample (needs curl)
# Env:      NATS_SERVER (default: nats-server on PATH), FOOTPRINT_DIR (parent of the scratch directory,
#           default: $TMPDIR or /tmp), FOOTPRINT_SAMPLES (idle samples, 1 s apart, default 5),
#           FOOTPRINT_ASK_TIMEOUT (seconds, default 180)
#
# RSS comes from `ps -o rss=` (KB on Linux and macOS). The idle figure is the largest sample.
set -eu

die() {
    printf 'footprint: %s\n' "$*" >&2
    exit 1
}

root=$(cd "$(dirname "$0")/.." && pwd)
ants=${ANTS_BIN:-}
port=${FOOTPRINT_PORT:-14722}
max_mb=${FOOTPRINT_MAX_MB:-20}
harness=${FOOTPRINT_HARNESS:-copilot}
ask=
metrics=
samples=${FOOTPRINT_SAMPLES:-5}
ask_timeout=${FOOTPRINT_ASK_TIMEOUT:-180}
nats_server=${NATS_SERVER:-nats-server}

while [ $# -gt 0 ]; do
    case "$1" in
    --ants) ants=$2; shift 2 ;;
    --port) port=$2; shift 2 ;;
    --max-mb) max_mb=$2; shift 2 ;;
    --harness) harness=$2; shift 2 ;;
    --ask) ask=$2; shift 2 ;;
    --metrics) metrics=$2; shift 2 ;;
    -h | --help) sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
    esac
done

if [ -z "$ants" ]; then
    ants=$root/target/release/ants
    if [ ! -x "$ants" ]; then
        (cd "$root" && cargo build --release --quiet) || die "building ants"
    fi
fi
[ -x "$ants" ] || die "no ants binary at $ants"
command -v "$nats_server" >/dev/null 2>&1 || die "nats-server not found (set NATS_SERVER)"
[ -z "$metrics" ] || command -v curl >/dev/null 2>&1 || die "curl not found, needed with --metrics"

work=$(mktemp -d "${FOOTPRINT_DIR:-${TMPDIR:-/tmp}}/ants-footprint.XXXXXX")
nats_pid=
daemon_pid=
ask_pid=
cleanup() {
    for pid in $ask_pid $daemon_pid; do
        kill -INT "$pid" 2>/dev/null || true
    done
    for pid in $ask_pid $daemon_pid; do
        i=0
        while kill -0 "$pid" 2>/dev/null && [ $i -lt 50 ]; do
            sleep 0.2
            i=$((i + 1))
        done
        kill -KILL "$pid" 2>/dev/null || true
    done
    if [ -n "$nats_pid" ]; then
        kill "$nats_pid" 2>/dev/null || true
        wait "$nats_pid" 2>/dev/null || true
    fi
    rm -rf "$work"
}
trap cleanup EXIT
trap 'exit 1' INT TERM HUP

# Waits up to $3 seconds for pattern $2 in log $1 while process $4 lives.
wait_for() {
    i=0
    while ! grep -q "$2" "$1" 2>/dev/null; do
        if ! kill -0 "$4" 2>/dev/null; then
            cat "$1" >&2
            die "process exited before: $2"
        fi
        if [ $i -ge $(($3 * 5)) ]; then
            cat "$1" >&2
            die "timed out waiting for: $2"
        fi
        sleep 0.2
        i=$((i + 1))
    done
}

rss_kb() {
    kb=$(ps -o rss= -p "$1" | tr -d ' ')
    [ -n "$kb" ] || die "process $1 is gone"
    echo "$kb"
}

# Summed RSS of every descendant of process $1, in KB.
tree_rss_kb() {
    ps -A -o pid= -o ppid= -o rss= | awk -v root="$1" '
        { parent[$1] = $2; rss[$1] = $3 }
        END {
            for (p in parent) {
                for (q = parent[p]; q != "" && q > 1; q = parent[q]) {
                    if (q == root) { sum += rss[p]; break }
                }
            }
            print sum + 0
        }'
}

mb() {
    awk -v kb="$1" 'BEGIN { printf "%.1f MB", kb / 1024 }'
}

mkdir -p "$work/js" "$work/home" "$work/workdir"
"$nats_server" -js -a 127.0.0.1 -p "$port" -sd "$work/js" >"$work/nats.log" 2>&1 &
nats_pid=$!
wait_for "$work/nats.log" "Server is ready" 15 "$nats_pid"

url=nats://127.0.0.1:$port
ANTS_LOG=${ANTS_LOG:-info} "$ants" --nats "$url" daemon --ant footprint --harness "$harness" \
    --workdir "$work/workdir" --home "$work/home" ${metrics:+--metrics "$metrics"} >"$work/daemon.log" 2>&1 &
daemon_pid=$!
wait_for "$work/daemon.log" "serving inbox" 30 "$daemon_pid"

sleep 2
idle=0
n=0
while [ $n -lt "$samples" ]; do
    if [ -n "$metrics" ]; then
        curl -fsS "http://$metrics/healthz" >/dev/null || die "GET /healthz failed"
        curl -fsS "http://$metrics/metrics" >"$work/metrics.txt" || die "GET /metrics failed"
    fi
    kb=$(rss_kb "$daemon_pid")
    if [ "$kb" -gt "$idle" ]; then idle=$kb; fi
    n=$((n + 1))
    if [ $n -lt "$samples" ]; then sleep 1; fi
done

printf 'ants       %s (%s)\n' "$("$ants" --version)" "$(uname -sm)"
printf 'idle       %s (%s KB), limit %s MB\n' "$(mb "$idle")" "$idle" "$max_mb"
[ -z "$metrics" ] || printf 'metrics    http://%s/metrics, %s lines\n' "$metrics" "$(wc -l <"$work/metrics.txt" | tr -d ' ')"

if [ -n "$ask" ]; then
    "$ants" --nats "$url" ask --wait @footprint "$ask" >"$work/ask.log" 2>&1 &
    ask_pid=$!
    i=0
    while kill -0 "$ask_pid" 2>/dev/null; do
        if [ $i -ge $((ask_timeout * 5)) ]; then
            cat "$work/daemon.log" >&2
            die "no reply within ${ask_timeout}s"
        fi
        sleep 0.2
        i=$((i + 1))
    done
    if ! wait "$ask_pid"; then
        cat "$work/ask.log" "$work/daemon.log" >&2
        die "ants ask failed"
    fi
    ask_pid=
    sleep 2
    session=$(rss_kb "$daemon_pid")
    harness=$(tree_rss_kb "$daemon_pid")
    printf 'session    %s (%s KB), %s over idle\n' "$(mb "$session")" "$session" "$(mb $((session - idle)))"
    printf 'harness    %s (%s KB), every harness process\n' "$(mb "$harness")" "$harness"
fi

[ "$idle" -lt $((max_mb * 1024)) ] || die "idle RSS $(mb "$idle") is at or above $max_mb MB"
