#!/usr/bin/env bash
# Run the checks of CI's check and deny jobs (.github/workflows/ci.yml) locally: formatting, lints, rustdoc, the test
# suite twice (as is and with nats-server 2.16's v2 ack and flow control subjects), cargo-deny and, with --footprint,
# the release build and the idle footprint check, all with lint and rustdoc warnings fatal.
#
#   scripts/check.sh               # every check but the release build and footprint; stops at the first failure
#   scripts/check.sh --footprint   # also `cargo build --release` and scripts/footprint.sh
#
# Without nats-server on PATH, the tests needing it skip instead of failing (no ANTS_REQUIRE_TOOLS=1). Without
# cargo-deny (`cargo install --locked cargo-deny`), the dependency check is skipped. With --footprint, the footprint
# check is skipped where scripts/footprint.sh cannot measure: off Linux and macOS, without nats-server, or on macOS
# without `footprint`. The summary lists what was skipped.
set -euo pipefail

die() {
    printf 'check: %s\n' "$*" >&2
    exit 1
}

skipped=()
skip() {
    skipped+=("$1")
    printf 'check: skipping %s\n' "$1" >&2
}

run() {
    printf '\n==> %s\n' "$*" >&2
    "$@"
}

footprint=0
while [ $# -gt 0 ]; do
    case "$1" in
    --footprint) footprint=1; shift ;;
    -h | --help) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
    esac
done

cd "$(dirname "$0")/.."

export CARGO_BUILD_WARNINGS=deny

# The tests take any value but 1 as unset.
require_tools=1
if ! command -v nats-server >/dev/null 2>&1; then
    require_tools=0
    skip "the tests needing nats-server (not on PATH)"
fi

# Why scripts/footprint.sh cannot measure here, if it cannot.
footprint_unmeasurable() {
    case "$(uname -s)" in
    Linux) ;;
    Darwin) command -v footprint >/dev/null 2>&1 || { echo "footprint not found"; return; } ;;
    *) echo "Linux and macOS only"; return ;;
    esac
    command -v nats-server >/dev/null 2>&1 || echo "nats-server not on PATH"
}

run cargo fmt --all --check
run cargo clippy --all-targets
run env RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --document-private-items
run env ANTS_REQUIRE_TOOLS=$require_tools ANTS_TEST_JS_ACK_FC_V2=0 cargo test
run env ANTS_REQUIRE_TOOLS=$require_tools ANTS_TEST_JS_ACK_FC_V2=1 cargo test

if [ $footprint -eq 0 ]; then
    skip "the release build and footprint check (pass --footprint)"
else
    run cargo build --release
    why=$(footprint_unmeasurable)
    if [ -z "$why" ]; then
        run scripts/footprint.sh --ants target/release/ants --port 14722 --max-mb 10
    else
        skip "the footprint check ($why)"
    fi
fi

if cargo deny --version >/dev/null 2>&1; then
    run cargo deny --locked check
else
    skip "cargo deny check (cargo-deny not installed: cargo install --locked cargo-deny)"
fi

if [ ${#skipped[@]} -eq 0 ]; then
    printf '\ncheck: all checks passed\n' >&2
else
    printf '\ncheck: checks passed, except the skipped:\n' >&2
    printf '  %s\n' "${skipped[@]}" >&2
fi
