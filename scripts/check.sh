#!/usr/bin/env bash
# Run the checks of CI (.github/workflows/ci.yml) locally: formatting, lints, the test suite twice (as is and with
# nats-server 2.16's v2 ack and flow control subjects) and cargo-deny, all with lint warnings fatal.
#
#   scripts/check.sh            # every check; stops at the first failure
#
# Without nats-server on PATH, the tests needing it skip instead of failing (no ANTS_REQUIRE_TOOLS=1). Without
# cargo-deny (`cargo install --locked cargo-deny`), the dependency check is skipped. The summary lists what was skipped.
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

while [ $# -gt 0 ]; do
    case "$1" in
    -h | --help) sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
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

run cargo fmt --all --check
run cargo clippy --all-targets
run env ANTS_REQUIRE_TOOLS=$require_tools ANTS_TEST_JS_ACK_FC_V2=0 cargo test
run env ANTS_REQUIRE_TOOLS=$require_tools ANTS_TEST_JS_ACK_FC_V2=1 cargo test

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
