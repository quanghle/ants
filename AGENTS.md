# Agent guide

## Work tracking: bd

All work and specs live in bd (`.bd/bd.db`). Do not keep TODO lists in markdown.

- `bd ready` lists unblocked work; `bd claim <id>` takes one under a lease.
- `bd show <id>` shows details; `bd close <id> -r "<what was done>"` closes it.
- Record discovered work with `bd create ... --dep discovered-from:<id>`.
- Specs: `bd spec list`, `bd spec show <area>`. Areas: wire, transport, daemon, harness, memory, cli, efficiency,
  messenger, channels, webhooks, admin, assistant.
- Link every piece of work to the requirement it serves (`bd spec link <id> <requirement>`) or mark it
  `bd spec none <id> -r "<why>"`. `bd spec drift` must report 0 problems.
- A behaviour change that contradicts a requirement goes through `bd spec change`, not a silent edit.
- The database is local; refresh the committed snapshot with `bd export -o .bd/issues.jsonl` in each commit that
  changes issues or specs. Release notes (`scripts/release-notes.sh`) are built from that snapshot, so the commit a
  release tag points at must carry an up-to-date export.

## Code

- Rust 2024, one workspace: `ants-proto` (wire contract, no I/O), `ants-daemon` (NATS + ACP), `ants` (binary),
  `ants-test-support` (dev-only test helpers: tools on PATH, throwaway nats-server).
- Keep the footprint small: tokio current-thread, minimal features, no per-session helper processes.
- TLS stays on rustls.
- A daemon acks a JetStream message only after its effects are stored.
- Also in the repo: `contrib/` (launchd and systemd units, agent skill), `scripts/` (`check.sh`,
  `install-units.sh`, `footprint.sh`, `release-notes.sh`), `docs/` (`architecture.md`, `cli.md`, `config.md`,
  `auth.md`, `webhooks.md`, `transport.md`, `hooks.md`, `operations.md`, `container-demo.md`),
  `deny.toml` (dependency policy), `.cargo/config.toml` (7-day dependency cooldown), `rust-toolchain.toml` (pinned
  toolchain; moves together with `rust-version` in `Cargo.toml` and the `toolchain:` of every CI job).
- Comment only code that needs clarification.

## Checks

Before finishing, run `scripts/check.sh`: the checks of CI's (`.github/workflows/ci.yml`) check and deny jobs, stopping
at the first failure.

- `cargo fmt --all --check`, `cargo clippy --all-targets`, `cargo test`, all with `CARGO_BUILD_WARNINGS=deny` as in
  CI, so any lint warning fails.
- Tests needing `nats-server` skip when it is not on PATH; `ANTS_REQUIRE_TOOLS=1` makes them fail instead. The script
  sets it when `nats-server` is on PATH.
- `ANTS_TEST_JS_ACK_FC_V2=1` runs the test servers with nats-server 2.16's v2 ack and flow control subjects; CI and
  the script run `cargo test` both ways.
- `cargo deny --locked check` (skipped without cargo-deny). A new license goes into `deny.toml`'s allow-list only if
  permissive.
- Dependency cooldown: `cargo update` and `cargo add` take only versions published at least 7 days ago, and a
  manifest requirement on a newer one fails to resolve: require the version `cargo add` picks or the one in
  `Cargo.lock`. `cargo update -p CRATE` can step a locked version younger than 7 days back down; keep such a
  downgrade out of a commit unless intended. For a needed newer release (a security fix):
  `CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE=allow cargo update -p CRATE --precise VERSION`.

## Docs

- `README.md` and `docs/` document what exists on main: update them with any change to the CLI, config file, wire or
  services.
- User-facing text (docs, CLI messages) avoids pronouns: impersonal or imperative phrasing.
