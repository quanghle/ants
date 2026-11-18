# Agent guide

## Work tracking: bd

All work and specs live in bd (`.bd/bd.db`). Do not keep TODO lists in markdown.

- `bd ready` lists unblocked work; `bd claim <id>` takes one under a lease.
- `bd show <id>` shows details; `bd close <id> -r "<what was done>"` closes it.
- Record discovered work with `bd create ... --dep discovered-from:<id>`.
- Specs: `bd spec list`, `bd spec show <area>`. Areas: wire, transport, daemon, harness, memory, cli, efficiency,
  messenger, channels, webhooks, admin, assistant, sense (with sense/identity, sense/memory, sense/brief, sense/work,
  sense/triggers, sense/store, sense/dreams).
- Link every piece of work to the requirement it serves (`bd spec link <id> <requirement>`) or mark it
  `bd spec none <id> -r "<why>"`. `bd spec drift` must report 0 problems.
- A behaviour change that contradicts a requirement goes through `bd spec change`, not a silent edit.
- The database is local; refresh the committed snapshot with `bd export -o .bd/issues.jsonl` in each commit that
  changes issues or specs. Release notes (`scripts/release-notes.sh`) are built from that snapshot, so the commit a
  release tag points at must carry an up-to-date export.

## Code

- Rust 2024, one workspace: `ants-proto` (wire contract, no I/O), `ants-sense` (built-in identity, memory and work
  provider, no daemon dependency), `ants-daemon` (NATS + ACP), `ants` (binary), `ants-test-support` (dev-only test
  helpers: tools on PATH, throwaway nats-server).
- Keep the footprint small: tokio current-thread, minimal features, no per-session helper processes.
- TLS stays on rustls.
- A daemon acks a JetStream message only after its effects are stored.
- Also in the repo: `contrib/` (launchd and systemd units, agent skill), `scripts/` (`check.sh`,
  `install-units.sh`, `footprint.sh`, `release-notes.sh`, `nats-server.sh`), `docs/` (`architecture.md`, `sense.md`, `cli.md`,
  `config.md`, `auth.md`, `webhooks.md`, `transport.md`, `hooks.md`, `operations.md`, `container-demo.md`),
  `deny.toml` (dependency policy), `.cargo/config.toml` (7-day dependency cooldown), `rust-toolchain.toml` (pinned
  toolchain; moves together with `rust-version` in `Cargo.toml` and the `toolchain:` of every CI job).
- `crates/ants/nats-server.sha256` pins the nats-server release bundled with ants: release archives, `ants setup`
  downloads and CI's tests all take it from there.
- Comment only code that needs clarification.
- Modules are `x.rs` with submodules in `x/`, never `x/mod.rs`. Unit tests of `x.rs` start inline
  (`#[cfg(test)] mod tests { … }` at its end); once they outgrow the code they test, they move to `x/tests.rs`
  (`#[cfg(test)] mod tests;`); once that grows too large, `x/tests.rs` keeps the shared helpers and declares
  submodules in `x/tests/`, one per behaviour (`x/tests/routing.rs`, …).

## Checks

Before finishing, run `scripts/check.sh`: the checks of CI's (`.github/workflows/ci.yml`) check and deny jobs, stopping
at the first failure. The check job's release build and footprint check (`scripts/footprint.sh`, idle `ants daemon`
with its ant's sense data open under 10 MB of private memory) run only with `scripts/check.sh --footprint`, as the
fat-LTO release build is slow: pass it for changes that may grow the daemon's memory or binary. The summary lists the
skipped checks.

- `cargo fmt --all --check`, `cargo clippy --all-targets`, `cargo test`, all with `CARGO_BUILD_WARNINGS=deny` as in
  CI, so any lint warning fails.
- Tests needing `nats-server` skip when it is not on PATH; `ANTS_REQUIRE_TOOLS=1` makes them fail instead. The script
  sets it when `nats-server` is on PATH.
- `ANTS_TEST_JS_ACK_FC_V2=1` runs the test servers with nats-server 2.16's v2 ack and flow control subjects; CI and
  the script run `cargo test` both ways.
- `cargo build --release` and `scripts/footprint.sh --ants target/release/ants --port 14722 --max-mb 10` with
  `--footprint`; the footprint check skips where `scripts/footprint.sh` cannot measure (Linux and macOS only, needs
  `nats-server`, and `footprint` on macOS).
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
