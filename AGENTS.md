# Agent guide

## Work tracking: bd

All work and specs live in bd (`.bd/bd.db`). Do not keep TODO lists in markdown.

- `bd ready` lists unblocked work; `bd claim <id>` takes one under a lease.
- `bd show <id>` shows the details; `bd close <id> -r "<what was done>"` closes it.
- Record new work found along the way with `bd create ... --dep discovered-from:<id>`.
- Specs: `bd spec list`, `bd spec show <area>`. Areas: wire, transport, daemon, harness, memory, cli, efficiency,
  messenger.
- Link every piece of work to the requirement it serves (`bd spec link <id> <requirement>`) or mark it
  `bd spec none <id> -r "<why>"`. `bd spec drift` must report 0 problems.
- A behaviour change that contradicts a requirement goes through `bd spec change`, not a silent edit.

## Code

- Rust 2024, one workspace: `ants-proto` (wire contract, no I/O), `ants-daemon` (NATS + ACP), `ants` (binary),
  `ants-test-support` (dev-only test helpers: tools on PATH, a throwaway nats-server).
- Keep the footprint small: tokio current-thread, minimal features, no per-session helper processes.
- TLS stays on rustls.
- A daemon acks a JetStream message only after its effects are stored.
- Also in the repo: `contrib/` (launchd and systemd units), `scripts/` (`install-units.sh`, `footprint.sh`),
  `docs/transport.md` (hub, proxy, leaf and tunnel findings), `deny.toml` (dependency policy).
- Comment only code that needs clarification.

## Checks

Before finishing, as CI (`.github/workflows/ci.yml`) does:

- `cargo fmt`, `cargo clippy --all-targets` (no warnings), `cargo test`.
- Tests needing `nats-server` skip when it is not on PATH; `ANTS_REQUIRE_TOOLS=1` makes them fail instead.
- `cargo deny check` after changing dependencies. A new license goes into `deny.toml`'s allow-list only if
  permissive.

## Docs

- README.md documents what exists on main: update it with any change to the CLI, config file, wire or services.
- User-facing text (docs, CLI messages) avoids pronouns: impersonal or imperative phrasing.
