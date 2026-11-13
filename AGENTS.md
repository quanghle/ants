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

- Rust 2024, one workspace: `ants-proto` (wire contract, no I/O), `ants-daemon` (NATS + ACP), `ants` (binary).
- Keep the footprint small: tokio current-thread, minimal features, no per-session helper processes.
- TLS stays on rustls.
- Before finishing: `cargo fmt`, `cargo clippy --all-targets`, `cargo test`.
- A daemon acks a JetStream message only after its effects are stored.
