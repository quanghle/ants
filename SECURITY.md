# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/quanghle/ants/security/advisories/new), not in a public
issue or pull request. Include the affected version or commit, steps to reproduce and the impact.

## Supported versions

Only the latest commit on `main` receives fixes.

## Scope

Areas of particular interest: NATS credential and permission handling (`ants auth`, `ants enroll`), tool approval
and allowlist enforcement, webhook signature verification, the HTTPS bridge and the per-session MCP server.
