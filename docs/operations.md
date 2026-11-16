# Operations, services, monitoring and footprint

## Platforms and upgrades

- **Linux & macOS:** supported natively with Unix domain sockets (`0600`) for the stdio MCP bridge, `0600` secret file modes, and user services via `systemd --user` or `launchd`.
- **Windows:** build with MSVC or cross-build `--target x86_64-pc-windows-gnu` (mingw-w64 on `PATH`). Stdio MCP uses a current-user named pipe (`\\.\pipe\ants-mcp-<random>`); bare harness names resolve `.exe`/`.cmd`/`.bat` on `PATH`; `ants daemon` handles Ctrl-C/Ctrl-Break/close/shutdown as `SIGTERM` and auto-reloads edited config files (`webhooks.toml` included). `scripts/install-units.sh` and automatic service reloads in `ants setup`, `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant`, and `ants tools approve --rule` cover launchd and systemd only (printing manual commands on Windows; pass `--upgrade-check 0` under Task Scheduler). Secret files inherit `%USERPROFILE%` ACLs instead of Unix `0600`.
- **Binary upgrades:** `ants daemon`, `ants channel serve`, `ants webhook serve`, `ants signer`, `ants bridge serve`, and `ants bridge connect` check every `30s` (`--upgrade-check DURATION` / `ANTS_UPGRADE_CHECK`, `0` disables) whether their executable on disk was replaced by a settled, non-empty executable file, then exit with status `75` (`ants daemon` drains prompts in flight first and `ants webhook serve` finishes in-flight requests, as on `SIGTERM`). Installed launchd and systemd units restart automatically on status `75`.

## Services

`ants setup` installs single-machine `nats` and `daemon` units. For multi-machine hubs, workers, and tunnels, `scripts/install-units.sh` renders and starts `contrib/launchd/` (`~/Library/LaunchAgents`) or `contrib/systemd/` (`~/.config/systemd/user`) units:

```sh
scripts/install-units.sh                                                                    # nats + daemon
scripts/install-units.sh --devtunnel ants-hub.usw2                                          # + devtunnel connect
scripts/install-units.sh --no-daemon --channel-manager ~/.config/ants/auth/channel-manager.creds --webhook --signer
scripts/install-units.sh --bridge https://hub.example/bridge --bridge-token ~/.config/ants/bridge.token
scripts/install-units.sh --out ./rendered-units                                             # render only
```

Options: `--ants PATH`, `--nats-server PATH`, `--nats-conf FILE` (default `~/.config/ants/leaf.conf`), `--config FILE` (default `~/.ants/config.toml`), `--no-daemon`, `--channel-manager CREDS`, `--signer`, `--webhook`, `--webhook-config FILE` (default `~/.ants/webhooks.toml`), `--devtunnel TUNNEL`, `--devtunnel-host TUNNEL`, `--devtunnel-bin PATH`, `--devtunnel-token FILE`, `--bridge URL`, `--bridge-serve`, `--bridge-token FILE`, `--bridge-proxy URL`, `--bridge-ca FILE` (`SSL_CERT_FILE`), `--out DIR`. `--devtunnel` and `--bridge` are mutually exclusive (`127.0.0.1:7422`). Installs shell completions unless `--out` is set.

| Service | macOS (`~/Library/Logs/ants/`) | Linux & WSL (`systemd --user`) | Installed when |
|---|---|---|---|
| NATS (`nats-server`) | `ants.nats` (`nats.log`) | `ants-nats.service` | Always |
| Ant daemon (`ants daemon`) | `ants.daemon` (`daemon.log`) | `ants-daemon.service` | Default (omit with `--no-daemon`) |
| Channel manager (`ants channel serve`) | `ants.channels` (`channels.log`) | `ants-channels.service` | `--channel-manager CREDS` |
| Webhook ingress (`ants webhook serve`) | `ants.webhook` (`webhook.log`) | `ants-webhook.service` | `--webhook` |
| Signer (`ants signer`) | `ants.signer` (`signer.log`) | `ants-signer.service` | `--signer` |
| Dev Tunnel (`devtunnel connect` / `host`) | `ants.devtunnel[-host]` | `ants-devtunnel[-host].service` | `--devtunnel` / `--devtunnel-host` |
| HTTPS bridge (`ants bridge connect` / `serve`) | `ants.bridge[-serve]` | `ants-bridge[-serve].service` | `--bridge URL` / `--bridge-serve` |
| Log rotation (macOS) | `ants.logrotate` (`logrotate.log`) | — (`journald` rotates) | Always on macOS |

- **Reload & restart:** `ants.daemon` and `ants.webhook` auto-reload edited config files (~6 s) and accept `SIGHUP` (`launchctl kill SIGHUP gui/$(id -u)/<label>`, `systemctl --user reload <unit>`); `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant`, and `ants tools approve --rule` reload or restart matching running units automatically. For renewed leaf uplink credentials (`ants add --enroll`, `ants renew`, `request_ant`, admin `add`/`clone`/`retire` orders), `ants.nats` gets a reload that reconnects only the uplink while keeping local client connections.
- **Logs & linger:** on macOS, `ants.logrotate` copy-truncates logs > 10 MiB hourly (keeping `.1`..`.5.gz`). On Linux, enable `loginctl enable-linger $USER` (`[boot] systemd=true` in `/etc/wsl.conf` on WSL 2). Remove units with `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`.

## Monitoring

`ants daemon`, `ants webhook serve`, `ants bridge serve`, `ants bridge connect`, and `ants dead watch` expose unauthenticated loopback HTTP health and Prometheus metrics when `--metrics ADDR` (`ANTS_METRICS` on daemon) or config `metrics` is set (read at startup):

- `GET /healthz`: `200 ok` when healthy (daemon: ≥ 1 ant served and all NATS connections up; webhook: all webhook and human connections up; bridge: running; dead watch: NATS connection up), else `503`.
- `GET /metrics`: Prometheus text format (`text/plain; version=0.0.4`).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `ants_nats_connected` | gauge | `ant` / `user` | NATS connection state (`1` up, `0` down) |
| `ants_harnesses` | gauge | `ant` | Live harness processes |
| `ants_memory_low` | gauge | `ant` | `1` while memory is low and the ant caps concurrent harnesses ([config.md](config.md#memory-pressure)), else `0` |
| `ants_memory_available_bytes` | gauge | | System memory available at the last pool check (absent until read) |
| `ants_messages_in_hand`, `ants_prompts_in_flight` | gauge | `ant` | Messages held by daemon; active harness prompts |
| `ants_prompts_total`, `ants_retries_total`, `ants_dead_letters_total` | counter | `ant` | Prompts started, redelivery retries, dead letters (max-deliveries backstop included in the process running it; `ants dead watch` exports these alone) |
| `ants_spend_tokens_total`, `ants_spend_cost_total` | counter | `ant` | Reported tokens and cost ([config.md](config.md#spend-report)) |
| `ants_jetstream_bytes`, `ants_jetstream_limit_bytes` | gauge | `domain`, `store` | Account JetStream storage used and limit ([architecture.md](architecture.md#jetstream-storage)) |
| `ants_webhook_requests_in_flight` | gauge | | Active HTTP webhook requests |
| `ants_webhook_events_total` | counter | `webhook`, `outcome` | `delivered`, `duplicate`, `refused` (4xx), `failed` (503, request timeout included) |
| `ants_bridge_sessions`, `ants_bridge_sessions_total` | gauge, counter | | Open and cumulative bridge sessions |
| `ants_bridge_failures_total` | counter | | Upstream/session failures |
| `ants_bridge_bytes_total` | counter | `direction` (`up`, `down`) | Bridge bytes transferred |

## Footprint

`scripts/footprint.sh` starts `ants daemon` serving one ant and fails when its idle private memory is at or above `--max-mb` (default `10` MB; CI runs it on Linux and macOS). Private memory is the memory the daemon alone holds: `RssAnon` of `/proc/PID/status` on Linux (heap, stacks, relocated data), the physical footprint reported by `footprint` on macOS. Resident memory (`ps -o rss=`, and `RssFile` on Linux) is reported too but not gated: most of it is clean, file-backed code pages of the `ants` binary, shared with every other `ants` process and reclaimable by the kernel, so it tracks binary size more than the daemon's own use (Linux maps code in 64 KB fault-around blocks around each touched page). `--harness NAME` sets the ant's harness (default `copilot`); `--ask TEXT` measures `ants daemon` again with an active session, plus the resident memory of its harness processes; `--metrics ADDR` scrapes `/healthz` and `/metrics` before each idle sample.

| `ants daemon` (`ants` 0.1.0 release) | Linux x86_64 | macOS arm64 |
|---|---|---|
| Idle (serving 1 ant), private | TBD | TBD |
| Idle (serving 1 ant), resident | TBD | TBD |
| Active MCP session, private | TBD | TBD |
| Active MCP session, resident | TBD | TBD |

External ACP harness child processes run outside the daemon's own memory budget. Below `[memory_pressure]` `min_available` (default `1GB`), each ant stops idle harnesses immediately and runs at most `[memory_pressure]` `max_harnesses` (default `1`) until available memory recovers ([config.md](config.md#memory-pressure)).
