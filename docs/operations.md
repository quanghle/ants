# Operations, services, monitoring and footprint

## Platforms and upgrades

- **Linux & macOS:** supported natively with Unix domain sockets (`0600`) for the stdio MCP bridge, `0600` secret file modes, and user services via `systemd --user` or `launchd`.
- **Windows:** build with MSVC or cross-build `--target x86_64-pc-windows-gnu` (mingw-w64 on `PATH`). Stdio MCP uses a current-user named pipe (`\\.\pipe\ants-mcp-<random>`); bare harness names resolve `.exe`/`.cmd`/`.bat` on `PATH`; `ants daemon` handles Ctrl-C/Ctrl-Break/close/shutdown as `SIGTERM` and auto-reloads edited config files (`webhooks.toml` included). `scripts/install-units.sh` and automatic service reloads in `ants setup`, `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant`, and `ants tools approve --rule` cover launchd and systemd only (printing manual commands on Windows; pass `--upgrade-check 0` under Task Scheduler). Secret files inherit `%USERPROFILE%` ACLs instead of Unix `0600`.
- **Binary upgrades:** `ants daemon`, `ants channel serve`, `ants webhook serve`, `ants signer`, `ants bridge serve`, and `ants bridge connect` check every `30s` (`--upgrade-check DURATION` / `ANTS_UPGRADE_CHECK`, `0` disables) whether their executable on disk was replaced by a settled, non-empty executable file, then exit with status `75` (`ants daemon` drains prompts in flight first and `ants webhook serve` finishes in-flight requests, as on `SIGTERM`). Installed launchd and systemd units restart automatically on status `75`.

## Services

`ants setup` installs single-machine `nats` and `daemon` units (the `daemon` unit alone with `--managed-nats`). For multi-machine hubs, workers, and tunnels, `scripts/install-units.sh` renders and starts `contrib/launchd/` (`~/Library/LaunchAgents`) or `contrib/systemd/` (`~/.config/systemd/user`) units:

```sh
scripts/install-units.sh                                                                    # nats + daemon
scripts/install-units.sh --managed-nats                                                     # daemon, running nats-server itself
scripts/install-units.sh --devtunnel ants-hub.usw2                                          # + devtunnel connect
scripts/install-units.sh --no-daemon --channel-manager ~/.config/ants/auth/channel-manager.creds --webhook --signer
scripts/install-units.sh --bridge https://hub.example/bridge --bridge-token ~/.config/ants/bridge.token
scripts/install-units.sh --out ./rendered-units                                             # render only
```

Options: `--ants PATH`, `--nats-server PATH`, `--nats-conf FILE` (default `~/.config/ants/leaf.conf`), `--config FILE` (default `~/.ants/config.toml`), `--managed-nats`, `--no-daemon`, `--channel-manager CREDS`, `--signer`, `--webhook`, `--webhook-config FILE` (default `~/.ants/webhooks.toml`), `--devtunnel TUNNEL`, `--devtunnel-host TUNNEL`, `--devtunnel-bin PATH`, `--devtunnel-token FILE`, `--bridge URL`, `--bridge-serve`, `--bridge-token FILE`, `--bridge-proxy URL`, `--bridge-ca FILE` (`SSL_CERT_FILE`), `--out DIR`. `--devtunnel` and `--bridge` are mutually exclusive (`127.0.0.1:7422`). `--managed-nats` installs no NATS unit, needs a `[nats_server]` table in `--config`, and refuses `--nats-conf`, `--nats-server` and `--no-daemon` ([Managed nats-server](#managed-nats-server)). Installs shell completions unless `--out` is set.

| Service | macOS (`~/Library/Logs/ants/`) | Linux & WSL (`systemd --user`) | Installed when |
|---|---|---|---|
| NATS (`nats-server`) | `ants.nats` (`nats.log`) | `ants-nats.service` | Default (omit with `--managed-nats`) |
| Ant daemon (`ants daemon`) | `ants.daemon` (`daemon.log`) | `ants-daemon.service` | Default (omit with `--no-daemon`) |
| Channel manager (`ants channel serve`) | `ants.channels` (`channels.log`) | `ants-channels.service` | `--channel-manager CREDS` |
| Webhook ingress (`ants webhook serve`) | `ants.webhook` (`webhook.log`) | `ants-webhook.service` | `--webhook` |
| Signer (`ants signer`) | `ants.signer` (`signer.log`) | `ants-signer.service` | `--signer` |
| Dev Tunnel (`devtunnel connect` / `host`) | `ants.devtunnel[-host]` | `ants-devtunnel[-host].service` | `--devtunnel` / `--devtunnel-host` |
| HTTPS bridge (`ants bridge connect` / `serve`) | `ants.bridge[-serve]` | `ants-bridge[-serve].service` | `--bridge URL` / `--bridge-serve` |
| Log rotation (macOS) | `ants.logrotate` (`logrotate.log`) | — (`journald` rotates) | Always on macOS |

- **Reload & restart:** `ants.daemon` and `ants.webhook` auto-reload edited config files (~6 s) and accept `SIGHUP` (`launchctl kill SIGHUP gui/$(id -u)/<label>`, `systemctl --user reload <unit>`); `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant`, and `ants tools approve --rule` reload or restart matching running units automatically. For renewed leaf uplink credentials (`ants add --enroll`, `ants renew`, `request_ant`, admin `add`/`clone`/`retire` orders), `ants.nats` (or the [managed nats-server](#managed-nats-server)) gets a reload that reconnects only the uplink while keeping local client connections.
- **Logs & linger:** on macOS, `ants.logrotate` copy-truncates logs > 10 MiB hourly (keeping `.1`..`.5.gz`). On Linux, enable `loginctl enable-linger $USER` (`[boot] systemd=true` in `/etc/wsl.conf` on WSL 2). Remove units with `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`.

### Managed nats-server

With a [`[nats_server]`](config.md#managed-nats-server) table in its config file, `ants daemon` (serving the whole file, not `--ant`) runs the machine's `nats-server` itself instead of the `ants.nats` / `ants-nats` service. Exactly one owner runs a configuration: `ants setup --managed-nats` and `scripts/install-units.sh --managed-nats` install the daemon unit alone, and both refuse while an `ants.nats` / `ants-nats` unit is installed for it, printing the commands that remove it (`launchctl bootout gui/$(id -u)/ants.nats && rm …`, `systemctl --user disable --now ants-nats.service && rm …`). Supported on macOS and Linux (WSL included); on Windows `ants daemon` refuses to start with the table (`[nats_server] is supported on macOS and Linux only`).

- **Start:** the daemon takes the lock file `<config>.lock` beside the configuration's real path (such as `~/.config/ants/leaf.conf.lock`), held while it runs and recording `daemon=<pid>` and `server=<pid>`. A server pid recorded there by a daemon killed with `SIGKILL`, still alive with the configuration on its command line, gets `SIGTERM`, then `SIGKILL` after 10 s, with a warning; no other process is touched. The daemon then runs `<command> -c <config>` without a shell, standard input closed, in a process group of its own (Ctrl-C at a terminal reaches the daemon alone), and on Linux with `PR_SET_PDEATHSIG` (`SIGTERM` to the server when the daemon dies). It is ready once a connection to the address its ants connect to (the config file's top-level `nats`, else `--nats`) receives the server's `INFO` line, checked every 200 ms up to `ready_timeout`; only then do the ants connect. Ctrl-C or `SIGTERM` meanwhile stops the server and the daemon exits `0`.
- **Start errors:** `ants daemon` exits non-zero, having stopped any server it started and released the lock (a service manager starts it again), on `another ants daemon (pid N) runs the nats-server of FILE` (no server started), `a server already listens on ADDR` (no server started: stop the other server or remove `[nats_server]`), `a managed nats-server needs the daemon to connect to it locally`, `nats-server not found: install nats-server, or set [nats_server] command to its path`, `… exited before accepting connections (exit status …)` (an invalid configuration or an occupied port, as its log says) and `… did not accept connections on ADDR within 30s ([nats_server] ready_timeout)`. A daemon never replaces or stops a server it does not own, and rejected credentials or TLS errors of its ants fail them as without `[nats_server]`, starting no server.
- **Restarts:** an exit the daemon did not ask for is logged (`nats-server exited`) and the server started again after 1 s, 2 s, 4 s … up to 30 s, readiness checked anew; the count starts over once a start ran 60 s. At the next exit after 5 restarts in a row, the daemon logs `giving up on nats-server`, stops serving at once and exits with an error, for its service manager to start it again. A restart requested through `ants leaf config` counts as one.
- **Stop:** on `SIGTERM`, Ctrl-C or a binary upgrade the daemon first drains its ants (prompts in flight finish), then sends the server `SIGTERM`, `SIGKILL` after 30 s, reaps it and releases the lock; an upgrade exits `75` only after that. The JetStream store stays in the configuration's `store_dir`. The systemd unit sets `KillMode=mixed`, so `systemctl --user stop ants-daemon` signals the daemon alone and `SIGKILL`s whatever is left at `TimeoutStopSec` (120 s) or once the daemon exits; launchd signals the daemon alone. On macOS a daemon killed with `SIGKILL` leaves the server running until the next daemon start stops it.
- **Reloads:** `ants leaf config`, `ants add`, `ants renew`, `ants retire`, `request_ant` and admin orders find the server through the lock: while a daemon holds the lock of that `leaf.conf` and its server runs, a reload is `kill -HUP <pid>` and a restart `kill -TERM <pid>` (the daemon starts it again after its backoff); otherwise they use the `ants.nats` / `ants-nats` service as before.
- **Logs:** the server's output goes to the daemon's log (`~/Library/Logs/ants/daemon.log`, `journalctl --user -u ants-daemon`) unless its configuration sets `log_file`; the daemon logs `nats-server started`, `ready`, `exited`, `restarting nats-server`, `stopping nats-server` and `stopped`.
- **Changes:** the daemon reads `[nats_server]` at start; a changed table logs `the [nats_server] table changed; it takes effect on a restart`.
- **Checks:** `ants doctor` reports the configuration `[nats_server]` names as run by `ants daemon`: `ok` while the daemon service runs, `fail` with the start command when it is stopped, `warn` with `scripts/install-units.sh --managed-nats --config FILE` when no daemon service is installed, and `fail` naming two owners when an `ants.nats` / `ants-nats` unit runs it too. A leaf node or server that does not answer gets the daemon's start command as its fix, or, with the daemon running, its log.

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
| Idle (serving 1 ant), private | 1.5 MB | 4.1 MB |
| Idle (serving 1 ant), resident | 25 MB | 14.9 MB |
| Active MCP session, private | — | 5.2 MB |
| Active MCP session, resident | — | 19.2 MB |

External ACP harness child processes run outside the daemon's own memory budget. Below `[memory_pressure]` `min_available` (default `1GB`), each ant stops idle harnesses immediately and runs at most `[memory_pressure]` `max_harnesses` (default `1`) until available memory recovers ([config.md](config.md#memory-pressure)).
