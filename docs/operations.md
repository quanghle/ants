# Operations, services, monitoring and footprint

## Platforms and upgrades

- **Linux & macOS:** supported natively with Unix domain sockets (`0600`) for the stdio MCP bridge, `0600` secret file modes, and user services via `systemd --user` or `launchd`.
- **Windows:** build with MSVC or cross-build `--target x86_64-pc-windows-gnu` (mingw-w64 on `PATH`). Stdio MCP uses a current-user named pipe (`\\.\pipe\ants-mcp-<random>`); bare harness names resolve `.exe`/`.cmd`/`.bat` on `PATH`; `ants daemon` handles Ctrl-C/Ctrl-Break/close/shutdown as `SIGTERM` and auto-reloads edited config files (`webhooks.toml` included). `scripts/install-units.sh` and automatic service reloads in `ants setup`, `ants add`, `ants webhook add`, `ants renew`, `ants leaf config`, `request_ant`, and `ants tools approve --rule` cover launchd and systemd only (printing manual commands on Windows; pass `--upgrade-check 0` under Task Scheduler). Secret files inherit `%USERPROFILE%` ACLs instead of Unix `0600`.
- **Binary upgrades:** `ants daemon`, `ants channel serve`, `ants webhook serve`, `ants signer`, `ants bridge serve`, and `ants bridge connect` check every `30s` (`--upgrade-check DURATION` / `ANTS_UPGRADE_CHECK`, `0` disables) whether their executable on disk was replaced by a settled, non-empty executable file, then exit with status `75` (`ants daemon` drains prompts in flight first and `ants webhook serve` finishes in-flight requests, as on `SIGTERM`). Installed launchd and systemd units restart automatically on status `75`.

### Updating from a release

`ants update` replaces the running `ants` executable with a [GitHub release](https://github.com/quanghle/ants/releases):

```sh
ants update --check            # exit 0: up to date; exit 10: a newer release is out
ants update                    # install the latest release when newer
ants update --version v0.2.0   # install this release, even when not newer
ants update --pre              # consider pre-releases too
```

- **What it does:** resolves the release through the GitHub API, downloads the archive for the compiled target (`x86_64`/`aarch64-unknown-linux-gnu`, `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`; other targets are refused) and the release's `SHA256SUMS`, checks the archive's SHA-256 digest, unpacks it with the system `tar` (Windows 10+ ships one that reads zip), runs the new binary with `--version`, then renames it over the real path of the executable in one step. A failed download, a missing asset, a digest mismatch or a failed `--version` leaves the executable untouched. On Windows the running `ants.exe` is first renamed aside to `ants.exe.old`, deleted by a later run of `ants`.
- **nats-server:** the [nats-server of the archive](#bundled-nats-server), once its `--version` answers, goes beside the executable first, when none is there or the one there reports an older version; a newer one, or one not telling its version, stays. On Windows a running `nats-server.exe` is renamed aside to `nats-server.exe.old` the same way.
- **Network:** the requests go through the HTTP CONNECT proxy `HTTPS_PROXY` names (`http://[user:password@]host:port`), except to hosts `NO_PROXY` lists.
- **Refusals:** an executable under `$CARGO_HOME/bin` (`~/.cargo/bin`) is left to `cargo install --locked --git https://github.com/quanghle/ants ants`; a directory not writable makes it print the command to rerun with `sudo` (an administrator's prompt on Windows).
- **Services:** running services pick the new executable up through their upgrade check (above); `ants update` prints the restart command of an installed unit whose check is off, and of the `ants.nats` / `ants-nats` unit running a nats-server it replaced. A [managed nats-server](#managed-nats-server) restarts on the new one with its daemon.

### Bundled nats-server

ants runs with the nats-server release pinned, with the SHA-256 digest of each platform's archive, in [`crates/ants/nats-server.sha256`](../crates/ants/nats-server.sha256), the release CI tests with.

- **Release archives** hold it beside `ants` (`nats-server`, `nats-server.exe` on Windows) with its license, `LICENSE-nats-server`; `ants update` keeps it current.
- **Lookup:** the default nats-server, of [`[nats_server]`](config.md#managed-nats-server), `ants setup` and `scripts/install-units.sh`, is the one beside the `ants` executable (its real path), else in `~/.ants/bin`, else on `PATH`. `--nats-server` and `[nats_server] command` override it.
- **Download:** when that finds none, `ants setup` downloads the pinned archive for this OS and architecture (macOS arm64 and x86_64, Linux x86_64 and arm64, Windows x86_64) from the nats-server GitHub releases through `HTTPS_PROXY` as `ants update` does, refuses a digest not matching the pin or an executable not reporting the pinned version, and puts `nats-server` into `~/.ants/bin`. `scripts/nats-server.sh PLATFORM DIR` does the same for CI and the release workflow.
- **Moving the pin:** replace the lines of `crates/ants/nats-server.sha256` with those of the new release's `SHA256SUMS`, for every platform listed.

### Automatic updates

The top-level `auto_update` of `~/.ants/config.toml` ([config.md](config.md)) lets `ants daemon`, serving the whole file (no `--ant`), keep the machine current, opt-in:

| `auto_update` | Effect |
|---|---|
| `"off"` (default) | No polling. |
| `"check"` | Logs a warning naming a newer stable release; `ants doctor` reports it as the `update` check (`warn`, fix `ants update`). |
| `"install"` | Installs a newer stable release as `ants update` does; the daemon's upgrade check then drains prompts in flight and exits with status `75` for its service to start the new executable (with `--upgrade-check 0` it keeps running the old one until restarted). |

The first poll comes within 10 minutes of the start, then about every 24 hours (± 1 hour, at random, so a team's machines do not poll together). Only stable releases count and an older release is never installed. A failed poll or install logs a warning and is retried after 1 hour, doubling up to 24 hours. A change to `auto_update` takes effect on a daemon restart. With `auto_update` on, `ants doctor` asks GitHub for the latest release too.

### Moving the streams into the store account

A team made before the store account kept its streams, KV buckets and object stores in the team's account; the store account ([transport.md](transport.md)) is now the only one with JetStream, and a server on the new `server.conf` finds none of them until they move. The move is a one-time step per server (the hub, and each leaf node for its outbox), on the files of the stopped server, so every entry keeps its sequence (and KV revision), timestamp and headers, and every durable consumer its position:

```sh
ants auth init                                    # on the machine holding the team keys: adds store.nk, rewrites server.conf
# stop the server (systemctl --user stop ants-nats, or the ants daemon running it)
cp ~/.config/ants/auth/server.conf /etc/nats/     # over the server.conf the server includes
ants auth move-stores --store-dir /var/lib/ants/js --server-conf /etc/nats/server.conf
# start the server again
ants auth push --nats URL                         # leaf nodes connected through the system account uplink apply it
```

- **What moves:** `<store_dir>/jetstream/<team account>/streams/*` into `<store_dir>/jetstream/<store account>/streams/`, the accounts' keys read from the `server.conf` given (default: the team directory's). `--store-dir` is the `store_dir` of the server's `jetstream` block; with no stream of the team's account there, nothing moves.
- **Streams created since:** a server started on the new `server.conf` before the move creates empty streams and buckets in the store account; the move then refuses, naming them, and `--replace` discards them for the team account's. Run it before any writes to them matter.
- **Leaf nodes:** each also needs `leaf-<machine>.store.creds` (`ants auth leaf`, then `ants leaf config`); its outbox (`ANTS_OUT`) moves like the hub's streams, with `--store-dir` the leaf's `jetstream` directory beside `leaf.conf`.
- **Checks:** `ants doctor` on the machine holding the team keys fails `store account` while the team has no store account or the hub's team account holds JetStream, and passes once the store account holds it.

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
- **Open files:** an ant takes about 4 open files idle and each live harness about 3 more, so 50 ants with `max_harnesses = 8` need over 1300. At start `ants daemon` raises its soft open-file limit (`RLIMIT_NOFILE`) to its hard one, on macOS at most `kern.maxfilesperproc` (falling back to `OPEN_MAX`, 10240), and logs `open-file limit` with `soft`, `hard`, `before` and `needed` (about 4 per ant plus 3 per `max_harnesses`, plus 64); below `needed` the line is a warning naming how to raise the hard limit. Its harnesses and managed nats-server inherit the raised limit. The `ants.daemon` plist sets `SoftResourceLimits` `NumberOfFiles` to 10240 (launchd's own soft limit is 256), the `ants-daemon` unit `LimitNOFILE=65536` (systemd's default soft limit is 1024). A harness start or prompt that still fails with `Too many open files` (EMFILE, ENFILE) spends no attempt and opens no harness breaker: its message waits in hand as for a rate limit (30 s doubling to 10 min, the colony entry's `limited` of kind `open_files`, `out-of-files` in `ants colony`) and the daemon logs `the daemon is out of open files; messages wait`. `ants doctor` reports the limit a daemon started from its shell would reach (`open files`), warning with the fix when it is below what the config file's ants need.
- **Host connection:** once the daemon's host connection (or, with `--ant`, sense's link of its own) stops with an error, such as credentials the server refuses, it logs a warning and is tried again after 1 s, the wait doubling up to 5 min, and at once on a reload; meanwhile the colony entries of the ants using sense say why ([sense.md](sense.md#items-and-cache)).
- **Logs & linger:** on macOS, `ants.logrotate` copy-truncates logs > 10 MiB hourly (keeping `.1`..`.5.gz`). On Linux, enable `loginctl enable-linger $USER` (`[boot] systemd=true` in `/etc/wsl.conf` on WSL 2). Remove units with `launchctl bootout gui/$(id -u)/<label>` or `systemctl --user disable --now <unit>`.

### Managed nats-server

With a [`[nats_server]`](config.md#managed-nats-server) table in its config file, `ants daemon` (serving the whole file, not `--ant`) runs the machine's `nats-server` itself instead of the `ants.nats` / `ants-nats` service. Exactly one owner runs a configuration: `ants setup --managed-nats` and `scripts/install-units.sh --managed-nats` install the daemon unit alone, and both refuse while an `ants.nats` / `ants-nats` unit is installed for it, printing the commands that remove it (`launchctl bootout gui/$(id -u)/ants.nats && rm …`, `systemctl --user disable --now ants-nats.service && rm …`). Supported on macOS and Linux (WSL included); on Windows `ants daemon` refuses to start with the table (`[nats_server] is supported on macOS and Linux only`).

- **Start:** the daemon takes the lock file `<config>.lock` beside the configuration's real path (such as `~/.config/ants/leaf.conf.lock`), held while it runs and recording `daemon=<pid>` and `server=<pid>`. A server pid recorded there by a daemon killed with `SIGKILL`, still alive with the configuration on its command line, gets `SIGTERM`, then `SIGKILL` after 10 s, with a warning; no other process is touched. The daemon then runs `<command> -c <config>` without a shell, standard input closed, in a process group of its own (Ctrl-C at a terminal reaches the daemon alone), and on Linux with `PR_SET_PDEATHSIG` (`SIGTERM` to the server when the daemon dies). It is ready once a connection to the address its ants connect to (the config file's top-level `nats`, else `--nats`) receives the server's `INFO` line, checked every 200 ms up to `ready_timeout`; only then do the ants connect. Ctrl-C or `SIGTERM` meanwhile stops the server and the daemon exits `0`.
- **Start errors:** `ants daemon` exits non-zero, having stopped any server it started and released the lock (a service manager starts it again), on `another ants daemon (pid N) runs the nats-server of FILE` (no server started), `a server already listens on ADDR` (no server started: stop the other server or remove `[nats_server]`), `a managed nats-server needs the daemon to connect to it locally`, ``nats-server not found beside the ants executable, in ~/.ants/bin or on PATH: `ants setup` downloads it, or set [nats_server] command to its path`` (`COMMAND not found: install nats-server, …` for another `command`), `… exited before accepting connections (exit status …)` (an invalid configuration or an occupied port, as its log says) and `… did not accept connections on ADDR within 30s ([nats_server] ready_timeout)`. A daemon never replaces or stops a server it does not own, and rejected credentials or TLS errors of its ants fail them as without `[nats_server]`, starting no server.
- **Restarts:** an exit the daemon did not ask for is logged (`nats-server exited`) and the server started again after 1 s, 2 s, 4 s … up to 30 s, readiness checked anew; the count starts over once a start has run 60 s. At the next exit after 5 restarts in a row, the daemon logs `giving up on nats-server`, stops serving at once and exits with an error, for its service manager to start it again. A restart requested through `ants leaf config` counts as one.
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
| `ants_memory_available_bytes` | gauge | — | System memory available at the last pool check (absent until read) |
| `ants_messages_in_hand`, `ants_prompts_in_flight` | gauge | `ant` | Messages held by daemon; active harness prompts |
| `ants_prompts_total`, `ants_retries_total`, `ants_dead_letters_total` | counter | `ant` | Prompts started, redelivery retries, dead letters (max-deliveries backstop included in the process running it; `ants dead watch` exports these alone) |
| `ants_spend_tokens_total`, `ants_spend_cost_total` | counter | `ant` | Reported tokens and cost ([config.md](config.md#spend-report)) |
| `ants_jetstream_bytes`, `ants_jetstream_limit_bytes` | gauge | `domain`, `store` | Account JetStream storage used and limit ([architecture.md](architecture.md#jetstream-storage)) |
| `ants_webhook_requests_in_flight` | gauge | — | Active HTTP webhook requests |
| `ants_webhook_events_total` | counter | `webhook`, `outcome` | `delivered`, `duplicate`, `refused` (4xx), `failed` (503, request timeout included) |
| `ants_bridge_sessions`, `ants_bridge_sessions_total` | gauge, counter | — | Open and cumulative bridge sessions |
| `ants_bridge_failures_total` | counter | — | Upstream/session failures |
| `ants_bridge_bytes_total` | counter | `direction` (`up`, `down`) | Bridge bytes transferred |

## Footprint

`scripts/footprint.sh` starts `ants daemon` serving one ant and fails when its idle private memory is at or above `--max-mb` (default `10` MB; CI runs it on Linux and macOS). The measurement includes sense: after `ants init`, the daemon serves a config file whose one ant has sense as its provider (the default), with the machine's host connection up, and the idle samples start once the `ant_added` event has opened the ant's sense data (its event log in `ANTS_SENSE` and its cache in `.sense/cache.db`) and recorded its card, with no harness session and no trigger due. `--bare` measures that ant served alone instead (`ants daemon --ant`), its provider `"none"`: no host connection, and no sense data, as sense would link with the host credentials for an ant using it. Private memory is the memory the daemon alone holds: `RssAnon` of `/proc/PID/status` on Linux (heap, stacks, relocated data), the physical footprint reported by `footprint` on macOS. Resident memory (`ps -o rss=`, and `RssFile` on Linux) is reported too but not gated: most of it is clean, file-backed code pages of the `ants` binary, shared with every other `ants` process and reclaimable by the kernel, so it tracks binary size more than the daemon's own use (Linux maps code in 64 KB fault-around blocks around each touched page). `--harness NAME` sets the ant's harness (default `copilot`); `--ask TEXT` measures `ants daemon` again with an active session, plus the resident memory of its harness processes; `--metrics ADDR` scrapes `/healthz` and `/metrics` before each idle sample.

| `ants daemon` (`ants` 0.1.0 release) | Linux x86_64 | macOS arm64 |
|---|---|---|
| Idle (serving 1 ant), private | 1.5 MB | 4.1 MB |
| Idle (serving 1 ant), resident | 25 MB | 14.9 MB |
| Active MCP session, private | — | 5.2 MB |
| Active MCP session, resident | — | 19.2 MB |

With `ants` 0.2.0 on macOS arm64, the idle daemon with the ant's sense data open holds 4.8 MB of private memory (18.8 MB resident), against 4.3 MB (15.8 MB resident) with `--bare`.

External ACP harness child processes run outside the daemon's own memory budget. Below `[memory_pressure]` `min_available` (default `1GB`), each ant stops idle harnesses immediately and runs at most `[memory_pressure]` `max_harnesses` (default `1`) until available memory recovers ([config.md](config.md#memory-pressure)).
