# Configuration

`~/.ants/config.toml` (`--config` / `ANTS_CONFIG`) configures multi-ant daemons, CLI defaults, custom harnesses, worker templates and schedules. Every ant needs a harness. A top-level `harness` (such as `harness = "claude"` alone) serves the built-in `system-messenger` unless disabled, and `system-sage` while an ant names it ([The sage](#the-sage)). On authenticated servers, each built-in runs only where its credentials are present ([Built-in ants on a server with authentication](#built-in-ants-on-a-server-with-authentication)). A file fails to load if any of its ants, including built-ins served on the machine, needs a harness and has none. `ants setup` writes the top-level `harness` of a new file ([Choosing a harness in `ants setup`](#choosing-a-harness-in-ants-setup)); `ants doctor` tells how to fix an existing file.

```toml
harness = "copilot"                        # for every ant and built-in messenger and sage naming none; no default
messenger = true                           # default: serve system-messenger when credentials allow
sage = "system-sage"                       # default reviewer; an ant's name or "none" (see The sage)
nats = "wss://hub.example/nats"            # default NATS URL for ants and schedules without `nats`
js_domain = "hub"                          # default hub JetStream domain; also per ant and schedule
human = "alice"                            # fallback human for ownerless escalations (default "human"); also per ant
metrics = "127.0.0.1:9464"                 # serve /healthz and /metrics (default: off)
timezone = "America/New_York"              # default IANA time zone for schedule cron (default UTC)
redact = true                              # redact secrets from daemon publications (default true)
machine = "dev-vm1"                        # machine named in colony entries, notices and admin orders (default: host name)
auto_update = "check"                      # "off" (default), "check" or "install" newer GitHub releases (docs/operations.md)
provider = "sense"                         # default provider of every ant: "sense" (default), "none" or a [providers] name (see Providers)

[cli]                                      # CLI defaults when no flag or env var is set
creds = "~/.config/ants/auth/alice.creds"  # human credentials (ant credentials are not used here)
nats = "nats://127.0.0.1:4222"             # default: top-level nats
js_domain = "hub"                          # default: top-level js_domain
auto_archive = "14d"                       # hide threads idle this long from thread lists, channel reads and chat (default "0": never; docs/cli.md)
summary_ant = "system-messenger"           # default ant asked for TL;DRs and digests (see Catch-up summaries)
summaries = "on-demand"                    # default; "ahead" also allows asking for TL;DRs ahead of time (needs the next key)
summary_daily_tokens = 50000               # most tokens summaries asked ahead of time spend per human and UTC day (default 0)
inline_images = true                       # draw image attachments in ants chat's thread view (default false; docs/cli.md)
theme = "auto"                             # avatar palette: "dark", "light" or "auto" (default: ask the terminal, else COLORFGBG, else dark)
animate = true                             # avatar heads and spinners in ants chat move (default true; false keeps heads still and spinners at a frame a second)
mouse = true                               # mouse wheel and clicks in ants chat (default true; false leaves native terminal selection)
notify = "bell"                            # new home items while chat is unfocused: "bell" (default), "terminal" or "off"

[storage]                                  # JetStream capacity warnings (see docs/architecture.md, JetStream storage)
warn_percent = 80                          # warn past this share of a store's limit (default 80; 0 disables)
max_file_store = "100GB"                   # hub max_file_store (default: unknown); bytes or 1024-based K/M/G/T[B]
max_memory_store = "1GB"                   # hub max_memory_store (default: unknown)

[memory_pressure]                          # stop idle harnesses early when memory runs low (see Memory pressure)
min_available = "1GB"                      # default; bytes, 1024-based K/M/G/T[B] or a share like "10%"; "0" disables
max_harnesses = 1                          # default; per-ant harness cap while memory is low (at least 1)

[trails]                                   # trail runs the daemon's ants conduct (see Trails)
workers = 4                                # default; trail workers computing at once across the process (1 to 16)

[host]                                     # the machine's host connection (docs/auth.md, Machine host credentials)
creds = "~/.config/ants/auth/host.creds"   # default: that file, else host-<machine>.creds beside it, if either exists
machine = "dev-vm1"                        # default: the credentials' name, else the top-level machine, else the host name

[admin]                                    # admin orders to this machine's ants (docs/auth.md, Admin orders)
remote = false                             # refuse every order (default true: take the orders an admin signed)

[nats_server]                              # ants daemon starts and supervises the local nats-server (see Managed nats-server)
config = "~/.config/ants/leaf.conf"        # its configuration, run as nats-server -c FILE; required
command = "nats-server"                    # default, found beside ants, in ~/.ants/bin or on PATH; a path, or a bare name looked up on PATH
ready_timeout = "30s"                      # default; how long each start may take to accept connections

[providers.memo]                           # an external provider, which provider settings name (see Providers)
command = "memo-provider"                  # a path, or a bare name looked up on PATH; required
args = ["--stdio"]
env = { MEMO_DB = "/srv/memo.db" }         # added to the daemon's environment
protocol = "provider"                      # default: speaks the provider protocol; "mcp": an MCP server, its tools alone

[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"                   # must exist
creds = "~/.config/ants/auth/dev-a.creds"
harness = "claude"                         # overrides top-level harness
tools = "ask"                              # optional; "auto-approve" (default) or "ask", see Asking ants about tools
provider = "memo"                          # overrides top-level provider
model = "claude-sonnet-4-5"                # optional exact ACP model ID
reasoning_effort = "high"                  # optional effort ID offered by the model
context = "1m"                             # optional key from the harness context table
history_budget = 131072                    # optional bytes of thread history seeding a rebuilt session
fallback = ["copilot", ":claude-opus-4-1"] # optional; tried in order while the harness cannot serve
home = "~/.ants/agents/dev-a"              # default: ~/.ants/agents/<name>
whoami = "Maintain Rust backend services." # published in colony and prepended to prompts
memory = "Use notes MCP for durable memory."
tasks = "Use bd for task tracking."
env = { BD_DB = "/srv/ants/dev-a/bd.db" }  # literal env overrides for harness and subprocesses
allow = ["read", "search", "execute:cargo test*"]
allow_tools = false                        # true equals allow = ["*"]
approval_timeout = "5m"                    # default
capabilities = ["rust", "backend"]         # routing tags published in colony
max_harnesses = 4                          # default
worktrees = true                           # git worktree per thread; default: on for a git workdir
                                           # not ignored by git, with max_harnesses > 1 (see Worktrees);
                                           # false shares the workdir
worktree_base = "head"                     # default; "upstream" starts its branch from the fetched remote default branch
publish = "off"                            # default; "draft-pr" pushes a branch with new commits, opens a draft PR
idle_timeout = "10m"                       # default
prompt_timeout = "30m"                     # default; "0" disables
max_tool_calls = 300                       # default; 0 disables
max_thread_cost = 5.0                      # optional; also max_thread_tokens
max_daily_cost = 50.0                      # optional; also max_daily_tokens
spend_warn_percent = 80                    # default; warn once past this % (< 100); 0 disables
escalate_to = ["lead", "dev-b"]            # optional escalation chain; first ant leads
sage = "lead"                              # optional; overrides top-level sage (see The sage)
max_escalation_hops = 3                    # default
escalate_after = "30m"                     # default; "0" waits indefinitely; also how long the human's assistant has before passing on
escalation_reminder = "4h"                 # default; first human reminder delay, then 1d and 3d later (3 at most); "0" disables
stand_in = "bob"                           # optional human relayed this ant's requests and escalations while their human is away
paused = true                              # optional; takes no new prompt, messages wait queued (see Pausing an ant)

[[ant_template]]
name = "worker"
managers = ["lead"]
max_ants = 3
workdirs = ["~/src/repo-a"]
models = { "claude-sonnet-4-5" = "high" }

[[ant]]                                    # templated ant: remaining settings come from [[ant_template]]
name = "lead-docs"
template = "worker"
added_by = "lead"
creds = "~/.config/ants/auth/lead-docs.creds"
model = "claude-sonnet-4-5"

[[schedule]]
name = "nightly"
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"
ant = "dev-a"
text = "Triage the failing nightly tests."
allow = ["read:*", "execute:cargo test*"]

[[schedule]]
name = "weekly-review"
creds = "~/.config/ants/auth/weekly-review.creds"
cron = "@weekly"
ant = "lead"
trail = "review"                           # start saved trail review on lead, in place of text
args = { repo = "app" }
limits = { steps = 100, wall = "2d" }      # what each run may spend; the rest default
```

`machine` (1–64 characters, no control characters) labels where the file's ants run, such as a VM or container whose host name is generated: `ants colony`, `ants dash`, `list_ants` and the sign-in, quota and failing-harness notices show it in place of the host name. A daemon serving a single ant from CLI flags alone uses the host name. When it is also a machine name (1–64 letters, digits, `-` and `_`, starting with a letter or digit) it names the machine for its [host credentials](auth.md#machine-host-credentials), its record in `ants_machines`, its admin orders (`--on MACHINE`) and the signer's record, where neither the `[host]` table's `machine` nor the host credentials name one; otherwise those use the host name's first label, and `ants doctor` warns.

`~` expands to the home directory; other relative paths resolve against the config file's directory (`env` values stay literal). Unknown keys, duplicate ant/template/schedule names, a schedule named after an ant, or an ant naming itself as its sage or a sage naming one ([The sage](#the-sage)) fail config loading. Unreachable NATS or JetStream endpoints retry with exponential backoff (`1s` to `30s`); credential or permission errors fail that ant immediately while remaining ants keep running, exiting non-zero only if none start.

## Precedence and reload

- `ants daemon`: serves all ants in `~/.ants/config.toml` (plus `system-messenger` when enabled and `system-sage` while an ant names it), and runs its schedules, refusing to start where that leaves no ant to serve.
- `ants daemon --ant dev-a` (`ANTS_ANT=dev-a`) without `--config`: configures `dev-a` from CLI flags, loading only `[harnesses.NAME]` from `~/.ants/config.toml` (if present) and `~/.ants/harnesses.d/`. `--provider sense` (the default) or `--provider none` sets its [provider](#providers); with `--config`, the file's `provider` stands and `--provider` is refused.
- `ants daemon --config FILE --ant dev-a`: serves only `dev-a` from `FILE`, with CLI ant flags overriding `FILE`; templated ants refuse ant flags. `--ant system-messenger` and `--ant system-sage` select the built-ins from the file, each failing where the file leaves it to another machine unless `--creds` gives its credentials. No schedules run when `--ant` is set.
- With `--ant` the daemon makes no host connection: no machine record, admin orders or shared settings written. For an ant using sense it opens a link of sense's own with the machine's host credentials, from `FILE`'s `[host]` table, else from `~/.ants/config.toml`'s for an ant from CLI flags, carrying the ant's sense log alone ([Providers](#providers)).
- Per-ant, per-schedule and top-level `nats`, `js_domain` and `creds` in the file override `--nats`, `--js-domain` and `--creds`, which fill in omitted values. `--creds` (`ANTS_CREDS`) are one ant's credentials: those of the ant `--ant` names, else of the file's one `[[ant]]`, never a built-in ant's unless named, nor a schedule's; given them, the server takes credentials, so the built-in ants without their own are left to another machine. Without `--ant`, a file listing several `[[ant]]` entries or none refuses them, and the daemon does not start.
- An ant whose serving fails, as when the `ANTS` stream holds as many consumers as it may (the log names the stream and its limit; `ants doctor` counts the consumers), the server stumbles or the process runs out of file descriptors, is served again after a wait of 1 s, doubling up to 60 s while it keeps failing, with no reload; only an error of its configuration (its `allow` rules) stops it until a reload lists it again.

`ants daemon` reloads on `SIGHUP`, at once after an admin's `set`, `pause` or `resume` order edited `config.toml` ([auth.md](auth.md#admin-orders)) or the shared settings of one of its ants changed ([Shared settings](#shared-settings)), or automatically when `config.toml`, `harnesses.d/*.toml`, the built-in ants' credentials (`~/.config/ants/auth/system-messenger.creds` and `system-sage.creds`, [Built-in ants on a server with authentication](#built-in-ants-on-a-server-with-authentication)) or, where the `[host]` table names no `creds`, the host credentials files looked for in order ([auth.md](auth.md#machine-host-credentials): `~/.config/ants/auth/host.creds`, then `host-<machine>.creds` there for the `[host]` table's `machine`, else for the top-level `machine` and the host name's first label), whether or not any exists, come, go or change on disk (polled every 3 s by mtime, size and inode; applied once unchanged across two checks, ~6 s after write):

- Added ants (and ants still listed that stopped on an error of their configuration) start; removed ants drain prompts in flight and return waiting messages for redelivery; ants whose resolved settings changed (including via top-level defaults, `[harnesses.NAME]`, or `[[ant_template]]`) drain and restart; unchanged ants continue uninterrupted (pool membership changes alone do not restart members, nor does `paused` alone: see [Pausing an ant](#pausing-an-ant)).
- After a valid reload of the whole file leaves no ants to serve, the daemon exits successfully once the last ant drains. On an authenticated server, removing `system-messenger.creds` from a messenger-only daemon triggers this shutdown. An explicit `--ant` that the reloaded file cannot serve still fails the reload.
- Added schedules start; removed schedules stop immediately; changed or stopped schedules restart.
- The machine's record of its ants in `ants_machines` follows the ants listed; a changed `[host]` or `[admin]` table, top-level `machine` or host credentials file reconnects the host. With `[admin] remote = false`, the host refuses every admin order to the machine's ants, recording `this machine's config sets [admin] remote = false` as its outcome; otherwise an admin's `set` order (`ants ant set`) edits the ant's `[[ant]]` table, or the `[harnesses]` table, in place, comments kept, refused naming the field when the field no longer holds the value the order expects, or with the loader's error when the file would not load, and changing nothing then. Only a daemon serving the whole file (no `--ant`) runs the host, with the top-level `nats` and `js_domain`, else `--nats` and `--js-domain`.
- Providers follow the ants: one no ant names any more stops, one whose `[providers.NAME]` table changed restarts, and the ants it serves restart with it.
- Invalid reloads log an error and keep running the previous config. `metrics`, `auto_update` and `[nats_server]` require a process restart (a changed `[nats_server]` table logs a warning).

## Managed nats-server

With a `[nats_server]` table, `ants daemon` serving the whole file starts one local `nats-server -c <config>` before connecting its ants, restarts it when it exits, and stops it once its ants are drained ([operations.md](operations.md#managed-nats-server)). Without the table, the daemon connects to a separately run server (the `ants.nats` / `ants-nats` service, or a hub) as before. `ants daemon --ant NAME` and every other command start no server.

| Key | Default | Meaning |
|---|---|---|
| `config` | required | The server's existing configuration (`leaf.conf` of `ants leaf config`, or `nats.conf` of `ants setup`), resolved like the file's other paths. Its own `store_dir` holds the JetStream data, kept across restarts. |
| `command` | `nats-server` | The executable: a path, resolved like the file's other paths, or a bare name looked up on the daemon's `PATH` (the `PATH` its service unit sets). The default is looked up first beside the `ants` executable (its real path), then in `~/.ants/bin`, then on `PATH` ([operations.md](operations.md#bundled-nats-server)). |
| `ready_timeout` | `30s` | How long each start may take until the server accepts connections; above `0`. |

- The daemon checks readiness at the URL its ants connect to: the file's top-level `nats`, else its `--nats` URL (`ANTS_NATS_URL`, default `nats://127.0.0.1:4222`); that must be a `nats://` or `tls://` URL (or one without a scheme) on `localhost`, `127.0.0.0/8` or `::1`; any other fails `ants daemon` at start. The server must listen there and send its `INFO` line first: a listener with TLS `handshake_first` never reports ready.
- An unknown key, an empty `config` or `command`, or a `ready_timeout` of `0` fails config loading.
- macOS and Linux (WSL included) only: on Windows `ants daemon` refuses to start with a `[nats_server]` table.
- Run the server under exactly one owner: either the daemon with `[nats_server]` (`ants setup --managed-nats`, `scripts/install-units.sh --managed-nats`) or the `ants.nats` / `ants-nats` service, never both.

## Providers

An ant's provider holds its identity, memories and work ([architecture.md](architecture.md#providers)). The `provider` setting, top-level for every ant (the built-in messenger and templated ants included) or per `[[ant]]`, picks it:

- `"sense"` (the default): ants' built-in provider, including private memories, personal tasks and commitments, and daemon-scheduled reminders ([work and reminders](sense.md#work-and-reminders)). Command conditions use the ant's `workdir`, `env` and `allow` rules; no approval request is raised for a disallowed condition. Sense writes the ant's log with the machine's host credentials ([auth.md](auth.md#machine-host-credentials)), over the host connection or, under `ants daemon --ant`, a link of its own. On a server with authentication a machine without them keeps none of its ants' sense data: `ants doctor` fails its `sense data` check naming the fixes, the daemon warns once per ant as it starts, and the ant's colony entry says why (`sense_unkept`), the ant still served without memories, tasks or brief. Issue them (`ants auth host MACHINE`), or set `provider = "none"` (`--provider none` for an ant described by flags) for an ant meant to go without.
- `"none"`: no provider. The ant gets no provider tools and no brief; its harness's own tools serve memory and tasks, installed and configured through the harness, as its `memory` and `tasks` instructions say.
- The name of a `[providers.NAME]` table: an external provider. With `protocol = "provider"` (the default) its command speaks the provider protocol; with `protocol = "mcp"` it is an MCP server over stdio, whose tools serve as a provider of tools alone, without brief, events, memories or export.

```toml
provider = "memo"                          # every ant of the file

[providers.memo]
command = "~/bin/memo-provider"

[providers.github]
command = "github-mcp-server"
args = ["stdio"]
env = { GITHUB_PERSONAL_ACCESS_TOKEN = "ghp_..." }
protocol = "mcp"

[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"
provider = "github"                        # GitHub's tools, served as github_<tool>

[[ant]]
name = "dev-b"
workdir = "~/src/repo-b"
provider = "none"                          # the harness's own tools alone
memory = "Use the notes MCP server for durable memory."
```

A provider name is 1-32 characters of `a-z`, `0-9` and `-`, starting with a letter; `sense` and `none` are reserved, and so is the first word of an ants tool (`send`, `read`, `notes`, …), as ants serves each provider tool as `<provider>_<tool>`. The command, when a path, resolves like other paths; `env` values stay literal. Naming an undefined provider fails config loading.

A daemon process starts each external provider an ant names once, for all the ants naming it, in the daemon's directory, and logs its standard error. When it exits, or fails to start or initialize, the daemon starts it again after 1 s, doubling the wait up to 60 s; a run that lasted a minute resets the wait to 1 s. Its tools are offered only while it is up; a call meanwhile fails with an `[ants]` line saying why, such as `memo is down: exited with exit status: 1; restarting in 4 s`. MCP server tool names are lowercased, other characters than `a-z`, `0-9` and `_` becoming `_`; a tool whose name still does not fit is skipped.

The `provider` setting and `[providers]` are this machine's: not shared settings, and not copied by `ants ant clone`.

## Shared settings

An ant's role settings and core memories are the ant's, the same on whichever machine serves it: `whoami`, `memory`, `capabilities`, `model`, `reasoning_effort`, `context`, `fallback`, `allow`, `escalate_to` and `sage`. They live in one record on the bus, `<ant>.settings` in `ants_identities`, which admins change with `set` orders (`ants ant set ANT model=o3`, [auth.md](auth.md#admin-orders)). Everything else stays the machine's own, in its `config.toml`: `workdir`, `env`, `harness` and the `[harnesses]` tables, `tasks` (the task database), `creds`, `nats`, caps and limits, worktrees, `paused`. An ant added from a template shares only the settings a request chooses (`whoami`, `memory`, `capabilities`, `model`, `reasoning_effort`, `context`): its `fallback`, `allow` and `escalate_to` follow the template (with its `allow_sets` and `added_by`), and its `sage` the file's top-level one, as the file loads, so a change of the template reaches the ant on the next reload: its record holds none of them, and its daemon ignores any it finds there.

- Seeding: a daemon serving the whole file with host credentials reads the shared settings of its ants at start (waiting up to 5 s for them before starting the ants) and every 10 s. An ant with no record yet gets one created from the fields its `[[ant]]` table sets, by create-only write signed with the ant's credentials, so the first machine to serve it wins and the others read that record. A record not signed with the ant's credentials, signed with revoked ones, put back by a host after it was replaced (stored at another stream sequence than the one it was signed for), or written in an earlier incarnation of the bucket (before `ants init --identities-restored`) is ignored and written anew in the same way, a record put back or of an earlier incarnation with the settings the daemon kept before it ([auth.md](auth.md#wire-acls-and-verification-invariants)); a machine without the ant's credentials leaves it to one that has them. In an `ants_identities` bucket without the incarnation marker `ants init` writes (`_incarnation`), as one created by `nats kv add`, no record is applied or written, and each daemon logs that `ants init` writes the marker.
- Serving: once an ant has a record, its daemon serves the record's values for these fields, a field the record leaves out taking its default, and ignores its table's; a change of the record reloads the daemon, restarting the ant as any change of its settings does; a change that only adds or removes drift restarts nothing, the colony entry telling the new drift. Shared settings the machine cannot serve (such as a `fallback` naming a harness it lacks) are logged, and the table's are served instead. A standby daemon taking the ant over serves the same record, so the ant answers alike on every machine; `ants ant memories` reads `whoami` and `memory` from it.
- Drift: a field the table sets otherwise than the record is drift (never one a template fixes). The daemon lists it in the ant's colony entry (`drift`) and its machine's record in `ants_machines` (`drift`, by ant); `ants colony` notes each drifted ant, and `ants doctor` (**shared settings**) lists each field by ant and machine. `ants ant show` lists the shared settings and marks the table's lines they override `(ignored: shared)`; a record that does not verify as daemons verify it shows marked `NOT VERIFIED` with the reason, and overrides nothing. To mend drift, remove the fields from the table, or set them with `ants ant set`, which also writes a field the serving machine's table sets.
- Without host credentials, or with `ants daemon --ant`, a daemon reads the shared settings all the same, at start (waiting up to 5 s) and every 10 s, over a connection as the first of its ants on each server, and serves them as above, but writes none: an ant without a record serves its table's settings until a daemon with host credentials creates one. Over a server without authentication it creates missing records as well. Without the `ants_identities` bucket a daemon shares nothing and serves its table's settings, and `set` orders change them in `config.toml`.
- After restoring `ants_identities` (stream `KV_ants_identities`) from a backup, run `ants init --identities-restored` with human credentials. It writes the bucket's incarnation marker anew, so no record signed before verifies, the backup's included; within 10 s each daemon holding an ant's credentials signs the ant's settings anew: those it kept, else its table's, so a change since made only to the record of an ant whose daemon restarted meanwhile is lost. Without this step, a record written after the backup was taken may verify again once the restored stream reaches the sequence it was signed for.

## Built-in ants on a server with authentication

`system-messenger` and `system-sage` are reserved built-in IDs, not valid `[[ant]]` names. `messenger` and `sage` are ordinary names, with no special behavior unless explicitly assigned to a channel or review. There is no `[[ant]].role`; `whoami` supplies descriptive prompt instructions, not an assignment.

On an authenticated server a daemon serves each built-in only where its credentials are present. The server counts as authenticated when the file names credentials anywhere: host credentials (`[host]` `creds` or one found in `~/.config/ants/auth/`), an `[[ant]]`'s `creds`, or the CLI's `[cli]` `creds`. Issue them with `ants auth builtin messenger` or `ants auth builtin sage` on the machine holding the team keys, then copy `system-messenger.creds` or `system-sage.creds` to `~/.config/ants/auth/` on the serving machine. Credentials appearing or disappearing take effect on [reload](#precedence-and-reload). `ants daemon --config FILE --ant system-sage --creds system-sage.creds` can select a built-in left to another machine. A built-in left elsewhere needs no harness. A file leaving every ant elsewhere, or with `messenger = false` and no `[[ant]]`, serves nothing: `ants daemon` refuses to start. `ants doctor` fails its `config` line there when host credentials are present, a daemon being meant to run; without them it passes the file as that of a machine where only humans run the CLI, such as the one `ants login` writes (`js_domain`, `nats` and `[cli]` `creds` alone), and skips the ant and trail worker checks. On an unauthenticated server, built-ins connect as no one, like the other ants in the file. `messenger = false` disables local hosting, not routing to `system-messenger`.

## The sage

An ant's `sage` names the ant that reviews it: `"system-sage"` (the built-in and default), another ant, or `"none"`, which turns its reviews off. The top-level `sage` applies when the ant omits the setting, and always for an ant added from a template. It is a shared setting ([Shared settings](#shared-settings)), so `ants ant set dev-a sage=lead` changes it on every machine. The reviewer can run on another machine.

A daemon serves `system-sage` while an ant of its file names it, using the top-level `harness` and `~/.ants/agents/system-sage` as its home and workdir. Where the authenticated server requires credentials and the file is absent, the configured reviewer remains `system-sage`, served elsewhere. Like `system-messenger`, it serves only while holding its colony entry; another daemon takes over within about 1 s of its exit or once its entry expires (30 s). `ants ant set system-sage model=o3 reasoning_effort=high` sets its shared model and effort. `ants daemon --config FILE --ant system-sage` serves it alone. Review instructions appear only on review turns, for either a built-in or an explicitly assigned reviewer.

Once a day at most an ant using sense sends its sage a review packet of its recent work, and the sage's findings come back to the ant to verify ([dreams](sense.md#dreams)); while no daemon serves the sage, or for 6 hours without its answer, the ant reviews the packet alone, as an ant with `sage = "none"` always does. The sage reviews with read-only tools on its harness's `review` variant ([Harnesses](#harnesses)); a sage on a harness running its tools unasked without one, such as `antigravity`, declines every review, the ant reviewing the packet alone, and `ants doctor` warns of it (`ant NAME reviews`).

A sage has no sage of its own: neither the built-in sage, nor the built-in messenger, nor an ant another ant of the file names as its sage, the top-level `sage` not applying to it. A sage whose `[[ant]]` names an ant as its sage, or an ant naming itself, fails config loading, and an `ants ant set` order naming one for the built-in ants or a sage is refused. Which ants are sages is told from the file's tables and top-level `sage`: the shared settings that name another ant change only the sage of the ant they belong to. Across machines and shared settings the colony tells the rest: each ant's colony entry names the sage its config and shared settings resolve (`sage`, missing for a sage and where reviews are off), and an ant that a live colony entry of another ant names as its sage sends no review packet, reviewing the packet alone whatever its own `sage`. So an ant made a sage only by another machine's file or by shared settings is reviewed by no one either, and `lead` and `dev-a` naming each other both review alone.

## Pausing an ant

`paused = true` on an `[[ant]]` pauses it: its daemon stops pulling from the ant's inbox and channel consumers, so the messages to it wait queued in the stream, neither drained nor returned and spending no delivery attempts, and the ant takes no new prompt; prompts already running go on to their end, and messages already in hand stay in hand, unprompted, until it resumes. Its colony entry says `paused` (`{since, by}`: when, and the admin whose order paused it, left out when the file did), which `ants colony`, `ants ant show`, the ant view of `ants chat` (`‖` in the nav), the `list_ants` MCP tool and the warnings at send time show; `route` picks a paused ant last, and a sender whose messages wait 15 min for a paused ant is told so, as for an ant no daemon serves. Removing the key, or `paused = false`, resumes it, and it takes the messages queued for it in turn. A reload that changes `paused` alone pauses or resumes the running ant in place, restarting no harness. Templated ants may set it too.

`ants ant pause ANT [--machine M] [--wait DUR]` and `ants ant resume ANT` send the admin order (`pause`, `resume`) that has the daemon of the ant's machine set `paused = true`, or remove it, and reload at once ([auth.md](auth.md#admin-orders)); `p` in the ant view of `ants chat` sends the one that flips it.

## Worktrees

With worktrees on, each thread works in its own git worktree at `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>`, created from `HEAD` on the thread's first prompt and removed once its session expires (a branch with unmerged commits is kept, and its thread is told). A workdir outside a git work tree stays shared, as does one whose repository has no commits yet (unborn `HEAD`, warned at daemon start, whether `worktrees` is unset or `true`, until the daemon restarts). A thread whose worktree cannot be created never runs in the shared workdir: its prompt does not start (warned in the daemon log), and the message is retried as after a failed attempt, becoming a dead letter with the git error after the last one.

Ants of one daemon may share a repository: the daemon creates and removes the worktrees and branches of one repository one at a time across its ants, tries a failed `git worktree add` once more (after `git worktree prune`), and deletes a branch left by an add that failed twice. Fetches for `worktree_base = "upstream"` from one repository also go one at a time, apart from worktree changes. A branch `ants/<ant>/<thread>` left without a worktree (as by a daemon killed while adding one) is deleted when worktrees are pruned, once its thread has no session and no live harness, unless it holds unmerged commits.

- Unset `worktrees` turns them on when the workdir is inside a git work tree (a `.git` directory or file in it or above it) that does not ignore it (`git check-ignore`, as for a directory in `target/`) and `max_harnesses > 1`, logging so at daemon start; otherwise threads share the workdir.
- `worktrees = true` (`--worktrees`) turns them on; `worktrees = false` (`--no-worktrees`) keeps every thread in the shared workdir, with a warning at daemon start and in `ants doctor` when `max_harnesses > 1` and the workdir is a git work tree, as threads running at once may clobber each other's edits.
- `worktree_base = "upstream"` starts each new branch from the fetched default branch of the default remote instead of `HEAD`, and `publish = "draft-pr"` pushes a branch with new commits and opens a draft pull request ([architecture.md](architecture.md#pool-worktrees-and-at-least-once-retries)); both apply whenever worktrees are on, set or by default.

Trade-offs of worktrees:

- **Disk:** one full checkout per thread, plus build outputs (`target/`, `node_modules`) each thread builds from scratch, for up to `max_harnesses` live threads and every thread whose session has not expired.
- **Branches:** a branch `ants/<ant>/<thread>` per thread in the repository; branches with unmerged commits remain after their worktree is removed until merged or deleted by hand.
- **Starting point:** a worktree starts from `HEAD` without uncommitted changes of the workdir, and edits land on the thread's branch, not in the workdir, until merged; tools keyed by the workdir path (trust prompts, per-directory config, caches) see a new directory per thread.

## Ant templates

An `[[ant_template]]` bounds worker ants added by manager ants. A templated `[[ant]]` sets `template`, `added_by`, `name`, `creds`, optional `nats`/`js_domain`, and any allowed request overrides below; any other key or out-of-bounds value fails config load and reload. Escalations go to `added_by` first (`escalate_to = [manager]`). The template's `allow` and `fallback` and the escalations follow the template as each load finds it, never the ant's [shared settings](#shared-settings) record. Templated ants cannot be managers (`managers` and `added_by` reject them).

```toml
[[ant_template]]
name = "worker"                            # lowercase letters, digits, '-' and '_'
managers = ["lead"]                        # exact ant names allowed to add workers; default []
name_pattern = "<manager>-*"               # default; <manager> = manager name, * = any text
max_ants = 3                               # required; max templated ants listed in config
workdirs = ["~/src/repo-a", "~/src/repo-b"] # required; first is default
harnesses = ["claude"]                     # first is default; default: top-level harness only (else required)
models = { "claude-sonnet-4-5" = "high", "claude-haiku-4-5" = "max" } # model ID = max reasoning_effort
contexts = ["1m"]                          # default: harness default only
max_thread_cost = 2.0                      # ceilings + defaults: max_thread_tokens/cost, max_daily_tokens/cost
max_daily_cost = 10.0
max_harnesses = 2                          # ceiling + default (default 4)
prompt_timeout = "20m"                     # ceiling + default (default "30m"; "0" = unlimited)
max_tool_calls = 300                       # ceiling + default (default 300; 0 = unlimited)
allow = ["read", "search"]                 # base tool rules for every ant from this template
allow_sets = { build = ["execute:cargo build*", "execute:cargo test*"] } # named rule sets a request may add
pool_daily_cost = 25.0                     # optional shared UTC daily cap across template ants; also pool_daily_tokens
whoami = "Implement what the manager assigns." # overridable defaults: whoami, memory, tasks, capabilities
capabilities = ["rust"]
env = { CARGO_TARGET_DIR = "target" }      # fixed on template: env, worktrees (unset: the ant default),
worktrees = true                           # worktree_base, publish, idle_timeout, approval_timeout,
                                           # spend_warn_percent, fallback
fallback = ["copilot", ":claude-haiku-4-5"] # fallbacks of every ant from this template, see below
```

Allowed per-ant overrides within template bounds:

- `workdir` (in `workdirs`), `harness` (in `harnesses`), `context` (in `contexts`), `allow_sets` (keys of `allow_sets`, appended to `allow`).
- `model` (required when `models` is non-empty, forbidden when empty) and `reasoning_effort` (`low` < `medium` < `high` < `xhigh` < `max`, at most the model's ceiling).
- `whoami`, `memory`, `tasks`, `capabilities`, `paused`.
- `max_thread_tokens`, `max_thread_cost`, `max_daily_tokens`, `max_daily_cost`, `max_harnesses`, `prompt_timeout`, `max_tool_calls` — each at most the template ceiling (unset caps have no ceiling; `0` for `prompt_timeout` or `max_tool_calls` is allowed only when the template's is `0`).

A template's `fallback` list ([Fallbacks](#fallbacks)) is the fallback list of every ant added from it: a request cannot set or change it, and the template's `harnesses` and `models` bound what a manager picks, not the fallbacks. Its entries must name a known harness, name a model after a colon and appear once; anything else fails config loading. Each ant leaves out an entry naming the harness and model it runs (such as `":claude-haiku-4-5"` for an ant whose manager picked that model) or a fallback an earlier entry names for its harness, and keeps the rest in order.

Removing a manager from `managers` blocks new additions while keeping existing workers.

## Manager ants

An ant listed in `managers` (when `ants daemon` serves the whole config file, not `--ant`) receives MCP tool `request_ant(name, template, reason, overrides?, timeout?)` (`other:request_ant`). Requests serialize one at a time per daemon process:

1. Validates `name`, `overrides` and `max_ants` against the local config's `[[ant_template]]`, refusing out-of-bounds requests immediately.
2. Generates a local user NKey (`~/.config/ants/auth/<name>.nk`, `0600`; refuses if `<name>.creds` exists).
3. Publishes signed `AntRequest`s on `ants.antreq.<manager>` every 5 s until `ants signer` answers or `timeout` elapses (default `15m`, max `30m`).
4. Writes `<name>.creds`, deletes `<name>.nk`, appends `[[ant]]` to `config.toml`, and returns once `<name>` appears in `ants_colony` (or after 60 s noting it is not yet listed).

Behind a leaf node, `ants signer` also renews the leaf uplink JWT and admits the ant to the outbox (refusing up front if the uplink JWT would exceed 252 KiB; see [transport.md](transport.md#enrollment-behind-a-leaf-node)). The daemon writes the renewed uplink credentials and toggles `./` (`.\` after a Windows directory) on the `credentials` path in `leaf.conf` before reloading `ants.nats` / `ants-nats`, or the nats-server an `ants daemon` runs from that file ([Managed nats-server](#managed-nats-server)), with `SIGHUP`, reconnecting only the uplink while local clients stay connected.

`ants signer` runs on the account-key machine (`scripts/install-units.sh --signer`):

```sh
ants signer --config ~/.ants/config.toml
```

It verifies the manager's account signature and evaluates the request against its **own** `--config` `[[ant_template]]` tables (reread per request) and live issued keys in `issued.jsonl` (`ants auth revoke NAME` frees a slot). Policy-compliant requests are signed immediately for the manager's `js_domain` (recording `template` and `added_by` in `issued.jsonl`). Out-of-policy requests publish a manager `ApprovalRequest` (`request_ant: add ant NAME from template T (why)`, `allow_once` or `reject`), visible in `ants tools`, `ants inbox -f`, `ants dash` and all `ants webhook serve` notification sinks, waiting up to `--approval-timeout` (default `15m`, capped by request expiry). Forged requests, human/manager/reserved/taken names, templated managers, and > 3 pending human approvals per manager are refused outright. Pending human requests drop on signer restart and re-trigger on the manager's next 5 s retry.

The signer also signs and revokes keys on admins' `add` and `retire` orders, which machines' daemons carry out with their host credentials, with no human asked again ([auth.md](auth.md#adding-and-retiring-an-ant-by-order)): an ant added so gets an `[[ant]]` table of its own below the comment `# added by <admin>'s order <id>`, no template's.

The signer also answers ants' reactions requests (MCP `react` and `list_reactions`, `ants react` with ant credentials) in queue group `reactions`: ant credentials neither read nor write `ants_reactions`, so the signer lists the reactions to a message, or adds or removes the ant's own, once it finds that the ant reads the message. With authentication, `react` therefore needs a running `ants signer` (listings are also answered by `ants webhook serve` with a `[[human]]` entry); a daemon without credentials answers its ants itself. Behind a leaf node, the signer's machine needs its uplink issued with `--enroll-approver` (see [auth.md](auth.md#wire-acls-and-verification-invariants)).

The signer also saves and removes ants' trails (MCP `save_trail` and `remove_trail`) in queue group `savetrail`: a saved trail's key is its name, which no subject ties to its saver, so ant credentials neither put nor delete in `ants_trails`, and the signer writes for an ant only a trail saved as the ant's own, under a new name or over a trail the ant saved, and removes only such a trail. With authentication, `save_trail` and `remove_trail` therefore need a running `ants signer`; a daemon without credentials writes itself, with the same checks.

### Retiring an added ant

Managers also get `retire_ant(name)` (`other:retire_ant`) for ants whose `added_by` matches them:

1. Removes the `[[ant]]` table from `config.toml` so the daemon drains and stops the ant, deleting its durable consumers `chan-<ant>` and `done-<ant>` and telling its provider of the retirement ([lifecycle events](architecture.md#lifecycle-events)); sense keeps the ant's identity dormant, its data kept, its commitments open and its reminders paused until it is served again ([sense identity](sense.md#identity)).
2. Waits up to `5m` for `<name>` to leave `ants_colony`.
3. Requests retirement from `ants signer` on `ants.antreq.<manager>` (every 5 s for up to `2m`); the signer verifies `issued.jsonl` (`template` + `added_by`), revokes `<name>`'s keys, and pushes the account to the hub. If no daemon is serving the ant and no live key remains, the signer deletes any `chan-<ant>` or `done-<ant>` consumer left behind. Behind a leaf node the request carries the uplink credentials, as `request_ant`'s does, and the signer renews them without `<name>`, drops it from the leaf's members and outbox sourcing, and the manager's machine writes them and reconnects the uplink ([transport.md](transport.md#enrollment-behind-a-leaf-node)); when that fails, the retirement stands and the manager's reply names `ants auth leaf M --remove-ant NAME` and `ants leaf outbox M --remove-ant NAME`.
4. Deletes `<name>.creds` (and any OS-stored seed).

`~/.ants/agents/<name>` (memory, task DB, worktrees) is preserved: a later `request_ant` of `<name>` from the same template restores it, its sense identity active again, and its next brief tells of the return with the last handoff and the open commitments, overdue first. Revocation frees the name, the `max_ants` slot, and the ant's share of `pool_daily_*` (restored if re-added the same UTC day). Interrupted retirements resume from `<name>.creds` if retried.

Humans retire any templated ant on the manager's machine with `ants retire NAME [--config FILE] [--dir DIR] [--no-push]`, revoking locally if `--dir` holds the account key, then dropping the ant from the leaf node it is behind as `ants auth revoke` does ([auth.md](auth.md#issuing-and-revoking-credentials)), or printing `ants auth revoke --key U... --earlier` otherwise, which, run where the account key is, revokes the ant's key and the live keys it was renewed from, then does that. Non-templated ants are removed by editing `config.toml` and running `ants auth revoke NAME`.

## Cloning an ant

An admin's `ants ant clone SRC NEW [settings]` has the daemon serving SRC start NEW as a copy of it, with its own key and identity record ([auth.md](auth.md#cloning-an-ant-by-order)). SRC is paused while it is copied, once its prompts in flight end, and resumed after. NEW gets:

- **Table:** SRC's `[[ant]]` table (for an ant added from a template, the settings the template gave it, so NEW's table stands alone), below the comment `# cloned from SRC by <admin>'s order <id>`, without `name`, `creds`, `paused`, `added_by`, `template`, `builtin`, `home` and `allow_sets`, with SRC's [shared settings](#shared-settings), where the hub keeps them, in place of its table's, and the settings flags of `ants add` over it; its whoami, memory, tasks, harness, model, permissions and escalation chain are SRC's unless given. A workdir in SRC's home moves into NEW's.
- **Shared settings:** where the hub keeps them, a record of NEW's own (`NEW.settings`), written before its table, in place of any left from an earlier ant of the name: SRC's with the order's settings over them, so NEW's table and record agree and no drift shows.
- **Home:** a copy of SRC's (`--home`, default `~/.ants/agents/NEW`; it must be empty), memory files, task DB and repositories included, but not `worktrees`, `answered`, `unsent`, `buried`, `lifecycle`, `.sense` (the clone mints its own sense id) and the MCP config and socket. The clone's provider is told `ant_cloned` ([architecture.md](architecture.md#lifecycle-events)); sense then forks SRC's identity into NEW's, keeping SRC's memories and self-model and dropping its tasks, commitments, reminders, relationships, resume registers and handoffs ([sense.md](sense.md#identity)).
- **Threads:** each thread SRC holds a session of, with its worktree, if any, at `<home>/worktrees/<thread>` on branch `ants/NEW/<thread>` from SRC's commit, SRC's uncommitted changes and untracked files on it. Its session is a fork of SRC's when the harness offers ACP `session/fork`; otherwise NEW's first prompt on the thread starts a new session seeded from the thread's history, as after a failed `session/load` ([Lost sessions](#lost-sessions-and-context-window)), its notice saying the session could not be forked.
- **Origins:** where each of SRC's threads came from as SRC's daemon settled it ([architecture.md](architecture.md#provider-protocol)), so a thread a webhook reached stays untrusted on NEW rather than taking the origin of NEW's first message there.
- **History:** what SRC had on each thread, kept in `<home>/history/<thread>.jsonl`; NEW reads a thread's history back with this copy first, then what came to it after.

Not copied: messages queued for SRC, channel memberships, references, spend counters and the pause. NEW's identity record gets an `id` and `born` of its own, SRC's count of prompts and `cloned_from`.

## Human registration

`ants signer` registers a human's new device after a GitHub sign-in (`ants login`, with the team's bootstrap credentials) when its `--config` has a `[registration]` table, reread per request, and renews the credentials such registrations granted. Without the table, every registration is refused as not enabled. The flow, the grants, name bindings, renewal and offboarding are in [auth.md](auth.md#registering-humans).

```toml
[registration]
client_id = "Ov23liAbCdEf"                    # the team's GitHub OAuth App, device flow enabled (required)
orgs = ["acme"]                               # members of these organizations are signed at once
teams = ["acme/core"]                         # ... and members of these teams (org/team slug)
email_domains = ["acme.example"]              # ... and users with a verified email address in these domains
ttl = "30d"                                   # validity of a registered device's credentials (default 30d, at least 1m)
sweep = "6h"                                  # how often memberships are checked again (default 6h; "0": never)
token_file = "~/.config/ants/github-token"    # the signer's GitHub token, mode 0600 (relative to the config file)
# token_store = "github-token"                # or its entry in the OS credential store (service ants)
# github_url = "https://github.com"           # where GitHub signs users in (default)
# api_url = "https://api.github.com"          # GitHub's REST API (default)
```

- **`client_id`:** the client ID of a GitHub OAuth App of the team with **Enable Device Flow** checked; no client secret is needed. Signing in grants it the scopes `read:org user:email`.
- **`orgs`, `teams`, `email_domains`:** the policy, checked in that order: an active member of a listed organization (grant `org`), then of a listed team (`team`), then a user with a verified email address in a listed domain (`email-domain`; that exact domain, no subdomains) is signed at once. Names are compared without case; organizations are GitHub logins and teams `org/slug`. Anyone else waits for a human's approval ([auth.md](auth.md#registration-policy)), within `ants signer --approval-timeout` (default `15m`). An empty policy sends every registration to the humans.
- **`ttl`:** how long a registered device's credentials stay valid; a renewal keeps the lifetime they were signed with.
- **`sweep`:** how often the signer checks again, with its token, the memberships of the GitHub users of live credentials an `org` or `team` grant admitted, revoking every key of a name whose user definitely left ([auth.md](auth.md#membership-sweep)); the first sweep runs at start. `"0"` turns it off; it is off without the signer's token too.
- **`token_file` / `token_store`:** the signer's own GitHub token, in a file readable by its owner only (`~` and paths relative to the config file allowed) or in an entry of the OS credential store (service `ants`); at most one of them. A classic token with `read:org`, or a fine-grained token with organization members read access, of a member of the listed organizations, as GitHub hides private memberships from others. Optional for registering, where the user's own token reads the memberships otherwise (an organization restricting OAuth App access then hides them unless it approved the app), but needed to renew devices an `org` or `team` grant admitted: each renewal checks the memberships again with it, and is refused without it. The [membership sweep](auth.md#membership-sweep) needs it too, and a token that sees every listed organization and team. `ants auth user NAME --human --github LOGIN` looks users up with it too.
- **`github_url`, `api_url`:** a GitHub Enterprise Server's own URL and REST API (such as `https://ghe.example/api/v3`). `ants login` shows only the device login page of its `--github-url` (default `https://github.com`), so with a GitHub Enterprise Server each person registers with `ants login --github-url https://ghe.example`; lookups by `ants auth user --github` and renewal checks use these URLs.

`ants signer` must connect to the hub itself (`--nats`), not through a leaf node, whose uplink never carries registration requests, and signs registered devices for its `--js-domain`. `ants daemon` ignores the table. GitHub is the only identity provider.

## Harnesses

Built-in ACP stdio harnesses live in `crates/ants-daemon/src/harnesses.toml`. None is a default: an ant names its harness, or the file's top-level `harness` does.

| Name | Command | Install | Sign-in | Runs tools unasked through |
|---|---|---|---|---|
| `copilot` | `copilot --acp --allow-all` | `npm install -g @github/copilot` | `copilot login` | `--allow-all` (agent mode, not autopilot) |
| `claude` | `claude-agent-acp` | `npm install -g @agentclientprotocol/claude-agent-acp` | `claude login` or `ANTHROPIC_API_KEY` | ACP mode `bypassPermissions` |
| `codex` | `codex-acp` | `npm install -g @agentclientprotocol/codex-acp` | `codex-acp login`, or `CODEX_API_KEY` / `OPENAI_API_KEY` | ACP mode `agent-full-access` (no approvals, no sandbox) |
| `antigravity` | `agy acp --disable-telemetry --dangerously-skip-permissions` | `curl -fsSL https://antigravity.google/cli/install.sh` piped to `bash` | `agy` (`agy acp --login`) or Gemini API key | `--dangerously-skip-permissions` |

**Built-in harnesses run their own tools unasked**, as does every harness of a [native protocol](#harness-protocols). Each built-in starts in its most permissive native setting, so its shell commands, edits and fetches never reach the daemon as tool requests: an ant's `allow` rules, tool approvals by humans or `escalate_to` ants, and [webhook and schedule narrowing](#tool-permissions) do not apply to them. `ants daemon` logs a warning at start (and at the first prompt each webhook or schedule narrows), and `ants doctor` warns (`ant NAME tool permissions`), when an ant on such a harness sets allow rules other than `*`, has `escalate_to` ants, or is the destination of a narrowing webhook or schedule. Checks the daemon makes itself still apply: [prompt limits and spending caps](#prompt-limits-and-spending-caps), and the [branch push](#tool-permissions) of `publish = "draft-pr"`. Run such ants in workdirs and accounts their harness may change freely, or set `tools = "ask"` ([below](#asking-ants-about-tools)).

- **Claude Code:** `bypassPermissions` is offered only when the adapter does not run as root (unless `IS_SANDBOX=1` is in its environment, e.g. `env = { IS_SANDBOX = "1" }` on the ant) and the Claude Code settings do not set `permissions.disableBypassPermissionsMode = "disable"`; otherwise sessions fail to start with the mode missing. Loads `CLAUDE.md`, `.claude/settings*.json` and hooks.
- **Codex:** reports MCP calls as `execute` titled `mcp.<server>.<tool>` (matched as `other:`). Loads `~/.codex/config.toml` and `AGENTS.md`. For an API key, set `env = { DEFAULT_AUTH_REQUEST = '{"methodId":"api-key"}' }` beside `CODEX_API_KEY` or `OPENAI_API_KEY`.
- **Antigravity:** loads `GEMINI.md`, `AGENTS.md` and `~/.gemini/antigravity-cli/settings.json`. Keychain login requires running the daemon as that OS user.
- **Skills:** an ant whose provider is sense offers its confirmed skills, `.sense/skills` in its home, to `copilot` as an extra skill directory (`COPILOT_SKILLS_DIRS`, after any it already names) and to `claude` as a local plugin (session `_meta`); `codex` and `antigravity` see them only in the brief ([sense.md](sense.md#persona-and-skills)).

### Asking ants about tools

`tools` says how a harness's tools are approved: `"auto-approve"` (the default) runs them unasked as above, for the fewest approval requests; `"ask"` starts each session in the harness's own gated or automatic mode instead, which runs what that mode deems safe and puts the rest to the client, and sends every such tool request to ants: the ant's `allow` rules, then a human or its `escalate_to` ants ([Tool permissions](#tool-permissions)), rejected after `approval_timeout`. Webhook and schedule narrowing apply too. Set it on an `[[ant]]` (for that ant, its fallbacks included) or in a `[harnesses.NAME]` definition (for every ant on it that sets none):

```toml
[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"
harness = "claude"
tools = "ask"
allow = ["read", "search", "execute:cargo test*"]
escalate_to = ["lead"]
```

A definition's `ask` table names the `args` and/or `mode` a session starts with under `"ask"`, in place of its own; `bypass_permissions` does not apply then. The built-ins':

| Name | `ask` | Asks ants about |
|---|---|---|
| `copilot` | `{ args = ["--acp"] }` | what Copilot CLI's own rules do not allow: commands, edits, fetches, MCP tools |
| `claude` | `{ mode = "auto" }` | what Claude Code's auto-mode classifier does not run on its own (`acceptEdits` where the model lacks auto mode) |
| `codex` | `{ mode = "agent" }` | commands and edits outside the workspace sandbox, and network access |
| `antigravity` | none | refuses `"ask"`: it applies workspace reads and edits unasked even gated |

An ACP harness without `bypass_permissions` asks on its own `args` and `mode`; one with it and without `ask` refuses `"ask"`, as does every Antigravity harness. A refusal, by the ant's harness or one of its fallbacks, fails loading the config, naming it. For a [native protocol](#harness-protocols), the translator forwards what the CLI asks to ants rather than denying it; `ask` there names the CLI's own gated setting, such as Claude Code's `mode = "auto"`, or the definition's `mode` and `args` apply.

To have a built-in ask before running tools without `tools`, redefine it whole under `[harnesses.NAME]` with a gated mode or without the permissive argument (a redefinition does not run tools unasked unless it sets `bypass_permissions = true`):

```toml
[harnesses.claude]
command = "claude-agent-acp"
mode = "default"                               # asks for edits and commands; `auto` approves internally
sign_in = "claude login"

[harnesses.codex]
command = "codex-acp"
mode = "read-only"                             # sandboxes reads unasked; asks before edits, network or unsandboxed commands
sign_in = "codex-acp login"

[harnesses.copilot]
command = "copilot"
args = ["--acp"]
sign_in = "copilot login"

[harnesses.antigravity]
command = "agy"
args = ["acp", "--disable-telemetry"]
mode = "default"
sign_in = "agy"
```

Even gated, Claude Code runs its built-in read-only tools (`Read`, `Grep`, `ls`, `echo`) and rules from `.claude/settings*.json` unasked, and Antigravity applies workspace reads/edits and its `settings.json` first.

**Reviews run gated.** A sage's review ([The sage](#the-sage)) runs in a session of its own started with the harness's `review` table in place of its `args` and `mode`: `copilot` drops `--allow-all` (`review = { args = ["--acp"] }`), `claude` takes mode `default` and `codex` mode `read-only`. The daemon rejects each tool request of that session outside the review's read-only rules, asking no one. `antigravity` sets no `review`, as it applies edits unasked even gated, so a sage on it declines reviews and the ant reviews the packet alone. A harness that asks before running tools reviews on its own `args` and `mode`; one running its tools unasked (`bypass_permissions`, or a native protocol) needs a `review` naming the arguments or mode that keep its tools read only or make it ask, else its sage declines reviews and `ants doctor` warns (`ant NAME reviews`). A redefinition of a built-in sets its own `review`; for a native protocol, `review` takes the CLI's own read-only setting, such as Claude Code's `plan` or codex's `read-only`. A redefinition replaces the built-in's quota and context-overflow phrases, `window` and `context` table too: copy those from `harnesses.toml` to keep them.

### Custom harnesses

Define other ACP harnesses under `[harnesses.NAME]` in `config.toml` or `harnesses.d/*.toml`:

```toml
[harnesses.custom-acp]
command = "~/.local/bin/custom-acp"            # path if containing '/', else looked up on PATH
protocol = "acp"                               # default; see Harness protocols
args = ["--stdio", "--log-level", "warn"]
env = { CUSTOM_ACP_TELEMETRY = "off" }         # precedence: daemon env < harness env < ant env < --env
mode = "default"                               # session fails if harness lacks this mode
sign_in = "custom-acp login"                   # shown when signed out
bypass_permissions = false                     # default; true when args or mode make it run tools unasked (warns as above)
tools = "auto-approve"                         # default; "ask" sends its tool requests to ants (see Asking ants about tools)
ask = { mode = "default" }                     # args and/or mode of a session under tools = "ask"
review = { mode = "read-only" }                # args and/or mode of a sage's review session; see Reviews run gated
context_overflow = ["context too long"]        # extra context-overflow error substrings
rate_limited = ["slow down"]                   # extra rate-limit error substrings
quota_exhausted = ["monthly cap reached"]      # extra quota-exhausted error substrings
window = 200000                                # optional context window (tokens); sizes history seed
history_budget = 65536                         # optional history seed budget (bytes)
memory_files = ["AGENTS.md", "~/.custom/*.md"] # memory files an admin's read shows (see Memory files)

[harnesses.custom-acp.context]
default = {}
large = { args = ["--context-window", "1m"], window = 1000000 }
huge = { model_suffix = "-1m", history_budget = 262144 }
```

### Harness protocols

`protocol` names what a definition's command speaks on stdio. The built-ins speak ACP.

| `protocol` | The command speaks |
|---|---|
| `acp` (default) | ACP: an agent or adapter the daemon drives directly, as described above |
| `claude` | Claude Code's stream-json (`claude -p --input-format stream-json --output-format stream-json`), from Claude Code 2.1.325 ([below](#claude-code-protocol--claude)) |
| `codex` | Codex's JSON-RPC app server (`codex app-server`); see [Codex](#codex-protocol--codex) |
| `antigravity` | Antigravity's headless stream-json (`agy --input-format stream-json --output-format stream-json`); see [Antigravity](#antigravity-protocol--antigravity) |
| `copilot-sdk` | Copilot CLI's JSON-RPC server (`copilot --server --stdio`) through the GitHub Copilot SDK ([below](#copilot-sdk-protocol--copilot-sdk)) |

Any other value fails loading the file defining the harness, naming that file.

Every protocol other than `acp` is native: the daemon runs a translator in its own process that starts the definition's command and speaks the CLI's protocol to it, and ACP to the rest of the daemon. The command is found and started as for ACP (a path, else looked up on `PATH`, as `.exe`, `.cmd` or `.bat` on Windows, without a shell), with the protocol's own arguments before the definition's `args` and those of the ant's `context`, and the same environment layering. The rest works as for ACP:

- **Sessions:** the CLI's own session id is the one stored in `ants_sessions` and resumed after an idle stop or restart; a cloned thread forks the session where the CLI can fork one, else starts over from the thread's history. A fork whose cost totals start at its parent's (Claude Code, Copilot CLI) has that base appended to its id, in micro-dollars (`<id>~<base>`), so that it reports only its own cost, after a restart too. A session the daemon leaves for a new one, on a [context-window retry](#lost-sessions-and-context-window), is closed (ACP `session/close`): the CLI process run for it alone ends (Claude Code, agy, whose private directory goes too), codex unloads its thread and Copilot CLI detaches it.
- **Settings:** the ant's `model` (with its `context`'s `model_suffix`) and `reasoning_effort` go to the CLI natively; unsupported values fail the session.
- **Errors:** the CLI's not-signed-in errors reach the daemon as ACP `auth_required` (`sign_in` names the command), and its provider limit, quota and context-overflow errors carry its message, HTTP status and retry hint, so [sign-in, limits](#sign-in-provider-limits-and-failures), [fallbacks](#fallbacks) and the [context-window retry](#lost-sessions-and-context-window) behave as for ACP. Credentials the provider refuses only at a prompt (a revoked or wrong API key, which still opens a session) stay refused where the translator can read them (Claude Code, codex, agy's Gemini API key): until they change, or the daemon restarts, opening a session fails `auth_required` too, so the sign-in check every 60 s keeps the harness signed out rather than finding it signed in for the next prompt to be refused again.
- **Updates and spend:** replies, reasoning, tool calls, plans, token usage and cost stream as status updates and count toward [caps](#prompt-limits-and-spending-caps).
- **MCP:** each session gets the ants MCP server in the CLI's own per-run MCP configuration, never on its command line.
- **Skills:** for an ant whose provider is sense, the confirmed skills in `.sense/skills` of its home reach Claude Code as a plugin (`--plugin-dir` on `.sense`), codex as an extra skill root (`skills/extraRoots/set`) and Copilot CLI as a session skill directory; agy sees them only in the brief ([sense.md](sense.md#persona-and-skills)).
- **Checks:** `ants harness check` starts the CLI, completes its handshake and opens a session, naming the CLI and its version.
- **Cancel:** a cancelled prompt interrupts the CLI natively where the protocol can, else stops the CLI, the session resuming on the next message.
- **Steering:** Claude Code, codex and Copilot CLI take a [steering message](#prompt-limits-and-spending-caps) into the running turn; agy does not, so its prompt is cancelled and the message runs next.

**Permissions are the CLI's own.** For a native protocol, `mode` names the CLI's own permission mode (each protocol accepts its own values; others fail loading), and `args` carry its other permission settings. The translator sends no tool request to ants and denies, with a logged warning, anything the CLI asks a human to approve, so no prompt waits on an approval. A native-protocol harness therefore runs its tools unasked: allow rules, approvals, `escalate_to` and webhook or schedule narrowing do not reach them, and `ants daemon` and `ants doctor` warn as for the built-ins, suggesting `tools = "ask"`. Under `tools = "ask"` ([Asking ants about tools](#asking-ants-about-tools)), the `claude`, `codex` and `copilot-sdk` translators send what the CLI asks to ants as a tool request instead, and answer the CLI with the decision; `antigravity` refuses it.

#### Claude Code (`protocol = "claude"`)

```toml
[harnesses.claude-native]
command = "claude"
protocol = "claude"
mode = "acceptEdits"                                  # Claude Code's --permission-mode
args = ["--disallowedTools=WebFetch,WebSearch"]       # its other permission settings
sign_in = "claude login"
```

Each session runs one `claude -p --input-format stream-json --output-format stream-json --verbose
--include-partial-messages --thinking-display summarized --permission-prompts none --strict-mcp-config` process for its
life, in the ant's workdir, taking each prompt as a user message on stdin; the process exits when the session closes
(stdin closed, killed after 10 s). Claude Code 2.1.325 or later is required: an older one fails the session, naming its
version.

- **Sessions:** a new session's id is chosen up front (`--session-id`), a stored one resumes with `--resume`, and a
  cloned thread forks with `--resume <parent> --fork-session --session-id <new>`, the fork written at once, without a
  model call. A session that never ran a prompt has no transcript; resuming it starts a new session.
- **Settings:** `model` (with its `context`'s `model_suffix`, as `opus[1m]`) is passed as `--model`; `reasoning_effort`
  as `--effort` and must be `low`, `medium`, `high`, `xhigh` or `max`, else the session fails. An unknown model fails the
  first prompt with Claude Code's message.
- **Permissions:** `mode` is `--permission-mode`: `default` (or `manual`), `acceptEdits`, `plan`, `auto`, `dontAsk` or
  `bypassPermissions`. With `--permission-prompts none`, a tool call Claude Code would ask a human about is denied at
  once (logged as a warning) and the turn goes on; under `tools = "ask"`, `--permission-prompts host
  --permission-prompt-tool stdio` has Claude Code ask the translator, which asks ants (set `ask = { mode = "auto" }` or
  another gated mode: in `bypassPermissions` nothing is asked). What runs unasked is decided by the mode, by permission flags in
  `args` (`--allowedTools`, `--disallowedTools`, `--tools`, `--settings` with `permissions` rules) and by Claude Code's
  own settings files (`~/.claude/settings.json`, the project's `.claude/settings*.json`; `--setting-sources` in `args`
  limits which load). The ants MCP server's tools are always allowed (`--allowedTools=mcp__ants`). Put a list-taking
  flag in its `--flag=value` form so that it does not take the arguments after it. `args` may not hold the flags the
  protocol passes itself (`--input-format`, `--output-format`, `--session-id`, `--resume`, `--continue`,
  `--fork-session`, `--no-session-persistence`, `--permission-mode`, `--permission-prompt-tool`,
  `--permission-prompts`): loading the file fails, naming it.
- **MCP:** the ants MCP server is given over streamable HTTP on stdin (Claude Code's `mcp_set_servers`), never on the
  command line. `--strict-mcp-config` keeps other MCP servers out: those of `~/.claude.json`, the project's `.mcp.json`,
  plugins and claude.ai connectors. Add servers with `--mcp-config` in `args`, and allow their tools there.
- **Cancel:** Claude Code's `interrupt` ends the turn, the session going on; a turn not over 10 s later has its process
  killed, and the session resumes on the next message.
- **Steering:** a steering message goes to stdin as a user message while the turn runs; Claude Code takes it between
  model calls, or after the turn's `result` as a turn of its own. The CLI runs with `--replay-user-messages` and
  `CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS=1`, and the prompt ends once Claude Code reports itself idle with each steer
  replayed, its answer the text of every turn and its usage their sum.
- **Errors:** "Not logged in · Please run /login", an invalid API key or an expired login are `auth_required` (a
  Claude Code with neither a login nor an API key already fails opening a session, so `ants harness check` sees it;
  one refused at a prompt keeps sessions from opening until `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`,
  `CLAUDE_CODE_OAUTH_TOKEN` or `.credentials.json` in the configuration directory changes, a login kept in the macOS
  keychain excepted);
  plan, spend and credit limits ("You've hit your session limit", `billing_error`, a rejected `rate_limit_event`) are an
  exhausted quota, retried after its reset time when Claude Code gives one; other 429 and 529 answers are rate limits;
  "Prompt is too long" and the like are a context overflow.
- **Spend:** the cost is Claude Code's `total_cost_usd`, which it restores when a session resumes; a fork's starts at its
  parent's, which the fork's stored id carries (`<id>~<base>`), so it reports only what it spent itself, after a
  restart too. The context window's use is that of the last model call. Prompts take text and images (base64 image
  blocks).
- **Environment:** unless the definition or the ant sets them, `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1` (no
  telemetry, error reports or auto-updates), `CLAUDE_CODE_ENABLE_TODO_TOOLS=1` and `CLAUDE_CODE_ENABLE_TASKS=0` (the
  TodoWrite tool, whose todo list streams as the plan). `ANTHROPIC_API_KEY` in the environment takes precedence over a
  claude.ai login.

#### Codex (`protocol = "codex"`)

The translator starts `codex app-server` (codex 0.169.0 or later; an older one fails the session, naming its version) with the definition's `args` after `app-server`, such as `-c KEY=VALUE` config overrides, and speaks its JSON-RPC on stdio: one server per harness session, hosting its threads.

```toml
[harnesses.codex-native]
command = "codex"
protocol = "codex"
mode = "workspace-write"                        # default; see below
sign_in = "codex login"
args = ["-c", "sandbox_workspace_write.network_access=true"]
env = { CODEX_HOME = "/srv/ants/codex" }        # optional: config, credentials and threads of its own
```

- **Sign-in:** at start the translator reads the account (`account/read`); none signed in, where the configured provider needs an OpenAI account, is `auth_required` (`codex login`). A turn the provider refuses with HTTP 401 (a revoked or wrong API key) or a failed token refresh is too, and since `account/read` still finds the stored account, sessions then fail `auth_required` until `auth.json` in `CODEX_HOME` changes (`codex login`; credentials kept in the OS keyring are not watched). `codex app-server` ignores `OPENAI_API_KEY` and `CODEX_API_KEY` in its environment: store a key with `printenv OPENAI_API_KEY | codex login --with-api-key`. `CODEX_HOME` selects the configuration and credentials, `~/.codex` by default.
- **`mode`** sets each thread's approval policy and sandbox; none asks, codex's OS sandbox being the restriction. Any other value fails loading.

    | `mode` | Approval policy | Sandbox |
    |---|---|---|
    | `workspace-write` (default) | `never` | `workspace-write`: writes in the workdir, `$TMPDIR` and `/tmp`; no network unless `sandbox_workspace_write.network_access` |
    | `read-only` | `never` | `read-only` |
    | `full-access` | `never` | `danger-full-access`: no sandbox |

  Both are sent on every thread start, resume and fork, so `approval_policy` and `sandbox_mode` from `config.toml` or `args` do not apply; `sandbox_workspace_write.*` settings do. What codex still asks (a command or file change approval, more permissions, an MCP elicitation, user input) is declined at once, logged. Under `tools = "ask"`, the approval policy is `on-request` with the mode's sandbox, and a command or file change approval goes to ants as an `execute` or `edit` tool request; the rest is still declined.
- **Model and effort:** the ant's `model` and `reasoning_effort` (as `model_reasoning_effort`) go with each thread start, resume and fork.
- **MCP:** the ants MCP server goes in the thread's own configuration (`mcp_servers.ants`: command, arguments, its token in `env`, `default_tools_approval_mode = "approve"` so its tools run unasked in the sandbox), sent in the request on stdin and again on resume and fork, as codex keeps none of it with a thread.
- **Sessions:** the thread id is the session id; resuming and cloning use `thread/resume` and `thread/fork`. Codex keeps a thread only once a turn has started in it, so resuming or forking one never prompted fails and the thread starts over from its history.
- **Cancel:** `turn/interrupt`; the turn ends as cancelled.
- **Steering:** `turn/steer` naming the running turn adds a steering message to it; one sent before the turn started, or after it was interrupted, is refused and waits.
- **Updates:** agent messages (set apart by blank lines) form the reply; reasoning summaries stream as thoughts; commands, file changes, MCP and dynamic tool calls, web searches and image views as tool calls; plan updates as plans. Token usage counts toward token caps; codex reports no cost, so cost caps see none.
- **Errors:** a context-window error is a context overflow; a usage limit is an exhausted quota, retried after the latest reset of a rate-limit window codex reported used up (ChatGPT sign-in only); a rate limit or HTTP 429 is a rate limit, as are an overloaded server and capacity errors.


#### Antigravity (`protocol = "antigravity"`)

Runs Antigravity CLI 1.5.9 or later headless, one `agy` process per session, as `agy --input-format stream-json --output-format stream-json --disable-slash-commands --settings FILE [--add-dir DIR] [--model MODEL] [--effort EFFORT] [--conversation ID]` followed by the definition's `args`. The environment gets `AGY_CLI_DISABLE_AUTO_UPDATE=true` unless the definition or the ant sets it (agy otherwise replaces its own binary in the background). `ants harness check` names it `agy` with the version `agy --version` prints; an older agy fails the check.

- **Arguments:** flags the protocol sets or that change how agy speaks (`--input-format`, `--output-format`, `--conversation`, `--continue`/`-c`, `--print`/`-p`/`--prompt`, `--prompt-interactive`/`-i`, `--print-timeout`) fail loading the definition. A prompt starting with `/` reaches the model as typed.
- **Settings:** agy reads only the last `--settings` given, so the definition's own `--settings` (inline JSON, else a file relative to the session's directory) is taken out of its arguments and merged into the session's settings file, with `mode` as `toolPermission` and an allow rule `mcp(NAME/*)` for each ants MCP server.
- **`mode`:** agy's tool permission mode (`toolPermission`): `request-review` (agy's default when unset), `proceed-in-sandbox`, `always-proceed` or `strict`; anything else fails loading. Headless agy asks no one: a tool its settings would ask about is denied at once and ends the turn, the reply being what agy said until then, and each denial is logged as a warning. Antigravity refuses `tools = "ask"`. `--dangerously-skip-permissions` in `args` approves every tool, allow, ask and deny rules (`permissions` in `--settings`, such as `command(git)` or `write_file(src/)`) restrict them, and `--mode accept-edits` or `--mode plan` sets agy's agent mode.
- **Sign-in:** `agy` run once on a terminal (a Google account), or a Gemini API key: `GEMINI_API_KEY` in `env` with `--settings '{"modelProvider": "gemini"}'`. Signed out, agy fails the session at once (`authentication failed or timed out`). agy opens a session with any API key, rejecting a wrong or revoked one only at a prompt (`API key not valid`), which counts as signed out; where agy's settings choose `"modelProvider": "gemini"` (the definition's `--settings`, else `~/.gemini/antigravity-cli/settings.json`; a settings file relative to the session's directory counts as choosing it), sessions then fail `auth_required` until `GEMINI_API_KEY` changes.
- **MCP:** ants' MCP server, over HTTP, goes in `.agents/mcp_config.json` of a directory private to the session (mode 0700 under the system's temporary directory, removed with the session), given to agy with `--add-dir`, which also makes that directory part of the session's workspace. Its tools load lazily (agy's `call_mcp_tool`) and each call may take up to 31 minutes.
- **Sessions:** the session id is agy's conversation id, resumed with `--conversation`. agy opens a new conversation for an id it does not have; that counts as a failed `session/load`, and the thread's history seeds the new conversation. agy cannot fork: a cloned thread starts over from the history.
- **Model and effort:** `model` goes to `--model` (the slugs of `agy models`, such as `gemini-3.1-pro-high`), `reasoning_effort` to `--effort` (`low`, `medium`, `high`, `xhigh` or `max`); an unknown model or a conflicting pair fails the session.
- **Updates:** reply text streams as agy writes it; tool and subagent steps are tool calls; agy streams no reasoning. A prompt's tokens are the sum of its steps' (agy's own totals cover the whole conversation); agy reports no cost.
- **Errors:** a failed turn ends the process; its error and agy's `AGY_ERROR` stderr line are classified: HTTP 429 or `RESOURCE_EXHAUSTED` as a rate limit (or a quota, by its wording, with its retry hint), "exceeds the maximum number of tokens allowed" as a context overflow, and a too-low AI credits balance or a daily quota as an exhausted quota. A turn whose 429 agy retried successfully is no failure, although agy reports it, and every later turn of the conversation, with status `ERROR` and `API error (attempt N)`.
- **Cancel:** SIGINT, on which agy ends the turn and exits; the next prompt starts agy again on the conversation, which keeps the cancelled prompt but not its partial answer. On Windows the process is stopped instead.
- **Attachments:** text only: attached files are named in the prompt.

```toml
[harnesses.agy]
command = "agy"
protocol = "antigravity"
sign_in = "agy"
mode = "request-review"
args = ["--settings", '{"permissions": {"allow": ["command(git)", "command(cargo)"]}}', "--mode", "accept-edits"]
memory_files = ["GEMINI.md", "AGENTS.md", ".agent/rules/*.md", "~/.gemini/GEMINI.md"]
```

#### Copilot SDK (`protocol = "copilot-sdk"`)

`protocol = "copilot-sdk"` runs Copilot CLI's JSON-RPC server, `copilot --server --stdio --no-auto-update --disable-client-telemetry` followed by the definition's `args`, one CLI per harness process, and drives it through the GitHub Copilot SDK for Rust (github-copilot-sdk 1.0.29, linked without its features: it neither downloads a CLI nor adds a TLS or HTTP client). The CLI must speak the SDK's protocol version (3, as Copilot CLI 1.0.112 does); another fails the start, naming the CLI's version and protocol and the SDK's.

- **Tools:** the CLI's server ignores tool and permission flags on its command line, so the translator takes `--available-tools` and `--excluded-tools` out of `args` (the definition's and those of its `context` values) and sets them on every session. A tool not available does not exist for the model, so nothing asks about it. The flag is followed by tool names as separate arguments, comma-separated, or after `=` (`--excluded-tools=bash,read_bash`); each tool is named on its own (excluding `bash` leaves `read_bash`, which reads the output of a shell still running); a source-qualified name (`builtin:view`, `mcp:ants-ask_ant`, `mcp:*`, `custom:*`) matches tools of that source only; a bare `*` fails loading, and so do `--allow-tool` and `--deny-tool`. An `--available-tools` list leaves out the ants tools unless it names them (`mcp:*`, or each `ants-<tool>`). A name the CLI does not know is ignored, the CLI noting it in its session events. The other `args` go to the CLI's command line.
- **Permissions:** every permission request of the CLI (a shell command, a file write, an MCP tool, a path or URL outside the working directory) is approved, the tools left being those the definition enables; one that managed policy reserves to a human is denied, with a logged warning. Under `tools = "ask"`, each request other than those goes to ants as a tool request (`execute` for a shell command, `edit` for a write, `read`, `fetch` for a URL, else `other`) and is approved or rejected as decided. `mode` fails loading: the SDK has no permission mode to select.
- **Settings:** the ant's `model` and `reasoning_effort` are set on each session; a `context` value whose `args` hold `--context TIER` (`default`, `long_context`) sets the session's context tier. A model the account cannot use fails the session.
- **Sessions:** a new session gets an id the translator chooses; `session/load` resumes the CLI's session by id and a cloned thread forks it with the CLI's own fork, whose AI credits start at its parent's (read from the CLI's `session.usage.getMetrics` and carried in the fork's stored id, so the fork reports only its own cost). Sessions are kept under the CLI's `COPILOT_HOME` (default `~/.copilot`).
- **MCP:** each session gets the ants MCP server (stdio, or HTTP with its bearer header) in its session configuration, never on the command line; the model sees its tools as `ants-<tool>`, such as `ants-ask_ant`. The CLI's server ignores `--additional-mcp-config` on its command line, so the translator takes it out of `args` (repeatable, `--additional-mcp-config VALUE` or `=VALUE`, the value JSON `{"mcpServers": {...}}` or `@FILE`, read at each start) and sets its servers on every session, a later value's server overriding an earlier one of the same name and the ants server overriding both; a server without `type` is `stdio` when it has a `command`, else `http`. Invalid JSON, or a file that does not read, fails the start. `ants ant mcp ANT` shows the servers' status and signs the CLI in to one asking for OAuth ([auth.md](auth.md#admin-orders)).
- **Microsoft Entra:** `entra_client_id` names the Application (client) ID of a Microsoft Entra app registration, sent on the SDK's handshake so the CLI can sign in to MCP servers authenticating through Entra (`ants ant mcp ANT --sign-in SERVER`). ants ships no Entra app registration, so it has no default: without it, the CLI's built-in Entra sign-in is off. A blank value fails loading, and so does `entra_client_id` on a harness of another protocol; a CLI whose server predates the SDK's `connect` handshake fails the start. No MATS application name is sent, so Entra sign-ins go unreported to Microsoft's auth telemetry.
- **MCP sign-in:** `mcp_sign_in` lists MCP servers, by the names the session's MCP configuration gives them (as `ants ant mcp ANT` shows them), to sign in to whenever one needs it. The CLI and the operating system's broker acquire, store and renew the tokens; ants handles none. As a session serving a thread is created or resumed, the translator has the CLI start the session's MCP servers (`session.mcp.list`) and follows each listed server's status in the session's events, which the CLI sends only live, never replaying them on resume. A server settles once its status is no longer pending and 1 s passes without another change of it; the session takes no prompt until each listed server settled and was dealt with, or 30 s passed. A connected server needs nothing. One needing authentication is signed in with `session.mcp.oauth.login`, which signs in without a browser while the kept credentials hold. A sign-in during which the session reconfigured the server (removed it, or had it reconnect) was cancelled: it runs again once the server settles, up to 3 times in a row before counting as failed. The session goes on watching for its life, signing in again whenever a listed server later needs authentication (its credentials not renewed, its configuration changed). A fork signs in to none, the session resumed on it next doing so; sessions serving no thread (those of `ants harness check` and `ants harness prompt`, `ants ant mcp`, and the daemon's checks of the harness) sign in to none either. A sign-in needing a browser logs a warning with the address to open, and the owner of the thread the session serves (else the default human) gets an `mcp_sign_in` notice on the thread, directly (pushed as `ANT: MCP sign-in needed`; `ants chat` shows the thread waiting on the human rather than stuck, as the ant answers without the server): `[ants] ANT's MCP server S needs a browser sign-in on MACHINE: open URL in a browser there. ...`, also naming `ants ant mcp ANT --sign-in S` with `--callback` for a browser on another machine. While that sign-in waits (until the server connects in any session, the session that started it reconfigures the server, the CLI exits, or 10 min pass), the sessions of the harness's CLIs keeping credentials in the same place (same `command` and `COPILOT_HOME`) start no other sign-in to the server, each answering the waiting one's address; once the server connects, the other open sessions listing it are told its credentials changed (`session.mcp.oauth.authenticationStateChanged`), so they use them without `ants ant restart`. As the session opens, a listed server failing to start, disabled, not configured in the session, or not settled within 30 s logs a warning and notifies the owner (`[ants] ANT could not sign in to MCP server S: ERROR`), and so does a sign-in failing or not answering within 30 s at any time; the session opens either way, without the server. Notices go out while a prompt of the thread runs; the owner gets one per server, place the credentials are kept and outcome, again only for another browser address, or after the server signed in since. Names must not be blank or repeated, and `mcp_sign_in` on a harness of another protocol fails loading.
- **Sign-in:** the start asks the CLI whether it is signed in, which checks its token (`COPILOT_GITHUB_TOKEN`, `GH_TOKEN`, `GITHUB_TOKEN`, else the stored login) with GitHub, so a revoked or wrong token already fails the start; signed out, the harness is (`sign_in = "copilot login"`).
- **Cancel:** aborts the CLI's turn, which ends cancelled; the CLI keeps running.
- **Steering:** a steering message is sent with immediate delivery; the prompt ends at the `session.idle` after the CLI took each (its `user.message`), one arriving as a turn ended running as a turn of its own.
- **Limits:** the CLI's turn errors are told apart by type: `rate_limit` is a rate limit except the weekly one (`user_weekly_rate_limited`), which counts as an exhausted quota as `quota` does (AI credits or premium requests used up); `context_limit` is a context overflow; `authentication` is signed out.
- **Updates and spend:** replies, reasoning, tool calls (with the file they work on), the context window's use, each turn's tokens and the session's cost, from the AI credits the CLI counts at US$0.01 each. Images attached to a message go to the CLI as images.
- **Telemetry:** besides `--disable-client-telemetry`, sessions run with session telemetry off. `--no-auto-update` keeps the CLI from updating itself; `env = { COPILOT_AUTO_UPDATE = "false" }` also stops its plugin updates.

```toml
[harnesses.copilot-sdk]
command = "copilot"
protocol = "copilot-sdk"
sign_in = "copilot login"
args = ["--excluded-tools", "bash", "read_bash"]   # no shell; files, search and the ants tools remain
entra_client_id = "00000000-0000-0000-0000-000000000000"   # optional: an Entra app registration's client ID
mcp_sign_in = ["icm", "ado"]   # optional: MCP servers to sign in to whenever they need it

[harnesses.copilot-sdk.context]
default = { args = ["--context", "default"] }
long_context = { args = ["--context", "long_context"] }
```

### Memory files

`memory_files` lists globs of the files a harness takes as memory, which an admin's `read_memories` order shows read-only (`ants ant memories ANT`, the memories pane of the ant view in `ants chat`; [auth.md](auth.md#admin-orders)). A glob is relative to the ant's workdir, `~/` names the home directory of the daemon's user and an absolute path is taken as is; `*` and `?` match within a path component, `**` any number of directories, and `..` is refused. The built-ins default to:

| Harness | `memory_files` |
|---|---|
| `copilot` | `AGENTS.md`, `.github/copilot-instructions.md`, `.github/instructions/**/*.instructions.md`, `CLAUDE.md`, `GEMINI.md`, `~/.copilot/copilot-instructions.md` |
| `claude` | `CLAUDE.md`, `CLAUDE.local.md`, `.claude/CLAUDE.md`, `~/.claude/CLAUDE.md` |
| `codex` | `AGENTS.md`, `AGENTS.override.md`, `~/.codex/AGENTS.md`, `~/.codex/AGENTS.override.md` |
| `antigravity` | `GEMINI.md`, `AGENTS.md`, `.agent/rules/*.md`, `~/.gemini/GEMINI.md` |

A `[harnesses.NAME]` redefining a harness without `memory_files` keeps the globs of the one it replaces; `memory_files = []` shows none. A read follows a symbolic link only when its target, resolved, still matches one of the globs and lies under the ant's workdir, the ant's home (`home`, default `~/.ants/agents/<name>`) or the home directory of the daemon's user; `**` follows no linked directory. Anything else is left out, named with the reason. With `BD_DB` in the ant's environment (the ant's `env`, else its harness's, else the daemon's), the read also lists the bd memories of that database (`bd memories --json`, run in the workdir, at most 10 s).

A read is capped so its outcome fits the `ants_machines` bucket: 32 KiB per file or memory (cut short beyond), 192 KiB of JSON for all of them, and 64 files; what is left out is listed with why. The ant's core memories (`whoami`, `memory`, `tasks` of its `[[ant]]`) come from the config file and are edited with a `set` order.

### Choosing a harness in `ants setup`

Without `--harness`, `ants setup` creating a config file chooses the file's top-level `harness` among the built-ins installed here, found by looking up each one's command on `PATH` as the daemon does (`.exe`, `.cmd`, `.bat` on Windows), without starting any. A provider CLI without its ACP adapter (`claude` without `claude-agent-acp`, `codex` without `codex-acp`) is reported as such. Then:

- **One installed:** it is picked and reported (`harness choice`).
- **Several installed:** on a terminal, a numbered prompt lists the four built-ins, marking the installed ones, and takes a number or name; without a terminal, setup fails, asking for `--harness NAME`.
- **None installed:** setup fails, listing each built-in's install and sign-in commands and pointing to custom harnesses.

`--dry-run` reports the choice without writing. With an existing file, setup chooses nothing: a file lacking a harness fails the `config` step, listing the built-ins installed here. `ants harness check` then runs on the chosen harness.

### Drop-in harness files

`*.toml` files in `harnesses.d/` beside the config file (`~/.ants/harnesses.d/` by default; hidden `.NAME` files and subdirectories skipped) load in filename order across `daemon`, `doctor`, `harness check` and `harness prompt`. Each file may contain only `[harnesses.NAME]` tables; relative `command` paths containing `/` resolve against `harnesses.d/`. Duplicate harness names across files or `config.toml` fail loading.

### Sign-in, provider limits and failures

When a harness cannot run prompts for an environmental reason, the daemon holds messages in hand with 20 s progress acks (up to `max_harnesses × 2`), spending no attempts, incrementing no delivery count and dead-lettering nothing (if a tool call already started before a sign-out or limit error, `<ant>.<seq>` is kept so the retry prepends the cut-off warning):

- **Signed out** (ACP `auth_required` `-32000`, or auth error text on error/stderr): publishes `signed_out` in `ants_colony` (`since`, `sign_in`) and `ants.status`, notifies the thread owner (else default human) once directly on the thread with the sign-in command and machine, and probes every 60 s (`initialize` + `session/new` in `workdir`) until signed in.
- **Provider limits** (classified from prompt errors, or from Copilot/Antigravity replies ≤ 4096 chars whose streamed chunks all start with `Error:`, `API Error:`, `Info:` or `⚠`):
  - *Rate limit / overload* (HTTP `429`/`529`, `rate limit`, `too many requests`, `overloaded`, `resource_exhausted`, `exhausted your capacity`, `rate_limited`, or a quota resetting within 5 m): backs off `30s` doubling to `10m` (logged only).
  - *Exhausted quota* (`insufficient_quota`, `quota exceeded`, `usage limit`, `spending limit`, `credit balance is too low`, `quota_exhausted`, plus built-in Copilot/Claude/Antigravity quota phrases): notifies the owner once on the thread and backs off `15m` doubling to `2h`.
  - Explicit retry hints in the error (`retry after 30s`, `resets in 2h27m3s`, `retryDelay`, epoch after `|`) override the delay within the cap. Publishes `limited` (`rate_limit` or `quota`, `since`, `until`) in `ants_colony`.
  - Retry notices the harness streams before an attempt that succeeds (Copilot's `Info: … Retrying...`, Antigravity's `⚠ API error (attempt N): …`) are dropped from the answer when the last of them ends a line, or ends a sentence the answer does not continue on the same line; status updates still show them. An answer whose own first chunk starts like an error line (`Error:` followed by more of the same sentence) is sent whole.
- **Out of open files** (an error saying `Too many open files`, EMFILE or ENFILE, at a harness start or prompt): a hold of the machine, not of the message, so no startup failure either: backs off as a rate limit (`30s` doubling to `10m`, logged as a warning), publishing `limited` of kind `open_files` ([operations.md](operations.md#services)).
- **Startup failures** (missing/broken binary, bad flags/mode/model): retried as normal prompt failures on a single thread (dead-lettered after 5 attempts). After **3 startup failures within 10 m across ≥ 2 threads** with no success in between, a breaker opens: holds messages, publishes `failing` (`since`, `error`) in `ants_colony`, sends one direct notice with a level-0 `harness` escalation (`ants harness check --harness NAME`), and probes every 60 s with the ant's settings. After a probe succeeds, one failure before a message completes reopens the breaker immediately without re-notifying.

### Fallbacks

An ant's optional `fallback` list names harnesses and models tried in order while its own harness is signed out, out of quota or behind an open startup breaker; a rate limit is still waited out. Each entry is one of:

- `"<harness>"`: that harness with its default model, e.g. `"claude"`.
- `"<harness>:<model>"`: that harness with that model (split at the first `:`), e.g. `"copilot:gpt-5"`.
- `":<model>"`: the ant's own harness with another model, e.g. `":claude-opus-4-1"`.

Entries must name a known harness (built-in, `[harnesses.NAME]` or `harnesses.d`), name a model after a colon, differ from the ant's own harness and model, and appear once; anything else fails config loading. A fallback runs with only its model and the ant's `history_budget`: the ant's `reasoning_effort` and `context` belong to its own harness. An ant added from a template takes the template's `fallback` list instead, less an entry naming its own harness and model ([Ant templates](#ant-templates)).

While the ant's own harness cannot serve, each message goes to the first fallback not passed over:

- **Sessions:** a thread whose stored session ran in another harness (recorded beside the session in `ants_sessions`, so across daemon restarts too) starts a new session seeded from the thread's history, as after a failed `session/load`; a `:<model>` fallback resumes the thread's session. A live harness on another backend is replaced once idle.
- **Turned away:** a fallback that is signed out, limited or fails to start is passed over for 5 min (or its limit's retry hint), spending no attempt and touching none of the own harness's holds; with every fallback passed over, messages wait as without fallbacks, and the owner (else the default human) gets one notice naming the fallbacks, why the last turned a message away and when one is tried again, sent again only after a fallback or the own harness answers.
- **Shown:** each reply ends with `[ants] Answered by fallback NAME, harness OWN being signed out.` (or `out of quota`, `failing to start`); `ants_colony` publishes `fallback` (`name`, `since`); `ants colony` shows `fallback` as the activity; `ants ask`, `ask_ant` and `route` treat the ant as served (`route` prefers ants answered by their own harness).
- **Told once:** the owner's sign-in, quota or breaker notice (one per outage, as without fallbacks) says the messages go to the fallbacks meanwhile.
- **Back:** a successful sign-in check or breaker probe, or after a quota's backoff the next message the own harness answers, sends messages to the own harness again and clears `fallback`; live fallback sessions are replaced as their threads come up.
- **Caps:** prompt limits and spending caps apply to every prompt whichever harness runs it.

### Lost sessions and context window

When ACP `session/load` fails, the daemon opens a new session and prepends, after the ant's instructions and the provider's [brief](architecture.md#briefs), an `[ants]` notice plus the thread's stored history from `ANTS` (read with the ant's credentials: inbound messages, own outbound messages via `sentread-<ant>`, thread notes, channel posts), truncated to the history budget (each entry's text capped at 1/8 of the budget, followed by one `[ants] Attached:` line per attachment naming its name, type, size and handle for `read_attachment`, and preceded by a summary of omitted counts, senders and time span).

When a prompt overflows the model's context window (`context_length_exceeded`, `maximum context length`, `prompt is too long`, `exceeds the context window`, or `context_overflow`), the daemon immediately rebuilds the session with **half** the history budget and retries without spending an attempt, closing the overflowed session first (ACP `session/close`) when the harness offers it. If the fresh session also overflows, the daemon escalates `context_window` on the thread and acks the message without dead-lettering.

History budget precedence: ant `history_budget` -> selected `context`'s `history_budget` or `window / 12 × 4` -> harness `history_budget` or `window / 12 × 4` -> `64 KiB`, capped at `window` bytes (1/4 of window at 4 bytes/token). Built-in `claude` sets `window = 200000` (`1000000` for `context = "1m"`); `copilot` and `antigravity` default to `64 KiB`. `history_budget = 0` includes only the omitted-entries summary line.

### Model, reasoning effort and context

`model` is applied before `reasoning_effort` via ACP selectors on new and resumed sessions; unsupported values or mismatched read-backs fail the session and list valid options. The ant's `ants_colony` entry publishes the ones set as `model` (`{model, reasoning_effort, context}`, each left out when unset; the whole field when none is), shown by `ants ant show` and the ant view of `ants chat`.

- **Copilot:** `context` accepts `default` or `long_context` (`--context ...`).
- **Claude:** `context` accepts `default`, or `1m` with an explicit `model` (selects `<model>[1m]`).
- **Antigravity:** no `context` selector; choose a model with the desired window.

### Memory pressure

External harness child processes dominate system memory use (see [operations.md](operations.md#footprint)). On every idle check of an ant's harness pool (every `idle_timeout`, at most 30 s), the daemon reads the system's available memory (Linux `MemAvailable` in `/proc/meminfo`; macOS `hw.memsize × kern.memorystatus_level`; Windows `GlobalMemoryStatusEx`; elsewhere, or when unreadable, memory never counts as low). Below `min_available`:

- Every idle harness (no prompt in flight) stops at once, least recently used first, instead of after `idle_timeout`; its session resumes via `session/load` on the next message. Busy harnesses finish their prompts.
- New threads get a harness only while fewer than `max_harnesses` from `[memory_pressure]` run (capped by the ant's own `max_harnesses`); other messages wait in hand as when the pool is full.
- The daemon logs a warning, sets `memory_low` (`{since, available, max_harnesses}`) in the ant's `ants_colony` entry (shown by `ants colony` and `list_ants`) and `ants_memory_low` to `1` in `/metrics`.

Memory counts as recovered once available memory reaches 150 % of `min_available`, so the state does not flap; the daemon logs it, clears `memory_low` and admits threads up to `max_harnesses` again. Each ant checks on its own, so the lower cap applies per ant. `[memory_pressure]` applies to every ant in the file; changing it on reload restarts them.

## Avatars

Every ant has a pixel-art avatar derived from its identity record in `ants_identities` (the avatar seed a human set,
else the `sense` id it names, else its `id`), or from its name while no record is readable. One sense identity so shows
one face on any record: an ant revoked and added again keeps its face, and a clone, with its own sense id, has its own.
An ant revoked and added again with the home of the earlier one keeps its birth and growth stage too: its new record
starts from those its sense identity kept of the earlier one ([sense.md](sense.md#identity)), while a new ant and a
clone start at birth, `callow`. `ants chat`, `ants colony`, `ants watch`, `ants inbox` and `ants ant show` put its
one-line face before its name, and the ant view of `ants chat` and `ants ant avatar` draw its head. `[cli]` `theme`
picks the palette for the terminal's background: `dark`, `light`, or `auto` (the default), which asks the terminal for
its background colour (OSC 11) and waits up to 200 ms for an answer, then reads `COLORFGBG`, else takes `dark`. With
`NO_COLOR` set, or off a terminal, faces show without colour and the head as ASCII. `[cli]` `animate = false` keeps the
heads in `ants chat` still and turns its spinners a frame a second; otherwise only the head on screen moves, spinners
turn 8 frames a second, and the screen is drawn again only when a frame changes.

## Exporting and importing an ant

`ants ant export ANT` writes the ant to one plain JSON file (default `./ANT.ant`, mode `0600`, never overwritten) for
another bus, as for a new job; `ants ant import FILE` there adds it from the file, as `ants add` does, with a new key.
The file is `{"ants_ant": 1, "created_at", "bus", "identity", "settings", "files", "provider"}`:

- `bus`: the public key of the account of the bus exported from.
- `identity`: the ant's identity record, unmarked: its `id`, `born`, `avatar`, `color`, `prompts`, `cloned_from` and
  `sense`.
- `settings`: `whoami`, `harness`, `model`, `reasoning_effort`, `context` and `fallback`, taken from the ant's
  [shared settings](#shared-settings) where the bus holds a record of them that verifies, as its daemon serves them
  (one that does not verify is left out with a warning), else from its `[[ant]]` (`harness`, the machine's, always
  from its `[[ant]]`).
- `files`: each file of its home (`home`, default `~/.ants/agents/<name>`) as `{path, kind, data}`, `data` in base64, at
  most 64 MiB in all. `kind` is `harness` for the files of its harness's [memory files](#memory-files) globs, taken
  relative to the home (`~/` globs are left out), and `core` for every other regular file of the home, symbolic
  links not followed.
- `provider`: what the ant's provider holds of it, `{provider, data}`, as the provider exported it, `data` its own
  form; left out when the ant's provider exports nothing, or with `--no-provider`. Only the daemon serving the ant
  reaches its provider, so the export sends this machine's daemon an `export_provider` admin order
  ([auth.md](auth.md#admin-orders)), which needs a human device's admin credentials and the daemon online: the daemon
  stores the provider's export in `ants_files` (at most 64 MiB), where the export reads it. A provider failing fails
  the export, naming `--no-provider`. The built-in sense's part carries the ant's sense log and skill files
  ([sense's part](#senses-part)).

Left out: the home's `worktrees`, `answered`, `unsent`, `buried`, `mcp.json`, `mcp.sock`, `lifecycle`, `.git`, `.beads` and `.bd`, the
files of `.sense` but the sense id and earlier names (`.sense/id` and `.sense/names`, which the ant keeps on its new
bus; its cache and chain status are not, and its skill files, `.sense/skills`, travel in sense's part with its log), the task database `BD_DB` names when it lies in the home, the rest of the
ant's settings (its role on the bus: `capabilities`, `escalate_to`, `allow`, `memory`, `tasks`; its machine's:
`workdir`, `env`, caps and limits), its credentials, sessions, threads and history. A templated ant imports as a plain `[[ant]]`.

The export runs on the machine whose config file lists the ant, with a human's credentials. While another machine
serves the ant it warns that the home here may be behind; while this one serves it working, it waits up to 2 minutes
for its prompts in flight to end, so the files are packed between prompts. Nothing signs or encrypts the file: keep it
safe, as it may hold sensitive memory.

`--move` then marks the ant moved in its identity record. The daemon serving it drains it as on a stop (it takes no
new message, hands back those waiting and finishes the prompts in flight) and leaves the colony, every daemon listing
it waits for the mark to go before serving it, and messages to it get the notice of an ant no daemon serves. `ants
colony`, `ants doctor`, `ants ant show` and the ant view of `ants chat` show it moved. `ants ant unseal ANT` clears
the mark; `ants auth revoke ANT` ends the ant on the old bus once the import succeeded
([auth.md](auth.md#moving-an-ant-to-another-bus)).

`ants ant import FILE [--as NEW]` takes the flags of `ants add` and:

1. Refuses a config file already listing the ant, and a home (`--home`, default `~/.ants/agents/<name>`) not empty.
2. Creates its identity record on the bus by a KV create, refused when a record holds its name, or its id under any
   name, so no bus holds two copies of one ant. The id is looked up by reading the whole `ants_identities` bucket, one
   record per ant.
3. Restores its files into its home, leaving out its harness memory files, with a warning, when the ant runs another
   harness here (`--harness`, else the exported one, else the file's top-level `harness`), and queues there the
   lifecycle events `ant_moved`, and `ant_renamed` under `--as`, for its provider
   ([architecture.md](architecture.md#lifecycle-events)). Under `--as`, it lists the old name among the ant's earlier
   names in `.sense/names`, so the ant keeps its sense data ([sense.md](sense.md#identity)).
   When the file carries sense's part, and without `--no-provider`, it marks the home as awaiting it
   (`.sense/importing`): until sense's part lands, sense opens none of the ant's data, so nothing is written to the
   ant's log on the new bus before the log arrives, and the lifecycle events wait.
4. Lists it in `config.toml` with the exported settings, the flags given winning, and gives it a new key, as `ants
   add` does: with `--jwt` or `--enroll` its credentials complete at once, else `ants add NAME --jwt ...` completes
   them later. The first daemon serving it creates its shared settings on the new bus from that table; when the bus
   holds shared settings of the name already, those serve instead, and the import warns which imported fields they
   set otherwise.
5. Hands the file's `provider` to the ant's provider through an `import_provider` admin order to this machine's
   daemon, the export stored in `ants_files` for it, which the daemon carries out once it reads its config file
   listing the ant: only to a provider of the same name, which takes exports. Whatever stops it (credentials
   without the admin role, the daemon offline, another provider) is only a warning, the rest imported:
   `--provider-only` hands that part again, alone, to the ant the config file lists already. `--no-provider` leaves it
   out.

A failed import takes back the record it created and the files it restored. `--as NEW` imports the ant under another
name, keeping its id, birth, avatar and prompts. The ant starts with no tasks.

### sense's part

The built-in sense exports an ant's sense log, as stream `ANTS_SENSE` stores it
([architecture.md](architecture.md#stores-and-subjects)), and its skill files, `.sense/skills/**` in its home, as
`{manifest, files}`:

- `manifest`: `{version, sense, ant, events, tip, accounts, files}`: the version (1), the sense id, the ant's name,
  the log's number of events and its last one (`tip`, `{index, sha256}`, left out for an empty log), the public keys
  of the team accounts whose ants' credentials signed the events (the bus exported from, and the buses before it the
  log came from; empty for a server without authentication), and each file as `{path, sha256, size}`.
- `files`: each file as `{path, data}`, `data` in base64: `log.jsonl`, each event as stored, one a line, then each
  skill file under `skills/`, its path relative to `.sense/skills`.

The export needs the daemon connected to its bus, and refuses while changes of the ant wait in its cache to be
appended. The import, under the export's sense id, which the home keeps (refused when it keeps another), takes in
nothing unless every file matches its size and hash, no file is missing or unlisted, skill paths stay under `skills/`,
and the log's chain verifies: each event at its position, chained to the one before, of the ant under its name or
the earlier names the home lists, and signed with credentials an account of `accounts` issued, valid when the event
was made, up to the `tip`. Revocations of the bus exported from are not seen on the new one, and nothing signs the
export: the checks find a file damaged or edited, not a log made anew with other credentials of the account.

The events are appended to the ant's log on the new bus as they were stored, so their hashes and signatures stay
whole, followed by an `ant_renamed` event when the ant is imported under another name, and a `log_imported` event
signed on the new bus vouching for them ([architecture.md](architecture.md#stores-and-subjects)). The log on the new
bus must be empty or hold the export's first events, as an import cut short leaves it: the import appends the rest.
A log holding the export already takes nothing more, and one holding other events refuses the import, merging
nothing. The skill files are then written into `.sense/skills`, the home stops awaiting the import, and the ant's
cache is built anew from its log.

## Catch-up summaries

`ants thread tldr THREAD` and `ants digest` ask the ant named by `[cli]` `summary_ant` (default: the messenger) for a
summary: a message on a new thread owned by the CLI's human, carrying a `summary` request (`thread` and `upto` for a
TL;DR, `since` and `upto` for a digest) and, as text, the newest stored entries within 16 KiB, each cut to 1 KiB. The
daemon prompts the ant to answer in at most 120 words of plain text without tools, and its reply carries the request
back with the tokens the turn spent (`tokens`). The CLI waits for that reply, marks it read and prints it:

- **TL;DR:** stored for every human at `summary.<thread>` in KV `ants_threads` (`{thread_id, text, by, for, at, upto,
  tokens}`) and printed again without asking while no entry came after entry `upto` (`--new` asks anyway; `--json`
  prints the stored object).
- **Digest:** of the threads with entries within `--since` (default `24h`, `0` all), stored nowhere.

Threads opened by summary requests are left out of `ants thread list`, `last`, `ants dash`, digests and the inbox
(`ants inbox` marks their replies read), and `ants webhook serve` pushes no completion for those replies. Each verb
then notes on stderr the tokens the human's summaries spent this UTC day, as the replies to its requests carry them.
`ants chat` asks the same way: `S` and `/tldr` for a TL;DR, `s` on home and `/digest [SINCE]` for a digest, shown in
an overlay. With `summaries = "ahead"` (and `summary_daily_tokens` above 0), `ants chat` also asks for the TL;DR of
each thread its work view lists whose TL;DR is missing or older than its last entry, one request at a time, and stops
for the UTC day once the human's summaries spent `summary_daily_tokens` that day; a config with `ahead` and no daily
tokens fails to load. No service generates summaries on its own.

## Prompt limits and spending caps

Per-prompt defaults: `prompt_timeout = "30m"` (includes approval wait; `"0"` disables) and `max_tool_calls = 300` (`0` disables), resetting every prompt. Optional spending caps bound tokens or cost per thread (`max_thread_tokens`, `max_thread_cost`) and per ant per UTC day (`max_daily_tokens`, `max_daily_cost`), plus shared `pool_daily_tokens` / `pool_daily_cost` on `[[ant_template]]` (summed across listed template ants via `ants_sessions`).

- **Exceeding a limit or cap:** cancels via ACP `session/cancel` (restarting the harness after 30 s if hung), replies with partial output plus an `[ants]` notice, and escalates ant-originated threads (`cap`; pool caps escalate to `added_by` first). Further agent messages on a capped thread escalate immediately without prompting.
- **Resets:** thread allowances reset on any human message on the thread, and once per escalation level when the leader replies; daily caps reset at 00:00 UTC (or on restart with a higher cap).
- **Warnings:** crossing `spend_warn_percent` (default `80`, `< 100`, `0` disables) sends one `[ants]` warning per allowance/UTC day on a new thread (thread cap -> thread owner else default human; daily/pool cap -> default human).
- **Manual cancel:** `ants cancel ANT THREAD` (human creds; core NATS `ants.cancel.<ant>.<thread>`) cancels the active prompt the same way, replies with partial output and `[ants] The prompt was cancelled by <human> (ants cancel).`, and acks without retry, dead letter or escalation. Queued messages behind it still run.
- **Automatic steering:** a human message (including channel replies waking the ant) or an ant message starting work on a thread with a running prompt steers it by default ([architecture.md](architecture.md#pool-worktrees-and-at-least-once-retries)): harnesses that [take steers](#harness-protocols) (`claude`, `codex`, `copilot-sdk`) fold it into the running turn, while others cancel the prompt like `ants cancel` (`[ants] <sender> interrupted the prompt to steer <ant> with a new message.`) and run it next in the same session ahead of ordinary queued messages. Messages marked `queue` (`ants ask --queue`, `ants post --queue`, `/later TEXT` in `ants chat`) wait behind the running prompt instead. A human's steer renews the turn budget and allowance; an ant's does not.
- **Reporting:** prompt token totals (from ACP prompt responses) and cumulative session cost (from ACP `usage_update`) persist at `<ant>.<thread>.spend` and `<ant>.day.<YYYY-MM-DD>` in `ants_sessions`. Claude and protocol `copilot-sdk` report tokens and USD cost; Codex and protocol `antigravity` report tokens only; built-in ACP `copilot` and `antigravity` report neither.

### Spend report

- `ants spend [--days N]`: one row per ant with spend in the last `N` UTC days (default 7, max 366): today's and total tokens/cost.
- `ants spend ANT`: daily breakdown, total, and threads with spend since the last human message (costliest first).
- `ants spend [ANT] --thread THREAD`: spend on `THREAD` since the last human message, then in all (`TOTAL_TOKENS`, `TOTAL_COST`; `total` in `--json`).

Ants listed with caps in the local `--config` show `spent/cap`; remote ants show spend only. `--json` emits one JSON object per line.

## Tool permissions

Tool calls a harness asks about are checked against `allow` (the [built-in harnesses](#harnesses) and harnesses of a [native protocol](#harness-protocols) run their own tools unasked unless `tools = "ask"` ([Asking ants about tools](#asking-ants-about-tools)), so neither rules nor approvals reach those) (`--allow RULE`; `allow_tools = true` / `--allow-tools` equals `*`, for sandboxed workdirs only):

| Rule | Matches |
|---|---|
| `*` | Every tool call |
| `<kind>` | ACP kind: `read`, `edit`, `delete`, `move`, `search`, `execute`, `think`, `fetch`, `switch_mode`, `other` |
| `execute:cargo test*` | Command glob; commands containing `;`, `&`, `<`, `>`, `$(`, pipes, backticks or newlines never match |
| `edit:src/*` | Relative paths inside `<workdir>/src` (or the thread's worktree; never outside it) |
| `edit:/srv/notes/*` | Absolute path glob |
| `fetch:https://docs.rs/*` | URL glob (falls back to title) |
| `other:send_message` / `other:bd-*` | MCP and `other` tools by title, or by `<tool>` in `mcp__<server>__<tool>` (Claude) and `mcp.<server>.<tool>` (Codex) |

Matched calls auto-approve (`allow_once`). Unmatched calls publish an `ApprovalRequest` on `ants.approvals.<ant>` and wait `approval_timeout` (default `5m`) for `allow_once`, `allow_always` or `reject`. The request carries the harness's names of the options it offers (`labels`: one line of up to 200 characters each, redacted as the input is), so cards, `ants tools show`, `ants dash`, notices and chain notices say what `allow_always` allows from then on, such as Claude Code's `Yes, and don't ask again for git push commands in /path`; the answer still picks the harness's option by kind, never by name:

- **Humans and push links:** `ants tools approve|reject <ant>/<id>`, `ants dash`, or signed `/_approval` links from `ants webhook serve` store an `ApprovalDecision` on `ants.approvals.<ant>.<id>` (`Nats-Expected-Last-Subject-Sequence` ensures first answer wins; verified by direct get).
- **Chain members:** ants in `escalate_to` receive an `approval` escalation notice on the thread and may answer via MCP `decide_approval(verdict)` (`escalate` is refused on approval notices). Earliest decision wins (human or chain).
- **Suggested rules & `--rule`:** once humans approve an identical call 3 times for an ant (`execute:<cmd>`, `<kind>:<abs-path>` for one file, `fetch:<url>`, else `<kind>:<title>`, excluding `*` and shell metacharacters), `ants tools approve` and approval notices print that rule. `ants tools approve ID --rule` appends it to the ant's `allow` in `config.toml` and reloads the daemon (for templated ants, `--rule` approves once and prints the rule to add to `[[ant_template]]`).
- **Webhook & schedule narrowing:** on harnesses that ask before running tools (not the [built-ins](#harnesses) nor [native protocols](#harness-protocols), which run them unasked, unless `tools = "ask"`), threads started by a webhook or schedule intersect the ant's allowlist with the entry's `allow` (default read-only: `read:*`, `search`, `think`, and MCP `list_ants`, `check_inbox`, `read_notes`, `post_notes`, `send_message`, `ask_ant`, `route`, `escalate`, `react`, `list_reactions`, `suggest_thread_title`, `complete`, `ask_human`, `trail_tips`, `find_trails`, `rate_trail`, the last rating only the trail run reporting on the thread ([trails.md](trails.md#trust)); `["*"]` opts out; `[]` requires human approval for all calls). The narrowing travels on every message, spin-off, channel post, escalation and approval notice on the thread (persisted at `<ant>.<thread>.spend`) until a human messages the ant on the thread or wakes it on the channel thread. An entry's own `allow` leaving out `rate_trail` still lets the turn of a trail run's rating invitation rate that run, and no other; `find_trails`, which the invitation of a failed run points to, stays out unless listed.
- **Stand-in:** each request names the human it waits for (`human`: the thread's human owner, else the default human) and the ant's `stand_in`, if set (escalations sent to a human carry it too). While that human is away, `ants webhook serve` relays the request or escalation to the stand-in, which overrides the human's own `[[human]] stand_in`; a human's assistant is never relayed to: it gets escalations, questions and tool requests first anyway (see [webhooks.md](webhooks.md#stand-in) and [architecture.md](architecture.md#escalations)). Decisions name their human (`by`); when another human decides, the human the request waited for gets an inbox notice (`[ants] <by> decided tool request <ant>/<id> … in place of <human>`); likewise when another human answers an escalation that reached a human, reminders on or off (`[ants] <by> answered the escalation of <ant> …`). `stand_in` must not name the ant itself; templated ants take no `stand_in` (the `[[human]]` one applies).
- **Branch pushes:** with `publish = "draft-pr"`, the daemon's own `git push <remote> ants/<ant>/<thread>` goes through the same check as an `execute` call titled `Push branch … and open a draft pull request`: `execute:git push origin ants/dev-a/*` lets dev-a push its thread branches to `origin` unasked; otherwise the push is an approval request on the thread, and a rejection (narrowed threads included) leaves the branch unpushed with an `[ants]` line in the reply.
- **Expiry:** after `approval_timeout`, the call is rejected; the thread owner (else default human) receives a level-0 `approval` escalation notice on a separate thread (so `ants ask --wait` keeps waiting for the reply), and asked chain ants receive a cancellation notice on the request's thread.

### Redaction

Before publishing tool requests (redacted before the 2000-byte input cut), status streams (buffered to whitespace / PEM boundaries so secrets never split across frames), replies, the final text of a turn that sends no reply (kept on the thread's timeline), escalations, thread notes, MCP messages/posts, files MCP tools attach (`attach`, `attach_to_reply`) whose bytes are UTF-8 text whatever their type (a `.env`, a config dump; the reference records the redacted size and SHA-256, and other files are stored as they are), and failing-harness errors, the daemon replaces secrets with `[redacted]`:

- **Environment values** (≥ 8 chars, not starting with `/`, `~`, `.` or a drive letter): values from the ant's `env`, harness `env`, and daemon env whose variable name contains `TOKEN`, `SECRET`, `PASSWORD`, `KEY`, `AUTH` or `CREDENTIALS`, plus token-like values (alphanumeric, no spaces/slashes/file extensions) in ant/harness `env`.
- **Patterns:** PEM private keys, `Bearer`/`Basic` headers, GitHub/GitLab/Slack/OpenAI/Anthropic/AWS/Google/Stripe/npm/Hugging Face/Telegram tokens, Slack/Discord webhook URLs, NKey seeds, JWTs, URL passwords (`scheme://user:pass@host`), and secret-named assignments containing a digit (`API_KEY=...`, `"password": "..."`, `--token ...`).

Top-level `redact = false` disables daemon redaction. Files a human attaches with the CLI are never redacted. `ants webhook serve` always redacts outbound notifications (patterns + secret-named process env vars). Local logs are not redacted.

## Schedules

`[[schedule]]` entries send `text` literally as a new thread to `ant` or `channel`, or start a run of a saved trail on `ant`, on a timetable (run by `ants daemon` when serving the whole config file, not `--ant`):

```toml
[[schedule]]
name = "nightly"                              # ants auth user nightly --webhook --to dev-a
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"                          # or every = "4h" (min "1m", anchored at Unix epoch)
timezone = "Europe/London"                    # optional (cron only): overrides top-level timezone, else UTC
ant = "dev-a"                                 # or channel = "ops" (with optional mention = [...] or broadcast = true)
owner = "alice"                               # optional human owner for replies and escalations
text = "Triage the failing nightly tests and summarize them."
allow = ["read:*", "execute:cargo test*"]     # optional; default read-only, ["*"] opts out
catch_up = true                               # optional: send latest missed firing within 7d
```

- **Timetable & DST:** set either `cron` (5-field cron with lists, ranges, steps, month/weekday names, `0`/`7` Sunday, or `@yearly`/`@monthly`/`@weekly`/`@daily`/`@hourly`; restricted day-of-month and day-of-week OR together) or `every` (≥ `1m`, epoch-aligned UTC). `cron` uses IANA `timezone` (`/usr/share/zoneinfo` or `TZDIR` on Unix; bundled DB on Windows): spring-forward gaps fire once at the jump; fall-back overlaps fire once on the first pass.
- **Webhook identity & deduplication:** schedules use `--webhook` credentials (`--webhook NAME` in `ants auth leaf`), publish on a webhook's own subjects (`ants.inbox.<ant>.<schedule>.webhook`, `ants.chan.<channel>.<schedule>.webhook`) with `webhook: true`, refresh their version `<schedule>.webhook` in `ants_webhooks` hourly, and narrow threads to `allow`. Multiple daemons or restarts deduplicate firings via `<schedule>.<unix_time>` in `ants_webhooks` (7 d) and `Nats-Msg-Id: schedule.<schedule>.<unix_time>` (24 h).
- **Trails:** `trail = "NAME"` in place of `text` starts a run of saved trail `NAME` ([architecture.md](architecture.md#trails)) on `ant`, its conductor, with `args` (a TOML table, the script's JSON `args`; none: `null`) under `limits`, a table of what each run may spend as `ants trail run`'s flags set it ([trails.md](trails.md#limits)): `steps` (default 50, at most 500), `at_once` (4, at most 16), `turns` (200, at most 2000) and `wall` (a duration, `24h`, at most `7d`), each left out taking its default. Each firing sends the ant a message carrying `trail_start`; its daemon starts the run on the firing's new thread instead of prompting the ant, the run owned by `owner` (else the default human) and as untrusted as a webhook's thread, once per firing (the run takes the message's id). A run that cannot start, such as for an unknown trail, is told to the owner on that thread. An entry gives exactly one of `text` and `trail`; `args` or `limits` without `trail`, a limit of 0 or over its maximum, an unknown key in `limits`, `trail` with `channel`, and `trail` with `allow` (the run's steps are narrowed as a webhook's threads are) fail config loading. The firing carries the entry's limits, which the conductor takes up to the maxima from any message of these credentials: what such credentials may start and spend is in [trails.md](trails.md#trust) and [webhooks.md](webhooks.md#trail-runs).
- **Grace & catch-up:** firings retry every 5 s for up to 1 m (`GRACE`). Firings missed by > 1 m are skipped unless `catch_up = true`, which sends only the **latest** missed firing within the past 7 d (unless the next firing is due within 1 m) with `(Sent late: due YYYY-MM-DD HH:MM UTC.)` appended, retrying every 5 s until the next firing time.

## Trails

`[trails]` applies to every ant in the file ([trails.md](trails.md)): `workers` (default 4, 1 to 16; others fail config loading) caps the trail workers computing at once across the daemon process's runs ([architecture.md](architecture.md#trails)), the first ant to start setting it for the process. Each worker is an `ants subprocess` process alive only while a run's script computes, so the cap bounds the memory and CPU trails take beside the harnesses. Changing it takes a restart of the daemon.
