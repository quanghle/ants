# Configuration

`~/.ants/config.toml` (`--config` / `ANTS_CONFIG`) configures multi-ant daemons, CLI defaults, custom harnesses, worker templates and schedules. An empty file is valid and serves the built-in `messenger` on `copilot`.

```toml
harness = "copilot"                        # default for every ant and built-in messenger
messenger = true                           # default: serve built-in messenger unless [[ant]] names "messenger"
nats = "wss://hub.example/nats"            # default NATS URL for ants and schedules without `nats`
js_domain = "hub"                          # default hub JetStream domain; also per ant and schedule
human = "alice"                            # fallback human for ownerless escalations (default "human"); also per ant
metrics = "127.0.0.1:9464"                 # serve /healthz and /metrics (default: off)
timezone = "America/New_York"              # default IANA time zone for schedule cron (default UTC)
redact = true                              # redact secrets from daemon publications (default true)

[cli]                                      # CLI defaults when no flag or env var is set
creds = "~/.config/ants/auth/alice.creds"  # human credentials (ant credentials are not used here)
nats = "nats://127.0.0.1:4222"             # default: top-level nats
js_domain = "hub"                          # default: top-level js_domain
auto_archive = "14d"                       # hide threads idle this long from thread list and dash (default "0": never; README.md, Auto-archive)

[storage]                                  # JetStream capacity warnings (see README.md, JetStream storage)
warn_percent = 80                          # warn past this share of a store's limit (default 80; 0 disables)
max_file_store = "100GB"                   # hub max_file_store (default: unknown); bytes or 1024-based K/M/G/T[B]
max_memory_store = "1GB"                   # hub max_memory_store (default: unknown)

[memory_pressure]                          # stop idle harnesses early when memory runs low (see Memory pressure)
min_available = "1GB"                      # default; bytes, 1024-based K/M/G/T[B] or a share like "10%"; "0" disables
max_harnesses = 1                          # default; per-ant harness cap while memory is low (at least 1)

[[ant]]
name = "dev-a"
workdir = "~/src/repo-a"                   # must exist
creds = "~/.config/ants/auth/dev-a.creds"
harness = "claude"                         # overrides top-level harness
model = "claude-sonnet-4-5"                # optional exact ACP model ID
reasoning_effort = "high"                  # optional effort ID offered by the model
context = "1m"                             # optional key from the harness context table
history_budget = 131072                    # optional bytes of thread history seeding a rebuilt session
fallback = ["copilot", ":claude-opus-4-1"] # optional; tried in order while the harness cannot serve
home = "~/.ants/dev-a"                     # default: ~/.ants/<name>
whoami = "Maintain Rust backend services." # published in roster and prepended to prompts
memory = "Use notes MCP for durable memory."
tasks = "Use bd for task tracking."
env = { BD_DB = "/srv/ants/dev-a/bd.db" }  # literal env overrides for harness and subprocesses
allow = ["read", "search", "execute:cargo test*"]
allow_tools = false                        # true equals allow = ["*"]
approval_timeout = "5m"                    # default
capabilities = ["rust", "backend"]         # routing tags published in roster
max_harnesses = 4                          # default
worktrees = true                           # git worktree per thread; default: on for a git workdir
                                           # not ignored by git, with max_harnesses > 1 (see Worktrees);
                                           # false shares the workdir
idle_timeout = "10m"                       # default
prompt_timeout = "30m"                     # default; "0" disables
max_tool_calls = 300                       # default; 0 disables
max_thread_cost = 5.0                      # optional; also max_thread_tokens
max_daily_cost = 50.0                      # optional; also max_daily_tokens
spend_warn_percent = 80                    # default; warn once past this % (< 100); 0 disables
escalate_to = ["lead", "dev-b"]            # optional escalation chain; first ant leads
max_escalation_hops = 3                    # default
escalate_after = "30m"                     # default; "0" waits indefinitely
escalation_reminder = "4h"                 # default; human reminder delay; "0" disables

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
```

`~` expands to the home directory; other relative paths resolve against the config file's directory (`env` values stay literal). Unknown keys, duplicate ant/template/schedule names, a schedule named after an ant, or `messenger = false` with no `[[ant]]` fail config loading. Unreachable NATS or JetStream endpoints retry with exponential backoff (`1s` to `30s`); credential or permission errors fail that ant immediately while remaining ants keep running, exiting non-zero only if none start.

## Precedence and reload

- `ants daemon`: serves all ants in `~/.ants/config.toml` (plus built-in `messenger` when enabled) and runs its schedules.
- `ants daemon --ant dev-a` (`ANTS_ANT=dev-a`) without `--config`: configures `dev-a` from CLI flags, loading only `[harnesses.NAME]` from `~/.ants/config.toml` (if present) and `~/.ants/harnesses.d/`.
- `ants daemon --config FILE --ant dev-a`: serves only `dev-a` from `FILE` (`--ant messenger` selects the built-in messenger) with CLI ant flags overriding `FILE`; templated ants refuse ant flags. No schedules run when `--ant` is set.
- Per-ant, per-schedule and top-level `nats`, `js_domain` and `creds` in the file override `--nats`, `--js-domain` and `--creds`, which fill in omitted values (`--creds` applies only when serving a single ant, never to schedules).

`ants daemon` reloads on `SIGHUP` or automatically when `config.toml` or `harnesses.d/*.toml` changes on disk (polled every 3 s by mtime, size and inode; applied once unchanged across two checks, ~6 s after write):

- Added ants (and previously failed ants still listed) start; removed ants drain prompts in flight and return waiting messages for redelivery; ants whose resolved settings changed (including via top-level defaults, `[harnesses.NAME]`, or `[[ant_template]]`) drain and restart; unchanged ants continue uninterrupted (pool membership changes alone do not restart members).
- Added schedules start; removed schedules stop immediately; changed or stopped schedules restart.
- Invalid reloads log an error and keep running the previous config. `metrics` requires a process restart.

## Worktrees

With worktrees on, each thread works in its own git worktree at `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>`, created from `HEAD` on the thread's first prompt and removed once its session expires (a branch with unmerged commits is kept, and its thread is told). A workdir outside a git work tree stays shared.

- Unset `worktrees` turns them on when the workdir is inside a git work tree (a `.git` directory or file in it or above it) that does not ignore it (`git check-ignore`, as for a directory in `target/`) and `max_harnesses > 1`, logging so at daemon start; otherwise threads share the workdir.
- `worktrees = true` (`--worktrees`) turns them on; `worktrees = false` (`--no-worktrees`) keeps every thread in the shared workdir, with a warning at daemon start and in `ants doctor` when `max_harnesses > 1` and the workdir is a git work tree, as threads running at once may clobber each other's edits.

Trade-offs of worktrees:

- **Disk:** one full checkout per thread, plus build outputs (`target/`, `node_modules`) each thread builds from scratch, for up to `max_harnesses` live threads and every thread whose session has not expired.
- **Branches:** a branch `ants/<ant>/<thread>` per thread in the repository; branches with unmerged commits remain after their worktree is removed until merged or deleted by hand.
- **Starting point:** a worktree starts from `HEAD` without uncommitted changes of the workdir, and edits land on the thread's branch, not in the workdir, until merged; tools keyed by the workdir path (trust prompts, per-directory config, caches) see a new directory per thread.

## Ant templates

An `[[ant_template]]` bounds worker ants added by manager ants. A templated `[[ant]]` sets `template`, `added_by`, `name`, `creds`, optional `nats`/`js_domain`, and any allowed request overrides below; any other key or out-of-bounds value fails config load and reload. Escalations go to `added_by` first (`escalate_to = [manager]`). Templated ants cannot be managers (`managers` and `added_by` reject them).

```toml
[[ant_template]]
name = "worker"                            # lowercase letters, digits, '-' and '_'
managers = ["lead"]                        # exact ant names allowed to add workers; default []
name_pattern = "<manager>-*"               # default; <manager> = manager name, * = any text
max_ants = 3                               # required; max templated ants listed in config
workdirs = ["~/src/repo-a", "~/src/repo-b"] # required; first is default
harnesses = ["claude"]                     # first is default; default: top-level harness only
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
worktrees = true                           # idle_timeout, approval_timeout, spend_warn_percent
```

Allowed per-ant overrides within template bounds:

- `workdir` (in `workdirs`), `harness` (in `harnesses`), `context` (in `contexts`), `allow_sets` (keys of `allow_sets`, appended to `allow`).
- `model` (required when `models` is non-empty, forbidden when empty) and `reasoning_effort` (`low` < `medium` < `high` < `xhigh` < `max`, at most the model's ceiling).
- `whoami`, `memory`, `tasks`, `capabilities`.
- `max_thread_tokens`, `max_thread_cost`, `max_daily_tokens`, `max_daily_cost`, `max_harnesses`, `prompt_timeout`, `max_tool_calls` — each at most the template ceiling (unset caps have no ceiling; `0` for `prompt_timeout` or `max_tool_calls` is allowed only when the template's is `0`).

Removing a manager from `managers` blocks new additions while keeping existing workers.

## Manager ants

An ant listed in `managers` (when `ants daemon` serves the whole config file, not `--ant`) receives MCP tool `request_ant(name, template, reason, overrides?, timeout?)` (`other:request_ant`). Requests serialize one at a time per daemon process:

1. Validates `name`, `overrides` and `max_ants` against the local config's `[[ant_template]]`, refusing out-of-bounds requests immediately.
2. Generates a local user NKey (`~/.config/ants/auth/<name>.nk`, `0600`; refuses if `<name>.creds` exists).
3. Publishes signed `AntRequest`s on `ants.antreq.<manager>` every 5 s until `ants signer` answers or `timeout` elapses (default `15m`, max `30m`).
4. Writes `<name>.creds`, deletes `<name>.nk`, appends `[[ant]]` to `config.toml`, and returns once `<name>` appears in `ants_roster` (or after 60 s noting it is not yet listed).

Behind a leaf node, `ants signer` also renews the leaf uplink JWT and admits the ant to the outbox (refusing up front if the uplink JWT would exceed 252 KiB; see [transport.md](transport.md#enrollment-behind-a-leaf-node)). The daemon writes the renewed uplink credentials and toggles `./` on the `credentials` path in `leaf.conf` before reloading `ants.nats` / `ants-nats` (`SIGHUP`), reconnecting only the uplink while local clients stay connected (requires `nats-server` 2.14+).

`ants signer` runs on the account-key machine (`scripts/install-units.sh --signer`):

```sh
ants signer --config ~/.ants/config.toml
```

It verifies the manager's account signature and evaluates the request against its **own** `--config` `[[ant_template]]` tables (reread per request) and live issued keys in `issued.jsonl` (`ants auth revoke NAME` frees a slot). Policy-compliant requests are signed immediately for the manager's `js_domain` (recording `template` and `added_by` in `issued.jsonl`). Out-of-policy requests publish a manager `ApprovalRequest` (`request_ant: add ant NAME from template T (why)`, `allow_once` or `reject`), visible in `ants tools`, `ants inbox -f`, `ants dash` and all `ants webhook serve` notification sinks, waiting up to `--approval-timeout` (default `15m`, capped by request expiry). Forged requests, human/manager/reserved/taken names, templated managers, and > 3 pending human approvals per manager are refused outright. Pending human requests drop on signer restart and re-trigger on the manager's next 5 s retry.

### Retiring an added ant

Managers also get `retire_ant(name)` (`other:retire_ant`) for ants whose `added_by` matches them:

1. Removes the `[[ant]]` table from `config.toml` so the daemon drains and stops the ant.
2. Waits up to `5m` for `<name>` to leave `ants_roster`.
3. Requests retirement from `ants signer` on `ants.antreq.<manager>` (every 5 s for up to `2m`); the signer verifies `issued.jsonl` (`template` + `added_by`), revokes `<name>`'s keys, and pushes the account to the hub.
4. Deletes `<name>.creds` (and any OS-stored seed).

`~/.ants/<name>` (memory, task DB, worktrees) is preserved: a later `request_ant` of `<name>` from the same template restores it. Revocation frees the name, the `max_ants` slot, and the ant's share of `pool_daily_*` (restored if re-added the same UTC day). Interrupted retirements resume from `<name>.creds` if retried.

Humans retire any templated ant on the manager's machine with `ants retire NAME [--config FILE] [--dir DIR] [--no-push]`, revoking locally if `--dir` holds the account key or printing `ants auth revoke --key U...` otherwise. Non-templated ants are removed by editing `config.toml` and running `ants auth revoke NAME`.

## Harnesses

Built-in ACP stdio harnesses live in `crates/ants-daemon/src/harnesses.toml`:

| Name | Command | Forced ACP mode | Sign-in |
|---|---|---|---|
| `copilot` (default) | `copilot --acp` | none | `copilot login` |
| `claude` | `claude-agent-acp` (`@agentclientprotocol/claude-agent-acp`) | `default` | `claude login` or `ANTHROPIC_API_KEY` |
| `antigravity` | `agy acp --disable-telemetry` | `default` | `agy` (`agy acp --login`) or Gemini API key |

Define custom harnesses or replace built-ins whole under `[harnesses.NAME]` in `config.toml` or `harnesses.d/*.toml`:

```toml
[harnesses.custom-acp]
command = "~/.local/bin/custom-acp"            # path if containing '/', else looked up on PATH
args = ["--stdio", "--log-level", "warn"]
env = { CUSTOM_ACP_TELEMETRY = "off" }         # precedence: daemon env < harness env < ant env < --env
mode = "default"                               # session fails if harness lacks this mode
sign_in = "custom-acp login"                   # shown when signed out
context_overflow = ["context too long"]        # extra context-overflow error substrings
rate_limited = ["slow down"]                   # extra rate-limit error substrings
quota_exhausted = ["monthly cap reached"]      # extra quota-exhausted error substrings
window = 200000                                # optional context window (tokens); sizes history seed
history_budget = 65536                         # optional history seed budget (bytes)

[harnesses.custom-acp.context]
default = {}
large = { args = ["--context-window", "1m"], window = 1000000 }
huge = { model_suffix = "-1m", history_budget = 262144 }
```

Codex (`npm install -g @agentclientprotocol/codex-acp`):

```toml
[harnesses.codex]
command = "codex-acp"
mode = "read-only"                             # default `agent` auto-approves; `workspace-write` edits workdir unasked
sign_in = "codex-acp login"
# env = { DEFAULT_AUTH_REQUEST = '{"methodId":"api-key"}' }  # for OPENAI_API_KEY / CODEX_API_KEY
```

- **Claude Code:** forced to `default` mode (`auto` approves internally). Built-in read-only tools (`Read`, `Grep`, `ls`, `echo`) and `.claude/settings*.json` still run unasked; loads `CLAUDE.md` and hooks.
- **Codex:** `read-only` sandboxes read commands unasked and prompts before edits, network or unsandboxed commands. Reports MCP calls as `execute` titled `mcp.<server>.<tool>` (matched as `other:`). Loads `~/.codex/config.toml` and `AGENTS.md`.
- **Antigravity:** forced to `default` mode; workspace reads/edits and `~/.gemini/antigravity-cli/settings.json` apply first; loads `GEMINI.md` and `AGENTS.md`. Keychain login requires running the daemon as that OS user.

### Drop-in harness files

`*.toml` files in `harnesses.d/` beside the config file (`~/.ants/harnesses.d/` by default; hidden `.NAME` files and subdirectories skipped) load in filename order across `daemon`, `doctor`, `harness check` and `harness prompt`. Each file may contain only `[harnesses.NAME]` tables; relative `command` paths containing `/` resolve against `harnesses.d/`. Duplicate harness names across files or `config.toml` fail loading.

### Sign-in, provider limits and failures

When a harness cannot run prompts for an environmental reason, the daemon holds messages in hand with 20 s progress acks (up to `max_harnesses × 2`), spending no attempts, incrementing no delivery count and dead-lettering nothing (if a tool call already started before a sign-out or limit error, `<ant>.<seq>` is kept so the retry prepends the cut-off warning):

- **Signed out** (ACP `auth_required` `-32000`, or auth error text on error/stderr): publishes `signed_out` in `ants_roster` (`since`, `sign_in`) and `ants.status`, notifies the thread owner (else default human) once directly on the thread with the sign-in command and machine, and probes every 60 s (`initialize` + `session/new` in `workdir`) until signed in.
- **Provider limits** (classified from prompt errors, or from Copilot/Antigravity replies ≤ 4096 chars whose streamed chunks all start with `Error:`, `API Error:`, `Info:` or `⚠`):
  - *Rate limit / overload* (HTTP `429`/`529`, `rate limit`, `too many requests`, `overloaded`, `resource_exhausted`, `exhausted your capacity`, `rate_limited`, or a quota resetting within 5 m): backs off `30s` doubling to `10m` (logged only).
  - *Exhausted quota* (`insufficient_quota`, `quota exceeded`, `usage limit`, `spending limit`, `credit balance is too low`, `quota_exhausted`, plus built-in Copilot/Claude/Antigravity quota phrases): notifies the owner once on the thread and backs off `15m` doubling to `2h`.
  - Explicit retry hints in the error (`retry after 30s`, `resets in 2h27m3s`, `retryDelay`, epoch after `|`) override the delay within the cap. Publishes `limited` (`rate_limit` or `quota`, `since`, `until`) in `ants_roster`.
  - Retry notices the harness streams before an attempt that succeeds (Copilot's `Info: … Retrying...`, Antigravity's `⚠ API error (attempt N): …`) are dropped from the answer when the last of them ends a line, or ends a sentence the answer does not continue on the same line; status updates still show them. An answer whose own first chunk starts like an error line (`Error:` followed by more of the same sentence) is sent whole.
- **Startup failures** (missing/broken binary, bad flags/mode/model): retried as normal prompt failures on a single thread (dead-lettered after 5 attempts). After **3 startup failures within 10 m across ≥ 2 threads** with no success in between, a breaker opens: holds messages, publishes `failing` (`since`, `error`) in `ants_roster`, sends one direct notice with a level-0 `harness` escalation (`ants harness check --harness NAME`), and probes every 60 s with the ant's settings. After a probe succeeds, one failure before a message completes reopens the breaker immediately without re-notifying.

### Fallbacks

An ant's optional `fallback` list names harnesses and models tried in order while its own harness is signed out, out of quota or behind an open startup breaker; a rate limit is still waited out. Each entry is one of:

- `"<harness>"`: that harness with its default model, e.g. `"claude"`.
- `"<harness>:<model>"`: that harness with that model (split at the first `:`), e.g. `"copilot:gpt-5"`.
- `":<model>"`: the ant's own harness with another model, e.g. `":claude-opus-4-1"`.

Entries must name a known harness (built-in, `[harnesses.NAME]` or `harnesses.d`), name a model after a colon, differ from the ant's own harness and model, and appear once; anything else fails config loading. A fallback runs with only its model and the ant's `history_budget`: the ant's `reasoning_effort` and `context` belong to its own harness. Templated ants take no `fallback`.

While the ant's own harness cannot serve, each message goes to the first fallback not passed over:

- **Sessions:** a thread whose stored session ran in another harness starts a new session seeded from the thread's history, as after a failed `session/load`; a `:<model>` fallback resumes the thread's session. A live harness on another backend is replaced once idle.
- **Turned away:** a fallback that is signed out, limited or fails to start is passed over for 5 min (or its limit's retry hint), spending no attempt and touching none of the own harness's holds; with every fallback passed over, messages wait as without fallbacks.
- **Shown:** each reply ends with `[ants] Answered by fallback NAME, harness OWN being signed out.` (or `out of quota`, `failing to start`); `ants_roster` publishes `fallback` (`name`, `since`); `ants roster` shows `fallback` as the activity; `ants ask`, `ask_ant` and `route` treat the ant as served (`route` prefers ants answered by their own harness).
- **Told once:** the owner's sign-in, quota or breaker notice (one per outage, as without fallbacks) says the messages go to the fallbacks meanwhile.
- **Back:** a successful sign-in check or breaker probe, or after a quota's backoff the next message the own harness answers, sends messages to the own harness again and clears `fallback`; live fallback sessions are replaced as their threads come up.
- **Caps:** prompt limits and spending caps apply to every prompt whichever harness runs it.

### Lost sessions and context window

When ACP `session/load` fails, the daemon opens a new session and prepends an `[ants]` notice plus the thread's stored history from `ANTS` (read with the ant's credentials: inbound messages, own outbound messages via `sentread-<ant>`, thread notes, channel posts), truncated to the history budget (each entry capped at 1/8 of the budget, preceded by a summary of omitted counts, senders and time span).

When a prompt overflows the model's context window (`context_length_exceeded`, `maximum context length`, `prompt is too long`, `exceeds the context window`, or `context_overflow`), the daemon immediately rebuilds the session with **half** the history budget and retries without spending an attempt. If the fresh session also overflows, the daemon escalates `context_window` on the thread and acks the message without dead-lettering.

History budget precedence: ant `history_budget` -> selected `context`'s `history_budget` or `window / 12 × 4` -> harness `history_budget` or `window / 12 × 4` -> `64 KiB`, capped at `window` bytes (1/4 of window at 4 bytes/token). Built-in `claude` sets `window = 200000` (`1000000` for `context = "1m"`); `copilot` and `antigravity` default to `64 KiB`. `history_budget = 0` includes only the omitted-entries summary line.

### Model, reasoning effort and context

`model` is applied before `reasoning_effort` via ACP selectors on new and resumed sessions; unsupported values or mismatched read-backs fail the session and list valid options.

- **Copilot:** `context` accepts `default` or `long_context` (`--context ...`).
- **Claude:** `context` accepts `default`, or `1m` with an explicit `model` (selects `<model>[1m]`).
- **Antigravity:** no `context` selector; choose a model with the desired window.

### Memory pressure

Each harness process takes hundreds of MB (about 255 MB for Copilot CLI, see README.md, Footprint). On every idle check of an ant's harness pool (every `idle_timeout`, at most 30 s), the daemon reads the system's available memory (Linux `MemAvailable` in `/proc/meminfo`; macOS `hw.memsize × kern.memorystatus_level`; Windows `GlobalMemoryStatusEx`; elsewhere, or when unreadable, memory never counts as low). Below `min_available`:

- Every idle harness (no prompt in flight) stops at once, least recently used first, instead of after `idle_timeout`; its session resumes via `session/load` on the next message. Busy harnesses finish their prompts.
- New threads get a harness only while fewer than `max_harnesses` from `[memory_pressure]` run (capped by the ant's own `max_harnesses`); other messages wait in hand as when the pool is full.
- The daemon logs a warning, sets `memory_low` (`{since, available, max_harnesses}`) in the ant's `ants_roster` entry (shown by `ants roster` and `list_ants`) and `ants_memory_low` to `1` in `/metrics`.

Memory counts as recovered once available memory reaches 150 % of `min_available`, so the state does not flap; the daemon logs it, clears `memory_low` and admits threads up to `max_harnesses` again. Each ant checks on its own, so the lower cap applies per ant. `[memory_pressure]` applies to every ant in the file; changing it on reload restarts them.

## Prompt limits and spending caps

Per-prompt defaults: `prompt_timeout = "30m"` (includes approval wait; `"0"` disables) and `max_tool_calls = 300` (`0` disables), resetting every prompt. Optional spending caps bound tokens or cost per thread (`max_thread_tokens`, `max_thread_cost`) and per ant per UTC day (`max_daily_tokens`, `max_daily_cost`), plus shared `pool_daily_tokens` / `pool_daily_cost` on `[[ant_template]]` (summed across listed template ants via `ants_sessions`).

- **Exceeding a limit or cap:** cancels via ACP `session/cancel` (restarting the harness after 30 s if hung), replies with partial output plus an `[ants]` notice, and escalates ant-originated threads (`cap`; pool caps escalate to `added_by` first). Further agent messages on a capped thread escalate immediately without prompting.
- **Resets:** thread allowances reset on any human message on the thread, and once per escalation level when the leader replies; daily caps reset at 00:00 UTC (or on restart with a higher cap).
- **Warnings:** crossing `spend_warn_percent` (default `80`, `< 100`, `0` disables) sends one `[ants]` warning per allowance/UTC day on a new thread (thread cap -> thread owner else default human; daily/pool cap -> default human).
- **Manual cancel:** `ants cancel ANT THREAD` (human creds; core NATS `ants.cancel.<ant>.<thread>`) cancels the active prompt the same way, replies with partial output and `[ants] The prompt was cancelled by <human> (ants cancel).`, and acks without retry, dead letter or escalation. Queued messages behind it still run.
- **Reporting:** ACP `usage_update` totals persist at `<ant>.<thread>.spend` and `<ant>.day.<YYYY-MM-DD>` in `ants_sessions`. Claude reports tokens and USD cost; Codex reports tokens only; Copilot CLI reports neither.

### Spend report

- `ants spend [--days N]`: one row per ant with spend in the last `N` UTC days (default 7, max 366): today's and total tokens/cost.
- `ants spend ANT`: daily breakdown, total, and threads with spend since the last human message (costliest first).
- `ants spend [ANT] --thread THREAD`: spend on `THREAD` since the last human message.

Ants listed with caps in the local `--config` show `spent/cap`; remote ants show spend only. `--json` emits one JSON object per line.

## Tool permissions

Harness tool calls are checked against `allow` (`--allow RULE`; `allow_tools = true` / `--allow-tools` equals `*`, for sandboxed workdirs only):

| Rule | Matches |
|---|---|
| `*` | Every tool call |
| `<kind>` | ACP kind: `read`, `edit`, `delete`, `move`, `search`, `execute`, `think`, `fetch`, `switch_mode`, `other` |
| `execute:cargo test*` | Command glob; commands containing `;`, `&`, `\|`, `<`, `>`, `` ` ``, `$(` or newlines never match |
| `edit:src/*` | Relative paths inside `<workdir>/src` (or the thread's worktree; never outside it) |
| `edit:/srv/notes/*` | Absolute path glob |
| `fetch:https://docs.rs/*` | URL glob (falls back to title) |
| `other:send_message` / `other:bd-*` | MCP and `other` tools by title, or by `<tool>` in `mcp__<server>__<tool>` (Claude) and `mcp.<server>.<tool>` (Codex) |

Matched calls auto-approve (`allow_once`). Unmatched calls publish an `ApprovalRequest` on `ants.approvals.<ant>` and wait `approval_timeout` (default `5m`) for `allow_once`, `allow_always` or `reject`:

- **Humans and push links:** `ants tools approve|reject <ant>/<id>`, `ants dash`, or signed `/_approval` links from `ants webhook serve` store an `ApprovalDecision` on `ants.approvals.<ant>.<id>` (`Nats-Expected-Last-Subject-Sequence` ensures first answer wins; verified by direct get).
- **Chain members:** ants in `escalate_to` receive an `approval` escalation notice on the thread and may answer via MCP `decide_approval(verdict)` (`escalate` is refused on approval notices). Earliest decision wins (human or chain).
- **Suggested rules & `--rule`:** once humans approve an identical call 3 times for an ant (`execute:<cmd>`, `<kind>:<abs-path>` for one file, `fetch:<url>`, else `<kind>:<title>`, excluding `*` and shell metacharacters), `ants tools approve` and approval notices print that rule. `ants tools approve ID --rule` appends it to the ant's `allow` in `config.toml` and reloads the daemon (for templated ants, `--rule` approves once and prints the rule to add to `[[ant_template]]`).
- **Webhook & schedule narrowing:** threads started by a webhook or schedule intersect the ant's allowlist with the entry's `allow` (default read-only: `read:*`, `search`, `think`, and MCP `list_ants`, `check_inbox`, `read_notes`, `post_notes`, `send_message`, `ask_ant`, `route`, `escalate`, `react`, `list_reactions`, `suggest_thread_title`; `["*"]` opts out; `[]` requires human approval for all calls). The narrowing travels on every message, spin-off, channel post, escalation and approval notice on the thread (persisted at `<ant>.<thread>.spend`) until a human messages the ant on the thread or wakes it on the channel thread.
- **Expiry:** after `approval_timeout`, the call is rejected; the thread owner (else default human) receives a level-0 `approval` escalation notice on a separate thread (so `ants ask --wait` keeps waiting for the reply), and asked chain ants receive a cancellation notice on the request's thread.

### Redaction

Before publishing tool requests (redacted before the 2000-byte input cut), status streams (buffered to whitespace / PEM boundaries so secrets never split across frames), replies, escalations, thread notes, MCP messages/posts, and failing-harness errors, the daemon replaces secrets with `[redacted]`:

- **Environment values** (≥ 8 chars, not starting with `/`, `~`, `.` or a drive letter): values from the ant's `env`, harness `env`, and daemon env whose variable name contains `TOKEN`, `SECRET`, `PASSWORD`, `KEY`, `AUTH` or `CREDENTIALS`, plus token-like values (alphanumeric, no spaces/slashes/file extensions) in ant/harness `env`.
- **Patterns:** PEM private keys, `Bearer`/`Basic` headers, GitHub/GitLab/Slack/OpenAI/Anthropic/AWS/Google/Stripe/npm/Hugging Face/Telegram tokens, Slack/Discord webhook URLs, NKey seeds, JWTs, URL passwords (`scheme://user:pass@host`), and secret-named assignments containing a digit (`API_KEY=...`, `"password": "..."`, `--token ...`).

Top-level `redact = false` disables daemon redaction. `ants webhook serve` always redacts outbound notifications (patterns + secret-named process env vars). Local logs are not redacted.

## Schedules

`[[schedule]]` entries send `text` literally as a new thread to `ant` or `channel` on a timetable (run by `ants daemon` when serving the whole config file, not `--ant`):

```toml
[[schedule]]
name = "nightly"                              # ants auth user nightly --webhook --to dev-a
creds = "~/.config/ants/auth/nightly.creds"
cron = "0 3 * * 1-5"                          # or every = "4h" (min "1m", anchored at Unix epoch)
timezone = "Europe/London"                    # optional (cron only): overrides top-level timezone, else UTC
ant = "dev-a"                                 # or channel = "ops" (with optional mention = [...] or broadcast = true)
owner = "alice"                               # optional human owner for replies and escalations
text = "Triage the failing nightly tests and summarise them."
allow = ["read:*", "execute:cargo test*"]     # optional; default read-only, ["*"] opts out
catch_up = true                               # optional: send latest missed firing within 7d
```

- **Timetable & DST:** set either `cron` (5-field cron with lists, ranges, steps, month/weekday names, `0`/`7` Sunday, or `@yearly`/`@monthly`/`@weekly`/`@daily`/`@hourly`; restricted day-of-month and day-of-week OR together) or `every` (≥ `1m`, epoch-aligned UTC). `cron` uses IANA `timezone` (`/usr/share/zoneinfo` or `TZDIR` on Unix; bundled DB on Windows): spring-forward gaps fire once at the jump; fall-back overlaps fire once on the first pass.
- **Webhook identity & deduplication:** schedules use `--webhook` credentials (`--webhook NAME` in `ants auth leaf`), set `webhook: true`, refresh `<schedule>.webhook` in `ants_webhooks` hourly, and narrow threads to `allow`. Multiple daemons or restarts deduplicate firings via `<schedule>.<unix_time>` in `ants_webhooks` (7 d) and `Nats-Msg-Id: schedule.<schedule>.<unix_time>` (24 h).
- **Grace & catch-up:** firings retry every 5 s for up to 1 m (`GRACE`). Firings missed by > 1 m are skipped unless `catch_up = true`, which sends only the **latest** missed firing within the past 7 d (unless the next firing is due within 1 m) with `(Sent late: due YYYY-MM-DD HH:MM UTC.)` appended, retrying every 5 s until the next firing time.
