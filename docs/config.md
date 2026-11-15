# Configuration

`~/.ants/config.toml` (`--config` / `ANTS_CONFIG`) configures multi-ant daemons, CLI defaults, custom harnesses, worker templates and schedules. Every ant needs a harness: a file with a top-level `harness` (such as `harness = "claude"` alone) serves the built-in `messenger` on it, and a file without one whose ants (or the built-in messenger) name none fails to load, naming the built-in harnesses. `ants setup` writes the top-level `harness` of a new file ([Choosing a harness in `ants setup`](#choosing-a-harness-in-ants-setup)); `ants doctor` tells how to fix an existing file.

```toml
harness = "copilot"                        # for every ant and built-in messenger naming none; no default
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
summary_ant = "messenger"                  # ant asked for TL;DRs and digests (default: the messenger; see Catch-up summaries)
summaries = "on-demand"                    # default; "ahead" also allows asking for TL;DRs ahead of time (needs the next key)
summary_daily_tokens = 50000               # most tokens summaries asked ahead of time spend per human and UTC day (default 0)
inline_images = true                       # draw image attachments in ants chat's thread view (default false; README.md)
theme = "auto"                             # avatar palette: "dark", "light" or "auto" (default: ask the terminal, else COLORFGBG, else dark)
animate = true                             # avatar heads in ants chat move with their ant's state (default true; false keeps them still)

[storage]                                  # JetStream capacity warnings (see README.md, JetStream storage)
warn_percent = 80                          # warn past this share of a store's limit (default 80; 0 disables)
max_file_store = "100GB"                   # hub max_file_store (default: unknown); bytes or 1024-based K/M/G/T[B]
max_memory_store = "1GB"                   # hub max_memory_store (default: unknown)

[memory_pressure]                          # stop idle harnesses early when memory runs low (see Memory pressure)
min_available = "1GB"                      # default; bytes, 1024-based K/M/G/T[B] or a share like "10%"; "0" disables
max_harnesses = 1                          # default; per-ant harness cap while memory is low (at least 1)

[host]                                     # the machine's host connection (docs/auth.md, Machine host credentials)
creds = "~/.config/ants/auth/host.creds"   # default: that file, else host-<machine>.creds beside it, if either exists
machine = "macbook"                        # default: the name the credentials carry, else the host name's first label

[admin]                                    # admin orders to this machine's ants (docs/auth.md, Admin orders)
remote = false                             # refuse every order (default true: take the orders an admin signed)

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
worktree_base = "head"                     # default; "upstream" starts its branch from the fetched remote default branch
publish = "off"                            # default; "draft-pr" pushes a branch with new commits, opens a draft PR
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
```

`~` expands to the home directory; other relative paths resolve against the config file's directory (`env` values stay literal). Unknown keys, duplicate ant/template/schedule names, a schedule named after an ant, or `messenger = false` with no `[[ant]]` fail config loading. Unreachable NATS or JetStream endpoints retry with exponential backoff (`1s` to `30s`); credential or permission errors fail that ant immediately while remaining ants keep running, exiting non-zero only if none start.

## Precedence and reload

- `ants daemon`: serves all ants in `~/.ants/config.toml` (plus built-in `messenger` when enabled) and runs its schedules.
- `ants daemon --ant dev-a` (`ANTS_ANT=dev-a`) without `--config`: configures `dev-a` from CLI flags, loading only `[harnesses.NAME]` from `~/.ants/config.toml` (if present) and `~/.ants/harnesses.d/`.
- `ants daemon --config FILE --ant dev-a`: serves only `dev-a` from `FILE` (`--ant messenger` selects the built-in messenger) with CLI ant flags overriding `FILE`; templated ants refuse ant flags. No schedules run when `--ant` is set.
- Per-ant, per-schedule and top-level `nats`, `js_domain` and `creds` in the file override `--nats`, `--js-domain` and `--creds`, which fill in omitted values (`--creds` applies only when serving a single ant, never to schedules).

`ants daemon` reloads on `SIGHUP`, at once after an admin's `set`, `pause` or `resume` order edited `config.toml` ([auth.md](auth.md#admin-orders)), or automatically when `config.toml` or `harnesses.d/*.toml` changes on disk (polled every 3 s by mtime, size and inode; applied once unchanged across two checks, ~6 s after write):

- Added ants (and previously failed ants still listed) start; removed ants drain prompts in flight and return waiting messages for redelivery; ants whose resolved settings changed (including via top-level defaults, `[harnesses.NAME]`, or `[[ant_template]]`) drain and restart; unchanged ants continue uninterrupted (pool membership changes alone do not restart members, nor does `paused` alone: see [Pausing an ant](#pausing-an-ant)).
- Added schedules start; removed schedules stop immediately; changed or stopped schedules restart.
- The machine's record of its ants in `ants_machines` follows the ants listed; a changed `[host]` or `[admin]` table or host credentials file reconnects the host. With `[admin] remote = false`, the host refuses every admin order to the machine's ants, recording `this machine's config sets [admin] remote = false` as its outcome; otherwise an admin's `set` order (`ants ant set`) edits the ant's `[[ant]]` table, or the `[harnesses]` table, in place, comments kept, refused naming the field when the field no longer holds the value the order expects, or with the loader's error when the file would not load, and changing nothing then. Only a daemon serving the whole file (no `--ant`) runs the host, with the top-level `nats` and `js_domain`, else `--nats` and `--js-domain`.
- Invalid reloads log an error and keep running the previous config. `metrics` requires a process restart.

## Pausing an ant

`paused = true` on an `[[ant]]` pauses it: its daemon stops pulling from the ant's inbox and channel consumers, so the messages to it wait queued in the stream, neither drained nor returned and spending no delivery attempts, and the ant takes no new prompt; prompts already running go on to their end, and messages already in hand stay in hand, unprompted, until it resumes. Its roster entry says `paused` (`{since, by}`: when, and the admin whose order paused it, left out when the file did), which `ants roster`, `ants ant show`, the ant view of `ants chat` (`‖` in the nav), the `list_ants` MCP tool and the warnings at send time show; `route` picks a paused ant last, and a sender whose messages wait 15 min for a paused ant is told so, as for an ant no daemon serves. Removing the key, or `paused = false`, resumes it, and it takes the messages queued for it in turn. A reload that changes `paused` alone pauses or resumes the running ant in place, restarting no harness. Templated ants may set it too.

`ants ant pause ANT [--machine M] [--wait DUR]` and `ants ant resume ANT` send the admin order (`pause`, `resume`) that has the daemon of the ant's machine set `paused = true`, or remove it, and reload at once ([auth.md](auth.md#admin-orders)); `p` in the ant view of `ants chat` sends the one that flips it.

## Worktrees

With worktrees on, each thread works in its own git worktree at `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>`, created from `HEAD` on the thread's first prompt and removed once its session expires (a branch with unmerged commits is kept, and its thread is told). A workdir outside a git work tree stays shared.

- Unset `worktrees` turns them on when the workdir is inside a git work tree (a `.git` directory or file in it or above it) that does not ignore it (`git check-ignore`, as for a directory in `target/`) and `max_harnesses > 1`, logging so at daemon start; otherwise threads share the workdir.
- `worktrees = true` (`--worktrees`) turns them on; `worktrees = false` (`--no-worktrees`) keeps every thread in the shared workdir, with a warning at daemon start and in `ants doctor` when `max_harnesses > 1` and the workdir is a git work tree, as threads running at once may clobber each other's edits.
- `worktree_base = "upstream"` starts each new branch from the fetched default branch of the default remote instead of `HEAD`, and `publish = "draft-pr"` pushes a branch with new commits and opens a draft pull request ([README](../README.md#pool-worktrees-and-at-least-once-retries)); both apply whenever worktrees are on, set or by default.

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
                                           # spend_warn_percent
```

Allowed per-ant overrides within template bounds:

- `workdir` (in `workdirs`), `harness` (in `harnesses`), `context` (in `contexts`), `allow_sets` (keys of `allow_sets`, appended to `allow`).
- `model` (required when `models` is non-empty, forbidden when empty) and `reasoning_effort` (`low` < `medium` < `high` < `xhigh` < `max`, at most the model's ceiling).
- `whoami`, `memory`, `tasks`, `capabilities`, `paused`.
- `max_thread_tokens`, `max_thread_cost`, `max_daily_tokens`, `max_daily_cost`, `max_harnesses`, `prompt_timeout`, `max_tool_calls` — each at most the template ceiling (unset caps have no ceiling; `0` for `prompt_timeout` or `max_tool_calls` is allowed only when the template's is `0`).

Removing a manager from `managers` blocks new additions while keeping existing workers.

## Manager ants

An ant listed in `managers` (when `ants daemon` serves the whole config file, not `--ant`) receives MCP tool `request_ant(name, template, reason, overrides?, timeout?)` (`other:request_ant`). Requests serialize one at a time per daemon process:

1. Validates `name`, `overrides` and `max_ants` against the local config's `[[ant_template]]`, refusing out-of-bounds requests immediately.
2. Generates a local user NKey (`~/.config/ants/auth/<name>.nk`, `0600`; refuses if `<name>.creds` exists).
3. Publishes signed `AntRequest`s on `ants.antreq.<manager>` every 5 s until `ants signer` answers or `timeout` elapses (default `15m`, max `30m`).
4. Writes `<name>.creds`, deletes `<name>.nk`, appends `[[ant]]` to `config.toml`, and returns once `<name>` appears in `ants_roster` (or after 60 s noting it is not yet listed).

Behind a leaf node, `ants signer` also renews the leaf uplink JWT and admits the ant to the outbox (refusing up front if the uplink JWT would exceed 252 KiB; see [transport.md](transport.md#enrollment-behind-a-leaf-node)). The daemon writes the renewed uplink credentials and toggles `./` on the `credentials` path in `leaf.conf` before reloading `ants.nats` / `ants-nats` (`SIGHUP`), reconnecting only the uplink while local clients stay connected.

`ants signer` runs on the account-key machine (`scripts/install-units.sh --signer`):

```sh
ants signer --config ~/.ants/config.toml
```

It verifies the manager's account signature and evaluates the request against its **own** `--config` `[[ant_template]]` tables (reread per request) and live issued keys in `issued.jsonl` (`ants auth revoke NAME` frees a slot). Policy-compliant requests are signed immediately for the manager's `js_domain` (recording `template` and `added_by` in `issued.jsonl`). Out-of-policy requests publish a manager `ApprovalRequest` (`request_ant: add ant NAME from template T (why)`, `allow_once` or `reject`), visible in `ants tools`, `ants inbox -f`, `ants dash` and all `ants webhook serve` notification sinks, waiting up to `--approval-timeout` (default `15m`, capped by request expiry). Forged requests, human/manager/reserved/taken names, templated managers, and > 3 pending human approvals per manager are refused outright. Pending human requests drop on signer restart and re-trigger on the manager's next 5 s retry.

The signer also signs and revokes keys on admins' `add` and `retire` orders, which machines' daemons carry out with their host credentials, with no human asked again ([auth.md](auth.md#adding-and-retiring-an-ant-by-order)): an ant added so gets an `[[ant]]` table of its own below the comment `# added by <admin>'s order <id>`, no template's.

The signer also answers ants' reactions requests (MCP `react` and `list_reactions`, `ants react` with ant credentials) in queue group `reactions`: ant credentials neither read nor write `ants_reactions`, so the signer lists the reactions to a message, or adds or removes the ant's own, once it finds that the ant reads the message. With authentication, `react` therefore needs a running `ants signer` (listings are also answered by `ants webhook serve` with a `[[human]]` entry); a daemon without credentials answers its ants itself. Behind a leaf node, the signer's machine needs its uplink issued with `--enroll-approver` (see [auth.md](auth.md#wire-acls-and-verification-invariants)).

### Retiring an added ant

Managers also get `retire_ant(name)` (`other:retire_ant`) for ants whose `added_by` matches them:

1. Removes the `[[ant]]` table from `config.toml` so the daemon drains and stops the ant.
2. Waits up to `5m` for `<name>` to leave `ants_roster`.
3. Requests retirement from `ants signer` on `ants.antreq.<manager>` (every 5 s for up to `2m`); the signer verifies `issued.jsonl` (`template` + `added_by`), revokes `<name>`'s keys, and pushes the account to the hub.
4. Deletes `<name>.creds` (and any OS-stored seed).

`~/.ants/<name>` (memory, task DB, worktrees) is preserved: a later `request_ant` of `<name>` from the same template restores it. Revocation frees the name, the `max_ants` slot, and the ant's share of `pool_daily_*` (restored if re-added the same UTC day). Interrupted retirements resume from `<name>.creds` if retried.

Humans retire any templated ant on the manager's machine with `ants retire NAME [--config FILE] [--dir DIR] [--no-push]`, revoking locally if `--dir` holds the account key or printing `ants auth revoke --key U...` otherwise. Non-templated ants are removed by editing `config.toml` and running `ants auth revoke NAME`.

## Cloning an ant

An admin's `ants ant clone SRC NEW [settings]` has the daemon serving SRC start NEW as a copy of it, with its own key and identity record ([auth.md](auth.md#cloning-an-ant-by-order)). SRC is paused while it is copied, once its prompts in flight end, and resumed after. NEW gets:

- **Table:** SRC's `[[ant]]` table (for an ant added from a template, the settings the template gave it, so NEW's table stands alone), below the comment `# cloned from SRC by <admin>'s order <id>`, without `name`, `creds`, `paused`, `added_by`, `template`, `builtin`, `home` and `allow_sets`, with the settings flags of `ants add` over it; its whoami, memory, tasks, harness, model, permissions and escalation chain are SRC's unless given. A workdir in SRC's home moves into NEW's.
- **Home:** a copy of SRC's (`--home`, default `~/.ants/NEW`; it must be empty), memory files, task DB and repositories included, but not `worktrees`, `answered`, `buried` and the MCP config and socket.
- **Threads:** each thread SRC holds a session of, with its worktree, if any, at `<home>/worktrees/<thread>` on branch `ants/NEW/<thread>` from SRC's commit, SRC's uncommitted changes and untracked files on it. Its session is a fork of SRC's when the harness offers ACP `session/fork`; otherwise NEW's first prompt on the thread starts a new session seeded from the thread's history, as after a failed `session/load` ([Lost sessions](#lost-sessions-and-context-window)), its notice saying the session could not be forked.
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
- **`github_url`, `api_url`:** a GitHub Enterprise Server's own URL and REST API (such as `https://ghe.example/api/v3`). `ants login` shows only github.com's device login page, so registration with a GitHub Enterprise Server stops at the sign-in; lookups by `ants auth user --github` and renewal checks use these URLs.

`ants signer` must connect to the hub itself (`--nats`), not through a leaf node, whose uplink never carries registration requests, and signs registered devices for its `--js-domain`. `ants daemon` ignores the table. GitHub is the only identity provider.

## Harnesses

Built-in ACP stdio harnesses live in `crates/ants-daemon/src/harnesses.toml`. None is a default: an ant names its harness, or the file's top-level `harness` does.

| Name | Command | Install | Sign-in | Runs tools unasked through |
|---|---|---|---|---|
| `copilot` | `copilot --acp --allow-all` | `npm install -g @github/copilot` | `copilot login` | `--allow-all` (agent mode, not autopilot) |
| `claude` | `claude-agent-acp` | `npm install -g @agentclientprotocol/claude-agent-acp` | `claude login` or `ANTHROPIC_API_KEY` | ACP mode `bypassPermissions` |
| `codex` | `codex-acp` | `npm install -g @agentclientprotocol/codex-acp` | `codex-acp login`, or `CODEX_API_KEY` / `OPENAI_API_KEY` | ACP mode `agent-full-access` (no approvals, no sandbox) |
| `antigravity` | `agy acp --disable-telemetry --dangerously-skip-permissions` | `curl -fsSL https://antigravity.google/cli/install.sh \| bash` | `agy` (`agy acp --login`) or Gemini API key | `--dangerously-skip-permissions` |

**Built-in harnesses run their own tools unasked.** Each starts in its most permissive native setting, so its shell commands, edits and fetches never reach the daemon as tool requests: an ant's `allow` rules, tool approvals by humans or `escalate_to` ants, and [webhook and schedule narrowing](#tool-permissions) do not apply to them. `ants daemon` logs a warning at start (and for each narrowed prompt), and `ants doctor` warns (`ant NAME tool permissions`), when an ant on such a harness sets allow rules other than `*`, has `escalate_to` ants, or is the destination of a narrowing webhook or schedule. Checks the daemon makes itself still apply: [prompt limits and spending caps](#prompt-limits-and-spending-caps), and the [branch push](#tool-permissions) of `publish = "draft-pr"`. Run such ants in workdirs and accounts their harness may change freely.

- **Claude Code:** `bypassPermissions` is offered only when the adapter does not run as root (unless `IS_SANDBOX=1` is in its environment, e.g. `env = { IS_SANDBOX = "1" }` on the ant) and the Claude Code settings do not set `permissions.disableBypassPermissionsMode = "disable"`; otherwise sessions fail to start with the mode missing. Loads `CLAUDE.md`, `.claude/settings*.json` and hooks.
- **Codex:** reports MCP calls as `execute` titled `mcp.<server>.<tool>` (matched as `other:`). Loads `~/.codex/config.toml` and `AGENTS.md`. For an API key, set `env = { DEFAULT_AUTH_REQUEST = '{"methodId":"api-key"}' }` beside `CODEX_API_KEY` or `OPENAI_API_KEY`.
- **Antigravity:** loads `GEMINI.md`, `AGENTS.md` and `~/.gemini/antigravity-cli/settings.json`. Keychain login requires running the daemon as that OS user.

To have a built-in ask before running tools, so that allow rules, approvals and narrowing apply, redefine it whole under `[harnesses.NAME]` with a gated mode or without the permissive argument (a redefinition does not run tools unasked unless it sets `bypass_permissions = true`):

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

Even gated, Claude Code runs its built-in read-only tools (`Read`, `Grep`, `ls`, `echo`) and rules from `.claude/settings*.json` unasked, and Antigravity applies workspace reads/edits and its `settings.json` first. A redefinition replaces the built-in's quota and context-overflow phrases, `window` and `context` table too: copy those from `harnesses.toml` to keep them.

### Custom harnesses

Define other ACP harnesses under `[harnesses.NAME]` in `config.toml` or `harnesses.d/*.toml`:

```toml
[harnesses.custom-acp]
command = "~/.local/bin/custom-acp"            # path if containing '/', else looked up on PATH
args = ["--stdio", "--log-level", "warn"]
env = { CUSTOM_ACP_TELEMETRY = "off" }         # precedence: daemon env < harness env < ant env < --env
mode = "default"                               # session fails if harness lacks this mode
sign_in = "custom-acp login"                   # shown when signed out
bypass_permissions = false                     # default; true when args or mode make it run tools unasked (warns as above)
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

### Memory files

`memory_files` lists globs of the files a harness takes as memory, which an admin's `read_memories` order shows read-only (`ants ant memories ANT`, the memories pane of the ant view in `ants chat`; [auth.md](auth.md#admin-orders)). A glob is relative to the ant's workdir, `~/` names the home directory of the daemon's user and an absolute path is taken as is; `*` and `?` match within a path component, `**` any number of directories, and `..` is refused. The built-ins default to:

| Harness | `memory_files` |
|---|---|
| `copilot` | `AGENTS.md`, `.github/copilot-instructions.md`, `.github/instructions/**/*.instructions.md`, `CLAUDE.md`, `GEMINI.md`, `~/.copilot/copilot-instructions.md` |
| `claude` | `CLAUDE.md`, `CLAUDE.local.md`, `.claude/CLAUDE.md`, `~/.claude/CLAUDE.md` |
| `codex` | `AGENTS.md`, `AGENTS.override.md`, `~/.codex/AGENTS.md`, `~/.codex/AGENTS.override.md` |
| `antigravity` | `GEMINI.md`, `AGENTS.md`, `.agent/rules/*.md`, `~/.gemini/GEMINI.md` |

A `[harnesses.NAME]` redefining a harness without `memory_files` keeps the globs of the one it replaces; `memory_files = []` shows none. A read follows a symbolic link only when its target, resolved, still matches one of the globs and lies under the ant's workdir, the ant's home (`home`, default `~/.ants/<name>`) or the home directory of the daemon's user; `**` follows no linked directory. Anything else is left out, named with the reason. With `BD_DB` in the ant's environment (the ant's `env`, else its harness's, else the daemon's), the read also lists the bd memories of that database (`bd memories --json`, run in the workdir, at most 10 s).

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

- **Sessions:** a thread whose stored session ran in another harness (recorded beside the session in `ants_sessions`, so across daemon restarts too) starts a new session seeded from the thread's history, as after a failed `session/load`; a `:<model>` fallback resumes the thread's session. A live harness on another backend is replaced once idle.
- **Turned away:** a fallback that is signed out, limited or fails to start is passed over for 5 min (or its limit's retry hint), spending no attempt and touching none of the own harness's holds; with every fallback passed over, messages wait as without fallbacks.
- **Shown:** each reply ends with `[ants] Answered by fallback NAME, harness OWN being signed out.` (or `out of quota`, `failing to start`); `ants_roster` publishes `fallback` (`name`, `since`); `ants roster` shows `fallback` as the activity; `ants ask`, `ask_ant` and `route` treat the ant as served (`route` prefers ants answered by their own harness).
- **Told once:** the owner's sign-in, quota or breaker notice (one per outage, as without fallbacks) says the messages go to the fallbacks meanwhile.
- **Back:** a successful sign-in check or breaker probe, or after a quota's backoff the next message the own harness answers, sends messages to the own harness again and clears `fallback`; live fallback sessions are replaced as their threads come up.
- **Caps:** prompt limits and spending caps apply to every prompt whichever harness runs it.

### Lost sessions and context window

When ACP `session/load` fails, the daemon opens a new session and prepends an `[ants]` notice plus the thread's stored history from `ANTS` (read with the ant's credentials: inbound messages, own outbound messages via `sentread-<ant>`, thread notes, channel posts), truncated to the history budget (each entry's text capped at 1/8 of the budget, followed by one `[ants] Attached:` line per attachment naming its name, type, size and handle for `read_attachment`, and preceded by a summary of omitted counts, senders and time span).

When a prompt overflows the model's context window (`context_length_exceeded`, `maximum context length`, `prompt is too long`, `exceeds the context window`, or `context_overflow`), the daemon immediately rebuilds the session with **half** the history budget and retries without spending an attempt. If the fresh session also overflows, the daemon escalates `context_window` on the thread and acks the message without dead-lettering.

History budget precedence: ant `history_budget` -> selected `context`'s `history_budget` or `window / 12 × 4` -> harness `history_budget` or `window / 12 × 4` -> `64 KiB`, capped at `window` bytes (1/4 of window at 4 bytes/token). Built-in `claude` sets `window = 200000` (`1000000` for `context = "1m"`); `copilot` and `antigravity` default to `64 KiB`. `history_budget = 0` includes only the omitted-entries summary line.

### Model, reasoning effort and context

`model` is applied before `reasoning_effort` via ACP selectors on new and resumed sessions; unsupported values or mismatched read-backs fail the session and list valid options. The ant's `ants_roster` entry publishes the ones set as `model` (`{model, reasoning_effort, context}`, each left out when unset; the whole field when none is), shown by `ants ant show` and the ant view of `ants chat`.

- **Copilot:** `context` accepts `default` or `long_context` (`--context ...`).
- **Claude:** `context` accepts `default`, or `1m` with an explicit `model` (selects `<model>[1m]`).
- **Antigravity:** no `context` selector; choose a model with the desired window.

### Memory pressure

Each harness process takes hundreds of MB (about 255 MB for Copilot CLI, see README.md, Footprint). On every idle check of an ant's harness pool (every `idle_timeout`, at most 30 s), the daemon reads the system's available memory (Linux `MemAvailable` in `/proc/meminfo`; macOS `hw.memsize × kern.memorystatus_level`; Windows `GlobalMemoryStatusEx`; elsewhere, or when unreadable, memory never counts as low). Below `min_available`:

- Every idle harness (no prompt in flight) stops at once, least recently used first, instead of after `idle_timeout`; its session resumes via `session/load` on the next message. Busy harnesses finish their prompts.
- New threads get a harness only while fewer than `max_harnesses` from `[memory_pressure]` run (capped by the ant's own `max_harnesses`); other messages wait in hand as when the pool is full.
- The daemon logs a warning, sets `memory_low` (`{since, available, max_harnesses}`) in the ant's `ants_roster` entry (shown by `ants roster` and `list_ants`) and `ants_memory_low` to `1` in `/metrics`.

Memory counts as recovered once available memory reaches 150 % of `min_available`, so the state does not flap; the daemon logs it, clears `memory_low` and admits threads up to `max_harnesses` again. Each ant checks on its own, so the lower cap applies per ant. `[memory_pressure]` applies to every ant in the file; changing it on reload restarts them.

## Avatars

Every ant has a pixel-art avatar derived from its identity record in `ants_identities` (its avatar seed, else its
`id`), or from its name while no record is readable. `ants chat`, `ants roster`, `ants watch`, `ants inbox` and
`ants ant show` put its one-line face before its name, and the ant view of `ants chat` and `ants ant avatar` draw its
head. `[cli]` `theme` picks the palette for the terminal's background: `dark`, `light`, or `auto` (the default), which
asks the terminal for its background colour (OSC 11) and waits up to 200 ms for an answer, then reads `COLORFGBG`,
else takes `dark`. With `NO_COLOR` set, or off a terminal, faces show without colour and the head as ASCII. `[cli]`
`animate = false` keeps the heads in `ants chat` still; otherwise only the head on screen moves, and the screen is drawn
again only when its frame changes.

## Exporting and importing an ant

`ants ant export ANT` writes the ant to one plain JSON file (default `./ANT.ant`, mode `0600`, never overwritten) for
another bus, as for a new job; `ants ant import FILE` there adds it from the file, as `ants add` does, with a new key.
The file is `{"ants_ant": 1, "created_at", "bus", "identity", "settings", "files"}`:

- `bus`: the public key of the account of the bus exported from.
- `identity`: the ant's identity record, unmarked: its `id`, `born`, `avatar`, `color`, `prompts` and `cloned_from`.
- `settings`: of its `[[ant]]`, `whoami`, `harness`, `model`, `reasoning_effort`, `context` and `fallback`.
- `files`: each file of its home (`home`, default `~/.ants/<name>`) as `{path, kind, data}`, `data` in base64, at
  most 64 MiB in all. `kind` is `harness` for the files of its harness's [memory files](#memory-files) globs, taken
  relative to the home (`~/` globs are left out), and `core` for every other regular file of the home, symbolic
  links not followed.

Left out: the home's `worktrees`, `answered`, `buried`, `mcp.json`, `mcp.sock`, `.git`, `.beads` and `.bd`, the task
database `BD_DB` names when it lies in the home, the rest of the ant's settings (its role on the bus: `capabilities`,
`escalate_to`, `allow`, `memory`, `tasks`; its machine's: `workdir`, `env`, caps and limits), its credentials,
sessions, threads and history. A templated ant imports as a plain `[[ant]]`.

The export runs on the machine whose config file lists the ant, with a human's credentials. While another machine
serves the ant it warns that the home here may be behind; while this one serves it working, it waits up to 2 minutes
for its prompts in flight to end, so the files are packed between prompts. Nothing signs or encrypts the file: keep it
safe, as it may hold sensitive memory.

`--move` then marks the ant moved in its identity record. The daemon serving it drains it as on a stop (it takes no
new message, hands back those waiting and finishes the prompts in flight) and leaves the roster, every daemon listing
it waits for the mark to go before serving it, and messages to it get the notice of an ant no daemon serves. `ants
roster`, `ants doctor`, `ants ant show` and the ant view of `ants chat` show it moved. `ants ant unseal ANT` clears
the mark; `ants auth revoke ANT` ends the ant on the old bus once the import succeeded
([auth.md](auth.md#moving-an-ant-to-another-bus)).

`ants ant import FILE [--as NEW]` takes the flags of `ants add` and:

1. Refuses a config file already listing the ant, and a home (`--home`, default `~/.ants/<name>`) not empty.
2. Creates its identity record on the bus by a KV create, refused when a record holds its name, or its id under any
   name, so no bus holds two copies of one ant. The id is looked up by reading the whole `ants_identities` bucket, one
   record per ant.
3. Restores its files into its home, leaving out its harness memory files, with a warning, when the ant runs another
   harness here (`--harness`, else the exported one, else the file's top-level `harness`).
4. Lists it in `config.toml` with the exported settings, the flags given winning, and gives it a new key, as `ants
   add` does: with `--jwt` or `--enroll` its credentials complete at once, else `ants add NAME --jwt ...` completes
   them later.

A failed import takes back the record it created and the files it restored. `--as NEW` imports the ant under another
name, keeping its id, birth, avatar and prompts. The ant starts with no tasks.

## Catch-up summaries

`ants thread tldr THREAD` and `ants digest` ask the ant named by `[cli]` `summary_ant` (default: the messenger) for a
summary: a message on a new thread owned by the CLI's human, carrying a `summary` request (`thread` and `upto` for a
TL;DR, `since` and `upto` for a digest) and, as text, the newest stored entries within 16 KiB, each cut to 1 KiB. The
daemon prompts the ant to answer in at most 120 words of plain text without tools, and its reply carries the request
back with the tokens the turn spent (`tokens`). The CLI waits for that reply, marks it read and prints it:

- **TL;DR:** stored for every human at `summary.<thread>` in KV `ants_threads` (`{thread_id, text, by, for, at, upto,
  tokens}`) and printed again without asking while no entry came after entry `upto` (`--new` asks anyway; `--json`
  prints the stored object). Credentials older than permissions version 2 warn once and neither read nor store it.
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
- **Steering:** `ants ask --steer --thread THREAD @ant TEXT`, or its shorthand `ants steer ANT THREAD TEXT` (human creds), sends a message carrying `"steer": true`. When it reaches an ant running a prompt on that thread, it cancels that prompt like `ants cancel` (in-flight tool calls are cancelled, not resumed), whose reply ends with `[ants] <human> interrupted the prompt to steer <ant> (ants ask --steer).`, and goes ahead of the messages waiting there (after earlier steers): the same ACP session gets it next, starting with an `[ants]` line saying the human interrupted the previous prompt. The waiting messages run after it, in order. With no prompt running, or from an ant or a channel post, the flag is ignored and the message waits its turn. A steer message counts as any human message (turn budget and allowance renewed).
- **Reporting:** ACP `usage_update` totals persist at `<ant>.<thread>.spend` and `<ant>.day.<YYYY-MM-DD>` in `ants_sessions`. Claude reports tokens and USD cost; Codex reports tokens only; Copilot CLI reports neither.

### Spend report

- `ants spend [--days N]`: one row per ant with spend in the last `N` UTC days (default 7, max 366): today's and total tokens/cost.
- `ants spend ANT`: daily breakdown, total, and threads with spend since the last human message (costliest first).
- `ants spend [ANT] --thread THREAD`: spend on `THREAD` since the last human message.

Ants listed with caps in the local `--config` show `spent/cap`; remote ants show spend only. `--json` emits one JSON object per line.

## Tool permissions

Tool calls a harness asks about are checked against `allow` (the [built-in harnesses](#harnesses) run their own tools unasked, so neither rules nor approvals reach those) (`--allow RULE`; `allow_tools = true` / `--allow-tools` equals `*`, for sandboxed workdirs only):

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
- **Webhook & schedule narrowing:** on harnesses that ask before running tools (not the [built-ins](#harnesses), which run them unasked), threads started by a webhook or schedule intersect the ant's allowlist with the entry's `allow` (default read-only: `read:*`, `search`, `think`, and MCP `list_ants`, `check_inbox`, `read_notes`, `post_notes`, `send_message`, `ask_ant`, `route`, `escalate`, `react`, `list_reactions`, `suggest_thread_title`, `ask_human`; `["*"]` opts out; `[]` requires human approval for all calls). The narrowing travels on every message, spin-off, channel post, escalation and approval notice on the thread (persisted at `<ant>.<thread>.spend`) until a human messages the ant on the thread or wakes it on the channel thread.
- **Stand-in:** each request names the human it waits for (`human`: the thread's human owner, else the default human) and the ant's `stand_in`, if set (escalations sent to a human carry it too). While that human is away, `ants webhook serve` relays the request or escalation to the stand-in, which overrides the human's own `[[human]] stand_in`; a human that set no `stand_in` of its own has it relayed to its assistant instead, when it has one (see [webhooks.md](webhooks.md#stand-in)). Decisions name their human (`by`); when another human decides, the human the request waited for gets an inbox notice (`[ants] <by> decided tool request <ant>/<id> … in place of <human>`); likewise when another human answers a pending human escalation or reminder (`[ants] <by> answered the escalation of <ant> …`). `stand_in` must not name the ant itself; templated ants take no `stand_in` (the `[[human]]` one applies).
- **Branch pushes:** with `publish = "draft-pr"`, the daemon's own `git push <remote> ants/<ant>/<thread>` goes through the same check as an `execute` call titled `Push branch … and open a draft pull request`: `execute:git push origin ants/dev-a/*` lets dev-a push its thread branches to `origin` unasked; otherwise the push is an approval request on the thread, and a rejection (narrowed threads included) leaves the branch unpushed with an `[ants]` line in the reply.
- **Expiry:** after `approval_timeout`, the call is rejected; the thread owner (else default human) receives a level-0 `approval` escalation notice on a separate thread (so `ants ask --wait` keeps waiting for the reply), and asked chain ants receive a cancellation notice on the request's thread.

### Redaction

Before publishing tool requests (redacted before the 2000-byte input cut), status streams (buffered to whitespace / PEM boundaries so secrets never split across frames), replies, escalations, thread notes, MCP messages/posts, files MCP tools attach (`attach`, `attach_to_reply`) whose bytes are UTF-8 text whatever their type (a `.env`, a config dump; the reference records the redacted size and SHA-256, and other files are stored as they are), and failing-harness errors, the daemon replaces secrets with `[redacted]`:

- **Environment values** (≥ 8 chars, not starting with `/`, `~`, `.` or a drive letter): values from the ant's `env`, harness `env`, and daemon env whose variable name contains `TOKEN`, `SECRET`, `PASSWORD`, `KEY`, `AUTH` or `CREDENTIALS`, plus token-like values (alphanumeric, no spaces/slashes/file extensions) in ant/harness `env`.
- **Patterns:** PEM private keys, `Bearer`/`Basic` headers, GitHub/GitLab/Slack/OpenAI/Anthropic/AWS/Google/Stripe/npm/Hugging Face/Telegram tokens, Slack/Discord webhook URLs, NKey seeds, JWTs, URL passwords (`scheme://user:pass@host`), and secret-named assignments containing a digit (`API_KEY=...`, `"password": "..."`, `--token ...`).

Top-level `redact = false` disables daemon redaction. Files a human attaches with the CLI are never redacted. `ants webhook serve` always redacts outbound notifications (patterns + secret-named process env vars). Local logs are not redacted.

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
