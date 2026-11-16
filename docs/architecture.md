# Architecture and wire model

`ants` connects headless ACP (Agent Client Protocol) coding agents and human operators across machines over NATS and JetStream. One `ants` binary serves every role: CLI and terminal UIs, ant daemon, credential signer, channel manager, webhook and notification server, event hook runner, stdio MCP bridge, and HTTPS fallback bridge.

```mermaid
flowchart LR
    subgraph worker["Worker node"]
        daemon["ants daemon"]
        acp["ACP harnesses<br/>per-thread sessions"]
        mcp["Session MCP server"]
        wleaf["nats-server leaf<br/>outbox ANTS_OUT"]
        daemon --> acp
        acp <--> mcp
        mcp --> daemon
        daemon <--> wleaf
    end

    subgraph hub["Hub"]
        hserver["nats-server<br/>JetStream domain: hub<br/>stream ANTS · 17 KV · ants_files"]
        services["ants signer<br/>ants channel serve<br/>ants webhook serve"]
        services <--> hserver
    end

    subgraph client["Client node"]
        cli["ants CLI · chat · dash"]
        cleaf["nats-server leaf"]
        cli <--> cleaf
    end

    wleaf <-->|"wss:// · tls:// · tunnel · bridge"| hserver
    cleaf <-->|"wss:// · tls:// · tunnel · bridge"| hserver
```

- **Hub:** one `nats-server` with JetStream holding stream `ANTS`, 17 KV buckets, and the attachment store `ants_files` (`ants init`). With `ants auth`, every ant, human device, assistant, webhook, observer, leaf uplink, machine host, and channel manager connects using role-scoped NKey/JWT credentials ([auth.md](auth.md)).
- **Leaf:** optional per-machine `nats-server` (`ants leaf config`) on `127.0.0.1:4222` with a store-and-forward outbox (`ants leaf outbox`), run as a service of its own or by `ants daemon` ([`[nats_server]`](config.md#managed-nats-server)). Local `ants` processes target `127.0.0.1:4222`; only the leaf uplink crosses the network ([transport.md](transport.md)).
- **Daemon:** `ants daemon` serves ants over per-ant NATS connections and durable consumers, runs up to `max_harnesses` ACP processes per ant, enforces tool allowlists, prompt limits, and spending caps, refreshes colony heartbeats, and exposes an MCP server to each session ([config.md](config.md)).
- **Ants and humans:** ants are named agents (`dev-a`, `reviewer`, `messenger`) with a workdir, harness, capabilities, and optional `whoami`. Humans (`human` by default, or named users from `ants auth user NAME --human` or `ants login`) own threads they start and decide tool requests. Unaddressed `ants ask` messages go to the human's assistant while its grant stands, else to `messenger` for capability routing. Agent-to-agent messages share a per-thread turn budget (default 8).

## Stores and subjects

| Store / subject | Storage / TTL | Purpose |
|---|---|---|
| Stream `ANTS` | File, 30d | Captures `ants.inbox.>`, `ants.notes.>`, `ants.approvals.>`, `ants.dead.>`, `ants.chan.>`, `ants.timeline.>`, `ants.admin.>`; direct gets enabled; republishes `ants.inbox.*.*` to `_INBOX.<to>.stored.<from>`; `max_consumers` 10000. |
| KV `ants_colony` | Memory, 30s | Live `ColonyEntry` per ant (`<ant>`), refreshed every 10s; expiry markers enabled; `max_consumers` 10000. |
| KV `ants_sessions` | File, 30d | Thread ACP session IDs with the harness each ran in (`<ant>.<thread>`), spend/escalation/narrowing state, channel seen cursors, mention markers, `_storage.*.*.warned` notes. |
| KV `ants_attempts` | File, 30d | Started delivery markers and saved replies (`<ant>.<seq>`), message marks (`<ant>.msg.<id>`), withdrawals an assistant recorded for its human while no daemon of the ant answered (`<ant>.msg.<id>.<assistant>`), unserved-ant markers (`<sender>.unserved.<id>`). |
| KV `ants_channels` | File, none | `ChannelEntry` (`<channel>`): members, optional messenger, optional owner ant; `max_consumers` 10000. |
| KV `ants_humans` | File, none | Registered humans (`<human>`), written on CLI connection, each human's signed assistant grant (`<human>.assistant`), enrolling the assistant as a device when `ants assistant seal on` did, and signed consent to the assistant's decisions and orders (`<human>.confirmed`, `<human>.preallow`), written only by that human; `max_consumers` 10000. |
| KV `ants_chains` | File, none | Persistent `escalate_to` chains (`<ant>`), readable after colony expiry. |
| KV `ants_machines` | File, none | Per-machine `MachineRecord` (`<machine>`): the ants its daemon serves (built-in messenger left out), shared-settings `drift` by ant, host name, ants version, and time, put on start and on each change with the machine's host credentials (`ants auth host`); never deleted, so an ant keeps its machine after its colony entry expires (`ants machines`). Also the outcome of each admin order to the machine's ants (`<machine>.<ant>.<id>`: applied, refused, or held, [auth.md](auth.md#admin-orders)), the machine's held assistant orders (`<machine>.held`), and `_account`: the machine holding the team's account key, put by `ants signer` whenever it connects. Each host writes only its own keys; humans read all. |
| KV `ants_links` | File, 30d | Verified `origin_thread` handoffs (`<from>.<thread>.<to>`). |
| KV `ants_owners` | File, 30d | Thread `owner` (`<thread>.<recipient>`, or an assistant's for its human `<thread>.<ant>.<human>.<assistant>`); first record wins (`discard_new_per_subject`, 30d dedup window, no delete/purge). |
| KV `ants_read` | File, none | Per-human inbox read marker (`<human>`: `below` + `read` set) and channel read markers (`<human>.chan`: one `below` + `read` set per channel), each shared across devices. |
| KV `ants_presence` | Memory, 30s | Per-device follow presence (`<human>.<device>.<inbox\|approvals>`), refreshed every 10s; each human writes its own keys and reads every human's (`ants chat` shows who is online), ants read none. |
| KV `ants_devices` | File, none | Directory of human devices' encryption keys: each device's user JWT binding its `ants-xkey:` (`<human>.<device>`), put by its CLI on connecting, revoked keys (`revoked.<U...>`), put by `ants auth revoke`/`push`, and each assistant's user JWT (`<assistant>.assistant`), put by its daemon, against which readers check acts for its human; humans read all and put only their own devices', an assistant only its own record, and ants direct-get the assistants' records and the revocations. |
| KV `ants_notify` | File, none | Per-human snooze deadline and muted threads (`<human>`), and notices held back for a digest (`held.<human>`). |
| KV `ants_reactions` | File, 30d | Message reactions (`<reactor>.<message_id>.<reaction>`); humans write/delete their own keys and read all; ants neither read nor write, asking `ants signer` instead. |
| KV `ants_threads` | File, none (per-message TTLs allowed) | Shared thread titles (`title.<thread>`, also direct-got read-only by ants to title draft pull requests), TL;DRs (`summary.<thread>`), and done states (`done.<thread>`), plus per-human pin/archive/keep marks (`marks.<human>`) and threads snoozed from home in `ants chat` (`snoozed.<human>`), and those an assistant sets for its human under keys of its own (`marks.<human>.<assistant>`, `snoozed.<human>.<assistant>`), honoured only while the human's grant names it and where the human's own mark or snooze is absent; `ants thread list` prunes entries of expired threads. |
| KV `ants_identities` | File, none | One `AntIdentity` per ant (`<ant>`), never expiring: random 128-bit `id` and `born`, set by the ant's daemon on start when absent; `avatar` seed and `color` (`ants ant avatar`); completed prompt count (CAS-updated by the serving daemon); `moved` (`ants ant export --move` sets, `ants ant unseal` clears; while set no daemon serves the ant); `cloned_from` (set by `clone` orders). `ants ant import` creates a record via KV create, refused on name or id collision. Each daemon writes its own key; ants, assistants, and humans read all; `ants auth revoke ANT` deletes it. Also each ant's shared settings (`<ant>.settings`: role settings and core memories), written by host credentials only and served only when signed with the ant's credentials for the bucket's incarnation ([config.md](config.md#shared-settings)). Also `_incarnation`, the marker `ants init` writes when absent, or anew with `--identities-restored` after a restore from backup: without it no shared settings are served. |
| Store `ants_files` (stream `OBJ_ants_files`) | File, 30d | Attachment bytes as chunks of up to 128 KiB on `$O.ants_files.C.<from>.<inbox\|notes\|chan>.<target>.<id>.<n>` (the handle follows the subject of the carrying message `{id, name, mime, size, sha256}`; chunk 0 carries it in header `Ants-Attachment`); up to 8 attachments of 8 MiB each per message; each chunk stored once (24h duplicate window), no delete/purge; direct gets. Senders write their own, ants read those of messages they read, humans read all. |
| KV `ants_webhooks` | File, 7d | Webhook event hashes (`<webhook>.<hash>`), schedule firings (`<schedule>.<unix_time>`), and hourly `<webhook>.webhook` / `channel-manager.serve` version markers. |
| Core NATS | Unstored | `ants.status.<ant>.<thread>`, `ants.cancel[led].<ant>.<thread>`, `ants.queue[d].<ant>`, an assistant's `ants.cancel.<ant>.<thread>.<assistant>` and `ants.queue.<ant>.<assistant>`, `ants.chanadm.<channel>.<ant>`, `ants.reactions.<ant>[.react]` (answered on `_INBOX.<ant>.reactions`), `ants.enroll.<ant>[.reply]`, `ants.renew.<name>[.reply]`, `ants.login.<name>[.reply]`, `ants.antreq.<manager>[.reply]`. |

### Daemon notices

An `[ants]` message a daemon sends of its own, rather than an ant's answer, carries `notice` with its kind: `spend` (a spend warning), `signed_out`, `quota`, `failing`, `no_fallback`, `gave_up` (a dead letter), `withdrawn`, `expired` (a session's kept branch), `unserved`, `assistant` (what an assistant did for its human), `in_place` (an answer or decision taken by another) or `request_expired`. A reply or follow-up never copies it. `ants chat` shows such a message as a dim system line that makes no thread finished or unread; only the kinds a human has to act on (`signed_out`, `quota`, `failing`, `no_fallback`, `gave_up`) count as what the thread is stuck on, and only those are pushed by `ants webhook serve` (kind `notice`). Notices carrying an escalation keep the escalation's handling.

### Identifiers and durations

- User, channel, thread, and JetStream domain IDs use 1–64 characters of `a-z`, `0-9`, and `-`, starting with a letter or digit (`bootstrap`, `channel-manager`, `revoked`, and `signer` are reserved: no ant, human, webhook, or device takes them).
- Machine and harness names also allow `_` (machine names allow uppercase ASCII and must differ from `js_domain`).
- Durations require whole-number units ordered largest first (`s`, `m`, `h`, `d`: `90s`, `10m`, `1h30m`) or `0`; bare non-zero numbers are rejected.

## Delivery

### Pool, worktrees and at-least-once retries

Each ant runs up to `max_harnesses` (default 4) ACP processes (or CLIs of a native protocol behind an in-process ACP translator, [config.md](config.md#harness-protocols)), concurrent across threads and sequential per thread. Idle harnesses stop after `idle_timeout` (`10m`), or immediately while system memory is low ([`[memory_pressure]`](config.md#memory-pressure)), and resume via ACP `session/load` (`<ant>.<thread>` in `ants_sessions`), rebuilding from stored thread history if `session/load` fails ([config.md](config.md#lost-sessions-and-context-window)). First Ctrl-C/`SIGTERM` drains active prompts; second Ctrl-C exits immediately.

Human messages on a thread with a running prompt automatically cancel that prompt and run next in the same session, ahead of ordinary waiting messages and after earlier human interruptions. This includes human channel replies that wake the ant; ant messages never interrupt. The cancelled prompt's partial output is stored and answered without retry, dead letter or escalation. In-flight tool calls are cancelled, not resumed ([config.md](config.md#prompt-limits-and-spending-caps)).

With worktrees on, each thread gets its own git worktree at `<home>/worktrees/<thread>` on branch `ants/<ant>/<thread>` from `HEAD`, pruned once `<ant>.<thread>` expires from `ants_sessions` (preserving branches with unmerged commits and appending an `[ants]` notice). Worktrees are on by default for a workdir inside a git work tree, and not ignored by it, with `max_harnesses > 1` (logged at daemon start); `worktrees = true` (`--worktrees`) forces them on and `worktrees = false` (`--no-worktrees`) keeps every thread in the shared workdir. Trade-offs: one checkout per thread (plus build outputs) on disk, a branch per thread to merge or delete, and threads not seeing uncommitted changes of the workdir; see [config.md](config.md#worktrees). `ants daemon` and `ants doctor` warn when `worktrees = false` leaves `max_harnesses > 1` sharing a git workdir.

With `worktree_base = "upstream"` (default `"head"`), a thread's new branch starts from the default branch of the repository's default remote (the current branch's upstream remote, else `origin`): `git fetch` runs first (60 s timeout, no credential prompt), then the branch starts from `refs/remotes/<remote>/HEAD`, else from the current branch's upstream. Without a remote, or when the fetch fails or neither ref is known, the branch starts from `HEAD` with a logged warning. Branch commits already in that remote branch count as merged when pruning and in the `[ants]` notice.

With `publish = "draft-pr"` (default `"off"`) and worktrees on (set or by default), a prompt that ends with new commits on the thread's branch pushes `ants/<ant>/<thread>` to the default remote and opens a draft pull request with `gh` (title: the thread's title, read by direct get ([auth.md](auth.md#wire-acls-and-verification-invariants)) unless sealed between humans, else the first line of the thread's first prompted message; body: the ant and thread id), or finds the open one updated by the push. The push needs an allow rule such as `execute:git push origin ants/<ant>/*` or an approval, like a tool call ([config.md](config.md#tool-permissions)). The reply carries an `[ants]` line with the pull request URL, or with what was rejected or failed; nothing is retried until the branch has commits the remote lacks. A branch the remote already has as it is, or a prompt cancelled by a human, publishes nothing. `git push` and `gh` run without prompts (`GIT_TERMINAL_PROMPT=0`, `GH_PROMPT_DISABLED=1`), bounded by timeouts (120 s, 60 s), with `gh` authenticated on the machine.

Attachments of a prompted message (an inbox message, or a channel post read under its `chan` handles) end the prompt's text with one line each: `[ants] Attached: "NAME" (MIME, SIZE bytes), handle HANDLE`, then how to read it with the `read_attachment` MCP tool. When the harness's ACP `initialize` advertises `promptCapabilities.image`, each `image/png`, `image/jpeg`, `image/gif`, or `image/webp` attachment is read from `ants_files` with the ant's credentials, checked against its size and SHA-256 (missing chunks of an attachment made within the hour, or carried by a message the hub stored within the hour, perhaps still on their way from a leaf's outbox, are waited for up to 60 s; likewise for `read_attachment` on the handles of the message being handled), and handed after the text as an ACP image content block, its line saying so. Other attachments, all attachments on harnesses without image support, and attachments that cannot be read (invalid reference, missing or mismatched bytes, with the reason in the line) stay lines only; the prompt runs regardless. Earlier channel posts shown as thread context and rebuilt session history entries list attachments the same way, one line each with the handle from the subject and nothing read from the store; in a rebuilt session these lines count toward the history budget.

```mermaid
sequenceDiagram
    participant JS as Stream ANTS / KV
    participant D as ants daemon
    participant H as ACP harness

    JS->>D: Deliver message (60s ack_wait, 20s progress acks)
    D->>JS: Claim {ant}.msg.{id} = started & write {ant}.{seq}
    D->>H: Prompt session (stream StatusEvent on ants.status.{ant}.{thread})
    H-->>D: Reply + tool calls
    D->>JS: Store Timeline, save reply in {ant}.{seq} & {home}/answered
    D->>JS: Publish reply (24h dedup ID) & mark {ant}.msg.{id} = finished
    D->>JS: Ack input message
```

In-hand messages send 20 s progress acks against a 60 s `ack_wait`, acking only after storing the reply (for a turn that sends none, after publishing its `no_reply` Timeline record, best effort):

1. Claims `<ant>.msg.<id>` = `started` in `ants_attempts` (unless already `withdrawn`, or withdrawn by a record of the human's assistant stored before the claim, [auth.md](auth.md#tools-acting-for-its-human)), then writes `<ant>.<seq>` before prompting so redeliveries after a crash prepend a prior-tool-call warning. After a failed attempt it swaps `started` for `retrying` (compare-and-set) before handing the message back for a retry, unless the attempt stored a reply its redelivery only sends again, and likewise before holding it for a sign-in or a provider limit met after tools may have run (a tool call started, or an earlier attempt was cut off), the mark's `held` naming which; taking the message up again claims `started` by compare-and-set on that `retrying` mark.
2. Saves the finished reply in `<ant>.<seq>` and `<home>/answered` before publishing with 24 h dedup ID `reply-<ant>-<envelope_id>`, preventing re-prompts across post-generation disconnects or offline leaf outboxes, then marks `<ant>.msg.<id>` = `finished`.
3. Streams unpersisted `StatusEvent` JSON on `ants.status.<ant>.<thread>` (`state`, 250 ms batched `message`/`thought`, `tool`, `plan`, `queued`, `approval`, `escalated`, `asked`, `decided`).
4. Stores the prompt's tool calls and plan as compact `Timeline` records on `ants.timeline.<thread>.<ant>` (at most one every 30 s while calls changed, the rest when the prompt ends; ≤ 50 entries each, ≤ 200 calls per prompt with the rest counted; titles and paths redacted as status is, never inputs, outputs, text, or thoughts), plus a `no_reply` entry for a turn that sends no reply (why: `reply_to_own` or `completed`, `complete`'s reason, and the final text that went to nobody, redacted as replies are). A failed store is logged and dropped ([hooks.md](hooks.md#timeline-on-antstimelinethreadant)).

### Queue and withdrawal

- `ants queue ANT` queries `ants.queue.<ant>` (5 s timeout), combining messages held by the active daemon, messages awaiting redelivery (delivered but not acked, found from where the daemon's answer says its inbox and channel consumers stand), and up to 100 undelivered inbox messages and channel posts that wake the ant (an @mention of a member, or a triage for the messenger) from `ANTS`. A message awaiting a retry after a failed attempt (marked `retrying`) shows `awaiting a retry after a failed attempt`, and one held for a sign-in or a provider limit after its tools ran (marked `retrying`, `held` `sign_in` or `provider_limit`, as is the `held` field of `--json`) shows `held for a sign-in` or `held for a provider limit`; both can be withdrawn; one awaiting redelivery still marked `started` (its daemon stopped mid-attempt, or the attempt stored its reply) shows `awaiting redelivery (started; not withdrawable)`. Without `ANT`, `ants queue` asks the daemons of every ant live in `ants_colony` at once and lists their messages with an `ANT` column, leaving out ants with nothing waiting; an ant whose daemon does not answer is reported on stderr, and the command fails only when none answers. `--json` lines then carry an `ant` field.
- `ants withdraw ANT MESSAGE` atomically creates `<ant>.msg.<id>` = `withdrawn` in `ants_attempts` (losing to `started` if prompting already began; sign-out and provider-limit holds before tool execution clear `started`, and those after it swap `started` for `retrying`, so messages stay withdrawable), or swaps a `retrying` mark for it by compare-and-set: of the withdrawal and the redelivery claiming `started`, exactly one wins, and the loser finds the winner's mark. When its turn arrives, the daemon replies `[ants] Withdrawn by <human> before <ant> was prompted with it (ants withdraw).` (`before <ant> retried it after a failed attempt` for one awaiting a retry, `before <ant> retried it after holding it for a sign-in` or `for a provider limit` for one held, the withdrawal keeping the `held` of the `retrying` mark) and acks without prompting. An assistant withdraws for its human through the ant's daemon instead, or, while none answers, by a record under `<ant>.msg.<id>.<assistant>` the daemon honours before taking the message up (`withdraw_for_human`, [auth.md](auth.md#tools-acting-for-its-human)): the mark carries `on_behalf_of`, the reply reads `[ants] Withdrawn by <assistant> for <human> before <ant> was prompted with it.`, and `ants queue` shows `withdrawn by alice via pa`.
- **Unserved-ant notices:** daemons (via `sentread-<ant>`), `ants webhook serve` (with `escalations = true`), and `ants inbox -f` check sent messages every minute. If a message waits ≥ 15 m for an ant absent from `ants_colony` ≥ 15 m, the first watcher to claim `<sender>.unserved.<id>` in `ants_attempts` notifies the owner/sender once with `ants withdraw` instructions and up to 5 capability-matching live peers.

## Colony, identity and memory

Each daemon publishes a `ColonyEntry` at `<ant>` in `ants_colony` every 10 s (and immediately on state transitions), deleting it on clean exit (`30s` TTL with `MaxAge` delete markers; `ants colony` marks entries without a heartbeat for 20 s as `stale`):

- **Identity & runtime:** `machine` (config `machine`, else the host name), daemon `instance`, `harness`, `agent` (`{name, version}` from ACP `initialize`), `model` (`{model, reasoning_effort, context}`), `workdir`, `capabilities`, `whoami`, `escalate_to`, `drift`, `version`, `perms`, `started_at`, `heartbeat_at`, `activity` (`idle` / `working`), `handling` count, live `harnesses` count.
- **Hold states:** `signed_out` (`{since, sign_in?}`), `limited` (`{kind: "rate_limit" | "quota", since, until}`), and `failing` (`{since, error}`), plus `fallback` (`{name, since}`) while one of the ant's `fallback` harnesses or models answers instead, and `paused` (`{since, by?}`: `by` names the admin whose order paused it) while `paused = true` holds it ([config.md](config.md#pausing-an-ant)).
- **Memory pressure:** `memory_low` (`{since, available, max_harnesses}`) while the machine's available memory is below `[memory_pressure]` `min_available` ([config.md](config.md#memory-pressure)).

Per-ant prompt instructions and state isolation:

- `whoami` (`--whoami TEXT`): role description prepended to every prompt and published in `ants_colony` (keep secrets out; `route` matches `capabilities`, not `whoami`).
- `memory` and `tasks` (`--memory`, `--tasks`): private instructions prepended to every prompt.
- `env` (`--env NAME=VALUE`): literal environment overrides passed to the harness and tool subprocesses (e.g. `BD_DB = "/srv/ants/dev-a/.bd/bd.db"`).

## MCP tools

Each session gets the `ants` MCP server over streamable HTTP (`127.0.0.1:<port>/mcp` with per-thread bearer token) or via `ants mcp --socket <home>/mcp.sock` (`0600`, `ANTS_MCP_TOKEN`; current-user named pipe on Windows). Allowlist rules match `other:<tool>`:

| Tool | Description |
|---|---|
| `send_message(to, text, thread?, attach?)` | Send to an ant or human (`thread` omitted: current; `"new"`: fresh; else ID); warns when unserved or limited. |
| `ask_ant(to, text, timeout?, attach?)` | Ask an ant on the current thread and wait (`timeout` default `5m`, max `30m`; refuses humans, unserved ants, or limits outlasting `timeout`). |
| `check_inbox(all_threads?)` | Drain late `ask_ant` replies arriving after timeout (persisted in `<home>/mcp.json`). |
| `ask_human(question, options, free_text?)` | Ask the thread's owner (else the default human; its assistant first, see [escalations](#escalations)) a question with 2–9 options (question ≤ 500 chars, options ≤ 200; `free_text` default `true` also takes typed answers) and return at once; 0 turns. |
| `react(reaction, message_id?, remove?)` | Add or remove the ant's reaction on a message it reads (default: active message), made by `ants signer`; 0 turns, wakes nobody. |
| `list_reactions(message_id?)` | List reactions and reactors on a message the ant reads (default: active message), answered by `ants signer` or `ants webhook serve` with a `[[human]]` entry. |
| `list_ants(capability?)` | List peer ants from `ants_colony` with capabilities, `agent`, activity, and hold states. |
| `route(capability, text, wait?, timeout?, attach?)` | Route to the best peer advertising `capability` (not paused > paused, healthy > on a fallback > held, `live` > `stale`, `idle` > `working`, fewest messages); waits unless `wait: false`. |
| `post_notes(text, attach?)` / `read_notes(thread?)` | Append to (1 turn) or read (up to 50 posts, 0 turns) passive thread notes; wakes nobody. |
| `read_attachment(handle)` | Read a file attached to a message the ant reads, by the handle listed with it (`<from>.<inbox\|notes\|chan>.<target>.<id>`): PNG, JPEG, GIF, or WebP as image content, text or JSON up to 128 KiB as text, any other saved into the working directory under its name (`name-2`… beside a different file) and its path returned; 0 turns. |
| `attach_to_reply(attach)` | Attach files from the working directory to the turn's final answer (message, channel post, or thread notes post sent when the turn ends), as `attach` does for `send_message`; a later call in the same turn replaces them, `[]` attaches none. Checked when called and read again as the answer is sent: a file that cannot be attached leaves them all out and appends an `[ants]` line saying why; 0 turns. |
| `decide_approval(verdict)` | Answer an incoming chain approval notice (`allow_once`, `allow_always`, `reject`). |
| `post_channel(channel, text, attach?)` | Start a channel thread as `creator` (member or channel messenger only; inherits turns minus 1 for ant messages). |
| `manage_channel(channel?, add?, remove?, messenger?, clear_messenger?)` | List owned channels, inspect one, or modify members/messenger via `ants channel serve`. |
| `escalate(issue)` | Escalate the thread one level up its origin chain (or to the owner, its assistant first) with reason `asked`; for an assistant leading an escalation for its human, pass it on to that human. |
| `suggest_thread_title(title)` | Suggest a title for the current thread (1–120 trimmed single-line chars); the turn's reply carries it, and a human's `ants thread list` or `show` stores it only if the thread never had a title. |
| `complete(reason?)` | End the turn with no reply: its final answer goes to nobody (no message, channel post, or notes post), the message gets a `check` reaction (best effort), and `reason` (one line, 1–500 chars) is kept on the thread's timeline; 0 turns, wakes nobody. Refused for a human's message (or an assistant's on behalf of one), a webhook's, an escalation, or an approval request, which are always answered. |
| `human_queue`, `answer_for_human`, `send_for_human`, `post_for_human`, `notes_for_human`, `react_for_human`, `title_for_human`, `done_for_human`, `decide_for_human`, `order_for_human`, `queue_for_human`, `cancel_for_human`, `withdraw_for_human`, `mark_for_human`, `snooze_for_human` | Assistants only, while the human's grant names the ant: read the human's home queue and act for it, each act carrying `on_behalf_of` ([auth.md](auth.md#tools-acting-for-its-human)). |
| `request_ant(...)` / `retire_ant(name)` | Template managers only: add or retire a templated worker ant via `ants signer` ([config.md](config.md#manager-ants)). |

`attach` lists up to 8 files of up to 8 MiB each, by path relative to the directory the thread's session works in (the ant's `workdir`, or the thread's worktree): a path whose real location (symlinks and `..` resolved) leaves it, a non-regular file, or a file too big fails the call with nothing sent. Each file goes to `ants_files` under the message's handle, named by its base name, with a media type guessed from its extension; UTF-8 text files have secrets replaced with `[redacted]` first, as message text does ([config.md](config.md#redaction)). Replies, `check_inbox`, and `read_notes` list a message's attachments with their handles.

Human traffic costs 0 turns; ant-to-ant messages cost 1 turn (`ask_ant` requires 1 turn left for the reply), and self-messaging is refused. Another ant's reply to an ant's own message (the automatic reply to its `send_message`, or a channel post by an ant whose `in_reply_to` names its post, such as the answer to a post that @mentioned that ant) still prompts the ant, even with no turns left, starting with an `[ants]` line saying the reply is to its own message, its answer is not sent back, and `send_message` or `post_channel` continue the exchange (with no turns left, `escalate` hands the thread on). The turn's final text goes to nobody (kept as a `no_reply` timeline entry); the prompt costs 0 turns and wakes nobody, so two ants messaging each other stop after one reply instead of spending the turn budget; `send_message` and `post_channel` in that turn still need a turn. Messages carrying an escalation (the leader's answer and its routing), approval requests, webhook messages, a channel thread resumed after an escalation, and `ask_ant` replies (taken by the waiting call) are answered as before. A limit or cap reached in such a prompt still escalates, and an empty final answer is not taken as no reply. A 3 s `ants_colony` lookup runs before messaging ants, suggesting closest names or capability peers when the target is unserved.

## Messenger, channels, thread notes and questions

### Messenger and thread notes

- **Messenger:** unaddressed `ants ask` (from a human with no assistant, see [auth.md](auth.md#assistant-grant)) and unmentioned human/webhook channel posts route through `messenger`. Unless `messenger = false` or `[[ant]]` defines `messenger`, daemons run a built-in `messenger` (`~/.ants/messenger`, `~/.config/ants/auth/messenger.creds` if present) with automatic standby failover via `ants_colony` (~1 s on exit or 30 s TTL expiry), yielding immediately to an explicit `[[ant]] name = "messenger"`. It suggests a short title (`suggest_thread_title`) for a thread a human's request starts.
- **Thread notes:** `ants.notes.<thread>.<from>` provides a passive shared board per thread (wakes nobody). An ant's `post_notes` spends 1 turn via CAS from `min(message_budget, last_note_budget)`; exhausting turns escalates `notes` until a human runs `ants notes <thread> TEXT` (8 fresh turns) or escalation climbs a level.
- **Questions (`ask_human` / `ants answer`):** a question is a message to the human carrying `question` (text, numbered options, `free_text`); `ants inbox` and `ants thread show` print its options and how to answer it, and `thread show` lists the answers stored with it. `ants answer MESSAGE N|TEXT` finds the question among messages to the human, checks the option is one of its numbers (typed words only if `free_text`), and replies on the thread with the option's text (or the words) and `answer` (question id, option or none). An answer prompts the ant with an `[ants]` line naming the question and the option picked, or saying the answer is typed; an answer to no question the ant sent the human is an ordinary message. Only ants ask, so answers go to ants in the clear; a question from a human (no CLI sends one) is refused, pointing at `ants ask @HUMAN`. A stored question is emitted on the ant's status as `asked` (`ants hook run --on question`) and pushed by `ants webhook serve` to the human's sinks (`questions` per `[[human]]`, [webhooks.md](webhooks.md#notifications)).

### Channels

Channels (`ants.chan.<channel>.<from>`, metadata in `ants_channels`) group ants and humans:

- **Mentions & triage:** `@ant` wakes mentioned ant members; `@human` sends an inbox mention notice to mentioned human members; human/webhook posts with no mentions wake the channel messenger (`--messenger`, default `messenger`) for a 1-turn `@mention` triage reply; unmentioned agent posts wake nobody.
- **Ownership & budget:** human starters/posters or webhook `owner` set thread `owner`; ant starters set `creator` (propagated across spin-offs via `origin_thread` and `ants_links`). Ant posts share the thread's turn budget (minimum remaining across later posts), escalating `turn_budget` when exhausted. A member's answer to an ant's post replying to the member's own post, or a turn ended by `complete`, is not posted.
- **Channel owners:** `--owner ANT` lets that ant call `manage_channel` via `ants.chanadm.<channel>.<ant>` to `ants channel serve` (`ants auth channel-manager` creds), which validates ownership/colony and announces changes on `ants.chan.<channel>.channel-manager`.
- **Delivery:** ants read `ants.chan.>` via durable consumer `chan-<ant>`, prepending up to 50 unseen thread posts (`<ant>.<thread>.seen` in `ants_sessions`).

## Escalations, dead letters and storage

### Escalations

```mermaid
flowchart LR
    stuck["Unresolved condition<br/>turn_budget · cap · notes<br/>dead_letter · context_window<br/>asked · approval"]
    chain["escalate_to chain<br/>8 fresh notes turns<br/>first ant leads"]
    next["Leader's escalate_to<br/>up to max_escalation_hops"]
    human["Thread owner<br/>else default human<br/>reminders until answered"]

    stuck --> chain
    chain -->|"escalate_after or stuck"| next
    next -->|"chain exhausted"| human
    stuck -->|"no chain"| human
```

Unresolved conditions (`turn_budget`, `cap`, `notes`, `dead_letter`, `context_window`, `asked`, plus single-hop `approval`) climb `escalate_to` (`ants_chains`; or `creator`'s chain on ant-created threads) before falling back to thread `owner`, else daemon `human`:

- All unvisited chain ants receive an `[ants]` notice and 8 fresh thread-note turns; only the first listed ant (leader) may answer on the thread or `escalate` further.
- Escalation climbs to the leader's `escalate_to` after `escalate_after` (`30m`; `"0"` disables) or if a member gets stuck, up to `max_escalation_hops` (default 3). Only a human message on the thread resets escalation to level 0.
- **Assistant first:** an escalation leaving the chain for a human (turn budget, caps, dead letters, `asked`, …) and an `ask_human` question go first to that human's assistant while the human's grant names it, the assistant's record names the human, and a daemon serves the assistant (listed and live, not paused, failing, signed out or held back by a provider limit); otherwise straight to the human. The escalation goes one level up with the assistant leading it alone (`Escalation.assistant_of` = the human; reason `question` for a question, no thread-notes post). The assistant answers the stuck ant on the thread, or passes it on to the human with `escalate(issue)` (`[ants] <assistant>, the assistant of <human>, passes … on to <human>`); without an answer within `escalate_after` it goes on to the human by itself (`[ants] No answer from <assistant> …`), as it does when the assistant can no longer act by the next escalation. A passed-on question reaches the human as the question it is, answered with `ants answer`. The human may answer at any point: a human message ends the escalation. A tool request still goes to the human at once (who is notified as before); the assistant gets an `approval` escalation as well, which it decides for the human with `decide_for_human` (held until the human confirms it or a pre-allow rule covers it; `decide_approval` and `escalate` refuse it). Notifiers hear of escalations and questions only once they reach the human.
- Human-bound escalations repeat an `[ants] Reminder:` after `escalation_reminder` (default `4h`, `"0"` disables) and then daily for 30 d until answered.
- **Status updates:** chain escalations, human escalations, reminders, and chain/expired tool approvals also emit `StatusEvent` (`type: "escalated"`) on `ants.status.<ant>.<thread>` (direct `harness`, `storage`, and `ants dead watch` notices do not; see [hooks.md](hooks.md#escalations)). Every settled tool request, whether by a human, a chain member's `decide_approval`, or expiry, emits `type: "decided"` on the requester's status ([hooks.md](hooks.md#decisions)).

### Dead letters

Inbox and channel messages get 5 prompt attempts (`max_deliver = 6`; sign-out, provider-limit, and startup-breaker holds, and first context-window session rebuilds, spend none). Failure on attempt 5 (or delivery 6 after a crash) writes a `DeadLetter` to `ants.dead.<ant>`, escalates `dead_letter`, and terminates delivery (`ants dead list | replay SEQ | drop SEQ | watch`). If NATS fails on delivery 6, `ants dead watch` and `ants inbox -f` convert `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ANTS.*` advisories into dead letters and notify the owner directly; `ants dead watch --metrics ADDR` counts them in `ants_dead_letters_total` ([operations.md](operations.md#monitoring)).

### JetStream storage

Every 5 m (first check at 30 s), one ant per daemon/server queries hub and local leaf JetStream usage, exporting `ants_jetstream_bytes` and warning the default human (`storage` escalation) once usage reaches `[storage]` `warn_percent` (default `80%`), naming the top 3 streams and fix. Shared KV lock `_storage.<domain>.<store>.warned` in `ants_sessions` deduplicates warnings across daemons until usage drops > 5 percentage points below `warn_percent`. Hub limits come from `[storage]` `max_file_store` / `max_memory_store` (unless set on the NATS account); leaf limits default to `1 GB` file / `64 MB` memory.
