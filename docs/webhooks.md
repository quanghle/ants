# Webhooks and notifications

`ants webhook serve` listens on plain HTTP (`127.0.0.1:8424` by default, behind a TLS reverse proxy), turning `POST <path>/<webhook>` requests into new inbox or channel threads and pushing human notifications (`~/.ants/webhooks.toml` or `--config FILE`).

## Webhook ingress

```toml
listen = "127.0.0.1:8424"                # overridden by --listen
metrics = "127.0.0.1:9465"               # serve /healthz and /metrics (default: off); overridden by --metrics
path = "/hooks"                          # POST /hooks/<name>, GET/POST /hooks/_approval
nats = "nats://127.0.0.1:4222"           # overridden by --nats / ANTS_NATS_URL
js_domain = "hub"                        # overridden by --js-domain / ANTS_JS_DOMAIN

[[webhook]]
name = "github"
creds = "~/.config/ants/auth/github.creds"    # ants auth user github --webhook --to dev-a
auth = "hmac-sha256"                          # signature_header defaults to X-Hub-Signature-256 ([sha256=]<hex>)
secret_file = "~/.ants/github.secret"         # mode 0600; or secret_env = "VAR"
id = "header:X-GitHub-Delivery"               # or "json:/pointer"; default "body-sha256"
ant = "dev-a"
owner = "alice"                               # optional human owner for replies and escalations
text = "GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}"
allow = ["read:*", "execute:gh pr view*"]     # optional thread tool rules; default read-only, ["*"] opts out

[[webhook]]
name = "alerts"                               # ants auth user alerts --webhook --channel ops
creds = "alerts.creds"
auth = "bearer"                               # Authorization: Bearer <secret>; or auth = "token" + token_header
secret_env = "ANTS_ALERTS_TOKEN"
id = "json:/alert/id"
channel = "ops"
mention = ["dev-a"]                           # optional; or broadcast = true for all current members

[[webhook]]
name = "gitlab"                               # ants webhook add gitlab --to dev-a --preset gitlab --signing-token
creds = "gitlab.creds"
auth = "standard-webhooks"                    # webhook-id, webhook-timestamp, webhook-signature (v1,<base64> ...)
secret_file = "gitlab.secret"                 # the signing secret: whsec_<base64>, such as GitLab's signing token
tolerance = "5m"                              # how far webhook-timestamp may be from now (default 5m, 1s to 1d)
id = "header:webhook-id"                      # the default with standard-webhooks
ant = "dev-a"
```

- **Auth & HTTP responses:** `hmac-sha256` (`signature_header`), `bearer` (`Authorization: Bearer <secret>`), `token` (`token_header`, e.g. `X-Gitlab-Token`) and `standard-webhooks` compare secrets and signatures in constant time (`401` on mismatch). Limits: `256 KiB` body (`413`), `15s` request timeout, `64` concurrent requests (`503` with `Retry-After: 5` when overloaded or NATS cannot store within 5 s; nothing buffers locally). Returns `202` (`{"thread": "...", "duplicate": false}`) when stored, `200` (`"duplicate": true`) on repeat, `400` bad JSON/ID, `404` unknown hook, `405` non-`POST`, `415` non-UTF-8 `{body}`.
- **Standard Webhooks:** `auth = "standard-webhooks"` checks [Standard Webhooks](https://www.standardwebhooks.com) signatures, such as those of a GitLab (19.0+) signing token: `webhook-signature` lists space-separated `v1,<base64>` HMAC-SHA256 signatures of `<webhook-id>.<webhook-timestamp>.<body>`, keyed with the base64 of the secret after `whsec_` (the prefix is optional), and any one matching passes; other versions (`v1a`) are ignored. A missing `webhook-id` or `webhook-signature`, or a `webhook-timestamp` (Unix seconds) further than `tolerance` before or after now, answers `401` before the body is read. `id` defaults to `header:webhook-id`, so a request replayed within the tolerance is a duplicate.
- **Deduplication & marker:** deduplicates for 7 d via `<webhook>.<hash>` in `ants_webhooks` plus 24 h `Nats-Msg-Id: webhook.<webhook>.<hash>`. Publishes `<webhook>.webhook` (`{machine, version, perms}`) on connect and hourly, for `ants doctor`; it says nothing of what the sender is.
- **Subjects:** events go to `ants.inbox.<ant>.<webhook>.webhook` or `ants.chan.<channel>.<webhook>.webhook`, subjects only a webhook's credentials (and humans') publish on and no ant's, so daemons take them as a webhook's, untrusted, whatever `ants_webhooks` holds ([auth.md](auth.md#wire-acls-and-verification-invariants)).
- **Templates & untrusted input:** `text` substitutes `{body}`, `{id}`, `{webhook}`, `{header:Name}` and `{json:/pointer}` (`{{`/`}}` escape braces). Mentions come strictly from `mention` or `broadcast`, never from payload text. Threads are narrowed to `allow` (default read-only; see [config.md](config.md#tool-permissions)) until a human messages the ant on the thread, though the turn of a trail run's rating invitation lets `rate_trail` through an `allow` leaving it out, for that run alone ([trails.md](trails.md#trust)); the [built-in harnesses](config.md#harnesses) run their own tools unasked unless the ant sets `tools = "ask"` ([Asking ants about tools](config.md#asking-ants-about-tools)), so narrowing does not reach those tools (`ants doctor` warns). Replies go to `owner` if set (else logged); webhook posts never reset turn budgets.
- **Reload:** reloads on `SIGHUP` or file change (polled every 3 s, applied once stable across two checks), restarting only changed/added/removed `[[webhook]]` and `[[human]]` workers while keeping in-flight HTTP requests alive. `listen` and `metrics` require a restart.
- **Stop:** on `SIGTERM`, Ctrl-C or a replaced executable (`--upgrade-check`), stops accepting connections, closes idle ones, and gives requests under way the rest of their `15s` timeout to be answered before exiting (status `75` after an upgrade); a second `SIGTERM` or Ctrl-C exits at once.

### Trail runs

A message from a webhook's credentials may start a [trail](trails.md) run instead of prompting its ant, as a schedule's
firing does ([config.md](config.md#schedules)): it carries the saved trail, its `args` and the run's limits, and the
ant's daemon conducts the run on the message's new thread, untrusted as the webhook's threads are
([trails.md](trails.md#trust)). `ants webhook serve` sends no such message, but the credentials themselves may: the
conductor takes the limits a firing asks for up to the maxima ([trails.md](trails.md#limits): 500 steps, 2000 turns,
7 days), where a plain message of the same sender spends at most 8 turns. Whoever holds a webhook's or a schedule's
credentials may so start a run of any saved trail on any ant those credentials reach, with any args, under limits of
their choosing: such credentials are the operator's to guard, as the secrets of `ants webhook serve` are. Each step of
the run still counts against the spending caps of the ant answering it
([config.md](config.md#prompt-limits-and-spending-caps)), which bound what the run costs on each ant.

## Adding a webhook

`ants webhook add` issues credentials (locally via `--dir`, or over NATS via `--enroll CREDS [--hub URL]` + `ants enroll approve NAME --webhook ...` on the hub, never through a leaf), writes a 64-hex secret to `<name>.secret` (`0600`), appends `[[webhook]]` to `webhooks.toml`, reloads `ants.webhook` / `ants-webhook`, and prints the URL and secret:

```sh
ants webhook add github --to dev-a --preset github --js-domain hub
ants webhook add alerts --channel ops --preset grafana
ants webhook add gitlab --to dev-a --preset gitlab --signing-token
```

`--secret-file FILE` (its first line; `-` for standard input) or `--secret-env VAR` takes the secret from there instead of making a new one, copied to `<name>.secret`. `--preset gitlab --signing-token` checks GitLab's signing token (`auth = "standard-webhooks"`, `id = "header:webhook-id"`) rather than a secret token. GitLab generates that token (Settings > Webhooks > Generate signing token, shown once): without `--secret-file` or `--secret-env`, the command prints the URL to set in GitLab and reads the pasted token from the terminal without echo. A token that is no `whsec_<base64>` is refused before anything is written.

| `--preset` | `auth` | Header | `id` | `text` |
|---|---|---|---|---|
| (none) | `bearer` | `Authorization: Bearer <secret>` | `body-sha256` | `[webhook {webhook}] {id}\n\n{body}` |
| `github` | `hmac-sha256` | `X-Hub-Signature-256` | `header:X-GitHub-Delivery` | `GitHub {header:X-GitHub-Event} on {json:/repository/full_name}\n\n{body}` (use `application/json`) |
| `gitlab` | `token` | `X-Gitlab-Token` | `header:Idempotency-Key` (17.4+) | `GitLab {header:X-Gitlab-Event} on {json:/project/path_with_namespace}\n\n{body}` (secret token) |
| `gitlab --signing-token` | `standard-webhooks` | `webhook-signature` | `header:webhook-id` (19.0+) | as `gitlab` |
| `grafana` | `hmac-sha256` | `X-Grafana-Alerting-Signature` | `body-sha256` | `Grafana: {json:/title}\n\n{json:/message}` |

## Notifications

`[[human]]` entries in `webhooks.toml` push pending tool requests, inbox escalations (including reminders, expired approvals, and unserved-ant notices), questions ants ask the human (`ask_human`), daemon notices a human has to act on (an ant signed out, out of quota, failing, out of fallbacks, or giving up on a message: kind `notice`, gated by `escalations`), messages from other humans, channel posts that @mention the human, the ends of trail runs told to the human, and optional reply completions:

```toml
public_url = "https://hub.example/hooks"      # enables signed approve/reject links at <public_url>/_approval
action_secret_file = "~/.ants/action.secret"  # HMAC key, 16+ chars, mode 0600; or action_secret_env
timezone = "Europe/London"                    # default IANA zone for quiet hours (default UTC)

[[human]]
name = "alice"
creds = "~/.config/ants/auth/alice.creds"     # ants auth user alice --human
ants = ["dev-a", "dev-b"]                     # optional filter for tool requests (default: all ants)
approvals = true                              # default true
escalations = true                            # default true (also enables unserved-ant notices)
completions = true                            # ants' replies to alice, but for TL;DRs, digests and daemon notices (default false)
completion_window = "1m"                      # push a thread's replies once it is quiet this long, folded into one (default 1m, 0 = each, max 1h)
questions = true                              # questions ants ask alice with ask_human (default: as escalations)
messages = true                               # messages other humans send alice (default true)
mentions = true                               # channel posts that @mention alice (default true)
runs = true                                   # the ends of trail runs told to alice (default true)
quiet = "22:00-07:00"                         # optional local quiet window (start inclusive, end exclusive)
timezone = "America/New_York"                 # optional per-human zone (requires `quiet`)
bypass = ["approvals"]                        # optional kinds bypassing quiet hours and snooze: approvals, escalations (and notices), questions
digest = true                                 # one digest of the held notices when quiet hours or snooze ends (default false)
stand_in = "bob"                              # optional human relayed what waits for alice while away
```

- **Delivery & presence:** each human reads shared durable consumer `notify-<human>` on `ANTS` (deliver new, explicit ack), so multiple `ants webhook serve` instances send each notice once (skipping decided/expired/forged requests and escalations > 24 h old). Sinks with `device = "<id>"` (`ants auth devices alice`) skip notices while that device records active presence in `ants_presence` (`ants tools list -f`, `ants dash`, `ants chat`, or `ants inbox -f` without `--thread`/`--json` for approvals; `ants chat` or `ants inbox -f` without `--thread` for everything of the inbox). Unreadable presence defaults to away.
- **Completions:** a reply is not pushed as it comes: the process that took it waits until the thread has had no reply for `completion_window` (at most five windows after the first), then pushes the latest reply, titled `<ant> replied (<n> replies on the thread)` when it folds several, so an ant answering in steps pushes once, when it seems done. Waiting replies live in memory: a restart drops them, and replies on one thread taken by different instances push once per instance. Holds, presence and digests apply when the push is due.
- **Questions:** a message carrying `question` (MCP `ask_human`) is notified as kind `question`, titled `<ant> asks: <question>` (`<ant> asks N questions: <first>` for several), its text numbering each question's options and giving `ants answer <id>` with what to add: an option's number (numbers joined by commas where several may be picked, or a typed answer where taken), one for each question in order; it is neither an escalation nor a completion, comes with no links and is not relayed to a stand-in. `questions` defaults to the value of `escalations`.
- **Messages, mentions and run ends:** each is a kind of its own, pushed as it comes (no folding), never a completion. A message another registered human sends the human (by the humans bucket; not a webhook's, a decision, a summary request or an answer) is kind `message`, titled `<human> wrote`. The mention envelope a channel post puts in the human's inbox ([channels](channels.md)) is kind `mention`, titled `<author> mentions <human> in #<channel>`, from a human author or an ant. The message a trail run's conductor sends at the run's end (`Envelope.trail_end`: a root run's end, done, failed or stopped, to its owner, else the default human) is kind `run`, titled `<conductor>: run of trail <name> <state>` (`run of an inline script` without a saved trail), its text the end message's with `ants trail show <run>`. Each is gated by its own key (`messages`, `mentions`, `runs`, default `true`), has no `bypass` entry, and comes with no links and no relay to a stand-in.
- **Reactions for ants:** each `[[human]]` connection also answers ants' `list_reactions` requests (`ants.reactions.<ant>`, queue group `reactions` shared with `ants signer`, so one process answers each), since ant credentials do not read `ants_reactions`. It lists the reactions only for a message the asking ant reads; otherwise it answers `unknown` (see [auth.md](auth.md#wire-acls-and-verification-invariants)). Requests to add or remove an ant's reaction (`react`) go to `ants signer` alone: human credentials write only the human's own reactions.
- **Sealed messages:** a notice about a [sealed message](auth.md#sealed-messages-between-humans) names its sender and thread and says only `a sealed message arrived`, with none of its text, title or attachments.
- **Test:** `ants webhook notify-test HUMAN [--config FILE]` sends a test notice to all sinks of `HUMAN` (ignoring presence and holds, without NATS), prints `ok`/`fail` per sink without leaking URLs or secrets, reports quiet-hours status, and exits `1` on any failure.

### Sinks

All sinks run concurrently within `timeout` (`1s`–`30s`, default `10s`); failures log only the sink label (`ntfy topic T`, `slack`, `telegram chat C`, `exec PROGRAM`, `http HOST`).

| `kind` | Config keys | Payload & actions |
|---|---|---|
| `ntfy` | `topic`, optional `url` (default `https://ntfy.sh`), optional `secret_file`/`secret_env` (Bearer) | JSON publish (`priority: 4` for approvals); one-tap HTTP `POST` action buttons |
| `slack` | `secret_file` or `secret_env` (incoming webhook URL; `url` rejected) | `mrkdwn` section + URL buttons opening `/_approval` confirm page |
| `telegram` | `chat`, `secret_file`/`secret_env` (bot token), optional `url` (default `https://api.telegram.org`) | `sendMessage` (previews off) + inline URL buttons opening confirm page |
| `exec` | `command = ["prog", ...]` (no shell; secrets rejected) | 1-line [JSON notice](#json-notice) on stdin; stdout dropped, stderr logged; `SIGKILL` on timeout |
| `http` | `url` (no user/pass/query), `secret_file`/`secret_env` (16+ chars) | `POST` [JSON notice](#json-notice) with `X-Ants-Timestamp: <unix_s>` and `X-Ants-Signature: sha256=<hex>` over `<timestamp>.<body>` |

### Quiet hours, snooze and muted threads

Held notices for approvals, escalations, questions, notices, messages, mentions, run ends and completions are dropped (logged as `held back a notice`, leaving requests pending and inbox messages unread); unserved-ant notices (`unserved`) wait until quiet hours or snooze ends. `bypass` never overrides a muted thread.

- **A trail run's questions:** a question a trail run's `human()` step asks (a question on a step's thread, `run` in its [JSON notice](#json-notice)) that quiet hours hold back is kept and pushed once they end (checked every minute), unless a snooze or a mute holds it then; it still counts in the digest. Kept questions live in memory, at most 64 per instance (the oldest dropped): a restart drops them. A snooze drops such a question as any other.

- **Digest:** with `digest = true`, each approval, escalation, question, notice, message, mention, run end or completion held back by quiet hours or a snooze (not by a mute) is counted per kind in KV `ants_notify` under `held.<human>` (`{since, counts}`), with compare-and-set by whichever instance took it. Once neither quiet hours nor a snooze holds (checked every minute and before the next notice sent, so `ants notify resume` ends one early), one instance claims the count by deleting it at the revision it read and sends a single `digest` notice to every sink: the counts per kind (`approval`, `escalation`, `question`, `notice`, `message`, `mention`, `run`, `completion`) since the first held notice, and the tool requests still pending (up to 10 in the text, all in the [JSON notice](#json-notice)). The count survives restarts and is shared by every instance listing the human, which should all set `digest` alike; a digest is sent at most once, a lost one is not retried, and nothing is marked read or decided.
- **Reminders:** a reminder set in `ants chat` (`/remind`, `b`; `/reminders` lists them) is stored per human in KV `ants_threads` under `reminders.<human>`. Every 30 seconds, each instance listing the human marks the reminders due `pushed` there with compare-and-set, and the one whose write lands sends a `reminder` notice of each to every sink: a reminder is pushed at most once, and neither quiet hours, a snooze nor a muted thread holds it back.

- **Snooze:** `ants notify snooze DUR` (`1s`–`365d`), `ants notify resume`, `ants notify status [--json]`; in `ants chat`, `/notify snooze DUR`, `/notify resume` and `/notify status`.
- **Thread mute:** `ants thread mute THREAD [--for DUR]` (default `30d`) and `ants thread unmute THREAD`.

Runtime holds live in KV `ants_notify` under `<human>` (`{snoozed_until, muted, channels}`; `channels`, the notify levels of `ants channel notify`, governs only unread counts, since channel posts push nothing beyond what reaches the inbox), updated via CAS across devices and read by direct get before each notice; the digest's count lives beside them under `held.<human>`.

### Stand-in

A human counts as away when quiet hours or a snooze holds back a notice and no `device` of its sinks follows what the notice is about (presence as above). While away, `ants webhook serve` relays to the human's stand-in:

- pending tool requests waiting for the human (`ApprovalRequest.human`: the thread's human owner, else the default human), and
- escalations an ant sent to the human's inbox, reminders included (not expired-request notices, not muted threads).

The stand-in is the one the ant's daemon stamps on the request or escalation (`stand_in` of its `[[ant]]`, see [config.md](config.md)), else the human's own `stand_in`; it must be a registered human other than the human away, otherwise nothing is relayed. No assistant stands in: what would reach a human with an assistant goes to that assistant first anyway ([architecture.md](architecture.md#escalations)), and reaches the human, to be relayed, only once passed on. Dead-letter, harness and storage escalations carry no ant stand-in. The relay is an inbox message from the human away to the stand-in, on the escalation's or request's thread, carrying the escalation (an `approval` escalation for a request) with `away = {human, ant}`; it is stored once per notice (`Nats-Msg-Id: standin-<human>-<seq>`), never relayed again, and pushed to the stand-in's own sinks as a notice titled `for <human> (away): …` (subject to the stand-in's own holds; a relayed tool request is skipped when the stand-in is notified of that ant's requests anyway). Relayed notices carry no approve/reject links: the text gives `ants tools approve|reject <ant>/<id>` or `ants ask --thread T @<ant> TEXT`.

The stand-in answers with the same rights any human has: the first stored decision on a tool request wins, and a human's message on a thread ends its escalation. The human away is then told in its inbox: the ant's daemon sends `<stand-in> decided tool request <ant>/<id> … in place of <human>` when the decision names (`ApprovalDecision.by`) another human than the one the request waited for, and `<stand-in> answered the escalation of <ant> … in place of <human>` when another human answers an escalation that reached the human away, whether or not `escalation_reminder` reminds of it. Only `ants webhook serve` decides who is away, so relays need the human away listed as a `[[human]]` of a running notifier. A relayed notice still counts in the human's own [digest](#quiet-hours-snooze-and-muted-threads) (with `digest = true`), which lists the tool requests still pending when the hold ends: those the stand-in decided no longer appear.

### Approve and reject links

With `public_url` and `action_secret_*`, approval notices include `<public_url>/_approval?ant=&id=&seq=&verdict=&by=&exp=&sig=` per offered verdict (`Approve`, `Always allow`, `Reject`). `GET` renders a confirmation page (safe against link previews, naming what `Always allow` allows when the harness says); `POST` verifies HMAC-SHA256 and expiry, then writes the `ApprovalDecision` using the human's credentials (`200` ok, `400` malformed, `403` bad signature, `404` unknown human/request, `405` method, `409` already decided/expired, `410` expired link, `503` hub unreachable). Keep sink channels private (ntfy topics are public unless token-gated).

### JSON notice

`exec` and `http` sinks receive schema `version: 1` (additive fields/kinds keep `version: 1`; breaking changes increment `version`; ignore unknown `kind`s):

| Field | Kinds | Meaning |
|---|---|---|
| `version` | all | `1` |
| `kind` | all | `approval`, `escalation`, `question`, `completion`, `notice`, `message`, `mention`, `run`, `unserved`, `digest`, `reminder`, or `test` |
| `id` | all | Request ID, message ID, first waiting message ID, or fresh digest, reminder or test UUID (deduplicate on `(kind, id)`) |
| `human`, `title`, `text`, `urgent`, `sent_at`, `links` | all | Target human, formatted notice, urgency (`true` for `approval`), RFC 3339 UTC timestamp, `{verdict, label, url}[]` |
| `ant`, `thread` | `approval`, `escalation`, `question`, `completion`, `notice`, `run` | Source ant (a run's conductor) and thread ID to answer on |
| `from`, `thread` | `message`, `mention` | The human who sent the message, or the post's author (a human or an ant), and the thread |
| `channel`, `post` | `mention` | The channel and the ID of the post that @mentions the human |
| `run`, `trail`, `state` | `run` | The run's ID, the saved trail it started on (`null` for an inline script), and `done`, `failed` or `stopped` |
| `run` | `question` | On a trail run's `human()` step only: the run asking |
| `replies` | `completion` | How many replies on the thread the notice folds (the latest is `id`) |
| `notice` | `notice` | The daemon notice's kind: `signed_out`, `quota`, `failing`, `no_fallback`, `gave_up` or `mcp_sign_in` |
| `request`, `seq`, `tool`, `options`, `labels`, `expires_at` | `approval` | `<ant>/<id>`, `ANTS` sequence, `{kind, title, locations?, input?}`, verdicts offered, the harness's names of the offered verdicts it names (`{verdict: label}`, redacted, possibly empty; the notice's text gives the `allow-always` one), RFC 3339 expiry |
| `reason`, `stuck`, `level` | `escalation` | Escalation reason, stuck ant, chain level (`0` = direct) |
| `stand_in_for` | `escalation` | Present on a [stand-in](#stand-in) relay: the human away; `ant` is then the ant to answer |
| `questions` | `question` | The questions in order, each `{question, header, options: [{label, description}], multi_select, free_text}` (redacted): its options in order (`N` in `ants answer <id>` picks the `N`th, from 1), whether several may be picked and whether a typed answer is taken |
| `ant`, `messages` | `unserved` | Unserved ant and waiting message UUIDs |
| `thread`, `message`, `due` | `reminder` | Thread reminded of, the message reminded of (`null` for the thread), RFC 3339 time it fell due |
| `since`, `counts`, `pending` | `digest` | RFC 3339 time of the first held notice, `{kind: n}` held back per kind, `{request, ant, thread, title, expires_at}[]` of the tool requests still pending (`urgent` when any) |
