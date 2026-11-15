# Hooks

External programs act on bus events as read-only **observer** NATS clients against the subjects and JSON schemas below (see [Versioning](#versioning)).

```sh
ants auth user audit --observer --js-domain hub         # audit.creds, on the machine holding the team keys
ants hook run --creds audit.creds --js-domain hub --on dead-letter -- ./file-ticket.sh
```

## Observer credentials

`ants auth user NAME --observer` issues them (`--public-key KEY` signs a key generated elsewhere, `--store-key` keeps
the seed in the OS credential store), as `Permissions::for_observer` in `ants-proto` sets out. They are listed by
`ants auth issued` (kind `observer`), revoked by `ants auth revoke NAME`, and renewed like ant credentials: with
`ants auth user NAME --observer`, or over NATS with `ants renew FILE` and `ants enroll renew NAME` (see
[Renewing over NATS](auth.md#renewing-over-nats)). The default human's name cannot be an observer's.

They may:

- read every approval request, approval decision and dead letter stored in stream `ANTS`, through consumers named
  after the observer, one per feed, whose filter is pinned in the subject creating them (below), and hear the same
  subjects live;
- hear every ant's live status (`ants.status.<ant>.<thread>`), escalations among them;
- read and watch the `ants_roster` bucket;
- ask for their own renewal, and hear their own reply inboxes (`_INBOX.<observer>.>`).

They publish no message, answer no tool request, cancel no prompt and write no bucket, so a leaked observer
credentials file lets no one act for an ant or a human. They read no inbox, notes or channel subject, nor `STREAM.INFO`
or `STREAM.MSG.GET` on `ANTS`. Every KV bucket other than `ants_roster` is denied too: `ants_sessions`, `ants_attempts`,
`ants_owners`, `ants_links`, `ants_chains`, `ants_humans`, `ants_channels`, `ants_webhooks`, `ants_read`,
`ants_presence`, `ants_notify`, `ants_reactions` and `ants_threads`.

Event payloads can still contain prompt or tool text:

- a dead letter holds the message given up on (its `envelope`, or the start of its `payload`);
- an approval request holds the tool call's title, files and input;
- status updates stream an ant's reply (`message`) and reasoning (`thought`) as it is written.

## Feeds

| `ants hook run --on` | Subject | Delivery | Document |
|---|---|---|---|
| `approval-request` | `ants.approvals.<ant>` | Stored; consumer `observe-requests-<observer>` | `ApprovalRequest` |
| `approval-decision` | `ants.approvals.<ant>.<request id>` | Stored; consumer `observe-decisions-<observer>` | `ApprovalDecision` |
| `dead-letter` | `ants.dead.<ant>` | Stored; consumer `observe-dead-<observer>` | `DeadLetter` |
| `escalation` | `ants.status.<ant>.<thread>` | Live (core NATS) | `StatusEvent` of type `escalated` |
| `status` | `ants.status.<ant>.<thread>` | Live (core NATS) | `StatusEvent` |
| `roster` | `$KV.ants_roster.<ant>` | Live (KV watch) | `RosterEntry` |

Stored feeds are read through a consumer of stream `ANTS` (`Feed` in `ants-proto`): the observer may create, read,
ack, look up and delete only `observe-requests-<observer>` filtered to `ants.approvals.*`, `observe-decisions-<observer>`
filtered to `ants.approvals.*.*` and `observe-dead-<observer>` filtered to `ants.dead.>`, so the create request names
both (`$JS.API.CONSUMER.CREATE.ANTS.<consumer>.<filter>`). A durable consumer keeps its place while the hook is down;
two processes with the same credentials share one consumer and split its messages, so give each hook its own
credentials. The stream keeps messages 30 days. The observer finds JetStream as webhooks do, through the info of the
consumer `inbox-<observer>`, which need not exist.

Live feeds carry only what happens while the hook listens, and an update may be dropped. The roster bucket also allows
reads (`$JS.API.DIRECT.GET.KV_ants_roster.>`) and stream info.

Every document is a JSON object. A hook checks it against where it was read, as `ants hook run` does, since credentials
pin the subject (or key) each user publishes on but not what the document says: a document naming another ant than its
subject is forged.

### `ApprovalRequest` on `ants.approvals.<ant>`

A tool call outside an ant's allowlist, stored by the ant's daemon. It waits for a decision until `expires_at`, then
the ant rejects it; no decision is stored for an expired request.

```json
{"id": "019a7d2e-…", "ant": "dev-a", "thread_id": "019a7d2c-…",
 "tool": {"kind": "execute", "title": "rm -rf target", "locations": ["/src/app"], "input": "{\"command\":\"…\"}"},
 "options": ["allow_once", "allow_always", "reject"],
 "requested_at": "2025-01-01T12:00:00Z", "expires_at": "2025-01-01T12:05:00Z"}
```

`tool.kind` is an ACP tool kind (`read`, `edit`, `delete`, `move`, `search`, `execute`, `think`, `fetch`,
`switch_mode`, `other`); `locations` and `input` may be absent. Check: `subject == "ants.approvals." + ant`.

### `ApprovalDecision` on `ants.approvals.<ant>.<request id>`

A human's verdict (`ants tools approve|reject`, or a notification's link). The first stored wins. Decisions of the
ants of the requester's chain (`decide_approval`) travel in the requester's inbox instead and are not part of the feed.

```json
{"request_id": "019a7d2e-…", "ant": "dev-a", "verdict": "allow_once", "decided_at": "2025-01-01T12:01:00Z"}
```

`verdict` is `allow_once`, `allow_always` or `reject`. Check: `subject == "ants.approvals." + ant + "." + request_id`.

### `DeadLetter` on `ants.dead.<ant>`

A message an ant gave up on, stored by its daemon or by `ants dead watch` (see
[Dead letters](../README.md#dead-letters)), with `Nats-Msg-Id` `dead-<stream_seq>`. Replaying or dropping it deletes
it from the stream, which no feed tells.

```json
{"ant": "dev-a", "stream_seq": 4211, "deliveries": 6, "error": "harness exited", "died_at": "2025-01-01T12:00:00Z",
 "envelope": {"id": "…", "thread_id": "…", "from": "alice", "to": "dev-a", "text": "…", "turn_budget": 8}}
```

`envelope` is the message itself, ready to replay, and absent when the payload was not an envelope: `payload` then
holds its first 4096 bytes. Check: `subject == "ants.dead." + ant`.

### `StatusEvent` on `ants.status.<ant>.<thread>`

Live progress of an ant on a thread, from its daemon: `ant`, `thread_id`, `at`, and the fields of one update, named by
`type`:

| `type` | Fields | Meaning |
|---|---|---|
| `state` | `state`: `working`, `waiting_approval`, `idle`, `failed`, `signed_out` | The thread's state changed |
| `message` | `text` | More of the reply, as streamed |
| `thought` | `text` | More of the reasoning, where the harness shares it |
| `tool` | `id`, `title`?, `tool_kind`?, `status`? (`pending`, `in_progress`, `completed`, `failed`) | A tool call started or changed |
| `plan` | `steps`: `[{"content", "status"}]` | The harness's whole plan |
| `queued` | `message` (id), `ahead` | A message waits behind `ahead` others |
| `approval` | `request_id` | A tool request waits for a decision |
| `escalated` | `to`, `escalation` | An escalation was sent (below) |

Check: `subject == "ants.status." + ant + "." + thread_id`.

#### Escalations

Each message carrying an escalation, once stored, is told as an `escalated` update on the status of the ant that sent
it, on the escalation message's thread: an escalation going up a chain (to each ant of it), reaching the thread's owner
or the default human, a reminder of one, a tool request sent up the chain, or an expired one. The text of the
message is not part of it.

```json
{"ant": "dev-a", "thread_id": "019a7d2c-…", "type": "escalated", "to": "lead",
 "escalation": {"stuck": "dev-a", "thread_id": "019a7d2c-…", "reason": "turn_budget", "level": 1, "leader": "lead",
                "visited": ["dev-a"]},
 "at": "2025-01-01T12:00:00Z"}
```

`escalation` is an `Escalation`: `reason` is `turn_budget`, `cap`, `notes`, `dead_letter`, `approval`, `asked` or
`context_window`; `request_id` names the tool request of an `approval` one; `level` is the chain level, 0 before it
went up any chain, and `leader` who leads at that level. Direct notices that emit no `StatusEvent` here: the
`dead_letter` notice `ants dead watch` sends with human credentials (observed via `ants.dead.<ant>`), the `harness`
notice of a harness failing to start (observed via `failing` in `ants_roster`), and the `storage` capacity warning.

### `RosterEntry` at `ants_roster` key `<ant>`

What a daemon says of an ant it serves: `ant`, `machine`, `instance`?, `harness`, `agent`? (`{name, version}`),
`capabilities`, `whoami`?, `workdir`, `started_at`, `heartbeat_at`, `activity`? (`idle`, `working`), `handling`?,
`harnesses`?, `escalate_to`, `version`?, `perms`?, `signed_out`?, `limited`?, `failing`?, `memory_low`?
(`{since, available, max_harnesses}`). The daemon puts it on start and every 10 s, and deletes it on a clean stop; the
bucket's entries expire after 30 s, so a daemon that died leaves none. `ants hook run --on roster` hands on puts only, every heartbeat included. Check: `key == ant`.

## `ants hook run`

`ants hook run --on EVENT [--concurrency N] [--timeout DUR] -- CMD [ARGS...]` runs `CMD` once per event of the feed,
with the event's document and a newline on its stdin, until stopped. Its stdout and stderr go to those of
`ants hook run`.

- **Stored feeds** (`approval-request`, `approval-decision`, `dead-letter`) read through the observer's consumer of
  the feed, from where it left off: on the first run, from the next event stored. Each event is acked once `CMD` ends,
  whatever its exit status, so a failing command does not stall the feed; an event whose `CMD` was still running when
  `ants hook run` stopped is handed on again. The consumer is removed after 7 days without a reader.
- **Live feeds** (`escalation`, `status`, `roster`) take events from now on.
- At most `--concurrency` (default 4) runs at once; a run lasting past `--timeout` (default `60s`) is killed. Failed
  runs, timeouts and documents that are forged or unreadable (skipped) are logged.

## Versioning

The subjects, consumer names, documents and their fields above stay as they are within a major version of ants.
Changes that may come in any release, and which hooks must accept:

- new fields in any document, and new values in enumerations (`type` of a status update, `reason`, `state`,
  `activity`, `tool.kind`): ignore what is not known;
- new feeds, subjects and observer rights;
- fields documented as optional (`?`, or absent from older daemons) being absent.

Removing or renaming a subject, a consumer name or a field, or changing what a field means, is a breaking change,
called out in the release notes; one changing what observer credentials allow also comes with a new permissions
version (see [Credentials across upgrades](auth.md#credentials-across-upgrades)).

Snapshot tests (`crates/ants-proto/tests/wire_snapshots.rs`) pin the JSON form of these documents and of every other
wire type, and check that documents in the shape older daemons wrote still parse.
