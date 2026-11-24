# Trails

A trail is a Python script that coordinates several ants and humans over time. A conductor, the ant a run is started
on, has its daemon run the script; each `agent()`, `human()` or `trail()` call of the script is a step (a message the
conductor sends on a fresh thread, or a child run of a saved trail), and the run waits for its answer without a
process, surviving restarts. The script's code, not a model, decides which ant takes each step, how many steps run at
once and how their results merge. A run may go on in another trail at one of its waypoints (`goto()`), so small
saved trails compose into routes at run time.

This document says what a trail is, how runs start, route, report, stop and resume, what they may spend and trust, the
tips their steps share, and where they are kept. How to write one is in the authoring guide,
[contrib/skills/ants-trails/SKILL.md](../contrib/skills/ants-trails/SKILL.md); how each part works is in
[architecture.md](architecture.md#trail-workers) (workers) and [architecture.md](architecture.md#trails) (the
daemon's part), and every command and setting in [cli.md](cli.md#commands) and [config.md](config.md#trails).

## Trails, skills and sense

- **A skill** tells one ant how to do something: instructions the ant follows turn by turn, its intermediate results in
  one context window.
- **Sense** is an ant's memory ([sense.md](sense.md)): its identity, memories and work, learned by the ant itself. No
  trail's script, tip or rating enters it; a memory may name a saved trail to take, and stays judged by its own votes.
- **A trail** is a script a conductor's daemon runs, coordinating several ants and humans over time: its plan lives in
  code, its intermediate results in script variables, each step in a clean context. Trails are colony-wide, written
  by humans or ants, and handed to ants: a trail itself is never learned.

Write a trail when work splits into many steps that each deserve a fresh context, when findings need a verifier other
than their author, when discovery must repeat until it runs dry, or when a human must approve a step mid-run. For a
task one ant finishes on one thread, send a message instead.

## The script

A trail is a `.py` file in [Monty](https://github.com/pydantic/monty)'s subset of Python whose first statement is a
literal `meta`, read without running the script:

```python
meta = {"name": "review", "description": "Reviews each changed file", "phases": ["review", "report"]}

phase("review")
reviews = await parallel(*[agent(f"Review {path} in {args['repo']}.", capability="rust") for path in args["files"]])
phase("report")
{"reviews": reviews}
```

Args:

```json
{"repo": "/src/app", "files": ["src/main.rs", "src/lib.rs"]}
```

The script runs top to bottom with `await` allowed at the top level; the value of its last expression, which must be
JSON, is the run's result (a trail with [waypoints](#waypoints) returns it from one). It reaches the host only through
these names:

| Name | What it does |
|---|---|
| `await agent(prompt, ant=None, capability=None, team=None, schema=None, label=None, tips=None)` | One step: `prompt` to the ant `ant` names, to an ant other than the conductor advertising `capability` (picked as `route` picks one), to the lead of the team `team` names (alone, without `ant` or `capability`; the step carries the team, as a message to a team does, and a team with no lead, nor any above it, no such team or a directory that cannot be read raises), or to the conductor itself. Returns the reply's text, or with `schema` (a JSON Schema, of which `type`, `properties`, `required`, `items`, `enum` and `additionalProperties: false` are checked, any other keyword passing unchecked) its JSON, asked for again at most twice on a mismatch. `tips` is the step's tip key ([Tips](#tips)). |
| `await human(questions)` | One step: asks the run's owner, else the default human, 1 to 4 questions as `ask_human` does; returns the answer's text. |
| `await trail(name, args=None)` | One step, answered by a child run of the saved trail `name` with `args` as its input; returns its result ([Child runs](#child-runs-trail)). |
| `await goto(trail=None, at=None, state=None)` | Ends the leg where called and goes on in `trail` (this one without it) at waypoint `at` (its start without it) with `state` as `args`; returns only by raising ([Routes](#routes-goto)). |
| `phase(title)`, `log(text)`, `print()` | Notes on the run's thread; never wait. The record keeps a phase's first 1000 characters. |
| `args` | The leg's JSON input (`None` without): the run's input on its first leg, the goto's `state` on later ones. |
| `await parallel(*coroutines)` | All results in argument order once every coroutine has finished (`asyncio.gather`). |
| `await pipeline(items, *stages)` | Each item through the async stages on its own pace; the last stage's results in item order. |

A step that fails raises `RuntimeError` where it is awaited: an `ant=` naming a human or no valid ant name, a
`capability` no ant but the conductor advertises, a `tips` key that is not one line of 1–120 characters, malformed
`human()` questions, a reply still off its schema after
two re-asks, a step's message withdrawn from its recipient's queue (`<ant> never answered`), an unknown saved trail,
`args` over 64 KiB, an ancestor-trail cycle or a failed child run in `trail()`, or the run's step limit
reached. A step to an ant no daemon serves waits in that ant's queue, as any message does, until the run ends or a
human stops it. An exception the script does not catch fails the run with its traceback, lines numbered as in the
file (`trail.py`).

Each JSON value a run takes or makes nests at most 100 lists and dicts (arrays and objects): a run's args deeper are
refused at its start, a call's arguments deeper (a goto's `state` among them, one level down) raise `TypeError` where
the call is made, a deeper result fails the run, a reply to a `schema=` step nested deeper does not match and is asked
for again, and any other answer nested deeper raises `RuntimeError` where it is awaited.

The script runs sandboxed: no files, environment, network, subprocesses or sleep; the clock reads the leg's start time
on every call and `random` is seeded once per leg, so the same answers make the same calls. The authoring guide
covers the API, the sandbox, the bundled modules, patterns and complete example trails, each run by the repository's
tests (`crates/ants/tests/trail_guide.rs`); the `run_trail` tool's description carries a short version of it.

A saved trail can also be a watcher's check ([sense.md](sense.md#watchers)): the ant's daemon runs it on an interval in
a one-shot worker, outside any run, for at most the watcher's `limit_seconds`. Such a script makes no step and writes
no note: `agent()`, `human()`, `trail()`, `goto()`, `phase()` and `log()` raise `RuntimeError`. It reads instead
through `command(cmd)`, `read_file(path)`, `bd(*args)` and `thread(id=None)`, which raise in any other run, and returns
`{"output": str, "done": bool}`.

## Waypoints

A trail's meta may list up to 32 `waypoints`, the places a run enters it: each names a top-level
`async def <name>(args)` of the script, which takes the leg's input and returns the leg's result. A run of the trail
starts at its first waypoint, a [`goto()`](#routes-goto) enters it at any. The top level of such a trail holds only its
meta, imports, definitions and constants, its steps in its waypoints: a top-level `await`, `if`, loop or expression
fails the check, as does an `await` in a def's decorators, defaults or annotations or in a class's decorators, bases,
keywords or body, all computed at the top level.

```python
meta = {"name": "triage", "description": "Triages an issue", "phases": [], "waypoints": ["classify", "report"]}

async def classify(args):
    kind = await agent(f"Is issue {args['issue']} a bug or a question? Answer bug or question.")
    if kind.strip() == "bug":
        await goto("fix", state={"issue": args["issue"]})
    return await report({"issue": args["issue"], "outcome": "answered"})

async def report(args):
    return await agent(f"Comment on issue {args['issue']}: {args['outcome']}.")
```

Args:

```json
{"issue": 413}
```

## Saved trails

`ants trail save FILE` checks a file, without running it, and saves it for the whole colony in `ants_trails` under its
meta's `name` or `--name`: 1–64 of `a-z`, `0-9` and `-`, not starting with `-`, not a UUID (meta's own `name` may
also hold capitals and `_`, needing `--name` then). A saved trail's script holds at most 256 KiB, its description one
line of at most 1000 characters, at most 32 phases of one line of 1–120 characters each (no control characters or characters
reordering text in a description or phase), and at most 32 waypoints. Every ant and human reads the saved trails.

`ants trail` prints the text scripts, ants and savers wrote (descriptions, phases, labels, prompts, answers, results
and errors) on one line each, a result or error of several lines indented under its own, with control characters and
those reordering text left out; `--json` gives them as written.

Humans save and remove any trail. An ant saves one with `save_trail` ([Starting a run](#starting-a-run)), from a script
or from a finished run of an inline script it conducted, under the same checks, and under a name neither the colony nor
the conductor machine's `~/.ants/trails` has a trail of. It replaces only a trail it saved, and only with `replace`, and
removes only one it saved, with `remove_trail`; it does neither from an untrusted thread ([Trust](#trust)). Ant
credentials write no saved trail: with authentication, `ants signer` saves and removes them for ants, checking each is
the ant's own and that the run it is saved from is a finished run of that script in the ant's own run records
([auth.md](auth.md)), so both tools need it running. A saved trail records who saved it, a human or an ant, and the run
an ant saved it from; a human saving under a taken name replaces the trail, an ant's included, and
becomes its saver.

Trails also come from dreams ([sense.md](sense.md#dreams)): a sage's finding of kind `trail` tells of a multi-step
approach the ant took again and again across its threads, cites those threads, and outlines a saved trail taking its
specifics as args (name, description, args with an example each, phases), without a script. The ant applies it on the
dream thread by writing the script, trying it with `run_trail` where a run is safe, saving it with `save_trail` naming
the finding in `finding`, and naming the trail in the `trails` of the finding's `sense_dream` verdict. A `trail`
finding marked untrusted, resting on text from outside, is neither run nor saved: the ant's prompt says so and tells how
to apply a trail finding only when a trusted one is among the findings, and the ant answers it rejected or deferred.
Such a save counts as one of the dream's 20 writes; it is refused when the finding rests on untrusted text, when the
dream covers untrusted text and the finding is no `trail` finding ([Trust](#trust)), or when a saved trail other than
the one saved already serves the task the finding suggests (`find_trails` would show it with more ratings worked than
failed): the finding is then rejected naming that trail. To spare such findings, the
sage's packet lists the colony's saved trails, their descriptions and ratings, and the sage's track record counts an
applied `trail` finding as holding only while its trail stays saved and is rated failed no more often than worked.

A conductor's daemon also finds trails in `~/.ants/trails/<name>.py` on its machine, and the CLI lists and runs the
`.py` files of the CLI machine's `~/.ants/trails` by the same name, the file's, whatever its meta's `name`. On a name
both have, the colony's trail runs, so every conductor runs the same trail by that name. When the colony's trails
cannot be read, a conductor runs no trail by name and names the error; a bus that keeps no saved trails leaves the
machine's trails running.

```sh
ants trail save review.py               # check and save for the colony; replaces a trail of that name
ants trail list                         # the colony's trails, then this machine's, with their ratings
ants trail show review                  # meta, source, latest ratings and script
ants trail rm review                    # the colony's only, with its tips and ratings: a local file stays until deleted
```

In `ants chat`, `/trail list` and `/trail show NAME` show what `ants trail list` and `ants trail show` print, in the
pager.

## Starting a run

A run is started three ways; each names a conductor, an owner and a thread to report on:

- **`ants trail run`** (human credentials): a saved trail by name, or a `.py` file checked and sent inline (a local
  trail is sent inline too, as the conductor may run on another machine). `--args` is the script's JSON `args` (at most
  64 KiB). The conductor defaults to where `ants ask` without `@ant` goes, the human's assistant, else the messenger;
  `--ant` names another. The run reports on `--thread` (an id, a unique start of one, or `last`) or a new thread,
  and is owned by the human. The command prints the trail, conductor and thread and what the run may spend, then asks
  `Start the run? [y/N]`; without a terminal on stdin and stderr `--yes` is required. `--follow` then follows the run
  until it ends, exiting 0 once it is done and non-zero once it has failed or stopped, or once it is past its wall time
  with its record unchanged for 2 minutes, as no daemon conducts it any more; records that stay unreadable for a minute
  after the last read that held, whether reads fail or get no answer, end the following with exit 3. `--json` prints
  `{"run","conductor","thread"}`; a start declined with `--json` prints nothing on stdout and exits non-zero.

  ```sh
  ants trail run review --args '{"repo": "/src/app", "files": ["src/main.rs"]}' --steps 100 --follow
  ants trail run ./draft.py --ant dev-a --thread last --yes
  ```

  In `ants chat`, `/trail run NAME [@ANT] [--steps N] [--at-once N] [--turns N] [--wall DUR] [--thread THREAD]
  [ARGS]` does the same for a saved trail, `--thread` also taking `here` for the thread open or selected: it shows the
  same summary over the view, and `y` starts the run ([cli.md](cli.md#ants-chat-command-bar)).

- **`run_trail`**, an MCP tool of ants ([architecture.md](architecture.md#mcp-tools)): the calling ant conducts a run of
  a saved trail (`name`) or of a script of its own (`script`), with `args`, on the current thread, owned by the thread's
  owner, under `limits` (`{"steps", "at_once", "turns", "wall"}`, as [Limits](#limits) names them; each left out takes
  its default). It returns the run's id at once, or with `wait: true` the run's result or error, waiting up to `timeout`
  (default `5m`, at most `30m`), else the run's id while the run goes on. A run's end invites the ant to rate it: the
  waited answer carries the `rate_trail` line, else the end wakes the ant with a message of its own ([The run
  thread](#the-run-thread), [Rating and finding trails](#rating-and-finding-trails)). For a `script` whose meta
  description matches saved trails rated worked more often than failed, the answer adds `Saved trails with good records
  for such tasks: NAME (N worked, N partly, N failed), …; run_trail(name: …) runs one.`, naming up to three.
  `trail_status` tells the ant how a run it conducts stands (an ended one's with the `rate_trail` line), or lists those
  not ended, and `stop_trail` stops one ([Restarts, stops and resumes](#restarts-stops-and-resumes)). The tools are
  offered only while the bus keeps trail runs.

  A run an ant starts with `run_trail` while answering a step spends from the step's run, within the step's grant
  ([Limits](#limits)): the step carries the grant (steps, turns, the end of its run's wall time) and the saved trails
  on its run's route and its ancestors'. The answering ant's daemon cuts each run started answering the step to what
  the grant leaves after the runs it already started answering it (the limits of those still running, the spending of
  those ended or stopped), refuses one with nothing left (`the step this thread answers has no steps left for a run
  started answering it: …`) and refuses a trail on that route (`trail NAME is on the route of run RUN, whose step this
  thread answers (ROUTE): …`). It tells the step's conductor what those runs spent in all, and how many still run,
  with each reply to the step and with a `run_spent` notice on the step's thread as each of them ends or stops, sent
  again until stored, a restart included; the conductor charges the part not charged before to the step's run and its ancestors, as it charges a child run's, so
  a message delivered twice charges once. Which runs answer a step is kept in the answering ant's records
  (`<ant>.answering.<thread>`), so a restart of its daemon tells all they spent. A human's resume of such a run stays
  within what the grant leaves after the other runs answering the step, and is refused when that leaves it nothing
  beyond what it spent. `run_trail`'s answer names the run it spends from, and so do `trail_status` and `ants trail
  show` (`started answering call N of run RUN of ANT, spending from that run's grant of S steps and T turns`).
  Neither `trail()` nor a run started answering a step takes a trail on the routes it spends from.

  Once an inline script works and its steps will recur, **`save_trail(name, script?, run?, replace?, finding?)`** saves
  it for the colony as the ant's: a `script`, or the script of a finished `run` of an inline script the ant conducted
  (done or failed), recorded as the trail's source. A run of a saved trail is refused, as that trail is saved already.
  A save rates nothing: a `rate_trail` of the source run made after it rates the saved trail too, as the answer says.
  The script is checked as `ants trail save` checks a file, not run, and every ant then runs it by name. A name a human
  or another ant saved is refused, so a changed copy goes under a new name; the ant's own is replaced only with
  `replace: true`. On a dream's thread, `finding` names the dream finding the save answers
  ([Saved trails](#saved-trails)). **`remove_trail(name)`** removes a trail the ant saved, refusing one a human or another ant saved
  and a name only `~/.ants/trails` holds. It leaves the trail's [tips](#tips): a trail saved later under that name
  inherits them, ranked as of an older script. Runs already started keep the script they started with.
- **A schedule** with `trail = "NAME"` in place of `text` ([config.md](config.md#schedules)): each firing sends `ant`,
  the conductor, a message `Scheduled run of trail NAME.` carrying the trail and `args` (a TOML table); its daemon
  starts the saved trail on the firing's new thread instead of prompting the ant, owned by the schedule's `owner`, under
  the message's id as run id, so a redelivered firing starts no second run. A run that cannot start, such as for an
  unknown trail, is told to the thread's owner there. The entry's `limits` table (`steps`, `at_once`, `turns`, `wall`)
  sets what each run may spend, the firing carrying them; a limit left out takes its default, and one of 0 or over its
  maximum fails config loading. Such a run is untrusted ([Trust](#trust)).

  ```toml
  [[schedule]]
  name = "weekly-review"
  creds = "~/.config/ants/auth/weekly-review.creds"
  cron = "@weekly"
  ant = "lead"
  owner = "alice"
  trail = "review"
  args = { repo = "/src/app", files = ["src/main.rs"] }
  limits = { steps = 100, wall = "2d" }
  ```

The conductor refuses limits over the maxima, a script over 256 KiB and args over 64 KiB; a run's id is a UUID.

## Routes: goto()

`await goto(trail=None, at=None, state=None)` ends the run's leg where it is called and starts the next: in the trail
`trail` names, else the leg's own, at its waypoint `at`, else at its start (its first waypoint), with `state` (JSON, at
most 64 KiB) as `args`. The leg left is given up as a stop gives up a run's steps: its session is dropped, the steps it
waits on and queued (those of a `parallel()` beside the goto too) are dropped, and its child runs stopped. The run keeps
its id, conductor, owner, origin, thread, limits and spending, and its new leg starts with no phase. A goto is no step:
it sends no message and spends nothing. The legs a run went through are its route; the result of the leg that ends is
the run's.

A named trail is read as saved when the goto is taken, the colony's else the conductor's `~/.ants/trails`; without
`trail`, the leg's own script goes on as the leg started it. The run's record pins each leg's script, so saving a trail
anew mid-run changes no leg started.

The conductor refuses a goto it cannot follow: `goto()` raises `RuntimeError` where it is awaited, naming why, and the
script may catch it and take another exit. It refuses a trail not saved or a waypoint the trail does not list, a
`state` over 64 KiB, a leg the run's record (1 MiB) has no room for, an inline script for an untrusted run
([Trust](#trust)), and a trail and waypoint the run entered before:

```text
RuntimeError: the run entered trail fix at waypoint patch before; a run enters a trail at each waypoint only once
```

A run enters each waypoint of each trail at most once, so every route ends: a loop, such as fix and review rounds,
belongs in one waypoint, bounded by its code. Of several gotos waiting at once, the first called that the conductor
takes ends the leg; those refused before it raise.

The route shows on the run thread as `triage@classify -> fix@reproduce -> fix@patch -> triage@report`: each leg's trail
(`inline` for an inline script), with `@<waypoint>` when entered at a waypoint ([The run thread](#the-run-thread)).

## Child runs: trail()

`await trail(name, args=None)` is a step answered by a child run of the saved trail `name` (the colony's, else the
conductor's `~/.ants/trails`; never an inline script), started at its start with `args` as its input. The conductor
runs it as the parent's, with the parent's owner and origin, on a spin-off of the parent's thread, the step's thread,
labelled `trail <name>`. Its steps and turns count against the limits of the root run, the run no other run started,
and it ends within the wall time its parent has left. The result of its route answers the call; a child that fails
raises `RuntimeError: trail <name> failed: <error>` there, which the script may catch. A trail on the route of a calling
run (the caller, its parent, and so on up to the root), whichever of its legs entered it, is refused: no trail calls
itself, not even through a goto(). A call that starts no child run, refused so or naming a trail not saved or
one that does not compile, raises `RuntimeError` naming why and counts as no step.

Stopping or ending a run stops its children, and resuming it resumes them: those stopped, and those that failed on a
limit, whose errors then no longer answer their calls, so a run failed on its child's limit resumes with that child,
as the run would on that limit of its own. A call whose child run left no record starts a child anew, from its start, its error no longer
answering the call either. A child tells its start and end in the notes of its own thread, messaging no one; its end
note points to resuming its parent.

## The run thread

A run reports on the thread it started with, as its conductor:

- **Notes:** the start posts `Run <run> of trail <name> started: <description>` to the thread's notes, suggesting the
  title `trail <name>: <description>`, `<name>` the name the trail is saved under (`(meta name <meta>)` follows it
  when its meta's `name` differs); a run of an inline script posts
  `Run <run> of an inline script (meta name <meta>) started: <description>`, suggesting `inline <meta>: <description>`;
  a resume posts `Run <run> resumed by <human>.`; each goto taken posts
  `route: <route>`, the legs so far, such as `route: triage@classify -> fix@reproduce`. The `phase()`, `log()` and
  `print()` output of each resume of the script becomes one note of `phase: …` and `log: …` lines (at most 4000
  characters). Notices and escalations arriving on a step's thread, and replies the run no longer waits on, are
  quoted in notes too. Notes wake nobody and spend no turn.
- **Step threads:** each step's thread is a spin-off of the run thread, keeps the run's owner and carries a
  `trail_step`; its recipient answers it as any message, and the first reply answers the call. A human may write on a
  step's thread too: a message steering the prompt answering the step takes the answer, which goes to the human, and the
  recipient's daemon tells the conductor with a `steered` notice on the step's thread, so the call raises `RuntimeError:
  <ant> never answered: <human> steered the prompt answering the step with message <id>, which takes the answer.` in the
  script. A step its recipient's daemon gives up on after its deliveries is dead-lettered as any message, and the
  conductor is told too, with a `gave_up` notice on the step's thread: the call raises `RuntimeError: <ant> never
  answered: Gave up on the message from <conductor> after 5 deliveries: <error>…`, and the step replayed later (`ants
  dead replay`) is answered with a reply the run no longer waits on, noted on the run thread. A step's ant may ask the
  run's owner with `ask_human`: the turn that asked sends the step no reply, its daemon telling the conductor with an
  `asked` notice (noted on the run thread), the step waits on the human, shown in `ants trail show` as waiting on a
  human it asked, and the run waits on humans once all its calls do; the ant's answer once the human, or the human's
  assistant, answered goes to the conductor as the step's reply and answers the call. The run's wall time bounds the
  wait. The invitation to rate a run started on the thread of a step to the conductor itself, which names the run in
  `rating_of`, answers no call: it prompts the conductor to rate the run.
- **Status events:** a `step` event (`run`, `call`, `label`, `to`, `step_thread`, `stage`: `started`, `answered`,
  `failed` or `dropped`) on the conductor's status of the run thread tells each step, and a `leg` event (`run`, `leg`
  counted from 0, `trail` unless inline, `waypoint` when entered at one) each leg's start, the first at the run's start
  ([hooks.md](hooks.md#statusevent-on-antsstatusantthread)).
- **In `ants chat`:** the thread view of the run thread lists each step where it started, with its label, the ant it
  went to, its stage and its thread, which `Enter` on the step opens ([cli.md](cli.md#ants-chat)). Status events are
  never stored, so the view also reads the steps from the records of the runs that the ants on the thread conduct
  when the thread shows: the markers each conductor keeps under the thread (`<ant>.on.<thread>.*`, read by a filtered
  direct get, so a thread's steps are read alone; each marker's time is its step's start, and it carries the step's
  label; a `trail()` step's marker sits there too), then for each run found its
  journal (`answered`, or `failed` for an error) and, while it goes on, the calls its session waits on (`started`); a
  step otherwise is `dropped`, as is one sent anew on a new thread after a resume. A run's thread opened after its
  steps ran lists them all the same, for as long as the run's records are kept.
- **End message:** once the run is done, failed or stopped, the conductor messages the owner, else the default human, on
  the run thread: `Run <run> of trail <name> is done (N steps, M turns):`, naming the trail the run started (`Run <run>
  of an inline script` for an inline one), with the result (cut at 8000 characters, `ants trail show <run>` showing the
  rest), the error (when a limit was met, followed by what a resume does: for steps, turns or wall time, `ants trail
  resume <run>` with the flag raising that limit, unless already at its maximum; for compute, that a resume runs the
  computation again under the same limit, failing again unless the machine was only busy; for the session or memory,
  that no resume continues it, or for a session size its start set below the maximum, that no flag raises it), or who
  stopped it and how to resume it; a run of more than one leg ends it with a line `route: <route>`. The end of a run
  done or failed is stored before it is told, a store that fails being tried again; the message is sent again until the
  outbox takes it, the run staying listed in `<ant>.runs` meanwhile, under an id derived from the run and the time it
  ended, so a daemon restarted in between sends it again as the same message, which the bus keeps once.
- **Rating invitation:** once the run is done or failed, child runs included, the conductor's daemon also messages the
  conductor itself on the run thread, answering to the run's owner: `[ants] Run <run> of trail <name> ended done (N
  steps, M turns). Result: …` (or `ended failed (…): <error>`), with the run's args, its route, the call a child run
  answers, the `rate_trail(run: …)` line ([Rating and finding trails](#rating-and-finding-trails)), for a failed run a
  `find_trails` line, and `After rating, end the turn with complete: nothing needs a reply.` It wakes the conductor
  like any message, spending a turn of its budget, and asks no reply, so rating starts no exchange. A `run_trail` call
  that waited and took the run's end carries the same line in its answer instead, and no message is sent; a stopped
  run, which may be resumed, gets none. The run stays listed in `<ant>.runs` until the message is sent, and its id is
  derived from the run and the time it ended, so a daemon restarted in between sends it again as the same message,
  which the bus keeps once. For a run on an untrusted thread the message is narrowed as the run's steps are, the
  default webhook rules letting `rate_trail` through; it names the run it invites a rating of, so its turn lets
  `rate_trail` through whatever rules narrow the thread, a webhook's own `allow` included ([Trust](#trust)).

`ants trail runs` lists the runs of every conductor, or `--ant CONDUCTOR`'s, each with the trail it started (its route
once it went on by `goto()`); `ants trail show RUN` prints a run (its id or a unique start of one): state, phase,
spending, args, its route (each leg's trail, waypoint, start and steps), its child runs with their states, the calls
waiting and queued (none once the run is done or failed), and its journal; `--follow` follows it as `ants trail run
--follow` does. These need human credentials. In `ants chat`, `/trail runs` shows the same list in the pager, and
`/trail show RUN` the run as `ants trail show RUN` prints it.

## Limits

A run's limits are set when it starts, by `ants trail run`'s flags (`/trail run`'s in `ants chat`), `run_trail`'s
`limits` or a schedule's `limits` table (the key of each, a duration such as `2d` for `wall`), and kept in its record;
they may be raised on resume:

| Limit | Default | Maximum | Flag, key | Past it |
|---|---|---|---|---|
| Steps, `agent()`, `human()` and `trail()` calls sent (child runs included; re-asks, and a step sent anew on resuming, not counted) | 50 | 500 | `--steps`, `steps` | the call raises `RuntimeError: the run reached its limit of N steps` |
| Steps outstanding at once | 4 | 16 | `--at-once`, `at_once` | later steps wait in the run; nothing raises |
| Turns the steps spend, as ant messages spend them | 200 | 2000 | `--turns`, `turns` | the run fails |
| Wall time from start, stopped time included | 24h | 7d | `--wall`, `wall` | the run fails, waiting or not |
| Compute of one resume of the script | 10 s | 10 s | none | `TimeoutError`, the run fails; the worker is killed 5 s later |
| Dumped session, the script's suspended state | 768 KiB | 768 KiB | none | the run fails, naming its size; so does a run whose session record, the dumped session with the calls it waits on, would pass 1 MiB less 8 KiB, before its steps go out |
| Result or error, in the run's record | 1 MiB less the rest of the record | same | none | a result fails the run, naming its size (a `trail()` call raises with that error); an error is cut to 8000 characters, then on a record near its room to what fits, its phase dropped last. The worker's own bounds (a 768 KiB result, a 16 KiB error) mostly apply first; this one catches a result within them that the rest of the record (its pinned scripts, args) leaves no room for |
| Memory of the script | 64 MiB | 64 MiB | none | `MemoryError`, the run fails |
| Notes of one resume: the text of `print()`, `log()` and `phase()`, and 16 bytes per note | 64 KiB | 64 KiB | none | the call raises `RuntimeError` naming the cap; a note's text over 4000 bytes is cut, ending with `…` |
| Arguments of the calls of one resume, as JSON, and 64 bytes per call | 1 MiB | 1 MiB | none | the call raises `RuntimeError` naming the cap |
| Result of the run, as JSON | 768 KiB | 768 KiB | none | the run fails, naming its size |
| Python call depth | 200 | 200 | none | `RecursionError` in the script |
| Lists and dicts nested in one JSON value | 100 | 100 | none | refused where the value is given ([The script](#the-script)) |

A resume is the script running from its start, or from one answered step, to its next wait; the caps of one resume
apply anew at the next. Calls answered at once, as `log()`, `phase()` and `os.urandom()`, do not restart the compute
limit: the script's computing before and after them counts toward the same resume. A `TimeoutError` or `MemoryError` the script raises itself fails the run as any other exception
does, on no limit, and an error over 16 KiB is cut.

A run failed on a limit keeps which in its record's `limit`: `steps` (a call past them raised and the script then
failed), `turns`, `wall`, `compute`, `session` or `memory`; a run failing as its child run failed on one keeps the
child's. `ants trail show RUN` names it (`state: failed on its turns limit: …`), and a failed run resumes only with one
other than `memory`, and with `session` only when the resume request's limits raise the run's session size (no flag
does; a run's session size is below its maximum only when its start request's limits set it so) ([Restarts, stops and
resumes](#restarts-stops-and-resumes)): a resume restores the same session and takes the same answers, so under the same
limits it would fail the same way. The compute limit is no flag's either, but a resume computes again, which fails
again unless the machine was only busy or the resume request's limits raise the run's compute (a run's compute is
below its maximum only when its start request's limits set it so): the restored session computes under the resume's
limits, not those it was dumped with.

Each `agent()` step reserves up to 8 of the run's turns as its message's turn budget; the turns its reply spent, and
one per re-ask, count against the run's turns, and a run that cannot reserve one fails on its turn limit. Each step
also counts against the spending caps of the ant answering it. A step reaching an ant at one of its spending caps, or
whose thread has no turns left, is not escalated: the ant's daemon acks it unprompted and tells the conductor with an
`unanswered` notice on the step's thread, and the call raises `RuntimeError: <ant> never answered: <cap>; <ant> does
not answer the step.` in the script, which may catch it and ask another ant; the run's thread notes the notice. A goto
spends nothing and changes no limit: the run's legs share its limits, spending and wall time. A child run's steps and
turns count against its root run's limits too, and its wall time ends with what its parent has left. Keep the
script's own work small: only the compute between steps counts against the compute limit, and every variable counts
against the session's size at each suspension.

Each `agent()` step also carries a grant: what the runs its recipient starts with `run_trail` while answering it may
spend together ([Starting a run](#starting-a-run)). The conductor takes it out of what the run's tree has left as
the step goes: steps and turns neither spent nor held by the tree's waiting steps, their turn budgets and grants, the
step's own turn budget included, so a run of 8 turns with one step out grants that step no turns. What is left is
shared equally among the steps going out together and the calls waiting for room, as many as the run may have out
at once: a lone step takes all of it, two steps sent by one `parallel()` half each. The grant is held, less what its
runs told they spent, while the run waits on the step and while those runs still run; it is given back once the step
is answered, or given up, and they have ended or stopped. What they spend is charged to the run and its ancestors as
it is told, even after the grant was given back, as for a run resumed after its end. A held grant is out of what
later grants take, not out of the tree's own steps and turn budgets: steps and child runs going on meanwhile may leave
less than the grant, and spending told past one of the run's limits fails the run, naming the limit.

## Trust

A run takes the trust of the thread it started on (its origin):

- **Trusted:** runs started by a human with `ants trail run`, and by an ant's `run_trail` on a thread a human or ant
  started. These run saved trails and inline scripts alike.
- **Untrusted:** runs on a thread a webhook or a schedule started, or one asking a review, which relays text from
  outside the bus. These run only saved trails: the colony's, which a human saved or an ant saved from a trusted
  thread (`save_trail` and `remove_trail` are refused on an untrusted thread, a step of an untrusted run included, and
  `save_trail` in answer to a dream finding marked untrusted, or on a dream's thread whose dream holds untrusted text
  without naming a trusted `trail` finding of it), and the files of the conductor machine's `~/.ants/trails`, which
  name no saver and pass no such check, so whoever writes there chooses what untrusted runs may run. An inline script is
  refused (`a run started from a webhook's thread
  runs only saved trails`), a goto to one too (`… goes only to saved trails`), and each step's thread is narrowed to
  the read-only tool rules webhook threads get by default ([config.md](config.md#tool-permissions)). A schedule
  starting a trail sets no `allow` of its own. Such a run is rated all the same: on its own thread, `rate_trail` rates
  that run and no other, and any trusted thread rates any run the ant conducted; every other untrusted thread is
  refused. The turn of the invitation to rate the run lets `rate_trail` through a webhook's own `allow` leaving it
  out, and rates that run alone, even with other runs reporting on the thread; the daemon honours an invitation only
  when it sent it itself, from the ant to itself on the run's thread under the id of the run's end. `find_trails` reads
  from any thread.

A run's legs and child runs keep its origin, so a goto or `trail()` never makes a run more trusted.

A firing sets its run's limits: the message starting the run carries the trail, `args` and limits, and the conductor
takes those limits up to the maxima ([Limits](#limits)), where a plain message of the same sender spends at most 8
turns. Whoever holds a schedule's or a webhook's credentials ([webhooks.md](webhooks.md#trail-runs)) may so start a run
of any saved trail, on any ant those credentials reach, with any args, under limits of their choosing: such
credentials are the operator's to guard. What the run costs stays bounded on each ant by its spending caps, which each
step counts against ([config.md](config.md#prompt-limits-and-spending-caps)).

Only humans start, stop and resume runs by request (`ants.trail.<ant>`, answered by the conductor's daemon while up),
admins stop and resume them by [admin order](auth.md#admin-orders) as well, save and remove any trail, and read every
run's records; an ant reads and writes only the runs it conducts, starts runs only with `run_trail`, reads and stops
those it conducts with `trail_status` and `stop_trail`, resumes none, and saves and removes only its own trails, with
`save_trail` and `remove_trail` ([auth.md](auth.md)). A `human()` step goes to the run's
owner, and a step's thread keeps that owner, so replies and escalations of the step's recipient reach the human who owns
the run.

## Tips

Each step starts on a fresh thread, often on whichever ant `capability=` picks, so what one step learns (a quirk of the
repository, a prompt easily misread, a check that needs a flag) would be learnt again on every run. A tip keeps it: a
note an ant answering a step of a saved trail leaves on that trail, for the ants answering its later steps, in any run.
Each step's message carries the best few, `trail_tips` searches the rest, and votes rank them. Tips belong to a trail
the colony saves and are shared colony-wide; an inline script has none, nor does a trail only the conductor's
`~/.ants/trails` holds, as the ants answering its steps on other machines know no trail of its name: its steps carry
no tips and name no trail to leave them on.

### Tip keys

`agent(..., tips=KEY)` gives a step a tip key, naming the tips it shares with every step under the same key, in any run
of the trail: a tip left answering the step goes under the key, and the step's message carries the key's tips first. A
key is one line of 1–120 characters. Give each kind of step a stable key, such as `tips="review"` for the review of
every file, rather than a label that changes from step to step (`label=f"review {path}"`), whose tips no later step
would share. A tip left answering a step without `tips=` is trail-wide.

```python
found = await agent(f"Review {path} on branch {branch}.", schema=FINDINGS, label=f"review {path}", tips="review")
```

### Leaving and voting

Ants handle tips with three MCP tools ([architecture.md](architecture.md#mcp-tools)):

- **`leave_tip(text, force?)`**, while answering a step: leaves a tip of up to 1000 characters on the step's trail,
  under the step's tip key, else trail-wide, with its writer, run and the SHA-256 of the script the step ran, and
  returns its id (8 characters, such as `k3f7q2ab`). It refuses a tip of the same words, in the same order, as a live
  tip of the same trail and tip key, naming it; one sharing at least 60% of its words with live tips of the same key,
  naming them and suggesting a vote instead, unless `force` is set; and any tip once the trail has 200 live tips. Only
  a step of a trail the colony still saves leaves tips (not of an inline script, nor of a trail only the conductor's
  machine has, nor of one `ants trail rm` or `remove_trail` removed while the run went on), and only from a trusted
  thread: not a thread a webhook or schedule started, nor a
  step of an untrusted run ([Trust](#trust)).
- **`vote_tip(tip, verdict, reason?, trail?, writer?)`**: votes a live tip `helpful`, `stale` (right once, no longer)
  or `wrong`, with an optional `reason` of up to 300 characters, on the trail of the step being answered or the trail
  `trail` names. A tip is its writer's and id's: when tips of different writers share the id, `writer` names the one
  meant, and without it the vote is refused naming the writers. Each ant counts once per tip: voting again replaces its
  earlier vote. No ant votes on its own tip, nor from an untrusted thread. A vote records the SHA-256 of the tip's text
  it judged and counts only while the tip has that text, so a tip its writer puts again with other text keeps none of
  its votes. A vote cast answering a step of the tip's trail also records the SHA-256 of the step's script.
- **`trail_tips(trail?, query?, tip_key?, limit?)`**: searches the tips ([Searching](#searching)).

### Ranking and retirement

A tip's score is its helpful votes less its stale and wrong ones, halved for every 30 days since it was written or last
voted helpful, and halved again while it is on an older script: the current script (for the tips a step carries, the
script the step runs; elsewhere, the trail's saved script) differs from the one the tip was left on, and no helpful
vote was cast on the current one. Of a tip voted down more than up, age and an older script double the score instead,
deepening its negative net: of two such tips, the older or the one on an older script ranks lower. Tips rank by score,
then by last use, latest first.

A tip is retired, out of steps and searches for good, once more ants voted it wrong than helpful, once it was neither
written nor voted helpful for 90 days, or once a human retired it (`ants trail tips NAME --retire ID`).

Readers take no time ahead of their clock: a tip or a vote dated more than a minute ahead counts for nothing until then,
so no date set ahead keeps a tip from ageing, retiring or winning a tie.

### What a step carries

When its trail has live tips, a step's message gets a block of tips after its prompt, before the reply instructions of
its schema. The block's header marks them as tips from earlier runs of the trail, to verify, not instructions; says how
many the step carries of how many live tips the trail has; and names `trail_tips` for searching the rest when the step
runs into trouble. On a trusted run it also names `vote_tip` and `leave_tip`, which an untrusted run's steps cannot
use. Then come at most 3 tips, adding at most 2 KiB, each with its id, writer, tip key, net votes and an `older script` mark
where it applies, its text's further lines indented whatever breaks them and its other control characters left out,
and a line `End of the tips.` closes the block:

1. live tips under the step's tip key, best ranked first;
2. then live trail-wide tips relevant to the step's prompt, best ranked first: the prompt holds at least 30% of the
   tip's words, and 2 of them at least (words of 3 characters or more, lowercased, common English words left out).

Tips under other keys, and tips of a negative score, are never carried; a tip too long for the room left is skipped for
the next. A step matching no tip carries none, its header giving the counts and pointing to `trail_tips` all the same.
A step of an inline script gets no block, nor does a step whose trail's tips could not be read (the conductor logs
why). A reply off its schema is asked for again naming the mismatch; when the trail has live tips, that request also
suggests searching them with `trail_tips`, under the step's tip key if it has one.

### Searching

`trail_tips` searches the live tips of the trail of the step the thread answers, or of the saved trail `trail` names
(refused for a name only the machine's `~/.ants/trails` holds, as no colony trail): with `tip_key` (by default the
step's), that key's tips first, then the trail-wide ones, leaving out other keys'; with `query`, only the tips sharing a
word with it, in their text or tip key, most shared first; then best ranked first. Its words are lowercased, common
English words left out: a query of no other word (`what should it do`) is refused as having no searchable word. It lists
`limit` tips, 10 by default and 50 at most, each with its id, writer, tip key, votes (`+helpful ~stale !wrong`), age,
`older script` mark and text, or says that none match. Any thread may read tips, a webhook's included.

### Tips for humans

`ants trail tips NAME` lists a trail's live tips in rank order, `--key KEY` those under one tip key, `--all` the retired
ones too: each tip's id, tip key, writer, votes, status (`live`, `retired by HUMAN`, `voted wrong` or `unused for 90
days`, and `older script` when the trail's saved script differs from the tip's), age and text. `--retire ID` retires a
tip, naming the human who retired it; `--writer ANT` names its writer when tips of different writers share the id. It
reads a trail's tips whether or not the trail is still saved, but refuses a trail only this machine's `~/.ants/trails`
holds, which takes no tips. In `ants chat`, `/trail tips NAME` shows the same list in the pager. `ants trail rm NAME`
removes the trail's tips with it, saying how many: the trail first, then its tips and ratings, so that none left or put
while it goes outlives the removal, and run again on a trail removed already, it purges what is left of it. `--retire`
and `ants trail rm` need human credentials ([cli.md](cli.md#commands)).

```sh
ants trail tips review                  # live tips, best first
ants trail tips review --key review --all
ants trail tips review --retire k3f7q2ab
```

Each ant writes only its own tips and votes, under keys naming it; a human retires a tip under a retirement key no ant
writes, so a retired tip stays retired whatever its writer puts, and votes count only for the text they judged, so a
writer that rewrites its tip loses them; a human purges a trail's tips when removing the trail
([auth.md](auth.md#wire-acls-and-verification-invariants)). An ant's `remove_trail` purges none.

## Rating and finding trails

Tips tell how to walk a trail; ratings tell which trail to take. Each conductor rates the runs it conducted, and ants
choosing a trail for a task find the saved trails whose runs served such tasks.

**`rate_trail(run, outcome, task, why)`** rates a run the ant conducted once it ended or was stopped: `outcome` is
`worked`, `partly` or `failed`; `task` the task the run served, in a line (at most 300 characters), as later searches
would name it; `why` a concrete reason (at most 500). It rates each saved trail the run took, its route's, with the
SHA-256 of the script each leg pinned, and each saved trail the ant saved from the run with `save_trail`, with that
trail's current script. A trail the run took that is no longer saved is not rated, the answer saying so, so a rating
given after `ants trail rm` brings back none it purged. A rating of an untrusted run, one a webhook or schedule
started ([Trust](#trust)), counts as any other and is marked as an untrusted run's. One rating per run and trail: rating the run again replaces it, the answer naming the earlier outcome. A child
run is rated by its own id. A run of an inline script rates no trail until its script is saved; when it worked, the
answer names a saved trail already serving such tasks, else, on a trusted thread, suggests `save_trail`. A `partly` or
`failed` rating's answer points to `find_trails` for the task.

**`find_trails(query, limit?)`** lists the saved trails (the colony's and the conductor machine's) for a task: those
whose name and description match `query`, and those rated on a task matching it. Matching takes the words of each text
as [tips' search](#searching) does (lowercase, three characters or more, common English words left out), each reduced
to a stem by a light English stemmer (a plural, `-ing` or `-ed`, then a suffix such as `-ation`, `-ment`, `-ness`, `-er`
or `-ly`, then a final `e`, each only when three letters or more remain), and a text matches when it shares two of the
query's stems, or all of them when the query has fewer; a query without such words (as `CI` or `do it`) matches none,
and `find_trails` refuses it. Each trail's ratings on matching tasks are counted, a `partly`
as half worked and half failed, a rating older than 90 days or of a script other than the trail's current one weighing
half, and the trails are ranked by a draw from each one's Beta(1 + worked, 1 + failed): proven trails mostly win, and
those with few ratings get tried. The draws are seeded by the thread and the query, so a search repeated on a thread
ranks alike. It lists `limit` trails (5 by default, at most 20), each with its description, saver, the share that
worked over its matching ratings with the counts, how many of them are untrusted runs', or `worth exploring` without
any, and its 3 latest matching ratings with their outcome, `an untrusted run's` where it applies, age, task and reason;
a rating dated more than a minute ahead counts for nothing, here and in every listing, until then; the description, task and reason show quoted, their line breaks and control
characters escaped, so each stays on its line. On a trusted thread it ends noting that a script of the ant's own runs
with `run_trail(script: …)` and `save_trail` saves it once it works.

`ants trail list` adds the columns `RATINGS` (worked/partly/failed) and `LATEST` (the latest reason) when any trail has
ratings, `ants trail show NAME` the counts and the 3 latest ratings, each with its run, conductor, age, task and
reason, `ants trail runs` a `RATED` column (each trail's outcome) and `ants trail show RUN` the run's ratings; `--json`
carries them under `ratings`, and every listing shows a rating's task and reason on one line, as tips' text, control
characters left out. In `ants chat`, `/trail list`, `/trail show` and `/trail runs` show the same.
`ants trail rm NAME` purges the trail's ratings with its tips; an ant's `remove_trail` purges none, so a trail saved
later under that name inherits them. `find_trails` weighs them half, as of another script; a sage's review packet,
and the track record judging whether a sage's applied trail finding holds, count every rating under the name whole,
those of an earlier script, of a trail of the name removed before and older than 90 days included.

Each ant writes only its own ratings, under keys naming it; humans read them all and purge a trail's when removing the
trail ([auth.md](auth.md#wire-acls-and-verification-invariants)).

## Storage

| Store | Kept | Holds |
|---|---|---|
| KV `ants_trails` | never expires | Saved trails, keyed by name: name, description, phases, waypoints, script, who saved it (a human or an ant) and when, and the run an ant saved it from. |
| KV `ants_trail_runs` | 37 days after the last write | Each conductor's runs, keyed under the conductor: `<ant>.<run>` the run's header (its route: each leg's trail or inline script, waypoint, args, first call id, start and steps; owner, origin, limits, state (a failed one's broken `limit`), spending, the grants its steps hold or whose runs told spending, thread, parent run, the step it spends from and its grant, times), `<ant>.<run>.session` the current leg's dumped session with the calls waiting and queued, `<ant>.<run>.call.<n>` one journal entry per answered call (`{call, function, args, answer}`, call ids numbered on across legs), `<ant>.runs` the conductor's unfinished runs, `<ant>.steps.<thread>` a marker per step's thread (`{run, call, to, run_thread, label?}`), and `<ant>.on.<thread>.<step thread>` the same marker under the thread of the step's run, put with it before the step goes out; a `trail()` step, whose thread is its child run's, keeps the latter alone. An ant answering a step keeps `<ant>.answering.<thread>`, the runs it started answering the step on that thread. |
| KV `ants_trail_tips` | never expires | The [tips](#tips) left on saved trails and their votes, each under the trail and the ant that wrote it: `<trail>.<writer>.<id>` a tip (trail, id, writer, text, tip key, run, script's SHA-256, time written), `<trail>.<voter>.<writer>.<id>` a vote on it (voter, verdict, reason, the SHA-256 of the tip's text it judged, the SHA-256 of the script voted on, time), `<trail>._retired.<writer>.<id>` a human's retirement of it (the human, time), a key no ant writes. |
| KV `ants_trail_ratings` | never expires | The [ratings](#rating-and-finding-trails) conductors gave saved trails, `<trail>.<conductor>.<run>` one per run and trail: trail, conductor, run, outcome, task, reason, the SHA-256 of the script rated, time. |

`ants init` creates the four buckets, and `ants doctor` checks them. Each bucket keeps every message of a key, not
only the last, so a message a stream relayed onto a key ([auth.md](auth.md#wire-acls-and-verification-invariants))
hides no entry: readers skip it and take the last message the key's writer put. A run's states are `running`, `waiting` (on human
steps only), `stopped`, `done` and `failed`. Its records stay 37 days after their last write: 30 days of keeping plus
the longest wall time, so a finished run stays at least 30 days. The full layout is in
[architecture.md](architecture.md#stores-and-subjects).

## Restarts, stops and resumes

- **Restart:** an answer's journal entry is stored before its reply is acked, and a run's session and header before
  its steps go out. The run's header, holding what the reply charged, is stored after the entry and before the ack: a
  daemon stopped between the two loses the turns the reply spent, while what the reply told runs answering the step
  spent is charged when the reply is delivered again, a step's grant recording what was charged already. A restarted daemon continues every run its `<ant>.runs` lists: steps sent before wait on, steps
  never sent go out, answers stored past the session are given to the script. A goto stores the run's header with
  the new leg, gives up the steps of the leg left, then stores the new leg's session: a daemon restarted between the
  two gives up those steps, still in the stored session, before the new leg goes on; a stop before then gives them up
  with the current leg's, listing those in other ants' queues. A run whose records the daemon cannot read as it starts
  (the bucket out of reach, as on a leaf node whose uplink is not up yet, or a record that does not parse) is tried
  again while the daemon lives, after 2 s and up to 5 min apart, and at once on a reply to one of its steps or a stop
  of it; until then such a reply is not acked: it is delivered again, as a message whose handling failed is. An ended
  run whose end the daemon could not tell is tried again the same way.
- **Replay:** a session the worker cannot restore (dumped by another Monty version, or lost) is rebuilt from the
  journal: the current leg's pinned script starts again at its waypoint with the leg's input, clock and seed, its
  steps answered from the journal in order. A script whose steps no longer match the journal fails the run, naming
  the first call that differs.
- **Stop:** `ants trail stop RUN` stops a run and its running child runs: no further step goes out and the replies of
  the steps waited on answer
  nothing. The conductor withdraws the steps sent to itself; the command lists those still in other ants' queues, each
  with the `ants withdraw ANT MESSAGE` withdrawing it. The conductor stops a run it conducts the same way with
  `stop_trail`, the run then stopped by the conductor, and the tool lists the steps in other ants' queues for a human to
  withdraw. An admin's `ants ant stop-trail ANT RUN` has the conductor's daemon stop it by [admin
  order](auth.md#admin-orders): unlike the request, which only a daemon up answers, the order waits for the machine's
  daemon while it is away and is recorded with the admin and time (`ants ant orders ANT`), its result listing the steps
  in other ants' queues. A stop arriving while the run's script computes waits for the computation to end, and gives up
  the steps the run then waits on; the conductor's daemon tells it pending at once, and `ants trail stop` says so when
  the pass outlasts its 15 s wait. A stop takes effect once the run's record stores it: one that cannot be stored is
  refused with the storing's error, the run going on. Stopping a stopped run again lists the steps still to withdraw,
  those whose messages went out. Ending a parent run stops its still-running children. Wall time keeps running while
  stopped.
- **Resume:** `ants trail resume RUN` resumes a stopped run, or one whose record names the limit it failed on (its
  steps, turns, wall time or compute; one failed on its memory is refused, as is one failed on its session size
  unless the request raises it), from
  its last stored session, resuming its child runs stopped or failed on a limit too (a child that left no record starts
  anew); the steps waited on go out anew, on new threads,
  and so do the calls refused since that session, as one past the run's steps: a run failing on a limit deletes their
  errors from its journal. Limit flags replace the run's limits they name, within the
  maxima. The steps sent anew count no step again, a resume tried again after its records were stored in part
  included. A run done, failed otherwise, running or waiting is refused. A run's stops and resumes are taken one at a
  time, however asked: a second resume asked beside one is refused as the run is no longer stopped, and a stop asked
  while a resume is under way stops the resumed run. A run continued from its records, by a firing delivered again or
  after a restart, is continued one at a time with these too, so a resume beside it resumes it once. An admin's `ants ant resume-trail ANT RUN`
  resumes it by admin order the same way, limit flags included.

```sh
ants trail runs
ants trail stop 0198c4
ants trail resume 0198c4 --turns 400 --wall 2d --follow
```

`/trail stop RUN` and `/trail resume RUN [--steps N] [--at-once N] [--turns N] [--wall DUR]` in `ants chat` stop and
resume a run the same way, a stop that leaves steps in other ants' queues showing in the pager what `ants trail stop`
prints, and a resume's options replacing the run's limits they name as `ants trail resume`'s flags do.

## Workers

Each start or resume of a script runs in a fresh worker, the `ants` binary started as `ants subprocess` with an empty
environment, which computes until the script waits on its steps, ends or raises, and exits: an idle daemon runs no
worker. `[trails] workers` in the config file caps the workers computing at once across a daemon process's runs
(default 4, 1 to 16), bounding the memory and CPU trails take beside the harnesses ([config.md](config.md#trails)):

```toml
[trails]
workers = 4
```

A worker that cannot start, crashes, or whose exit status or output cannot be read is tried again, the run unchanged,
after 1 s and up to 1 min apart; a run whose workers crash 3 times in a row, as a stack overflow's abort brings again,
fails, its owner told. A worker past its compute or memory limit, one writing a response larger than its request
takes, or one that exits normally with a response the daemon cannot read, fails the run, as the same request would bring the same again.

`ants doctor` starts the worker of the daemon service's `ants`, else its own, as a daemon does, with a request it
refuses before running anything. With a human's credentials it lists the runs stuck: those stopped or failed on a limit
(steps, turns or wall time, as their records say) that `ants trail resume` may still continue, naming the limits to
raise; those whose steps went to ants not in the colony, naming `ants trail stop`; those running or waiting over
5 min past their wall time, which the conductor's daemon does not drive, naming `ants trail stop` or a restart of that
daemon; and those whose conductor is not in the colony ([cli.md](cli.md#identity-setup-completions-and-doctor)).
