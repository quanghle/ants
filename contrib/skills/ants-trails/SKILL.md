---
name: ants-trails
description: Write trails, Python scripts that orchestrate ants step by step (fan out over many items, verify findings with a second ant, loop until nothing new turns up, ask a human mid-run). Use when asked to write, review or fix a trail, or when work needs many fresh-context steps whose order, concurrency and merging a script should decide rather than one ant's judgment.
---

# Writing trails

A trail is a script in Monty's subset of Python whose code, not a model, decides which ant takes each step, how many
steps run at once and how their results merge. The conductor, the ant whose daemon runs the trail, sends each step as
a message on a fresh thread and stores the run after every step, so a run survives restarts and waits hours for its
steps without a process.

A trail is handed to ants, never learned. A skill is instructions one ant follows turn by turn, its intermediate
results in one context window; a trail moves the plan into code and keeps intermediate results in script variables, so
no item is skipped out of laziness, no ant verifies its own work and no goal drifts through lossy summaries. Sense is
an ant's own identity (its persona, skills and memories, learned by itself): no trail's script, tip or rating ever
enters it, though a memory may name a saved trail to take. Trails are
colony-wide, written by humans or ants.

Write a trail when work splits into many steps that each deserve a clean context, when findings need a verifier other
than their author, or when discovery must repeat until it runs dry. For a task one ant finishes on one thread, send a
message instead.

## The file

A trail is a `.py` file whose first statement is a literal `meta`: a `name` of letters, digits, `-` and `_`, a
`description`, the `phases` the script announces, and optional `waypoints` (up to 32 top-level `async def` entry points
taking one state argument, the first entered at start, with the top level then holding only `meta`, imports,
definitions and constants). The meta is read without running the script; a file without it
is refused. A trail is saved under a name of 1-64 lowercase letters, digits and `-`, not a run's id: its meta's `name`
when it is one, else the name `ants trail save --name` or `save_trail`'s `name` gives. The script runs top to bottom with `await` allowed at the top level, and
the value of its last expression, which must be JSON (None, bools, numbers, strings, lists, dicts with string keys),
is the run's result.

```python
meta = {
    "name": "summarize",
    "description": "Summarizes one file in three bullet points",
    "phases": [],
}

summary = await agent(
    f"Summarize {args['path']} in the repository at {args['repo']} in three bullet points. "
    "Reply with the bullet points only.",
    label=f"summarize {args['path']}",
)
{"path": args["path"], "summary": summary}
```

Args:

```json
{"repo": "/src/ants", "path": "crates/ants-trail/src/run.rs"}
```

A meta may also list `waypoints`, at most 32: the places a run enters the trail ([Routes](#routes)). Each names a
top-level `async def <name>(args)`, which takes the leg's input and returns the leg's result. A run starts such a
trail at its first waypoint, and a `goto()` enters it at any. Its top level then holds only the meta, imports,
definitions and constants, its steps in its waypoints: a top-level `await`, `if`, loop or expression is refused, as
is an `await` in a def's decorators, defaults or annotations or in a class's decorators, bases, keywords or body. A
waypoint is a function too, which another waypoint may await without leaving its leg.

```python
meta = {
    "name": "greet",
    "description": "Greets a new contributor, or welcomes one back",
    "phases": [],
    "waypoints": ["start", "welcome"],
}


async def start(args):
    if args.get("returning"):
        await goto(at="welcome", state=args)
    return await agent(f"Greet {args['who']}, a new contributor to {args['repo']}, in one sentence.", label="greet")


async def welcome(args):
    return await agent(f"Welcome {args['who']} back to {args['repo']} in one sentence.", label="welcome")
```

Args:

```json
{"repo": "/src/ants", "who": "ada", "returning": true}
```

## The API

Nothing else of the host is reachable: these names are defined before the script runs.

- `await agent(prompt, ant=None, capability=None, schema=None, label=None, tips=None)`: one step. The conductor sends
  `prompt` on a fresh thread: to the ant `ant=` names, to an ant advertising `capability=` (picked as the `route` MCP
  tool picks one), or with neither to the conductor itself. The step's thread keeps the run's owner and links to the
  run's thread. Returns the reply's text; with `schema=`, a JSON Schema as a dict, returns the reply parsed as JSON. A
  reply that does not match the schema is asked for again on the step's thread, naming the mismatch, at most twice;
  then the call raises. `label=` names the step in the run's thread and status events, kept as one line of at most 200
  characters, control characters left out. `tips=` is the step's tip key
  (one line of 1-120 characters): the step carries the best [tips](#tips) left under it on earlier runs, and a tip
  left answering it goes under it. A step's ant may ask the run's owner with the `ask_human` MCP tool: the step then
  waits on the human, and the ant's answer once the human answered is the reply.
- `await human(questions)`: asks the run's owner, as the `ask_human` MCP tool does. `questions` is a list of 1-4
  dicts, each with a `question` (1-500 characters) and 2-9 `options` (labels, or dicts with a `label` and a
  `description`), and optionally a `header`, `multi_select` (default False) and `free_text` (default True). Returns the
  answer as text: for one question, the label picked (labels joined by `, ` with `multi_select`) or the text typed; for
  several, one line per question, `title: answer`. Set `"free_text": False` to compare the answer with a label.
- `await trail(name, args=None)`: one step, answered by a child run of the saved trail `name`, started at its start
  with `args` as its input, on a thread of its own spun off the run's, for the run's owner; returns its result. Its
  steps and turns count against the run's limits. A trail on the route of a calling run, whichever of its legs entered it,
  is refused: no trail calls itself, directly, through others or through a goto(). A child run that fails raises
  `RuntimeError: trail <name> failed: …`.
- `await goto(trail=None, at=None, state=None)`: ends the run's leg here and goes on in the saved trail `trail` (the
  leg's own script without it), at its waypoint `at` (its start, its first waypoint, without it), with `state` as
  `args`. It never returns: a goto taken ends the leg, and one refused raises `RuntimeError` naming why
  ([Routes](#routes)).
- `phase(title)`: starts a phase, named as in meta's `phases`, shown as a note on the run's thread; the run's record
  keeps it as one line, control characters left out.
- `log(text)`: a note on the run's thread; `print()` writes notes too. Neither waits.
- `args`: the leg's input (None without): the JSON value the run was started with on its first leg, the goto's
  `state` on later ones.
- `await parallel(*coroutines)`: runs the coroutines at once and returns their results in argument order once every
  one has finished: a barrier.
- `await pipeline(items, *stages)`: passes each item through `stages`, async functions of one argument, each item
  moving on to its next stage as soon as its own step is answered, with no barrier between stages; returns the last
  stage's results in item order.

Only `agent()`, `human()`, `trail()` and `goto()` suspend a run, and a watcher's reads below. They are coroutine
functions: await them, or hand their coroutines to `parallel`. Their arguments must be JSON: anything else raises
TypeError.

### A watcher's reads

A saved trail can be a watcher's check (`sense_remind`'s `watch` trigger, `{"kind":"trail","name":...}`): the ant's
daemon runs it on the watcher's interval, for at most its `limit_seconds` (30 s by default), and wakes the ant when its
result's `output` changes. Such a run prompts no ant: `agent()`, `human()`, `trail()`, `goto()`, `phase()` and `log()`
raise `RuntimeError`. It reads instead, and only it may:

- `await command(cmd)`: runs `cmd` in the ant's workdir under the ant's tool allow rules, for at most 30 s; returns
  `{"status": exit status, "stdout": text}`.
- `await read_file(path)`: a workdir-relative file of at most 64 KiB as text; None when there is none.
- `await bd(*args)`: the JSON of `bd <args> --json` for a subcommand that only reads (`show`, `list`, `ready`,
  `blocked`, `search`, `count`, `stale`, `comments`).
- `await thread(id=None)`: `{"register", "record"}`, the resume register and the record of thread `id`, the watcher's
  own thread without it.

A read that fails raises `RuntimeError`. The script returns `{"output": str, "done": bool}`: `done` true wakes the ant
a last time and retires the watcher.

```python
# Saved as pr-checks, watched with {"kind":"trail","name":"pr-checks","args":{"pr":7}}.
meta = {"name": "pr-checks", "description": "Watch a pull request's checks", "phases": ["check"]}
checks = await command(f"gh pr checks {args['pr']}")
{"output": checks["stdout"], "done": checks["status"] == 0}
```

### Errors

A step that fails (an `ant=` naming a human or no valid ant name, a `capability=` no ant but the conductor advertises, a
`tips=` key that is not one line of 1-120 characters, malformed questions, a reply still off its schema after two
retries, the step's message withdrawn from its recipient's queue, the recipient at one of its spending caps or the
step's thread out of turns, a human steering the recipient's answer to the step, the step dead-lettered after its
deliveries, an unknown trail, ancestor cycle or failed child run in `trail()`, the run's step limit reached) raises
`RuntimeError` with the reason where the step is awaited. A step to an ant no daemon serves raises nothing: it waits in
that ant's queue, as any message does, until the run ends or a human stops it. An exception the script does not catch
fails the run with its traceback, lines numbered as in the file. Inside `parallel` or `pipeline`, an uncaught exception
of one coroutine fails the whole gather, and the run abandons the other steps. Catch a step's error inside the per-item
coroutine when the trail can go on without that item, and log what was lost.

## The sandbox

- No files, environment, network or subprocesses: `open()` raises PermissionError, `os.getenv()` RuntimeError, and
  modules Monty does not bundle (`socket`, `subprocess`, third-party packages) are not found. Monty bundles `json`,
  `re`, `math`, `datetime`, `random`, `collections`, `itertools`, `functools`, `dataclasses` and a few more.
- The clock and `random` are fixed per leg: `datetime.datetime.now()` returns the leg's start time on every call,
  and `random` is seeded once per leg, so a replay of the leg makes the same calls. `time.sleep()` raises: to wait,
  ask a step to.
- No type checking: annotations are not checked, and a mistake surfaces only when its line runs. Try a trail on a
  small input first.

## Limits

| Limit | Default (maximum) | Past it |
|---|---|---|
| Steps, `agent()`, `human()` and `trail()` calls (child runs included) | 50 (500) | the call raises `RuntimeError: the run reached its limit of N steps` |
| Steps at once | 4 (16) | later steps wait in the run until a step is answered; nothing raises |
| Turns, spent by the steps as ant messages spend turns | 200 (2000) | the run fails, naming the limit |
| Wall time | 24 h (7 d) | the run fails, naming the limit |
| Compute of one resume | 10 s | `TimeoutError`: the run fails, naming the limit |
| Stored session, the script's suspended state | 768 KiB | the run fails, naming its size and the limit |
| Memory | 64 MiB | `MemoryError`: the run fails |
| Notes of one resume: `print()`, `log()` and `phase()` text, 16 bytes more per note | 64 KiB | `RuntimeError` naming the cap; a note over 4000 bytes is cut |
| Arguments of the calls of one resume, as JSON | 1 MiB | `RuntimeError` naming the cap |
| Result of the run, as JSON | 768 KiB | the run fails, naming its size |
| Nested calls | 200 | `RecursionError` in the script |
| Lists and dicts nested in one JSON value: args, a call's arguments, an answer, the result | 100 | deeper arguments raise `TypeError` at the call; a deeper result fails the run |

A run's limits are set when it starts, and each step also counts against the spending caps of the ant that answers it:
a step to an ant at a cap, or out of turns, raises `RuntimeError: <ant> never answered: …` naming the cap, so catch it
to go on with another ant.
A run's legs share its limits and what it spent, and a goto spends nothing. A child run's steps and turns count
against the limits of its root run, the one no `trail()` started, and its wall time ends with its parent's. A run
started with `run_trail` while answering a step spends from the step's grant, a share of what the step's run has left:
its limits are cut to what the grant leaves after the runs already started answering the step, what it spends counts
against the step's run, and a trail on that run's route is refused. A run failed on a limit
continues from its stored record with `ants trail resume`; a `TimeoutError` or `MemoryError` the script raises itself
is no limit, and fails the run as any exception does. Keep the script's own work small: only the compute between steps
(parsing, sorting, building prompts) counts against the compute limit, and large variables (whole files, long lists of
replies) count against the session limit at every suspension.

## Routes

A run's legs are its route: the first enters the trail the run was started with, each `goto()` the next, and the leg
whose script ends gives the run's result. On a goto, the leg's suspended state is dropped with every step it still
waits on (a goto's `parallel()` siblings too), and its child runs stop: the next leg knows only its `state`, at most
64 KiB of JSON. The run keeps its id, thread, owner, limits and spending. A goto is no step: it sends no message.

The conductor refuses a goto it cannot follow, raising `RuntimeError` where it is awaited: to a trail not saved, to a
waypoint the trail lacks, to anything but a saved trail for a run a webhook or schedule started, and to a trail at a
waypoint the run entered before. A run enters each waypoint of each trail once, so no route cycles. Catch the error to
take another exit, such as asking a human:

```text
RuntimeError: the run entered trail hotfix at waypoint patch before; a run enters a trail at each waypoint only once
```

A goto without `trail` goes on in the leg's own script as it was when the leg started; a trail a goto names is read as
saved then, so saving a trail anew changes only the legs started after. The run's thread notes each goto taken with
the route so far (`route: triage@classify -> fix@reproduce`), its end message names the route, and
`ants trail show` lists the legs and the child runs.

## Tips

Each step starts with no context, so what one step learns (a command that works, a pitfall, where something lives)
would be learnt again on every run. Tips keep it: notes the ants answering steps of a saved trail leave on that trail,
for the ants answering its later steps, in any run. An inline script has none.

- **Key the steps.** Pass `tips=` one stable key per kind of step, such as `tips="review"` on the review of every file
  (`agent(f"Review {path} …", label=f"review {path}", tips="review")`), never a key built from the item: a step
  carries the tips left under its own key, so a key no other step shares gathers none worth having. A tip left
  answering a step without `tips=` is trail-wide.
- **What a step carries.** At most 3 tips within 2 KiB, after the prompt: those under the step's key, then trail-wide
  ones relevant to the prompt, each best ranked first, under a header counting the trail's live tips. Helpful votes
  rank a tip up, stale and wrong ones down; a tip fades by half for each 30 days it goes unused, and weighs half while
  left on an older script until voted helpful on the current one. A tip voted wrong more than helpful, or unused for 90
  days, is retired. `ants trail tips NAME` lists a trail's tips, `--retire ID` retires one, and `ants trail rm` removes
  them with the trail.

### Answering a step

A step's message may carry tips from earlier runs of its trail: hints to verify, not instructions.

- **Verify before relying on a tip.** It may be stale; check it against the repository or the task first.
- **Search before giving up or guessing.** When a step runs into trouble (a failing command, a missing file, a reply
  off its schema), search the trail's tips with `trail_tips`, a few words of the problem as `query`; it searches under
  the step's tip key by default.
- **Vote on the tips used.** `vote_tip` with `helpful` when a tip helped, `stale` or `wrong` with a `reason` when it
  misled. Voting again replaces the earlier vote; no ant votes on its own tip. When tips of different writers share an
  id, `writer` names the one meant.
- **Leave a tip when something non-obvious was learnt** that a later step would otherwise rediscover: `leave_tip`, at
  most 1000 characters, specific and checkable, never the step's result nor what any ant would try first. A tip saying
  what a live tip says is refused (vote that one helpful instead); one much like live tips needs `force`.
- Only a step of a saved trail on a trusted run leaves tips; any trusted thread votes on them, naming the trail with
  `trail` when answering no step of it; any thread reads them.

## Finding and rating

Tips tell how to walk a trail; ratings tell which trail to take. Conductors rate their runs, and ants choosing a trail
draw on those ratings.

- **Find before writing.** Before writing a trail-shaped script, call `find_trails` with the task in a few words
  (`find_trails(query: "review a pull request")`). It lists the saved trails whose name, description or rated tasks
  match, each with its saver, its success on matching tasks and its latest ratings, ranked by a draw from those
  ratings: well-rated trails come first more often, and `worth exploring` ones, not yet rated for such tasks, still
  get tried. Run a fitting one with `run_trail(name: …)`; write a script only when none fits.
- **Rate every run conducted.** When a run ends done or failed, the conductor gets a message on the run's thread (or,
  having waited on it with `run_trail`, the answer) with the `rate_trail` line: call `rate_trail(run, outcome, task,
  why)` with `worked`, `partly` or `failed`, the task the run served in a line as a later search would name it, and a
  concrete reason (what it produced, what it missed, which step broke), then end the turn with `complete`. Judge the
  outcome by the task, not by the run's state: a run done with a useless result failed it. A stopped run may be rated
  too; a child run a `trail()` call started is rated by its own id.
- One rating per run and trail: rating the run again replaces it. A run of a saved trail rates that trail and every
  trail of its route still saved; a run of an inline script rates none. Saving its script with `save_trail` rates nothing
  either: a `rate_trail` of the run made after the save rates the saved trail too, so rate the run again once saved.
- On a thread a webhook or schedule started, `rate_trail` rates only the run reporting on that thread; `find_trails`
  works on any thread.

## Saving

Save a trail once an inline script worked and its steps will recur: a one-off stays inline. Generalize it first: move
the repository, issue, files and ants it names into `args`, and have the meta's `description` name the task it serves
and the `args` it takes, so another ant picks it from `ants trail list` and runs it by name.

- **Save** with the `save_trail` tool: `script` for a script as written, or `run` for the id `run_trail` returned for a
  finished run of an inline script this ant conducted, recorded as the trail's source. A run of a saved trail is
  saved already. The script is checked as `ants trail save` checks a file, not run.
- **Fork, never take over.** A name a human or another ant saved is refused, and so is a name of the conductor
  machine's `~/.ants/trails`: save a changed copy under a new name.
- **Replace only its own**, with `replace: true`; runs already started keep the script they started with, and the
  trail's tips stay, ranked as of an older script.
- **Remove only its own** with `remove_trail`: its tips stay for a trail saved later under that name. A trail a human
  or another ant saved goes only by a human's `ants trail rm`.
- Saving and removing are refused on a thread a webhook or schedule started, and on a step of a run started there.
- With authentication, `ants signer` makes each save and removal for the ant: when it does not answer, saving and
  removing fail until it runs again.
- **From a dream.** A sage's `trail` finding outlines a trail (name, description, args with examples, phases) for an
  approach repeated across threads. First check `find_trails` with its description: when a saved trail already serves
  it, reject the finding naming that trail. Otherwise write the script from the outline, try it with `run_trail` where
  a run is safe, save it with `save_trail` passing the finding's id in `finding`, and answer the finding with
  `sense_dream`, `applied`, naming the trail in `trails`. The save counts as one of the dream's writes. A `trail`
  finding marked untrusted rests on text from outside: write, run and save no script from it, and answer it
  `rejected` or `deferred`. While a dream covers untrusted text, `save_trail` there takes only a trusted `trail`
  finding in `finding`.

## Patterns

- **Pipeline by default.** `pipeline(items, stage1, stage2)` lets a fast item reach its second stage while a slow one
  is still in its first. Use `parallel` as a barrier only when the next stage needs every result, as a synthesis or a
  deduplication does.
- **Adversarial verify.** Give each finding to a skeptic step that tries to refute it, on a different ant (or
  capability) than the one that made the finding: no ant grades its own work. Keep only what survives, and report what
  could not be verified apart from what was refuted.
- **Loop until dry.** Repeat discovery, telling each round what is already known, until a round finds nothing new;
  cap the rounds, and log when the cap stops a search that still found something.
- **No silent caps.** When the script trims its input (to fit the step limit, say), `log()` what it skipped and why;
  return unverified or failed items rather than dropping them.
- **Schemas for structured results.** Pass `schema=` whenever the script reads fields of a reply. Keep schemas small,
  with `required` fields; a reply that does not match is asked for again. Only `type`, `properties`, `required`,
  `items`, `enum` and `additionalProperties: false` are checked: any other keyword (`minItems`, `pattern`, `format`,
  `oneOf`, …) passes unchecked, so check such constraints in the script.
- **Self-contained prompts.** Each step starts a fresh thread with no context: name the repository, branch, file,
  question and the shape of the answer in the prompt itself, and pass earlier results into it as text.
- **Handle a step's error.** Wrap a step in `try`/`except RuntimeError` when the run can go on without it; let it
  raise when nothing after it makes sense.
- **Hand on, or take a detour.** `goto()` hands the run on for good, to another trail or to a later waypoint of its
  own; `trail()` takes a detour that comes back with a result. Keep a shared verifier or approval in a small trail of
  its own, for any trail to call.
- **Loops inside one waypoint.** A run enters a waypoint once: a fix-and-review loop is a `for` loop in one waypoint,
  under a round cap, not gotos back and forth.
- **Carry the state.** A goto's `state` is all the next leg knows: pass on the repository, the item and the earlier
  results it needs.
- **Stable tip keys.** Key each kind of step (`tips="review"`, `tips="verify"`), so every run's steps of that kind
  share what earlier ones learnt ([Tips](#tips)).
- **Budget the steps.** Count the steps the input implies (files × steps per file) against the step limit before
  starting a large run; a run needing more takes a higher limit.

## Examples

Each example below is a complete trail, run with the args after it by the repository's tests. A `goto()` or
`trail()` of an example leads to the example of that name, as it would to a saved trail.

### Review every changed file, with adversarial verify

A reviewer ant reviews each file; a skeptic ant tries to refute each finding as soon as its file's review arrives. The
pipeline gives each file its own pace; within a file, `parallel` verifies the file's findings at once. Each step's
label names its file, but its tip key stays `"review"` or `"verify"`, so every review, in this run and later ones,
carries the tips left on any earlier review.

```python
meta = {
    "name": "review-changes",
    "description": "Reviews each changed file, then has a second ant try to refute every finding",
    "phases": ["review", "report"],
}

FINDINGS = {
    "type": "object",
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"line": {"type": "integer"}, "issue": {"type": "string"}},
                "required": ["line", "issue"],
            },
        },
    },
    "required": ["findings"],
}

VERDICT = {
    "type": "object",
    "properties": {"real": {"type": "boolean"}, "reason": {"type": "string"}},
    "required": ["real", "reason"],
}

repo, branch = args["repo"], args["branch"]
files = args["files"]
limit = args.get("max_files", 20)
if len(files) > limit:
    log(f"reviewing the first {limit} of {len(files)} files; skipped: {', '.join(files[limit:])}")
    files = files[:limit]


async def review(path):
    try:
        found = await agent(
            f"Review the change to {path} on branch {branch} of the repository at {repo} "
            f"(git diff main...{branch} -- {path}). Report only defects that change behaviour, "
            "each with its line in the new file and one sentence on what goes wrong.",
            ant=args["reviewer"],
            schema=FINDINGS,
            label=f"review {path}",
            tips="review",
        )
    except RuntimeError as e:
        log(f"no review of {path}: {e}")
        return []
    return [{"path": path, "line": f["line"], "issue": f["issue"]} for f in found["findings"]]


async def verify(finding):
    where = f"{finding['path']}:{finding['line']}"
    try:
        verdict = await agent(
            f"A reviewer claims a defect at {where} on branch {branch} of the repository at {repo}: "
            f"{finding['issue']}\nRead the code and try to refute the claim. "
            "Set real to true only if the defect holds, and give the reason.",
            ant=args["skeptic"],
            schema=VERDICT,
            label=f"verify {where}",
            tips="verify",
        )
    except RuntimeError as e:
        log(f"could not verify {where}: {e}")
        return {**finding, "real": None, "reason": str(e)}
    return {**finding, "real": verdict["real"], "reason": verdict["reason"]}


async def verify_all(findings):
    return await parallel(*[verify(f) for f in findings])


phase("review")
checked = [f for per_file in await pipeline(files, review, verify_all) for f in per_file]
phase("report")
confirmed = [f for f in checked if f["real"] is True]
unverified = [f for f in checked if f["real"] is None]
refuted = len(checked) - len(confirmed) - len(unverified)
log(f"{len(confirmed)} confirmed, {refuted} refuted, {len(unverified)} unverified")
{"confirmed": confirmed, "unverified": unverified}
```

Args:

```json
{
  "repo": "/src/ants",
  "branch": "trails",
  "files": ["crates/ants-trail/src/run.rs", "crates/ants-trail/src/worker.rs", "crates/ants/src/main.rs"],
  "reviewer": "dev-a",
  "skeptic": "dev-b"
}
```

### Research a question across sources, then synthesize

Each source is searched on its own; the synthesis needs every source's notes, so `parallel` is the barrier here.

```python
meta = {
    "name": "research",
    "description": "Researches a question in several sources at once, then writes one answer citing them",
    "phases": ["gather", "synthesize"],
}

NOTES = {
    "type": "object",
    "properties": {
        "relevant": {"type": "boolean"},
        "notes": {"type": "string"},
        "citations": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["relevant", "notes", "citations"],
}

question = args["question"]


async def gather(source):
    try:
        found = await agent(
            f"Research this question using only {source}: {question}\n"
            "Reply with notes on what this source says and a citation (path, URL or issue id) for each claim. "
            "Set relevant to false when the source says nothing about the question.",
            capability=args.get("capability"),
            schema=NOTES,
            label=f"research {source}",
        )
    except RuntimeError as e:
        log(f"no notes from {source}: {e}")
        return None
    return {"source": source, **found}


phase("gather")
results = await parallel(*[gather(source) for source in args["sources"]])
notes = [r for r in results if r is not None and r["relevant"]]
silent = [r["source"] for r in results if r is not None and not r["relevant"]]
if silent:
    log(f"nothing on the question in {', '.join(silent)}")

phase("synthesize")
if notes:
    gathered = "\n\n".join(
        f"From {n['source']}:\n{n['notes']}\nCitations: {', '.join(n['citations'])}" for n in notes
    )
    answer = await agent(
        "Answer this question from the research notes below, citing them; say where the sources disagree "
        f"and what stays open.\n\nQuestion: {question}\n\n{gathered}",
        label="synthesize",
    )
else:
    answer = None
    log("no source had anything on the question")
{"question": question, "answer": answer, "sources": [n["source"] for n in notes]}
```

Args:

```json
{
  "question": "How does a trail run survive a daemon restart?",
  "sources": [
    "the docs/ folder of the repository at /src/ants",
    "the bd issue tracker of /src/ants (bd search)",
    "the code of crates/ants-trail in /src/ants"
  ],
  "capability": "rust"
}
```

### Loop until dry

Discovery repeats, each round told what earlier rounds found, until a round brings nothing new or the round cap
stops it.

```python
meta = {
    "name": "find-until-dry",
    "description": "Lists the problems of an area round after round until a round finds nothing new",
    "phases": ["search", "report"],
}

ITEMS = {
    "type": "object",
    "properties": {"items": {"type": "array", "items": {"type": "string"}}},
    "required": ["items"],
}

area, rounds = args["area"], args.get("max_rounds", 5)
known = []
dry = False

phase("search")
for n in range(1, rounds + 1):
    already = "\n".join(f"- {item}" for item in known) or "(none yet)"
    found = await agent(
        f"List problems in {area} of the repository at {args['repo']}: dead code, missing tests, "
        f"unclear error messages. Leave out these, found already:\n{already}\n"
        "Reply with each new problem as one line naming its file; an empty list when nothing new is left.",
        capability=args.get("capability"),
        schema=ITEMS,
        label=f"search round {n}",
    )
    new = [item for item in found["items"] if item not in known]
    log(f"round {n}: {len(new)} new")
    if not new:
        dry = True
        break
    known.extend(new)

phase("report")
if not dry:
    log(f"stopped after {rounds} rounds while rounds still found problems; more may remain")
{"problems": known, "dry": dry}
```

Args:

```json
{"repo": "/src/ants", "area": "crates/ants-daemon/src/sense.rs", "max_rounds": 4}
```

### Ask the owner before acting

A plan goes to the run's thread as a note, the owner approves it or not, and only then does a step apply it. The apply
step starts with no context, so its prompt carries the plan.

```python
meta = {
    "name": "plan-and-apply",
    "description": "Drafts a plan for a change, asks the run's owner to approve it, then applies it",
    "phases": ["plan", "approve", "apply"],
}

repo, change = args["repo"], args["change"]

phase("plan")
plan = await agent(
    f"Draft a step-by-step plan for this change in the repository at {repo}: {change}\n"
    "Change nothing yet; reply with the plan only.",
    ant=args["ant"],
    label="plan",
)
log(plan)

phase("approve")
choice = await human([
    {
        "question": f"Apply the plan noted on this run's thread for: {change}",
        "header": "Plan",
        "options": [
            {"label": "apply", "description": "a step applies the plan on a new branch"},
            {"label": "stop", "description": "the run ends with nothing changed"},
        ],
        "free_text": False,
    }
])

if choice == "apply":
    phase("apply")
    report = await agent(
        f"Apply this plan in the repository at {repo} on a new branch and commit; "
        f"reply with the branch and what changed.\n\nChange: {change}\n\nPlan:\n{plan}",
        ant=args["ant"],
        label="apply",
    )
else:
    report = None
    log("the owner stopped the run; nothing applied")
{"applied": report is not None, "plan": plan, "report": report}
```

Args:

```json
{"repo": "/src/ants", "change": "rename the retired term persona to ant in docs/", "ant": "dev-a"}
```

### Triage an issue, through a fix and back

`triage` classifies an issue. A bug goes on in `fix`, which reproduces it, patches it under review and rejoins
`triage` at its `report` waypoint: `triage@classify -> fix@reproduce -> fix@patch -> triage@report`. A question stays
in its leg, `classify` awaiting `report` as a function. The patch-review loop is a `for` loop inside `patch`, under a
round cap.

```python
meta = {
    "name": "triage",
    "description": "Classifies an issue, routes a bug to the fix trail, answers a question, and reports on the issue",
    "phases": ["classify", "report"],
    "waypoints": ["classify", "report"],
}

ISSUE = {
    "type": "object",
    "properties": {"kind": {"type": "string", "enum": ["bug", "question"]}, "summary": {"type": "string"}},
    "required": ["kind", "summary"],
}


async def classify(args):
    phase("classify")
    issue = await agent(
        f"Read issue {args['issue']} of the repository at {args['repo']}. Classify it as a bug (the code "
        "misbehaves) or a question (how to use it), and summarize it in one sentence.",
        schema=ISSUE,
        label="classify",
    )
    state = {**args, "summary": issue["summary"]}
    if issue["kind"] == "bug":
        await goto("fix", state=state)
    answer = await agent(
        f"Answer this question from the docs of the repository at {args['repo']}: {issue['summary']}",
        label="answer",
    )
    return await report({**state, "outcome": answer})


async def report(args):
    phase("report")
    await agent(
        f"Comment on issue {args['issue']} of the repository at {args['repo']} with this outcome, and close the "
        f"issue if the outcome settles it:\n{args['outcome']}",
        label="report",
    )
    return {"issue": args["issue"], "outcome": args["outcome"]}
```

Args:

```json
{"repo": "/src/app", "issue": 413}
```

```python
meta = {
    "name": "fix",
    "description": "Reproduces a bug, patches it under review, and reports back to the triage trail",
    "phases": ["reproduce", "patch"],
    "waypoints": ["reproduce", "patch"],
}

REPRODUCED = {
    "type": "object",
    "properties": {"reproduced": {"type": "boolean"}, "test": {"type": "string"}},
    "required": ["reproduced", "test"],
}

REVIEW = {
    "type": "object",
    "properties": {"approved": {"type": "boolean"}, "comments": {"type": "string"}},
    "required": ["approved", "comments"],
}


async def reproduce(args):
    phase("reproduce")
    found = await agent(
        f"Write a failing test that reproduces this bug, issue {args['issue']} of the repository at {args['repo']}: "
        f"{args['summary']}\nReply with whether the test reproduced it, and the test's name.",
        capability="rust",
        schema=REPRODUCED,
        label="reproduce",
    )
    if not found["reproduced"]:
        outcome = "Could not reproduce the bug; more detail is needed."
        await goto("triage", at="report", state={**args, "outcome": outcome})
    await goto(at="patch", state={**args, "test": found["test"]})


async def patch(args):
    phase("patch")
    comments = "none yet"
    for n in range(1, 4):
        branch = await agent(
            f"Fix issue {args['issue']} of the repository at {args['repo']} on branch fix-{args['issue']}, so that "
            f"test {args['test']} passes: {args['summary']}\nAddress these review comments: {comments}\n"
            "Commit, and reply with the branch only.",
            capability="rust",
            label=f"patch {n}",
        )
        review = await agent(
            f"Review branch {branch} of the repository at {args['repo']}, a fix of issue {args['issue']}. "
            "Approve it only if it fixes the bug without breaking anything else.",
            capability="review",
            schema=REVIEW,
            label=f"review {n}",
        )
        if review["approved"]:
            await goto("triage", at="report", state={**args, "outcome": f"Fixed on branch {branch}."})
        comments = review["comments"]
    log(f"no approval after 3 rounds: {comments}")
    outcome = f"Not fixed yet; the review still asks: {comments}"
    await goto("triage", at="report", state={**args, "outcome": outcome})
```

Args:

```json
{"repo": "/src/app", "issue": 413, "summary": "A draft saved twice is duplicated."}
```

### Review with a security detour

`review` lists a pull request's findings. When one bears on security, the run goes on in `security-audit`, which
models the threats, has each checked by the shared `verify` trail and rejoins `review` at its `verdict` waypoint:
`review@findings -> security-audit@model -> security-audit@confirm -> review@verdict`. The audit runs once: a second
would enter `security-audit@model` again, which the run refuses.

```python
meta = {
    "name": "review",
    "description": "Reviews a pull request, detouring through a security audit when a finding bears on security",
    "phases": ["findings", "verdict"],
    "waypoints": ["findings", "verdict"],
}

FINDINGS = {
    "type": "object",
    "properties": {
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"issue": {"type": "string"}, "security": {"type": "boolean"}},
                "required": ["issue", "security"],
            },
        },
    },
    "required": ["findings"],
}


async def findings(args):
    phase("findings")
    found = await agent(
        f"Review pull request {args['pr']} of the repository at {args['repo']}. Report each defect in one sentence, "
        "and whether it bears on security: authentication, injection, secrets or permissions.",
        capability="review",
        schema=FINDINGS,
        label="findings",
    )
    issues = [f["issue"] for f in found["findings"]]
    if any(f["security"] for f in found["findings"]):
        await goto("security-audit", state={**args, "findings": issues})
    return await verdict({**args, "findings": issues, "threats": []})


async def verdict(args):
    phase("verdict")
    listed = "\n".join(f"- {issue}" for issue in args["findings"]) or "none"
    threats = "\n".join(f"- {t['claim']}: {t['reason']}" for t in args["threats"]) or "none"
    await agent(
        f"Post a review of pull request {args['pr']} of the repository at {args['repo']}: request changes if a "
        f"finding or threat below stands, else approve.\nFindings:\n{listed}\nConfirmed threats:\n{threats}",
        capability="review",
        label="verdict",
    )
    return {"pr": args["pr"], "findings": args["findings"], "threats": args["threats"]}
```

Args:

```json
{"repo": "/src/app", "pr": 80}
```

```python
meta = {
    "name": "security-audit",
    "description": "Models the threats of a review's findings, verifies each, and returns to the review's verdict",
    "phases": ["threat model", "confirm"],
    "waypoints": ["model", "confirm"],
}

THREATS = {
    "type": "object",
    "properties": {"threats": {"type": "array", "items": {"type": "string"}}},
    "required": ["threats"],
}


async def model(args):
    phase("threat model")
    listed = "\n".join(f"- {issue}" for issue in args["findings"])
    found = await agent(
        f"Threat-model pull request {args['pr']} of the repository at {args['repo']} given these review findings:\n"
        f"{listed}\nReply with each concrete threat in one sentence naming the code at risk.",
        capability="security",
        schema=THREATS,
        label="threat model",
    )
    await goto(at="confirm", state={**args, "threats": found["threats"]})


async def check(repo, threat):
    try:
        return await trail("verify", {"repo": repo, "claim": threat})
    except RuntimeError as e:
        log(f"unverified, kept: {threat}: {e}")
        return {"claim": threat, "real": True, "reason": "not verified"}


async def confirm(args):
    phase("confirm")
    verdicts = await parallel(*[check(args["repo"], threat) for threat in args["threats"]])
    confirmed = [v for v in verdicts if v["real"]]
    log(f"{len(confirmed)} of {len(verdicts)} threats confirmed")
    await goto("review", at="verdict", state={**args, "threats": confirmed})
```

Args:

```json
{"repo": "/src/app", "pr": 80, "findings": ["The session token is compared with ==, not in constant time."]}
```

### A shared verifier

`verify` is one skeptic step, a trail of its own so that any trail verifies a claim with
`await trail("verify", {"repo": ..., "claim": ...})`, each call a child run on its own thread.

```python
meta = {
    "name": "verify",
    "description": "Has a skeptic try to refute one claim about a repository, and says whether it holds",
    "phases": [],
}

VERDICT = {
    "type": "object",
    "properties": {"real": {"type": "boolean"}, "reason": {"type": "string"}},
    "required": ["real", "reason"],
}

verdict = await agent(
    f"Someone claims this about the repository at {args['repo']}: {args['claim']}\n"
    "Read the code and try to refute the claim. Set real to true only if it holds, and give the reason.",
    capability="review",
    schema=VERDICT,
    label="verify",
)
{"claim": args["claim"], "real": verdict["real"], "reason": verdict["reason"]}
```

Args:

```json
{"repo": "/src/app", "claim": "src/auth.rs compares the session token with ==, leaking its length in time."}
```

### Upgrade a dependency

`upgrade` bumps a dependency, migrates the code only when the bump breaks the build, then runs the tests:
`upgrade@bump -> upgrade@migrate -> upgrade@test`, or `upgrade@bump -> upgrade@test`. A license change detours
through `license-review`, a trail asking the run's owner, whose answer comes back to the step.

```python
meta = {
    "name": "upgrade",
    "description": "Upgrades a dependency: bumps it, migrates the code when the bump breaks the build, then tests",
    "phases": ["bump", "migrate", "test"],
    "waypoints": ["bump", "migrate", "test"],
}

BUMP = {
    "type": "object",
    "properties": {"builds": {"type": "boolean"}, "license": {"type": "string"}, "errors": {"type": "string"}},
    "required": ["builds", "license", "errors"],
}

TESTS = {
    "type": "object",
    "properties": {"passed": {"type": "boolean"}, "failures": {"type": "string"}},
    "required": ["passed", "failures"],
}


async def bump(args):
    phase("bump")
    bumped = await agent(
        f"On a new branch of the repository at {args['repo']}, bump {args['crate']} to {args['version']} and "
        "build. Reply with whether it builds, the new version's license (an SPDX id) and the build errors.",
        ant=args["dev"],
        schema=BUMP,
        label="bump",
    )
    if bumped["license"] != args["license"]:
        review = {"crate": args["crate"], "from": args["license"], "to": bumped["license"]}
        if await trail("license-review", review) != "accept":
            return {"upgraded": False, "why": f"license {bumped['license']} not accepted"}
    state = {**args, "errors": bumped["errors"]}
    if not bumped["builds"]:
        await goto(at="migrate", state=state)
    await goto(at="test", state=state)


async def migrate(args):
    phase("migrate")
    await agent(
        f"Read the changelog of {args['crate']} {args['version']}, and migrate the code of the repository at "
        f"{args['repo']} on its upgrade branch until it builds. The build failed with:\n{args['errors']}",
        ant=args["dev"],
        label="migrate",
    )
    await goto(at="test", state=args)


async def test(args):
    phase("test")
    tested = await agent(
        f"Run the tests of the repository at {args['repo']} on the branch upgrading {args['crate']} to "
        f"{args['version']}. Reply with whether they pass and the failures.",
        ant=args["dev"],
        schema=TESTS,
        label="test",
    )
    return {"upgraded": tested["passed"], "failures": tested["failures"]}
```

Args:

```json
{"repo": "/src/app", "crate": "serde_yaml", "version": "0.10.0", "license": "MIT", "dev": "dev-a"}
```

```python
meta = {
    "name": "license-review",
    "description": "Asks the run's owner whether a dependency's new license is acceptable",
    "phases": [],
}

answer = await human([
    {
        "question": f"{args['crate']} moves from the {args['from']} license to {args['to']}. Accept it?",
        "header": "License",
        "options": [
            {"label": "accept", "description": "the upgrade goes on"},
            {"label": "reject", "description": "the upgrade stops"},
        ],
        "free_text": False,
    }
])
answer
```

Args:

```json
{"crate": "serde_yaml", "from": "MIT", "to": "BUSL-1.1"}
```

### Handle an incident

`incident` diagnoses an incident. A bad deploy goes on in `rollback`, which asks the run's owner before rolling back
and rejoins `incident` at its `postmortem` waypoint: `incident@diagnose -> rollback -> incident@postmortem`. A bad
config change is reverted within the leg, which then goes on at `postmortem` too.

```python
meta = {
    "name": "incident",
    "description": "Diagnoses an incident, rolls back a bad deploy or reverts a bad config, then writes the postmortem",
    "phases": ["diagnose", "postmortem"],
    "waypoints": ["diagnose", "postmortem"],
}

CAUSE = {
    "type": "object",
    "properties": {
        "cause": {"type": "string", "enum": ["deploy", "config", "unknown"]},
        "detail": {"type": "string"},
    },
    "required": ["cause", "detail"],
}


async def diagnose(args):
    phase("diagnose")
    found = await agent(
        f"Diagnose this incident of service {args['service']}: {args['alert']}\nSay whether the last deploy, a "
        "config change or something else caused it, and what points to it.",
        capability="ops",
        schema=CAUSE,
        label="diagnose",
    )
    state = {**args, "cause": found["cause"], "detail": found["detail"]}
    if found["cause"] == "deploy":
        await goto("rollback", state=state)
    action = "none: the cause is unknown"
    if found["cause"] == "config":
        action = await agent(
            f"Revert the config change of service {args['service']} that caused this: {found['detail']}\n"
            "Reply with what was reverted.",
            capability="ops",
            label="revert config",
        )
    await goto(at="postmortem", state={**state, "action": action})


async def postmortem(args):
    phase("postmortem")
    text = await agent(
        f"Write the postmortem of the incident of service {args['service']}: alert {args['alert']}, cause "
        f"{args['cause']} ({args['detail']}), action taken: {args['action']}. Reply with its text.",
        label="postmortem",
    )
    return {"cause": args["cause"], "action": args["action"], "postmortem": text}
```

Args:

```json
{"service": "checkout", "alert": "error rate over 5% for 7 minutes since 14:02"}
```

```python
meta = {
    "name": "rollback",
    "description": "Rolls a service back to its previous release once the run's owner approves",
    "phases": ["approve", "roll back"],
}

phase("approve")
choice = await human([
    {
        "question": f"Roll {args['service']} back to its previous release? The last deploy caused: {args['detail']}",
        "header": "Rollback",
        "options": [
            {"label": "roll back", "description": "a step rolls the service back now"},
            {"label": "hold", "description": "nothing changes, and the postmortem says so"},
        ],
        "free_text": False,
    }
])
action = "none: the rollback was held"
if choice == "roll back":
    phase("roll back")
    action = await agent(
        f"Roll service {args['service']} back to its previous release and check it is healthy; "
        "reply with the release it runs now.",
        capability="ops",
        label="roll back",
    )
await goto("incident", at="postmortem", state={**args, "action": action})
```

Args:

```json
{
  "service": "checkout",
  "alert": "error rate over 5% for 7 minutes since 14:02",
  "cause": "deploy",
  "detail": "release 2024.6.3 at 14:00 changed the payment client"
}
```

### A loop the run refuses

`hotfix` patches a bug, then checks the patch at its `check` waypoint. A rejected patch cannot go back to `patch`, a
waypoint the run entered already: the goto raises, and `check` asks a human instead. A loop meant to retry belongs in
one waypoint, as `fix`'s `patch` above.

```python
meta = {
    "name": "hotfix",
    "description": "Patches a production bug and has the patch checked, asking a human when the check rejects it",
    "phases": ["patch", "check"],
    "waypoints": ["patch", "check"],
}

CHECK = {
    "type": "object",
    "properties": {"approved": {"type": "boolean"}, "comments": {"type": "string"}},
    "required": ["approved", "comments"],
}


async def patch(args):
    phase("patch")
    branch = await agent(
        f"Patch this production bug of the repository at {args['repo']} on a new branch off {args['release']}: "
        f"{args['bug']}\nAddress these review comments, if any: {args.get('comments', 'none')}\n"
        "Commit, and reply with the branch only.",
        ant=args["dev"],
        label="patch",
    )
    await goto(at="check", state={**args, "branch": branch})


async def check(args):
    phase("check")
    review = await agent(
        f"Check branch {args['branch']} of the repository at {args['repo']}, a hotfix of: {args['bug']}\n"
        "Approve it only if it fixes the bug and is safe to ship today.",
        ant=args["reviewer"],
        schema=CHECK,
        label="check",
    )
    if review["approved"]:
        return {"branch": args["branch"], "approved": True}
    try:
        await goto(at="patch", state={**args, "comments": review["comments"]})
    except RuntimeError as e:
        log(f"no second patch: {e}")
    choice = await human([
        {
            "question": f"The check rejected hotfix branch {args['branch']}: {review['comments']}",
            "header": "Hotfix",
            "options": [
                {"label": "take over", "description": "a human finishes the branch"},
                {"label": "drop", "description": "the fix waits for the next release"},
            ],
            "free_text": False,
        }
    ])
    return {"branch": args["branch"], "approved": False, "decision": choice}
```

Args:

```json
{
  "repo": "/src/app",
  "release": "release-2024.6",
  "bug": "Checkout fails for carts holding a gift card.",
  "dev": "dev-a",
  "reviewer": "dev-b"
}
```

## Running a trail

A human starts a run with `ants trail run`, which shows what the run may spend before it starts and takes the limit
flags `--steps`, `--at-once`, `--turns` and `--wall`; an ant starts one with the `run_trail` MCP tool, from a saved
trail's `name` or a `script` of its own, on its current thread, getting the run's id at once or, with `wait`, its
result; a `[[schedule]]` entry with `trail =` (and `args =`, a table) starts a saved trail on its `ant` at each firing.
`run_trail`'s `limits` (`{"steps", "at_once", "turns", "wall"}`) and a schedule's `limits` table set the same limits as
those flags. `trail_status` tells how a run the ant conducts stands (state, steps and turns spent of its limits, the
steps it waits on, the result), and `stop_trail` stops one, which only a human resumes. `find_trails` finds saved
trails for a task by their ratings, and `rate_trail` rates a run once it ends ([Finding and
rating](#finding-and-rating)). Saved trails are colony-wide:
humans put and remove any, and an ant puts and removes its own with `save_trail` and `remove_trail` ([Saving](#saving));
a conductor's daemon also reads `~/.ants/trails/<name>.py` on its machine, the colony's
trail winning a name both have. A run started on a thread a webhook or schedule started runs only saved trails, its
steps narrowed to read-only tools. `ants trail runs`, `show`, `stop` and `resume` manage runs, and an admin's `ants ant
stop-trail` and `resume-trail` stop and resume one by admin order, which waits for a conductor's daemon while it is
away; see the repository's `docs/trails.md`.
