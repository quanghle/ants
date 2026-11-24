A trail is a Python script (Monty's subset) that orchestrates ants: its code decides which ant takes each step, how
many run at once and how results merge. Its first statement is a literal
`meta = {"name": "...", "description": "...", "phases": ["..."]}`; `await` works at the top level, and the value of
the last expression (JSON) is the run's result. With `"waypoints": ["a", "b"]` in meta (at most 32), each naming a
top-level `async def a(args)`, a run enters at a waypoint (the first unless a goto names one), whose return value is
the result; the top level then holds only meta, imports, definitions and constants.
- `await agent(prompt, ant=None, capability=None, team=None, schema=None, label=None, tips=None)`: one step, a
  message on a fresh thread with no context, to the ant named, to an ant with the capability, to the lead of the team
  named (team= alone), or else to the conductor itself.
  Returns the reply's text, or with `schema=` (a JSON Schema dict) the reply's JSON, asked for again at most twice on
  a mismatch. `tips=KEY` (one line) is the step's tip key: the step carries the best tips ants left answering steps
  under the key on earlier runs, and a tip left answering it goes under the key. Give each kind of step one stable
  key (`tips="review"` on every file's review), not a per-item label; without `tips=`, tips are trail-wide.
- `await human(questions)`: asks the run's owner as ask_human does (1-4 `{"question", "options": [2-9 labels]}`);
  returns the answer's text, for one question the label picked.
- `await trail(name, args=None)`: one step answered by a child run of saved trail `name` on a spin-off thread (none
  on a calling run's route); returns its result; its steps and turns count against the root run's limits.
- `await goto(trail=None, at=None, state=None)`: ends this leg where called (its steps waited on dropped) and goes on
  in saved trail `trail` (this one without it) at waypoint `at` (its start without it), `state` (JSON) its `args`. A
  run enters each (trail, waypoint) once: a goto to one entered before, or to one missing, raises RuntimeError, to
  catch and take another exit. Loops belong inside one waypoint.
- `phase(title)`, `log(text)`, `print()`: notes on the run's thread; they never wait. `args`: the leg's JSON input.
- A watcher's trail (sense_remind's `watch` trigger) prompts no ant and writes no note: it reads with
  `await command(cmd)` (`{"status", "stdout"}`, under the ant's allow rules), `await read_file(path)` (workdir text or
  None), `await bd(*args)` (a reading subcommand's JSON) and `await thread(id=None)` (`{"register", "record"}`), and
  returns `{"output": str, "done": bool}`; a run outside a watcher may not call these.
- `await parallel(*coroutines)`: every result in argument order once all finish (a barrier).
- `await pipeline(items, *stages)`: each item through the async stages on its own, no barrier; results in item order.
- A failed step raises RuntimeError where awaited; an uncaught exception fails the run with its traceback.
- No files, environment, network or sleep; the clock and random are fixed per leg; no type checking.
- Limits: 50 steps (500 at most, child runs included; the call past it raises), 4 at once (16; the rest wait),
  200 turns (2000), 24 h of wall time (7 d), 10 s of compute per resume and a 768 KiB stored session; past one the run
  fails, naming it. A run's start sets the first four (run_trail's `limits`: `steps`, `at_once`, `turns`, `wall`).
  Per resume, 64 KiB of print(), log() and phase() text and 1 MiB of call arguments (past either the call raises
  RuntimeError); notes over 4000 bytes are cut; a result over 768 KiB fails the run.
Patterns: pipeline by default, parallel only when a stage needs every result; verify each finding with a skeptic step
on another ant than its author; repeat discovery until a round finds nothing new, under a round cap; log whatever is
skipped or capped; schema= for every reply whose fields are read; self-contained prompts; try/except RuntimeError
around a step the run can do without; small saved trails composed with goto() and trail() over one large script.
The full guide, with complete examples: contrib/skills/ants-trails/SKILL.md in the ants repository.
