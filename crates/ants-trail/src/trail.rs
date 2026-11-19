//! A trail's script: its literal meta, read without running it, and the prelude every script runs after.

use monty::MontyRun;
use monty_types::{CompileOptions, MontyException, StackFrame};
use ruff_python_ast::helpers::AwaitVisitor;
use ruff_python_ast::visitor::Visitor as _;
use ruff_python_ast::{Expr, Stmt, StmtFunctionDef};
use ruff_text_size::Ranged as _;
use serde::{Deserialize, Serialize};

/// The file name a trail's frames carry in tracebacks.
pub const SCRIPT_NAME: &str = "trail.py";
/// Most waypoints a trail's meta lists.
const MAX_WAYPOINTS: usize = 32;
const PRELUDE_NAME: &str = "<prelude>";

/// The trail API, defined before the script runs. The `__ants_*` externals are answered by the worker: agent, human,
/// trail and goto suspend the run on a future, phase and log return at once.
const PRELUDE: &str = r#"import asyncio

async def agent(prompt, ant=None, capability=None, schema=None, label=None, tips=None):
    request = {"prompt": prompt, "ant": ant, "capability": capability, "schema": schema, "label": label, "tips": tips}
    return await __ants_agent({k: v for k, v in request.items() if v is not None})

async def human(questions):
    return await __ants_human({"questions": questions})

async def trail(name, args=None):
    request = {"name": name, "args": args}
    return await __ants_trail({k: v for k, v in request.items() if v is not None})

async def goto(trail=None, at=None, state=None):
    request = {"trail": trail, "at": at, "state": state}
    return await __ants_goto({k: v for k, v in request.items() if v is not None})

def phase(title):
    __ants_phase(str(title))

def log(text):
    __ants_log(str(text))

def parallel(*coroutines):
    return asyncio.gather(*coroutines)

async def __ants_through(item, stages):
    for stage in stages:
        item = await stage(item)
    return item

def pipeline(items, *stages):
    return asyncio.gather(*[__ants_through(item, stages) for item in items])

"#;

/// The script's inputs, in this order.
pub(crate) const INPUTS: [&str; 7] =
    ["args", "__ants_agent", "__ants_human", "__ants_trail", "__ants_goto", "__ants_phase", "__ants_log"];

/// A trail's description, the literal `meta = {"name": …, "description": …, "phases": […], "waypoints": […]}` it
/// starts with, `"waypoints"` optional.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    /// Letters, digits, `-` and `_`.
    pub name: String,
    pub description: String,
    pub phases: Vec<String>,
    /// The top-level async defs a run enters the trail at, each awaited with the leg's input, the first its start.
    /// None for a trail entered only at its start, the script's top level.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub waypoints: Vec<String>,
}

impl Meta {
    /// The waypoint a run entering the trail at `at` enters: `at`, one of its waypoints, else its start, its first
    /// waypoint. None for a trail without waypoints, entered at its start.
    pub fn entry<'a>(&'a self, at: Option<&'a str>) -> Result<Option<&'a str>, String> {
        let name = &self.name;
        match at {
            None => Ok(self.waypoints.first().map(String::as_str)),
            Some(at) if self.waypoints.iter().any(|w| w == at) => Ok(Some(at)),
            Some(at) if self.waypoints.is_empty() => {
                Err(format!("trail {name} has no waypoints, so no waypoint {at:?}: it is entered only at its start"))
            }
            Some(at) => {
                Err(format!("trail {name} has no waypoint {at:?}; its waypoints are {}", self.waypoints.join(", ")))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrailError {
    /// Monty cannot compile the script: the SyntaxError traceback, with the script's line numbers.
    #[error("{0}")]
    Syntax(String),
    #[error("{0}")]
    Meta(String),
}

/// Checks that a script compiles and starts with its literal meta, its waypoints, if any, defined and its top level
/// then holding only definitions, and returns the meta. No type checking.
pub fn check(script: &str) -> Result<Meta, TrailError> {
    compile(script).map_err(|e| TrailError::Syntax(render(e)))?;
    meta(script).map_err(TrailError::Meta)
}

/// The program entering `script` at `waypoint`: its top level, then the waypoint awaited with `args`, whose result
/// is the program's. Refused unless the script's meta lists `waypoint`.
pub(crate) fn enter(script: &str, waypoint: &str) -> Result<String, String> {
    meta(script)?.entry(Some(waypoint))?;
    // On a line past the script's, so its lines keep their numbers in tracebacks.
    Ok(format!("{script}\nawait {waypoint}(args)\n"))
}

/// Compiles the prelude and the script as one program.
pub(crate) fn compile(script: &str) -> Result<MontyRun, MontyException> {
    let inputs = INPUTS.iter().map(|&name| name.to_owned()).collect();
    MontyRun::new(format!("{PRELUDE}{script}"), SCRIPT_NAME, inputs, CompileOptions::default())
}

/// The traceback of an exception, its frames numbered as in the script, the prelude's named apart.
pub(crate) fn render(exception: MontyException) -> String {
    let offset = u32::try_from(PRELUDE.lines().count()).unwrap_or(u32::MAX);
    let frames = exception
        .traceback()
        .iter()
        .map(|frame| {
            let mut frame: StackFrame = frame.clone();
            if frame.filename == SCRIPT_NAME {
                if frame.start.line > offset {
                    frame.start.line -= offset;
                    frame.end.line = frame.end.line.saturating_sub(offset);
                } else {
                    PRELUDE_NAME.clone_into(&mut frame.filename);
                }
            }
            frame
        })
        .collect();
    let (kind, message) = (exception.exc_type(), exception.message().map(str::to_owned));
    MontyException::with_traceback(kind, message, frames).to_string().trim_end().to_owned()
}

fn meta(script: &str) -> Result<Meta, String> {
    const SHAPE: &str = r#"a trail starts with `meta = {"name": "...", "description": "...", "phases": ["..."]}`"#;
    let parsed = ruff_python_parser::parse_module(script).map_err(|e| e.to_string())?;
    let body = &parsed.syntax().body;
    let Some(Stmt::Assign(assign)) = body.first() else {
        return Err(SHAPE.to_owned());
    };
    let [Expr::Name(target)] = assign.targets.as_slice() else {
        return Err(SHAPE.to_owned());
    };
    let Expr::Dict(dict) = assign.value.as_ref() else {
        return Err(SHAPE.to_owned());
    };
    if target.id.as_str() != "meta" {
        return Err(SHAPE.to_owned());
    }
    let (mut name, mut description, mut phases, mut waypoints) = (None, None, None, Vec::new());
    for item in &dict.items {
        let Some(Expr::StringLiteral(key)) = &item.key else {
            return Err(format!("{SHAPE}: its keys are literal strings"));
        };
        match key.value.to_str() {
            "name" => name = Some(string(&item.value, "name")?),
            "description" => description = Some(string(&item.value, "description")?),
            "phases" => phases = Some(strings(&item.value, "phases")?),
            "waypoints" => waypoints = strings(&item.value, "waypoints")?,
            other => return Err(format!("meta has no key {other:?}: {SHAPE}")),
        }
    }
    let (Some(name), Some(description), Some(phases)) = (name, description, phases) else {
        return Err(format!("meta lacks a key: {SHAPE}"));
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!("meta's name {name:?} is not made of letters, digits, '-' and '_'"));
    }
    if !waypoints.is_empty() {
        self::waypoints(script, body, &waypoints)?;
    }
    Ok(Meta { name, description, phases, waypoints })
}

/// Checks a trail's waypoints: each the name of a top-level `async def` taking one argument, the state, at the top
/// level of a script holding nothing a run entering it at a waypoint should not run again: its meta, imports,
/// definitions and constants.
fn waypoints(script: &str, body: &[Stmt], waypoints: &[String]) -> Result<(), String> {
    if waypoints.len() > MAX_WAYPOINTS {
        return Err(format!("meta lists {} waypoints; a trail has at most {MAX_WAYPOINTS}", waypoints.len()));
    }
    for (at, waypoint) in waypoints.iter().enumerate() {
        let mut chars = waypoint.chars();
        let named = chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !waypoint.starts_with("__");
        if !named {
            return Err(format!(
                "meta's waypoint {waypoint:?} is no Python name of letters, digits and '_' not starting with a digit or \"__\""
            ));
        }
        if waypoints[..at].contains(waypoint) {
            return Err(format!("meta lists waypoint {waypoint:?} twice"));
        }
        let defined = body.iter().rev().find_map(|stmt| match stmt {
            Stmt::FunctionDef(def) if def.name.as_str() == waypoint => Some(def),
            _ => None,
        });
        match defined {
            None => return Err(format!("meta's waypoint {waypoint:?} names no top-level async def")),
            Some(def) if !def.is_async => {
                return Err(format!("meta's waypoint {waypoint:?} names a def, not an async def"));
            }
            Some(def) if !takes_one(def) => {
                return Err(format!(
                    "waypoint {waypoint}() takes one argument, the state, as `async def {waypoint}(state)`"
                ));
            }
            Some(_) => {}
        }
    }
    let awaits = |stmt: &Stmt| {
        let mut visitor = AwaitVisitor::default();
        visitor.visit_stmt(stmt);
        visitor.seen_await
    };
    for stmt in &body[1..] {
        let kept = match stmt {
            Stmt::Import(_) | Stmt::ImportFrom(_) | Stmt::FunctionDef(_) | Stmt::Pass(_) => true,
            Stmt::ClassDef(class) => !class.body.iter().any(awaits),
            Stmt::Assign(_) | Stmt::AnnAssign(_) => !awaits(stmt),
            Stmt::Expr(expr) => matches!(expr.value.as_ref(), Expr::StringLiteral(_)),
            _ => false,
        };
        if !kept {
            let line = script[..stmt.start().to_usize()].matches('\n').count() + 1;
            return Err(format!(
                "line {line}: a trail with waypoints holds only its meta, imports, definitions and constants at its \
                 top level, its steps in its waypoints"
            ));
        }
    }
    Ok(())
}

/// Whether a def takes exactly one argument without a default, a positional one.
fn takes_one(def: &StmtFunctionDef) -> bool {
    let parameters = &def.parameters;
    let positional = parameters.posonlyargs.iter().chain(&parameters.args);
    positional.filter(|p| p.default.is_none()).count() == 1 && parameters.kwonlyargs.iter().all(|p| p.default.is_some())
}

fn string(expr: &Expr, key: &str) -> Result<String, String> {
    match expr {
        Expr::StringLiteral(s) => Ok(s.value.to_str().to_owned()),
        _ => Err(format!("meta's {key} is a literal string")),
    }
}

fn strings(expr: &Expr, key: &str) -> Result<Vec<String>, String> {
    let (Expr::List(ruff_python_ast::ExprList { elts, .. }) | Expr::Tuple(ruff_python_ast::ExprTuple { elts, .. })) =
        expr
    else {
        return Err(format!("meta's {key} is a literal list of strings"));
    };
    elts.iter().map(|e| string(e, key)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const META: &str = r#"meta = {"name": "review", "description": "Reviews files", "phases": ["scan", "check"]}
"#;

    #[test]
    fn the_literal_meta_is_read_without_running_the_script() {
        let script = format!("{META}raise ValueError('never run')\n");
        let meta = check(&script).unwrap();
        assert_eq!(meta.name, "review");
        assert_eq!(meta.description, "Reviews files");
        assert_eq!(meta.phases, ["scan", "check"]);
    }

    #[test]
    fn a_script_without_a_literal_meta_first_is_refused() {
        for script in [
            "x = 1\n",
            "import asyncio\nmeta = {\"name\": \"a\", \"description\": \"b\", \"phases\": []}\n",
            "meta = dict(name=\"a\", description=\"b\", phases=[])\n",
            "meta = {\"name\": NAME, \"description\": \"b\", \"phases\": []}\n",
            "meta = {\"name\": \"a\", \"description\": \"b\"}\n",
            "meta = {\"name\": \"a\", \"description\": \"b\", \"phases\": [1]}\n",
            "meta = {\"name\": \"a b\", \"description\": \"b\", \"phases\": []}\n",
            "meta = {\"name\": \"a\", \"description\": \"b\", \"phases\": [], \"extra\": 1}\n",
            "",
        ] {
            assert!(matches!(check(script), Err(TrailError::Meta(_))), "{script:?}: {:?}", check(script));
        }
    }

    #[test]
    fn a_syntax_error_names_the_script_line() {
        let script = format!("{META}\nx = (\n");
        let Err(TrailError::Syntax(error)) = check(&script) else { panic!("{:?}", check(&script)) };
        assert!(error.contains(r#"File "trail.py", line 4"#), "{error}");
        assert!(error.ends_with("SyntaxError: unexpected EOF while parsing"), "{error}");
    }

    const WAYPOINTS: &str = r#"meta = {"name": "fix", "description": "Fixes", "phases": [], "waypoints": ["triage", "patch"]}
"""Triage, then patch."""
import json
from asyncio import gather
LABELS = ["bug", "chore"]
LIMIT: int = 3
class Plan:
    steps = [1, 2]
def label(text):
    return text
async def triage(state):
    return await agent("triage")
async def patch(state, retries=1, *, draft=False):
    pass
pass
"#;

    fn refused(script: &str) -> String {
        match check(script) {
            Err(TrailError::Meta(why)) => why,
            other => panic!("{script}: {other:?}"),
        }
    }

    #[test]
    fn waypoints_are_read_with_the_meta_and_the_first_is_the_start() {
        let meta = check(WAYPOINTS).unwrap();
        assert_eq!(meta.waypoints, ["triage", "patch"]);
        assert_eq!(meta.entry(None), Ok(Some("triage")));
        assert_eq!(meta.entry(Some("patch")), Ok(Some("patch")));
        assert_eq!(
            meta.entry(Some("review")),
            Err("trail fix has no waypoint \"review\"; its waypoints are triage, patch".to_owned())
        );
        let plain = check(&format!("{META}x = 1\n")).unwrap();
        assert!(plain.waypoints.is_empty());
        assert_eq!(plain.entry(None), Ok(None));
        assert!(plain.entry(Some("x")).unwrap_err().contains("is entered only at its start"));
    }

    #[test]
    fn a_waypoint_without_its_async_def_or_listed_twice_or_misnamed_is_refused() {
        let with = |waypoints: &str, defs: &str| {
            format!(
                "meta = {{\"name\": \"a\", \"description\": \"\", \"phases\": [], \"waypoints\": {waypoints}}}\n{defs}"
            )
        };
        let cases = [
            (with(r#"["go"]"#, "async def other(state):\n    pass\n"), "names no top-level async def"),
            (with(r#"["go"]"#, "def go(state):\n    pass\n"), "names a def, not an async def"),
            (with(r#"["go"]"#, "class C:\n    async def go(state):\n        pass\n"), "names no top-level async def"),
            (with(r#"["go"]"#, "async def go():\n    pass\n"), "takes one argument"),
            (with(r#"["go"]"#, "async def go(a, b):\n    pass\n"), "takes one argument"),
            (with(r#"["go"]"#, "async def go(state, *, how):\n    pass\n"), "takes one argument"),
            (with(r#"["go", "go"]"#, "async def go(state):\n    pass\n"), "lists waypoint \"go\" twice"),
            (with(r#""go""#, "async def go(state):\n    pass\n"), "waypoints is a literal list of strings"),
            (with(&format!("{:?}", vec!["w"; MAX_WAYPOINTS + 1]), ""), "a trail has at most 32"),
        ];
        for (script, why) in cases {
            let error = refused(&script);
            assert!(error.contains(why), "{script}: {error}");
        }
        for name in ["", "a b", "1go", "__go", "go-on", "café"] {
            let error = refused(&with(&format!("[{name:?}]"), ""));
            assert!(error.contains("is no Python name"), "{name:?}: {error}");
        }
    }

    #[test]
    fn a_trail_with_waypoints_holds_no_steps_at_its_top_level() {
        for (statement, line) in [
            ("await agent('a')", 4),
            ("x = await agent('a')", 4),
            ("if args:\n    LIMIT = 1", 4),
            ("for i in range(2):\n    pass", 4),
            ("log('hi')", 4),
            ("class C:\n    x = await agent('a')", 4),
        ] {
            let script = WAYPOINTS.replacen("import json\n", &format!("import json\n{statement}\n"), 1);
            let error = refused(&script);
            assert!(
                error.starts_with(&format!("line {line}: a trail with waypoints holds only")),
                "{statement}: {error}"
            );
        }
    }
}
