//! A trail's script: its literal meta, read without running it, and the prelude every script runs after.

use monty::MontyRun;
use monty_types::{CompileOptions, MontyException, StackFrame};
use ruff_python_ast::{Expr, Stmt};
use serde::{Deserialize, Serialize};

/// The file name a trail's frames carry in tracebacks.
pub const SCRIPT_NAME: &str = "trail.py";
const PRELUDE_NAME: &str = "<prelude>";

/// The trail API, defined before the script runs. The `__ants_*` externals are answered by the worker: agent, human
/// and trail suspend the run on a future, phase and log return at once.
const PRELUDE: &str = r#"import asyncio

async def agent(prompt, ant=None, capability=None, schema=None, label=None):
    request = {"prompt": prompt, "ant": ant, "capability": capability, "schema": schema, "label": label}
    return await __ants_agent({k: v for k, v in request.items() if v is not None})

async def human(questions):
    return await __ants_human({"questions": questions})

async def trail(name, args=None):
    request = {"name": name, "args": args}
    return await __ants_trail({k: v for k, v in request.items() if v is not None})

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
pub(crate) const INPUTS: [&str; 6] =
    ["args", "__ants_agent", "__ants_human", "__ants_trail", "__ants_phase", "__ants_log"];

/// A trail's description, the literal `meta = {"name": …, "description": …, "phases": […]}` it starts with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    /// Letters, digits, `-` and `_`.
    pub name: String,
    pub description: String,
    pub phases: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TrailError {
    /// Monty cannot compile the script: the SyntaxError traceback, with the script's line numbers.
    #[error("{0}")]
    Syntax(String),
    #[error("{0}")]
    Meta(String),
}

/// Checks that a script compiles and starts with its literal meta, and returns the meta. No type checking.
pub fn check(script: &str) -> Result<Meta, TrailError> {
    compile(script).map_err(|e| TrailError::Syntax(render(e)))?;
    meta(script).map_err(TrailError::Meta)
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
    let Some(Stmt::Assign(assign)) = parsed.syntax().body.first() else {
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
    let (mut name, mut description, mut phases) = (None, None, None);
    for item in &dict.items {
        let Some(Expr::StringLiteral(key)) = &item.key else {
            return Err(format!("{SHAPE}: its keys are literal strings"));
        };
        match key.value.to_str() {
            "name" => name = Some(string(&item.value, "name")?),
            "description" => description = Some(string(&item.value, "description")?),
            "phases" => {
                let (Expr::List(ruff_python_ast::ExprList { elts, .. })
                | Expr::Tuple(ruff_python_ast::ExprTuple { elts, .. })) = &item.value
                else {
                    return Err("meta's phases is a literal list of strings".to_owned());
                };
                phases = Some(elts.iter().map(|e| string(e, "phases")).collect::<Result<Vec<_>, _>>()?);
            }
            other => return Err(format!("meta has no key {other:?}: {SHAPE}")),
        }
    }
    let (Some(name), Some(description), Some(phases)) = (name, description, phases) else {
        return Err(format!("meta lacks a key: {SHAPE}"));
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
        return Err(format!("meta's name {name:?} is not made of letters, digits, '-' and '_'"));
    }
    Ok(Meta { name, description, phases })
}

fn string(expr: &Expr, key: &str) -> Result<String, String> {
    match expr {
        Expr::StringLiteral(s) => Ok(s.value.to_str().to_owned()),
        _ => Err(format!("meta's {key} is a literal string")),
    }
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
}
