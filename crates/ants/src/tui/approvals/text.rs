//! What a tool request asks, as lines for a human to read: an execute request's command, an edit's or a write's path
//! and content with their line breaks, another tool's title and input as indented JSON. The daemon cuts an input at
//! 2,000 characters, ending it with an ellipsis: what of the JSON can be read before the cut shows.

use ants_proto::ApprovalRequest;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use serde_json::{Map, Value};

use super::super::{markdown, style};

/// The keys of an edit's input naming its file.
const PATHS: [&str; 3] = ["file_path", "path", "notebook_path"];

/// The lines of what `request` asks, unwrapped, none holding a line break or a control character.
pub fn asked(request: &ApprovalRequest) -> Vec<Line<'static>> {
    let tool = &request.tool;
    let (input, cut) = match &tool.input {
        Some(input) => match parse(input) {
            Some((value, cut)) => (Some(value), cut),
            None => return raw(&tool.title, input),
        },
        None => (None, false),
    };
    let object = input.as_ref().and_then(Value::as_object);
    let mut lines = match (tool.kind.as_str(), object) {
        ("execute", Some(object)) if let Some(command) = command(object) => {
            let mut lines = text(&command, "");
            lines.extend(object.iter().filter(|(k, _)| k.as_str() != "command").flat_map(|(k, v)| field(k, v)));
            lines
        }
        ("edit" | "write", Some(object)) => {
            let path = PATHS.iter().find_map(|k| object.get(*k)?.as_str().map(str::to_owned));
            let path = path.or_else(|| tool.locations.first().cloned()).unwrap_or_else(|| tool.title.clone());
            let mut lines = vec![Line::styled(clean(&path), Style::new().bold())];
            let rest = object.iter().filter(|(k, _)| !PATHS.contains(&k.as_str()));
            lines.extend(rest.flat_map(|(k, v)| field(k, v)));
            lines
        }
        _ => {
            let mut lines = if tool.title.trim().is_empty() { Vec::new() } else { text(&tool.title, "") };
            if let Some(input) = &input {
                let pretty = serde_json::to_string_pretty(input).unwrap_or_default();
                lines.extend(pretty.lines().map(|l| Line::styled(clean(l), style::dim())));
            }
            lines
        }
    };
    if lines.is_empty() {
        lines.push(Line::raw(clean(&tool.title)));
    }
    if cut {
        lines.push(Line::styled("… the rest of the input was cut at 2,000 characters", style::dim()));
    }
    lines
}

/// `lines` wrapped at words into rows at most `width` columns wide.
pub fn wrapped(lines: &[Line<'static>], width: usize) -> Vec<Line<'static>> {
    let width = width.max(1);
    lines.iter().flat_map(|line| markdown::wrap(&line.spans, width).into_iter().map(Line::from)).collect()
}

/// The command of an execute request's input: a string, or its words.
fn command(input: &Map<String, Value>) -> Option<String> {
    match input.get("command")? {
        Value::String(s) => Some(s.clone()),
        Value::Array(parts) => parts.iter().map(Value::as_str).collect::<Option<Vec<_>>>().map(|p| p.join(" ")),
        _ => None,
    }
}

/// An input's field other than the command or the path: a string on its own lines under its name when it holds a line
/// break or is long, else on one line with its name.
fn field(key: &str, value: &Value) -> Vec<Line<'static>> {
    let name = clean(key);
    match value {
        Value::String(s) if s.contains('\n') || s.chars().count() > 60 => {
            let mut lines = vec![Line::styled(format!("{name}:"), style::dim())];
            lines.extend(text(s, "  "));
            lines
        }
        Value::String(s) => vec![Line::styled(format!("{name}: {}", clean(s)), style::dim())],
        Value::Array(_) | Value::Object(_) => {
            let mut lines = vec![Line::styled(format!("{name}:"), style::dim())];
            let pretty = serde_json::to_string_pretty(value).unwrap_or_default();
            lines.extend(pretty.lines().map(|l| Line::styled(format!("  {}", clean(l)), style::dim())));
            lines
        }
        other => vec![Line::styled(format!("{name}: {other}"), style::dim())],
    }
}

/// `text`'s lines, each begun with `indent`.
fn text(text: &str, indent: &str) -> Vec<Line<'static>> {
    text.lines().map(|l| Line::from(vec![Span::raw(indent.to_owned()), Span::raw(clean(l))])).collect()
}

/// An input that is no JSON, under the tool's title.
fn raw(title: &str, input: &str) -> Vec<Line<'static>> {
    let mut lines = if title.trim().is_empty() { Vec::new() } else { text(title, "") };
    lines.extend(input.lines().map(|l| Line::styled(clean(l), style::dim())));
    lines
}

/// `text` without control characters, a tab as four spaces.
fn clean(text: &str) -> String {
    let mut clean = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\t' => clean.push_str("    "),
            c if c.is_control() => {}
            c => clean.push(c),
        }
    }
    clean
}

/// `input` as JSON, and whether the daemon cut it: cut, what it held before the cut, its last value closed where it was
/// cut or left out.
fn parse(input: &str) -> Option<(Value, bool)> {
    if let Ok(value) = serde_json::from_str(input) {
        return Some((value, false));
    }
    let head = input.strip_suffix('…')?;
    let ends = head.char_indices().map(|(at, _)| at).rev().take(64);
    std::iter::once(head.len()).chain(ends).find_map(|end| {
        let head = &head[..end];
        serde_json::from_str(&format!("{head}{}", closing(head))).ok().map(|value| (value, true))
    })
}

/// What closes the JSON `head` breaks off: the string it is in, then the arrays and objects open.
fn closing(head: &str) -> String {
    let (mut open, mut string, mut escaped) = (Vec::new(), false, false);
    for c in head.chars() {
        match (string, escaped, c) {
            (true, true, _) => escaped = false,
            (true, false, '\\') => escaped = true,
            (_, _, '"') => string = !string,
            (false, _, '{') => open.push('}'),
            (false, _, '[') => open.push(']'),
            (false, _, '}' | ']') => _ = open.pop(),
            _ => {}
        }
    }
    let mut closing = if string { "\"".to_owned() } else { String::new() };
    closing.extend(open.iter().rev());
    closing
}

#[cfg(test)]
mod tests {
    use ants_proto::Verdict;

    use super::*;
    use crate::test_support::live::request;

    fn shown(kind: &str, input: &str) -> Vec<String> {
        let mut request = request("dev-a", "a title", vec![Verdict::AllowOnce]);
        request.tool.kind = kind.to_owned();
        request.tool.input = Some(input.to_owned());
        asked(&request).iter().map(ToString::to_string).collect()
    }

    #[test]
    fn a_command_shows_once_as_text_and_a_file_with_its_line_breaks() {
        let command = r#"{"command":"cargo build &&\n\tcargo test","description":"Build"}"#;
        assert_eq!(shown("execute", command), ["cargo build &&", "    cargo test", "description: Build"]);
        assert_eq!(shown("execute", r#"{"command":["bash","-lc","ls"]}"#), ["bash -lc ls"]);
        let write = r#"{"file_path":"src/a.rs","content":"fn main() {\n    run();\n}\n"}"#;
        assert_eq!(shown("edit", write), ["src/a.rs", "content:", "  fn main() {", "      run();", "  }"]);
        assert_eq!(shown("fetch", r#"{"url":"https://x"}"#), ["a title", "{", r#"  "url": "https://x""#, "}"]);
        assert_eq!(shown("execute", "not json \u{1b}[2J"), ["a title", "not json [2J"], "no control character");
    }

    #[test]
    fn an_input_cut_by_the_daemon_shows_what_came_before_the_cut() {
        let cut = format!(r#"{{"command":"{}"#, "x".repeat(10)) + "…";
        assert_eq!(shown("execute", &cut), ["xxxxxxxxxx", "… the rest of the input was cut at 2,000 characters"]);
        let cut = r#"{"file_path":"a.rs","content":"one\ntwo\u00"#.to_owned() + "…";
        assert_eq!(shown("edit", &cut)[..4], ["a.rs", "content:", "  one", "  two"]);
        let cut = r#"{"a":"x","b"#.to_owned() + "…";
        assert_eq!(shown("other", &cut)[1..4], ["{", r#"  "a": "x""#, "}"]);
    }
}
