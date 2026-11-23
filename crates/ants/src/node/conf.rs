//! The text of a nats-server configuration: a string quoted for one ([`quote`]), the entries of any one, a user's own
//! included, read leniently ([`uncommented`], [`entries`], [`unquote`]), and a value of one `ants leaf config` wrote,
//! read back as written ([`value`]).

use std::path::Path;

use anyhow::{Context as _, Result};

/// `text` without its comments: from a `#` outside a quoted string, or a `//` opening a line, to the line's end.
pub(crate) fn uncommented(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for line in text.lines() {
        if !line.trim_start().starts_with("//") {
            let mut quote = None;
            let end = line.char_indices().find_map(|(at, c)| {
                match (quote, c) {
                    (None, '#') => return Some(at),
                    (None, '"' | '\'') => quote = Some(c),
                    (Some(q), c) if q == c => quote = None,
                    _ => {}
                }
                None
            });
            out.push_str(&line[..end.unwrap_or(line.len())]);
        }
        out.push('\n');
    }
    out
}

/// The entries of the nats-server configuration `text`, comments removed, at its top level: each key with its value,
/// the inside of a `{ }` block or a `[ ]` list, else what follows up to a comma or the line's end.
pub(crate) fn entries(text: &str) -> Vec<(&str, &str)> {
    let mut entries = Vec::new();
    let mut rest = text;
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || [',', ';', '}', ']'].contains(&c));
        if rest.is_empty() {
            return entries;
        }
        let key_end = rest.find(|c: char| c.is_whitespace() || [':', '=', '{', '['].contains(&c)).unwrap_or(rest.len());
        let (key, after) = rest.split_at(key_end);
        let after = after.trim_start_matches([' ', '\t']);
        let after = after.strip_prefix([':', '=']).unwrap_or(after).trim_start_matches([' ', '\t']);
        let (value, next) = match after.chars().next() {
            Some(open @ ('{' | '[')) => {
                let end = closing(after, open).unwrap_or(after.len());
                (&after[1..end], &after[(end + 1).min(after.len())..])
            }
            _ => after.split_at(after.find(['\n', ',']).unwrap_or(after.len())),
        };
        entries.push((key, value));
        rest = next;
    }
}

/// Where the bracket opening `text` closes, brackets in quoted strings left out.
fn closing(text: &str, open: char) -> Option<usize> {
    let close = if open == '{' { '}' } else { ']' };
    let (mut depth, mut quote) = (0_usize, None);
    for (at, c) in text.char_indices() {
        match (quote, c) {
            (None, '"' | '\'') => quote = Some(c),
            (Some(q), c) if q == c => quote = None,
            (None, c) if c == open => depth += 1,
            (None, c) if c == close => {
                depth -= 1;
                if depth == 0 {
                    return Some(at);
                }
            }
            _ => {}
        }
    }
    None
}

/// `value` without the quotes and spaces around it.
pub(crate) fn unquote(value: &str) -> &str {
    value.trim().trim_matches(['"', '\''])
}

/// A double-quoted string for the nats-server configuration, which takes JSON escapes, as a TOML basic string does.
pub(crate) fn quote(s: &str) -> String {
    serde_json::to_string(s).expect("a string serializes")
}

/// The quoted value of the first `key: "..."` line of `text`, the configuration at `path` as `ants leaf config`
/// writes one: each such value alone on its line, quoted by [`quote`].
pub(crate) fn value(text: &str, path: &Path, key: &str) -> Result<String> {
    let line = text
        .lines()
        .find_map(|line| line.trim().strip_prefix(key)?.trim_start().strip_prefix(':'))
        .with_context(|| format!("{}: no {key} (written by `ants leaf config`?)", path.display()))?;
    serde_json::from_str(line.trim()).with_context(|| format!("{}: unreadable {key}", path.display()))
}
