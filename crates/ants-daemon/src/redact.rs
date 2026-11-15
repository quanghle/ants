//! Redacting secrets from what leaves the daemon: replies, streamed status, tool requests and messages the ant sends.
//!
//! A [`Redactor`] replaces each secret it finds with [`MARK`]:
//!
//! - the values of the environment the harness gets: those of the ant's `env` and of its harness's `env`, and of the
//!   daemon's own environment variables, whose name looks secret ([`secret_name`]: a part such as `TOKEN`, `SECRET`,
//!   `PASSWORD`, `KEY`, `AUTH` or `CREDENTIALS`), and the other values of the ant's and harness's `env` that look like
//!   a token: letters and digits, no space, slash or file extension. A value shorter than [`MIN_VALUE`] characters,
//!   or one that is a path (starting with `/`, `~`, `.` or a drive letter), is left alone, so `1`, `true`, `info`, a
//!   file name, a host or a directory never mangle the text.
//! - common secret patterns, wherever they come from: PEM private key blocks, `Bearer` and `Basic` credentials, GitHub,
//!   GitLab, Slack, OpenAI and Anthropic, AWS, Google, Stripe, npm, Hugging Face and Telegram bot tokens, Slack and
//!   Discord webhook URLs, NATS NKey seeds, JWTs, the password of a URL (`scheme://user:password@host`), and the value
//!   given to a name that looks secret (`API_KEY=…`, `"password": "…"`, `--token …`) when it holds a digit.

use std::borrow::Cow;
use std::fmt;
use std::ops::Range;

use ants_proto::{StatusUpdate, ToolCall};
use serde_json::Value;

/// What a secret is replaced with.
pub const MARK: &str = "[redacted]";
/// Shortest environment value redacted, in characters.
pub const MIN_VALUE: usize = 8;

/// Finds and replaces secrets in text. [`Redactor::default`] knows the common patterns only.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Redactor {
    off: bool,
    /// Environment values to redact, longest first.
    values: Vec<String>,
}

impl fmt::Debug for Redactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Redactor").field("off", &self.off).field("values", &self.values.len()).finish()
    }
}

impl Redactor {
    /// Redact nothing.
    pub fn off() -> Self {
        Self { off: true, values: Vec::new() }
    }

    /// The same, also redacting the values of `vars` whose name looks secret ([`secret_name`]) or that look like a
    /// token ([`token_like`]), short ones and paths aside ([`MIN_VALUE`]).
    #[must_use]
    pub fn with_env<K: AsRef<str>, V: AsRef<str>>(self, vars: impl IntoIterator<Item = (K, V)>) -> Self {
        self.with_values(vars, |name, value| secret_name(name) || token_like(value))
    }

    /// The same, also redacting the values of `vars` whose name looks secret ([`secret_name`]), short ones and paths
    /// aside.
    #[must_use]
    pub fn with_secret_env<K: AsRef<str>, V: AsRef<str>>(self, vars: impl IntoIterator<Item = (K, V)>) -> Self {
        self.with_values(vars, |name, _| secret_name(name))
    }

    fn with_values<K: AsRef<str>, V: AsRef<str>>(
        mut self,
        vars: impl IntoIterator<Item = (K, V)>,
        secret: impl Fn(&str, &str) -> bool,
    ) -> Self {
        let values = vars.into_iter().map(|(k, v)| (k, v.as_ref().trim().to_owned()));
        let values = values.filter(|(k, v)| worth(v) && secret(k.as_ref(), v)).map(|(_, v)| v);
        self.values.extend(values);
        self.sort();
        self
    }

    /// The same, also redacting the daemon's own environment variables whose name looks secret.
    #[must_use]
    pub fn with_process_env(self) -> Self {
        let vars = std::env::vars_os().filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?)));
        self.with_secret_env(vars)
    }

    fn sort(&mut self) {
        self.values.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        self.values.dedup();
    }

    /// `text` with every secret found replaced with [`MARK`].
    pub fn redact<'a>(&self, text: &'a str) -> Cow<'a, str> {
        if self.off {
            return Cow::Borrowed(text);
        }
        let mut found = Vec::new();
        for value in &self.values {
            found.extend(text.match_indices(value.as_str()).map(|(at, v)| at..at + v.len()));
        }
        patterns(text, &mut found);
        if found.is_empty() {
            return Cow::Borrowed(text);
        }
        found.sort_by_key(|r| (r.start, std::cmp::Reverse(r.end)));
        let mut out = String::with_capacity(text.len());
        let mut at = 0;
        for range in found {
            if range.end <= at {
                continue;
            }
            if range.start >= at {
                out.push_str(&text[at..range.start]);
                out.push_str(MARK);
            }
            at = range.end;
        }
        out.push_str(&text[at..]);
        Cow::Owned(out)
    }

    /// Redact `text` in place.
    pub fn redact_string(&self, text: &mut String) {
        if let Cow::Owned(redacted) = self.redact(text) {
            *text = redacted;
        }
    }

    /// Redact every string of `value`, keys aside.
    pub fn redact_json(&self, value: &mut Value) {
        match value {
            Value::String(s) => self.redact_string(s),
            Value::Array(items) => items.iter_mut().for_each(|v| self.redact_json(v)),
            Value::Object(map) => map.values_mut().for_each(|v| self.redact_json(v)),
            _ => {}
        }
    }

    /// Redact the text of a tool call: its title, files and input.
    pub fn redact_tool(&self, tool: &mut ToolCall) {
        self.redact_string(&mut tool.title);
        tool.locations.iter_mut().for_each(|l| self.redact_string(l));
        if let Some(input) = &mut tool.input {
            self.redact_string(input);
        }
    }

    /// Redact the text of a status update.
    pub fn redact_status(&self, update: &mut StatusUpdate) {
        match update {
            StatusUpdate::Message { text } | StatusUpdate::Thought { text } => self.redact_string(text),
            StatusUpdate::Tool { title: Some(title), .. } => self.redact_string(title),
            StatusUpdate::Plan { steps } => steps.iter_mut().for_each(|s| self.redact_string(&mut s.content)),
            _ => {}
        }
    }

    /// How much of streamed `text` may go out before more arrives without splitting a secret: up to its last
    /// whitespace, and short of a private key block not ended yet.
    pub fn settled(&self, text: &str) -> usize {
        if self.off {
            return text.len();
        }
        let mut settled = text.rfind(char::is_whitespace).map_or(0, |at| at + 1);
        let mut from = 0;
        while let Some(key) = private_key_at(text, from) {
            if key.end == text.len() {
                settled = settled.min(key.start);
                break;
            }
            from = key.end;
        }
        settled
    }
}

/// Whether an environment variable's value is worth redacting: long enough, and no path.
fn worth(value: &str) -> bool {
    let b = value.as_bytes();
    let path = value.starts_with(['/', '~', '.', '\\'])
        || (b.len() > 2 && b[0].is_ascii_alphabetic() && b[1] == b':' && matches!(b[2], b'\\' | b'/'));
    value.chars().count() >= MIN_VALUE && !path
}

/// Whether an environment value looks like a token rather than a setting: it has letters and digits, and no space,
/// slash or file extension (`.db`, `.com`), so neither a sentence, a version, a file nor a host.
fn token_like(value: &str) -> bool {
    let b = value.as_bytes();
    let extension = value
        .rsplit_once('.')
        .is_some_and(|(_, ext)| (1..=4).contains(&ext.len()) && ext.bytes().all(|c| c.is_ascii_lowercase()));
    b.iter().any(u8::is_ascii_digit)
        && b.iter().any(u8::is_ascii_alphabetic)
        && !b.iter().any(|&c| c.is_ascii_whitespace() || c == b'/' || c == b'\\')
        && !extension
}

/// Whether an environment variable or field name looks like it holds a secret: one of its parts, split at anything
/// but letters and digits, is a word such as `TOKEN`, `SECRET`, `PASSWORD`, `KEY`, `AUTH` or `CREDENTIALS`, or ends
/// with `TOKEN`, `SECRET`, `PASSWORD` or `APIKEY` (`accessToken`).
pub fn secret_name(name: &str) -> bool {
    const WORDS: [&str; 18] = [
        "TOKEN",
        "TOKENS",
        "SECRET",
        "SECRETS",
        "PASSWORD",
        "PASSWORDS",
        "PASSWD",
        "PASSPHRASE",
        "PASS",
        "APIKEY",
        "KEY",
        "KEYS",
        "AUTH",
        "CREDENTIAL",
        "CREDENTIALS",
        "CREDS",
        "SEED",
        "PAT",
    ];
    const ENDINGS: [&str; 4] = ["TOKEN", "SECRET", "PASSWORD", "APIKEY"];
    name.split(|c: char| !c.is_ascii_alphanumeric()).any(|part| {
        let part = part.to_ascii_uppercase();
        WORDS.contains(&part.as_str()) || ENDINGS.iter().any(|end| part.ends_with(end))
    })
}

fn word(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Whether a token may start at `at`: no letter, digit or `_` just before.
fn starts_word(b: &[u8], at: usize) -> bool {
    at == 0 || !word(b[at - 1])
}

fn run_while(b: &[u8], from: usize, ok: impl Fn(u8) -> bool) -> usize {
    b[from..].iter().position(|c| !ok(*c)).map_or(b.len(), |n| from + n)
}

fn alnum(c: u8) -> bool {
    c.is_ascii_alphanumeric()
}

fn alnum_dash(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'-' || c == b'_'
}

/// A token prefix and what follows it: at least `min` bytes of `chars`, a digit among them if `digit`.
struct Prefixed {
    prefix: &'static str,
    min: usize,
    chars: fn(u8) -> bool,
    digit: bool,
}

const PREFIXED: &[Prefixed] = &[
    Prefixed { prefix: "ghp_", min: 20, chars: alnum, digit: false },
    Prefixed { prefix: "gho_", min: 20, chars: alnum, digit: false },
    Prefixed { prefix: "ghu_", min: 20, chars: alnum, digit: false },
    Prefixed { prefix: "ghs_", min: 20, chars: alnum, digit: false },
    Prefixed { prefix: "ghr_", min: 20, chars: alnum, digit: false },
    Prefixed { prefix: "github_pat_", min: 20, chars: alnum_dash, digit: false },
    Prefixed { prefix: "glpat-", min: 20, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xoxb-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xoxp-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xoxa-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xoxr-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xoxs-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xoxe-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "xapp-", min: 10, chars: alnum_dash, digit: false },
    Prefixed { prefix: "sk-", min: 20, chars: alnum_dash, digit: true },
    Prefixed { prefix: "sk_live_", min: 16, chars: alnum, digit: false },
    Prefixed { prefix: "rk_live_", min: 16, chars: alnum, digit: false },
    Prefixed { prefix: "AKIA", min: 16, chars: |c| c.is_ascii_uppercase() || c.is_ascii_digit(), digit: false },
    Prefixed { prefix: "ASIA", min: 16, chars: |c| c.is_ascii_uppercase() || c.is_ascii_digit(), digit: false },
    Prefixed { prefix: "AIza", min: 30, chars: alnum_dash, digit: false },
    Prefixed { prefix: "npm_", min: 30, chars: alnum, digit: false },
    Prefixed { prefix: "hf_", min: 30, chars: alnum, digit: false },
    Prefixed { prefix: "pypi-AgE", min: 40, chars: alnum_dash, digit: false },
];

/// Add the ranges of `text` the common secret patterns find to `found`.
fn patterns(text: &str, found: &mut Vec<Range<usize>>) {
    let b = text.as_bytes();
    let mut at = 0;
    while at < b.len() {
        let next = match b[at] {
            b'-' => private_key(text, at, found),
            b':' => url_password(b, at, found).max(telegram(b, at, found)),
            b'e' => jwt(b, at, found),
            b'S' => nkey_seed(b, at, found),
            b'b' | b'B' => bearer(b, at, found),
            _ => None,
        };
        let next = next.or_else(|| prefixed(b, at, found)).or_else(|| assignment(text, at, found));
        // Every pattern starts a word, or at a character no word holds: none starts within this one.
        at = next.unwrap_or_else(|| run_while(b, at, word)).max(at + 1);
    }
    webhook_urls(text, found);
}

fn prefixed(b: &[u8], at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    if !starts_word(b, at) {
        return None;
    }
    PREFIXED.iter().find_map(|p| {
        let rest = at + p.prefix.len();
        if !b[at..].starts_with(p.prefix.as_bytes()) {
            return None;
        }
        let end = run_while(b, rest, p.chars);
        let long = end - rest >= p.min;
        (long && (!p.digit || b[rest..end].iter().any(u8::is_ascii_digit))).then(|| {
            found.push(at..end);
            end
        })
    })
}

/// The block of a PEM private key starting at `from` or after: from its `-----BEGIN … PRIVATE KEY…-----` to the end
/// of its `-----END …-----` line, else to the end of the text.
fn private_key_at(text: &str, from: usize) -> Option<Range<usize>> {
    let mut search = from;
    loop {
        let start = search + text[search..].find("-----BEGIN ")?;
        let label_end = text[start + 11..].find("-----").map(|n| start + 11 + n);
        let line_end = text[start..].find('\n').map_or(text.len(), |n| start + n);
        if let Some(label_end) = label_end.filter(|e| *e <= line_end)
            && text[start..label_end].contains("PRIVATE KEY")
        {
            let end = match text[label_end..].find("-----END ") {
                Some(n) => {
                    let end = label_end + n + 9;
                    text[end..].find("-----").map_or(text.len(), |n| end + n + 5)
                }
                None => text.len(),
            };
            return Some(start..end);
        }
        search = start + 11;
    }
}

fn private_key(text: &str, at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    let rest = &text[at..];
    if rest.starts_with("-----BEGIN ") {
        let key = private_key_at(text, at).filter(|k| k.start == at)?;
        let end = key.end;
        found.push(key);
        return Some(end);
    }
    // The end of a block whose start is not in the text, as in streamed text sent before it.
    if rest.starts_with("-----END ") && !text[..at].contains("-----BEGIN ") {
        let line = &rest[..rest.find('\n').unwrap_or(rest.len())];
        if line.contains("PRIVATE KEY") {
            let end = at + 9 + rest[9..].find("-----").map_or(line.len() - 9, |n| n + 5);
            found.push(0..end);
            return Some(end);
        }
    }
    None
}

/// The password of a URL whose `://` ends at `at` + 2: `scheme://user:password@host`.
fn url_password(b: &[u8], at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    if !b[at..].starts_with(b"://") {
        return None;
    }
    let start = at + 3;
    let authority = run_while(b, start, |c| !c.is_ascii_whitespace() && !b"/?#\"'<>`[]{}|\\".contains(&c));
    let host = b[start..authority].iter().rposition(|c| *c == b'@')? + start;
    let colon = b[start..host].iter().position(|c| *c == b':')? + start;
    (host > colon + 1).then(|| {
        found.push(colon + 1..host);
        host
    })
}

/// A Telegram bot token whose `:` is at `at`: 8 to 10 digits, a colon and at least 30 more characters.
fn telegram(b: &[u8], at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    let digits = b[..at].iter().rev().take_while(|c| c.is_ascii_digit()).count();
    let start = at - digits;
    if !(8..=10).contains(&digits) || !starts_word(b, start) {
        return None;
    }
    let end = run_while(b, at + 1, alnum_dash);
    (end - at > 30).then(|| {
        found.push(start..end);
        end
    })
}

/// A JWT: three dot-separated base64url parts, the first two starting `eyJ`.
fn jwt(b: &[u8], at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    if !b[at..].starts_with(b"eyJ") || !starts_word(b, at) {
        return None;
    }
    let mut end = at;
    for part in 0..3 {
        if part > 0 {
            if b.get(end) != Some(&b'.') || (part == 1 && !b[end + 1..].starts_with(b"eyJ")) {
                return None;
            }
            end += 1;
        }
        let part_end = run_while(b, end, alnum_dash);
        if part_end - end < 10 {
            return None;
        }
        end = part_end;
    }
    found.push(at..end);
    Some(end)
}

/// A NATS NKey seed: `S` and a key type, 58 base32 characters in all.
fn nkey_seed(b: &[u8], at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    if !starts_word(b, at) || !b.get(at + 1).is_some_and(|c| b"UAONCXP".contains(c)) {
        return None;
    }
    let end = run_while(b, at, |c| c.is_ascii_uppercase() || (b'2'..=b'7').contains(&c));
    (end - at == 58 && b.get(end).is_none_or(|c| !word(*c))).then(|| {
        found.push(at..end);
        end
    })
}

/// The credentials after `Bearer` or `Basic`: at least 8 characters, with a digit or in an `Authorization` header
/// (or, after `Basic`, ending with `=` padding).
fn bearer(b: &[u8], at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    let scheme = [&b"bearer"[..], b"basic"]
        .into_iter()
        .find(|s| b.len() >= at + s.len() && b[at..at + s.len()].eq_ignore_ascii_case(s) && starts_word(b, at))?;
    let gap = run_while(b, at + scheme.len(), |c| c == b' ' || c == b'\t');
    if gap == at + scheme.len() {
        return None;
    }
    let end = run_while(b, gap, |c| c.is_ascii_alphanumeric() || b"-_.~+/=".contains(&c));
    let token = &b[gap..end];
    let header = String::from_utf8_lossy(&b[at.saturating_sub(24)..at]).to_ascii_lowercase().contains("authorization");
    let secret = token.len() >= 8
        && (header || token.iter().any(u8::is_ascii_digit) || (scheme == b"basic" && token.ends_with(b"=")));
    secret.then(|| {
        found.push(gap..end);
        end
    })
}

/// Webhook URLs whose path is the secret: Slack's and Discord's.
fn webhook_urls(text: &str, found: &mut Vec<Range<usize>>) {
    let b = text.as_bytes();
    for host in ["hooks.slack.com/services/", "discord.com/api/webhooks/", "discordapp.com/api/webhooks/"] {
        for (at, _) in text.match_indices(host) {
            let start = at + host.len();
            let end = run_while(b, start, |c| c.is_ascii_alphanumeric() || b"-_/".contains(&c));
            if end - start >= 10 {
                found.push(start..end);
            }
        }
    }
}

/// The value given to a name that looks secret starting at `at`: `NAME=value`, `NAME: value`, `"name": "value"` or
/// `--name value`, when the value is at least [`MIN_VALUE`] long, holds a digit and is not all digits.
fn assignment(text: &str, at: usize, found: &mut Vec<Range<usize>>) -> Option<usize> {
    let b = text.as_bytes();
    if !starts_word(b, at) || !(word(b[at]) || b[at] == b'-') || (at > 0 && b[at - 1] == b'-') {
        return None;
    }
    let name_end = run_while(b, at, |c| word(c) || c == b'-' || c == b'.');
    let name = &text[at..name_end];
    if !secret_name(name) {
        return None;
    }
    let mut i = run_while(b, name_end, |c| c == b'"' || c == b'\'');
    i = run_while(b, i, |c| c == b' ');
    match b.get(i) {
        Some(b'=' | b':') => i += 1,
        Some(_) if name.starts_with('-') && i > name_end => {}
        _ => return None,
    }
    i = run_while(b, i, |c| c == b' ');
    i = run_while(b, i, |c| c == b'"' || c == b'\'');
    let end = run_while(b, i, |c| !c.is_ascii_whitespace() && !b"\"'`,;&)}]".contains(&c));
    let value = &text[i..end];
    let secret =
        worth(value) && value.bytes().any(|c| c.is_ascii_digit()) && !value.bytes().all(|c| c.is_ascii_digit());
    secret.then(|| {
        found.push(i..end);
        end
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redact(text: &str) -> String {
        Redactor::default().redact(text).into_owned()
    }

    #[test]
    fn env_values_are_redacted_unless_short_or_paths() {
        let r = Redactor::default().with_env([
            ("TRACKER_TOKEN", "s3cr3t-value"),
            ("DEBUG", "1"),
            ("MODE", "true"),
            ("DB", "/srv/ants/tasks.db"),
            ("CACHE", "~/cache/dir"),
            ("BD_DB", "unmanaged.db"),
            ("HOST", "db1.example.com"),
            ("RELEASE", "release-2026"),
            ("GREETING", "hello there 42"),
            ("ID", "a1b2c3d4e5f6"),
            ("TRACKER_PASSWORD", "correct horse battery"),
        ]);
        assert_eq!(
            r.redact("token s3cr3t-value, db /srv/ants/tasks.db, debug 1 true, unmanaged.db on db1.example.com"),
            "token [redacted], db /srv/ants/tasks.db, debug 1 true, unmanaged.db on db1.example.com"
        );
        assert_eq!(
            r.redact("release-2026: hello there 42 as a1b2c3d4e5f6, correct horse battery"),
            "[redacted]: hello there 42 as [redacted], [redacted]"
        );
        let process = Redactor::default().with_secret_env([
            ("OPENAI_API_KEY", "abcdefghijkl"),
            ("GIT_AUTHOR_NAME", "Jane Developer"),
            ("SSH_AUTH_SOCK", "/private/tmp/agent.sock"),
            ("PWD", "workspace-dir"),
        ]);
        assert_eq!(
            process.redact("abcdefghijkl by Jane Developer in workspace-dir"),
            "[redacted] by Jane Developer in workspace-dir"
        );
        assert_eq!(Redactor::off().with_env([("A", "s3cr3t-value")]).redact("s3cr3t-value"), "s3cr3t-value");
    }

    #[test]
    fn tokens_are_redacted() {
        let cases = [
            ("push with ghp_0123456789abcdefghijABCDEFGHIJ012345 now", "push with [redacted] now"),
            ("github_pat_11ABCDEFG0123456789_abcdefghijklmnop", "[redacted]"),
            ("glpat-abcdefghij0123456789", "[redacted]"),
            ("slack xoxb-1234567890-abcdefghij", "slack [redacted]"),
            ("key sk-proj-abcdefghij0123456789XYZ", "key [redacted]"),
            ("aws AKIAIOSFODNN7EXAMPLE id", "aws [redacted] id"),
            ("AIzaSyA-1234567890abcdefghijklmnopqrstu", "[redacted]"),
            ("bot 123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw0 ok", "bot [redacted] ok"),
            ("seed SUAM3DXLB2BV6IWW5N2RCYGS7GDXJUGJH6KXMYZZUWEDPMB4UBN4XXWSQE", "seed [redacted]"),
            ("Authorization: Bearer abc.def-123_456", "Authorization: Bearer [redacted]"),
            ("Authorization: Basic dXNlcjpwYXNz", "Authorization: Basic [redacted]"),
            ("jwt eyJhbGciOiJI.eyJzdWIiOiIx.SflKxwRJSMeKKF2QT4", "jwt [redacted]"),
            ("https://hooks.slack.com/services/T000/B000/XXXXXXXX", "https://hooks.slack.com/services/[redacted]"),
        ];
        for (text, redacted) in cases {
            assert_eq!(redact(text), redacted, "{text}");
        }
    }

    #[test]
    fn url_passwords_and_secret_assignments_are_redacted() {
        assert_eq!(
            redact("git clone https://bot:hunter22@git.example/x.git"),
            "git clone https://bot:[redacted]@git.example/x.git"
        );
        assert_eq!(
            redact("nats://alice:pw@hub:4222 and nats://hub:4222"),
            "nats://alice:[redacted]@hub:4222 and nats://hub:4222"
        );
        assert_eq!(redact("export API_TOKEN=abc123def456"), "export API_TOKEN=[redacted]");
        assert_eq!(
            redact(r#"{"password": "hunter2-hunter2", "user": "bob"}"#),
            r#"{"password": "[redacted]", "user": "bob"}"#
        );
        assert_eq!(redact("tool --token 0f9e8d7c6b5a run"), "tool --token [redacted] run");
    }

    #[test]
    fn ordinary_text_is_left_alone() {
        let text = "Bearer authentication uses the token: see docs. max_tokens = 100000000, \
                    the API_KEY=process.env.API_KEY, task-1234 and risk-assessment-2024-report, \
                    https://example.com/a:b@c, eyJ only, SUMMARY, 12345678: done, ghp_short, desk-1234567890abcdefghij";
        assert_eq!(redact(text), text);
    }

    #[test]
    fn private_keys_are_redacted_even_in_part() {
        let key = "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXk\nAAAA\n-----END OPENSSH PRIVATE KEY-----";
        assert_eq!(redact(&format!("key:\n{key}\nthat's it")), "key:\n[redacted]\nthat's it");
        assert_eq!(redact("start\n-----BEGIN RSA PRIVATE KEY-----\nMIIE"), "start\n[redacted]");
        assert_eq!(redact("MIIE\nAAAA\n-----END RSA PRIVATE KEY-----\nrest"), "[redacted]\nrest");
        let cert = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----";
        assert_eq!(redact(cert), cert);
    }

    #[test]
    fn streamed_text_settles_at_whitespace_and_outside_keys() {
        let r = Redactor::default();
        assert_eq!(r.settled("pushing with ghp_0123"), "pushing with ".len());
        assert_eq!(r.settled("a\n-----BEGIN EC PRIVATE KEY-----\nMHc "), 2);
        let ended = "-----BEGIN EC PRIVATE KEY-----\nM\n-----END EC PRIVATE KEY----- done ";
        assert_eq!(r.settled(ended), ended.len());
        assert_eq!(Redactor::off().settled("abc"), 3);
    }

    #[test]
    fn json_strings_are_redacted() {
        let r = Redactor::default().with_env([("T", "s3cr3t-value")]);
        let mut input = serde_json::json!({ "command": ["curl", "-u", "x:s3cr3t-value"], "s3cr3t-value": 1 });
        r.redact_json(&mut input);
        assert_eq!(input, serde_json::json!({ "command": ["curl", "-u", "x:[redacted]"], "s3cr3t-value": 1 }));
    }
}
