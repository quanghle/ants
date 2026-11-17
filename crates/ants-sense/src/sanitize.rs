//! The sanitizer every sense write passes first (requirement ants-x39hk.7.2).
//!
//! [`sanitize`] refuses a text over its size limit, measured on the text as given, before anything is stripped or
//! redacted: the size an ant sees is the size checked. A redaction marker can be longer than the secret it replaces,
//! so a sanitized text can exceed its limit by that growth.
//!
//! It then strips characters that hide or reorder text:
//!
//! - control characters but `\n` and `\t`; `\r\n`, a lone `\r` and U+2028/U+2029 become `\n`;
//! - bidirectional controls: U+061C, U+200E, U+200F, U+202A–U+202E and U+2066–U+2069;
//! - invisible characters: the soft hyphen U+00AD, U+180E, the zero-width space U+200B, U+2060–U+2064, the byte order
//!   mark U+FEFF, U+FFF9–U+FFFB and the tag characters U+E0000–U+E007F (which smuggle invisible ASCII);
//! - the zero-width joiners U+200C and U+200D unless both their neighbours are non-ASCII, where they shape emoji and
//!   scripts such as Persian: next to ASCII they only split a word or a token.
//!
//! A single-line text (a title) also has its newlines and tabs turned into spaces.
//!
//! Last, it replaces known secret shapes with `[redacted:<kind>]`, [`SecretKind`] naming the kinds: PEM private key
//! blocks, NATS creds blocks and NKey seeds, JWTs, AWS access key ids and secret access keys, GitHub, Slack,
//! Anthropic, OpenAI-style, Google and Stripe keys, bearer tokens and `Authorization` header values, the password of a
//! `scheme://user:password@host` connection string (keeping the rest), and the value of an assignment (`NAME=value`,
//! `export NAME="value"`, `name: value`, `"name": "value"`, `--name=value`) whose name looks secret: a word of it
//! containing `TOKEN`, `SECRET`, `PASSWORD`, `PASSWD`, `PASSPHRASE` or `CREDENTIAL`, or ending in `API_KEY`,
//! `APIKEY`, `PRIVATE_KEY`, `ACCESS_KEY`, … or, all upper case, in `_KEY` or `_PAT`. Placeholders (`<token>`,
//! `${VAR}`, `$VAR`, `***`, empty values), code (`os.getenv(…)`, `settings.SECRET_KEY`, type annotations) and names
//! about secrets rather than holding one (`MAX_TOKENS`, `PASSWORD_FILE`, `TOKEN_URL`) stay as they are.
//!
//! Sanitizing a sanitized text changes nothing.

use std::fmt;
use std::ops::Range;
use std::sync::LazyLock;

use regex::{Captures, Regex};

/// The most a text may hold, in characters or in bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    Chars(usize),
    Bytes(usize),
}

impl Limit {
    fn max(self) -> usize {
        match self {
            Self::Chars(max) | Self::Bytes(max) => max,
        }
    }

    fn measure(self, text: &str) -> usize {
        match self {
            Self::Chars(_) => text.chars().count(),
            Self::Bytes(_) => text.len(),
        }
    }

    fn unit(self) -> &'static str {
        match self {
            Self::Chars(_) => "characters",
            Self::Bytes(_) => "bytes",
        }
    }
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.max(), self.unit())
    }
}

/// Whether a text keeps its line breaks and tabs (a body) or has them turned into spaces (a title).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lines {
    Single,
    Multi,
}

/// A kind of secret, named in its `[redacted:<kind>]` marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SecretKind {
    PrivateKey,
    NatsCreds,
    NkeySeed,
    Jwt,
    AwsAccessKey,
    AwsSecretKey,
    GithubToken,
    SlackToken,
    AnthropicKey,
    OpenaiKey,
    GoogleApiKey,
    StripeKey,
    BearerToken,
    Authorization,
    ConnectionString,
    Secret,
}

impl SecretKind {
    pub const fn name(self) -> &'static str {
        match self {
            Self::PrivateKey => "private-key",
            Self::NatsCreds => "nats-creds",
            Self::NkeySeed => "nkey-seed",
            Self::Jwt => "jwt",
            Self::AwsAccessKey => "aws-access-key",
            Self::AwsSecretKey => "aws-secret-key",
            Self::GithubToken => "github-token",
            Self::SlackToken => "slack-token",
            Self::AnthropicKey => "anthropic-key",
            Self::OpenaiKey => "openai-key",
            Self::GoogleApiKey => "google-api-key",
            Self::StripeKey => "stripe-key",
            Self::BearerToken => "bearer-token",
            Self::Authorization => "authorization",
            Self::ConnectionString => "connection-string",
            Self::Secret => "secret",
        }
    }
}

impl fmt::Display for SecretKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How many secrets of a kind were redacted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Redaction {
    pub kind: SecretKind,
    pub count: usize,
}

/// A text fit to store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sanitized {
    pub text: String,
    /// The secrets redacted, by kind, in [`SecretKind`] order.
    pub redactions: Vec<Redaction>,
    /// How many characters were stripped.
    pub stripped: usize,
}

/// A text over its size limit, `size` measured in the limit's unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refused {
    pub limit: Limit,
    pub size: usize,
}

impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "text of {} {} exceeds the limit of {}", self.size, self.limit.unit(), self.limit)
    }
}

impl std::error::Error for Refused {}

/// Sanitizes a single-line text, a title.
pub fn sanitize_title(text: &str, limit: Limit) -> Result<Sanitized, Refused> {
    sanitize(text, Lines::Single, limit)
}

/// Sanitizes a multi-line text, a body.
pub fn sanitize_body(text: &str, limit: Limit) -> Result<Sanitized, Refused> {
    sanitize(text, Lines::Multi, limit)
}

/// Refuses `text` over `limit`, then strips it and redacts its secrets (see the module documentation).
pub fn sanitize(text: &str, lines: Lines, limit: Limit) -> Result<Sanitized, Refused> {
    let size = limit.measure(text);
    if size > limit.max() {
        return Err(Refused { limit, size });
    }
    let (text, stripped) = strip(text, lines);
    let mut redactions = Vec::new();
    let text = redact(text, &mut redactions);
    redactions.sort_by_key(|r: &Redaction| r.kind);
    Ok(Sanitized { text, redactions, stripped })
}

fn strip(text: &str, lines: Lines) -> (String, usize) {
    let mut out = String::with_capacity(text.len());
    let mut stripped = 0;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        let c = match c {
            '\r' => {
                if chars.as_str().starts_with('\n') {
                    chars.next();
                }
                '\n'
            }
            '\u{2028}' | '\u{2029}' => '\n',
            c => c,
        };
        let keep = match c {
            '\n' | '\t' => {
                out.push(if lines == Lines::Multi { c } else { ' ' });
                continue;
            }
            '\u{200C}' | '\u{200D}' => {
                let before = out.chars().rev().find(|&c| !joiner(c));
                let after = chars.clone().find(|&c| !joiner(c) && !hidden(c));
                before.is_some_and(|c| !c.is_ascii()) && after.is_some_and(|c| !c.is_ascii())
            }
            c => !hidden(c),
        };
        if keep {
            out.push(c);
        } else {
            stripped += 1;
        }
    }
    (out, stripped)
}

fn joiner(c: char) -> bool {
    matches!(c, '\u{200C}' | '\u{200D}')
}

/// Whether `c` is always stripped: a control (but `\n`, `\t` and `\r`), bidirectional or invisible character.
fn hidden(c: char) -> bool {
    (c.is_control() && !matches!(c, '\n' | '\t' | '\r'))
        || matches!(c,
            '\u{061C}' | '\u{200E}' | '\u{200F}' | '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}'
            | '\u{00AD}' | '\u{180E}' | '\u{200B}' | '\u{2060}'..='\u{2064}' | '\u{FEFF}' | '\u{FFF9}'..='\u{FFFB}'
            | '\u{E0000}'..='\u{E007F}')
}

/// The part of a match to redact, and as what.
type Hit = Option<(Range<usize>, SecretKind)>;

/// What a rule's match redacts: the whole match, or what a function picks of it (`None` keeping it).
enum Pick {
    Whole(SecretKind),
    With(fn(&Captures<'_>, &str) -> Hit),
}

struct Rule {
    re: Regex,
    pick: Pick,
}

fn rule(re: &str, pick: Pick) -> Rule {
    Rule { re: Regex::new(re).expect("sanitizer pattern"), pick }
}

/// The rules, most specific first: a later rule leaves alone what an earlier one redacted, its marker reading as a
/// placeholder. Compiled on the first write, they take about 300 KB with their search caches; case-insensitive parts
/// are ASCII-only (`(?i-u:…)`), Unicode case folding growing a pattern several times over.
static RULES: LazyLock<Vec<Rule>> = LazyLock::new(|| {
    use Pick::{Whole, With};
    use SecretKind::*;
    vec![
        rule(
            r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY(?: BLOCK)?-----|\z)",
            Whole(PrivateKey),
        ),
        rule(
            r"(?s)-{5,}BEGIN (?:NATS [A-Z]+ JWT|[A-Z]+ NKEY SEED)-{5,}.*?(?:-{5,}END (?:NATS [A-Z]+ JWT|[A-Z]+ NKEY SEED)-{5,}|\z)",
            Whole(NatsCreds),
        ),
        rule(r"(?-u:\b)S[UAONCX][A-Z2-7]{56}(?-u:\b)", Whole(NkeySeed)),
        rule(r"(?-u:\b)eyJ[A-Za-z0-9_-]{5,}\.eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]*", Whole(Jwt)),
        rule(r"(?-u:\b)(?:AKIA|ASIA)[A-Z0-9]{16}(?-u:\b)", Whole(AwsAccessKey)),
        rule(
            r#"(?-u:\b)(?i-u:aws_?secret_?access_?key)["']?[ \t]*[:=][ \t]*["']?([A-Za-z0-9/+=]+)"#,
            With(|c, _| c.get(1).filter(|key| key.len() == 40).map(|key| (key.range(), AwsSecretKey))),
        ),
        rule(r"(?-u:\b)(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})", Whole(GithubToken)),
        rule(r"(?-u:\b)xox[abposr]-[A-Za-z0-9-]{10,}", Whole(SlackToken)),
        rule(r"(?-u:\b)sk-[A-Za-z0-9_-]{20,}", With(sk_key)),
        rule(r"(?-u:\b)AIza[A-Za-z0-9_-]{35}", Whole(GoogleApiKey)),
        rule(r"(?-u:\b)[rs]k_live_[A-Za-z0-9]{10,}", Whole(StripeKey)),
        rule(
            r#"(?-u:\b)(?i-u:authorization)["']?[ \t]*[:=][ \t]*["']?((?i-u:bearer|basic|token|bot|digest))[ \t]+([^\s"'\\,;]+)"#,
            With(authorization),
        ),
        rule(r"(?-u:\b)(?i-u:bearer)[ \t]+([A-Za-z0-9._~+/=-]+)", With(bearer)),
        rule(r"(?-u:\b)[A-Za-z][A-Za-z0-9+.-]*://[^\s:/@]*:([^\s/?#]*)@", With(connection_string)),
        rule(
            r#"(?-u:\b)([A-Za-z_][A-Za-z0-9_.-]*)["'`]?[ \t]*(=>?|:)([ \t]*)(?:"([^"\n]*)"|'([^'\n]*)'|([^\s"'`,;}]*))"#,
            With(assignment),
        ),
    ]
});

fn redact(mut text: String, redactions: &mut Vec<Redaction>) -> String {
    for rule in RULES.iter() {
        let mut out = String::new();
        let mut copied = 0;
        for caps in rule.re.captures_iter(&text) {
            let hit = match rule.pick {
                Pick::Whole(kind) => caps.get(0).map(|m| (m.range(), kind)),
                Pick::With(pick) => pick(&caps, &text),
            };
            let Some((range, kind)) = hit else { continue };
            out.push_str(&text[copied..range.start]);
            out.push_str("[redacted:");
            out.push_str(kind.name());
            out.push(']');
            copied = range.end;
            match redactions.iter_mut().find(|r| r.kind == kind) {
                Some(r) => r.count += 1,
                None => redactions.push(Redaction { kind, count: 1 }),
            }
        }
        if copied > 0 {
            out.push_str(&text[copied..]);
            text = out;
        }
    }
    text
}

fn sk_key(caps: &Captures<'_>, _: &str) -> Hit {
    let key = caps.get(0)?;
    // Real keys mix in digits; a long kebab-case word after `sk-` does not.
    if !key.as_str().bytes().any(|b| b.is_ascii_digit()) {
        return None;
    }
    let kind = if key.as_str().starts_with("sk-ant-") { SecretKind::AnthropicKey } else { SecretKind::OpenaiKey };
    Some((key.range(), kind))
}

fn authorization(caps: &Captures<'_>, _: &str) -> Hit {
    let value = caps.get(2)?;
    if placeholder(value.as_str()) {
        return None;
    }
    let bearer = caps.get(1)?.as_str().eq_ignore_ascii_case("bearer");
    Some((value.range(), if bearer { SecretKind::BearerToken } else { SecretKind::Authorization }))
}

fn bearer(caps: &Captures<'_>, _: &str) -> Hit {
    let token = caps.get(1)?;
    let t = token.as_str();
    // Prose ("bearer authentication") has neither the length nor the digits of a token.
    let tokenish = t.len() >= 20 || (t.len() >= 8 && t.bytes().any(|b| b.is_ascii_digit()));
    (tokenish && !placeholder(t)).then(|| (token.range(), SecretKind::BearerToken))
}

fn connection_string(caps: &Captures<'_>, _: &str) -> Hit {
    let password = caps.get(1)?;
    (!placeholder(password.as_str())).then(|| (password.range(), SecretKind::ConnectionString))
}

fn assignment(caps: &Captures<'_>, text: &str) -> Hit {
    let name = caps.get(1)?.as_str();
    let colon = &caps[2] == ":";
    if !secret_name(name) {
        return None;
    }
    let (value, quoted) = match (caps.get(4), caps.get(5)) {
        (Some(v), _) | (_, Some(v)) => (v, true),
        _ => (caps.get(6)?, false),
    };
    let v = value.as_str();
    if placeholder(v) || v.starts_with('=') || v.starts_with('>') {
        return None;
    }
    if !quoted {
        if code(v) {
            return None;
        }
        // `name: value` is a key-value line only when the value stands alone on it, unlike prose ("Password: rotate
        // monthly").
        if colon && (caps[3].is_empty() || !rest_of_line_empty(&text[value.end()..])) {
            return None;
        }
    }
    Some((value.range(), SecretKind::Secret))
}

fn rest_of_line_empty(rest: &str) -> bool {
    let line = rest.split('\n').next().unwrap_or_default();
    let line = line.trim_start_matches([',', ';', '}', ']', ' ', '\t']);
    line.is_empty() || line.starts_with('#') || line.starts_with("//")
}

/// Words that make a name about secrets rather than holding one: `MAX_TOKENS`, `PASSWORD_FILE`, `TOKEN_URL`.
const NOT_SECRET: &[&str] = &[
    "BUDGET",
    "COUNT",
    "DIR",
    "ENDPOINT",
    "ENV",
    "EXPIRATION",
    "EXPIRE",
    "EXPIRES",
    "EXPIRY",
    "FIELD",
    "FILE",
    "HEADER",
    "HINT",
    "ID",
    "LABEL",
    "LEN",
    "LENGTH",
    "LIMIT",
    "MAX",
    "MIN",
    "NAME",
    "PARAM",
    "PATH",
    "PATTERN",
    "POLICY",
    "PROMPT",
    "REGEX",
    "SIZE",
    "TIMEOUT",
    "TOKENS",
    "TTL",
    "TYPE",
    "URI",
    "URL",
    "USAGE",
    "VAR",
];

const SECRET_WORDS: &[&str] = &["TOKEN", "SECRET", "PASSWORD", "PASSWD", "PASSPHRASE", "CREDENTIAL"];

/// What makes a `…_KEY` name secret in any case (`api_key`), where `primary_key` or `sort_key` is not.
const KEY_QUALIFIERS: &[&str] =
    &["ACCESS", "API", "AUTH", "CLIENT", "ENCRYPTION", "LICENSE", "MASTER", "PRIVATE", "SIGNING"];

fn secret_name(name: &str) -> bool {
    let words = words(name);
    if words.iter().any(|w| NOT_SECRET.contains(&w.as_str()) || w.starts_with("TOKENIZ")) {
        return false;
    }
    if words.iter().any(|w| SECRET_WORDS.iter().any(|s| w.contains(s))) {
        return true;
    }
    let env_style = !name.bytes().any(|b| b.is_ascii_lowercase());
    match words.as_slice() {
        [.., last] if last == "APIKEY" => true,
        [.., qualifier, last] if last == "KEY" => env_style || KEY_QUALIFIERS.contains(&qualifier.as_str()),
        [_, .., last] if last == "PAT" => env_style,
        _ => false,
    }
}

/// The upper-case words of a name, split at `_`, `-`, `.` and camelCase humps.
fn words(name: &str) -> Vec<String> {
    let mut words = vec![String::new()];
    let mut lower = false;
    for c in name.chars() {
        if matches!(c, '_' | '-' | '.') {
            words.push(String::new());
            lower = false;
            continue;
        }
        if c.is_ascii_uppercase() && lower {
            words.push(String::new());
        }
        lower = c.is_ascii_lowercase();
        words.last_mut().expect("a word").push(c.to_ascii_uppercase());
    }
    words.retain(|w| !w.is_empty());
    words
}

/// Whether a value stands for a secret rather than holding one, or is an earlier rule's marker.
fn placeholder(v: &str) -> bool {
    let v = v.trim();
    v.is_empty()
        || v.starts_with('$')
        || v.starts_with("{{")
        || v.starts_with('%')
        || (v.starts_with('<') && v.ends_with('>'))
        || v.contains("[redacted:")
        || v.chars().all(|c| matches!(c, '*' | 'x' | 'X' | '.' | '…' | '-' | '_' | '#'))
        || ["null", "none", "nil", "true", "false", "undefined", "yes", "no", "on", "off"]
            .iter()
            .any(|w| v.eq_ignore_ascii_case(w))
}

/// Whether an unquoted value reads as code: a call, an index, a type, or a dotted path like `settings.SECRET_KEY`.
fn code(v: &str) -> bool {
    const TYPES: &[&str] = &[
        "any",
        "bool",
        "boolean",
        "bytes",
        "Bytes",
        "int",
        "number",
        "object",
        "Option",
        "Secret",
        "SecretString",
        "str",
        "string",
        "String",
        "unknown",
    ];
    let ident = |s: &str| {
        s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
    };
    v.contains(['(', ')', '[', ']', '{', '<'])
        || v.starts_with('&')
        || TYPES.contains(&v)
        || (v.contains('.') && v.split('.').all(ident))
}

#[cfg(test)]
mod tests;
