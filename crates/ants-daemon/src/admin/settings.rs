//! [`AdminOp::ReadSettings`]: read the ant's `[[ant]]` table, and the `[harnesses]` table, from the config file the
//! daemon serves, for an admin to see and change ([`Settings`]). Every human reads an order's outcome, so a string
//! holding a secret, as the daemon's redaction finds them ([`Redactor::with_env`] of the ant's and harnesses' `env`),
//! reads `[redacted]` there, and the field holding it, and those holding that one, carry the [`digest`] of their value
//! instead, which a change of the field expects.

use std::collections::BTreeSet;
use std::path::Path;

#[cfg(doc)]
use ants_proto::AdminOp;
use ants_proto::{AdminOrder, AdminOutcome, HARNESSES_FIELD, Hidden, Settings, UserId};
use anyhow::Result;
use serde_json::Value;

use super::{NO_CONFIG, Orders, applied, refused};
use crate::config::edit::{Edit, digest};
use crate::redact::Redactor;

pub(super) fn apply(orders: &Orders, order: &AdminOrder) -> AdminOutcome {
    let Some(config) = &orders.config else { return refused(order, NO_CONFIG) };
    match read(&config.path, &order.ant) {
        Ok(settings) => applied(order, Some(serde_json::to_value(settings).expect("settings serialize"))),
        Err(e) => refused(order, &format!("{e:#}")),
    }
}

fn read(path: &Path, ant: &UserId) -> Result<Settings> {
    settings(&Edit::open(path)?, ant)
}

/// `ant`'s settings in `file`, secrets hidden.
fn settings(file: &Edit, ant: &UserId) -> Result<Settings> {
    let mut fields = file.table(ant)?;
    if let Some(harnesses) = file.get(ant, &[HARNESSES_FIELD.to_owned()])? {
        fields.insert(HARNESSES_FIELD.to_owned(), harnesses);
    }
    let read = Settings { fields: fields.clone(), hidden: Vec::new() };
    let redactor = redactor(&read);
    let mut secrets = BTreeSet::new();
    for (key, value) in &mut fields {
        hide(&redactor, &mut vec![key.clone()], value, &mut secrets);
    }
    let holding: BTreeSet<Vec<String>> =
        secrets.iter().flat_map(|field| (1..=field.len()).map(|n| field[..n].to_vec())).collect();
    let hidden =
        holding.into_iter().filter_map(|field| Some(Hidden { digest: digest(read.get(&field)?), field })).collect();
    Ok(Settings { fields, hidden })
}

/// The daemon's redaction, knowing the values of the ant's `env` and of every harness's.
fn redactor(read: &Settings) -> Redactor {
    let env = |value: Option<&Value>| -> Vec<(String, String)> {
        let vars = value.and_then(Value::as_object).into_iter().flatten();
        vars.filter_map(|(name, value)| Some((name.clone(), value.as_str()?.to_owned()))).collect()
    };
    let mut vars = env(read.fields.get("env"));
    let harnesses = read.fields.get(HARNESSES_FIELD).and_then(Value::as_object).into_iter().flatten();
    for (_, harness) in harnesses {
        vars.extend(env(harness.get("env")));
    }
    Redactor::default().with_env(vars)
}

/// Redact the secrets `value`, the field at `path`, holds, adding to `secrets` the fields that held one: those of
/// tables, else the field itself.
fn hide(redactor: &Redactor, path: &mut Vec<String>, value: &mut Value, secrets: &mut BTreeSet<Vec<String>>) {
    if let Value::Object(map) = value {
        for (key, value) in map {
            path.push(key.clone());
            hide(redactor, path, value, secrets);
            path.pop();
        }
        return;
    }
    let before = value.clone();
    redactor.redact_json(value);
    if *value != before {
        secrets.insert(path.clone());
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const FILE: &str = r#"harness = "copilot"

[harnesses.custom]
command = "custom-acp"
args = ["--token", "ghp_0123456789abcdefghijklmnopqrstuvwxyz"]

[[ant]]
name = "dev-a"
workdir = "."
model = "gpt-5"
env = { RUST_LOG = "info", API_TOKEN = "s3cr3t-value-42", BD_DB = "/srv/dev-a/bd.db" }
"#;

    fn f(path: &str) -> Vec<String> {
        path.split('.').map(str::to_owned).collect()
    }

    #[test]
    fn a_read_hides_secrets_and_a_change_expecting_their_digest_applies() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut file = Edit::of(FILE, &dir.join("config.toml"), dir.parent().unwrap()).unwrap();
        let dev_a: UserId = "dev-a".parse().unwrap();
        let read = settings(&file, &dev_a).unwrap();
        assert_eq!(read.get(&f("model")), Some(&json!("gpt-5")));
        assert_eq!(read.get(&f("env.RUST_LOG")), Some(&json!("info")));
        assert_eq!(read.get(&f("env.BD_DB")), Some(&json!("/srv/dev-a/bd.db")), "a path is no secret");
        assert_eq!(read.get(&f("env.API_TOKEN")), Some(&json!("[redacted]")));
        assert_eq!(read.get(&f("harnesses.custom.command")), Some(&json!("custom-acp")));
        assert_eq!(read.get(&f("harnesses.custom.args")), Some(&json!(["--token", "[redacted]"])));
        let hidden: Vec<String> = read.hidden.iter().map(|h| h.field.join(".")).collect();
        assert_eq!(hidden, ["env", "env.API_TOKEN", "harnesses", "harnesses.custom", "harnesses.custom.args"]);
        let text = serde_json::to_string(&read).unwrap();
        assert!(!text.contains("s3cr3t") && !text.contains("ghp_"), "{text}");

        let token = read.change(f("env.API_TOKEN"), Some(json!("n3w-s3cr3t-value")));
        let command = read.change(f("harnesses.custom.command"), Some(json!("other-acp")));
        assert!(file.change(&dev_a, &[token.clone(), command]).unwrap());
        let text = file.text();
        assert!(text.contains("API_TOKEN = \"n3w-s3cr3t-value\"") && text.contains("command = \"other-acp\""));
        file.check().unwrap();

        let stale = file.change(&dev_a, &[read.change(f("env.API_TOKEN"), None)]).unwrap_err().to_string();
        assert_eq!(
            stale,
            "env.API_TOKEN changed since the order was sent: it no longer holds the value read, hidden as a secret"
        );
        assert!(!file.change(&dev_a, &[token]).unwrap(), "holding the new value already");
    }
}
