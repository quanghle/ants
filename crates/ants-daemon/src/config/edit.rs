//! Editing an ant's `[[ant]]` table in the config file in place, as admin orders do ([`crate::admin`]): the text
//! around stays as it is, comments and formatting included, and the result must load ([`super::parse`]) before it is
//! written, beside the file and renamed over it ([`super::write`]).
//!
//! A field is named by the keys leading to it from the ant's table: `["model"]`, `["env", "RUST_LOG"]`. Values are
//! JSON, as orders carry them: strings, numbers, booleans, arrays and objects, which become inline tables; a field
//! holding a datetime reads as its text.

use std::path::{Path, PathBuf};

use ants_proto::{FieldChange, UserId};
use anyhow::{Context as _, Result, bail};
use serde_json::{Map, Number, Value};
use toml_edit::{DocumentMut, InlineTable, Item, TableLike};

use super::Loaded;

/// The config file at a path, as read, being edited.
#[derive(Debug, Clone)]
pub struct Edit {
    path: PathBuf,
    home: PathBuf,
    doc: DocumentMut,
}

impl Edit {
    /// The config file at `path`, read for editing.
    pub fn open(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        Self::of(&text, &std::path::absolute(path)?, &super::home_dir()?)
    }

    /// `text`, the config file at `path` with `home` as the home directory `~` stands for.
    pub fn of(text: &str, path: &Path, home: &Path) -> Result<Self> {
        let doc = text.parse().with_context(|| format!("parsing config {}", path.display()))?;
        Ok(Self { path: path.to_owned(), home: home.to_owned(), doc })
    }

    /// The value `field` of `ant`'s `[[ant]]` table holds; none when absent. Fails when the file lists no such ant.
    pub fn get(&self, ant: &UserId, field: &[String]) -> Result<Option<Value>> {
        let mut table = self.ant(ant)?;
        let Some((last, parents)) = field.split_last() else { bail!("no field named") };
        for key in parents {
            match table.get(key).and_then(Item::as_table_like) {
                Some(inner) => table = inner,
                None => return Ok(None),
            }
        }
        Ok(table.get(last).map(item_json).filter(|value| !value.is_null()))
    }

    /// Set `field` of `ant`'s `[[ant]]` table to `value`, or remove it with none. A value replaced keeps the spacing
    /// and comment around it; a field added goes after the table's last key, inside inline tables made for its
    /// missing parents.
    pub fn set(&mut self, ant: &UserId, field: &[String], value: Option<&Value>) -> Result<()> {
        let path = self.path.display().to_string();
        let (last, parents) = field.split_last().context("no field named")?;
        let mut table = self.ant_mut(ant)?;
        for (i, key) in parents.iter().enumerate() {
            if value.is_none() && !table.contains_key(key) {
                return Ok(());
            }
            let current = table;
            let item = current.entry(key).or_insert(Item::Value(InlineTable::new().into()));
            table = item
                .as_table_like_mut()
                .with_context(|| format!("config {path}: ant {ant}: {} is no table", field[..=i].join(".")))?;
        }
        let Some(value) = value else {
            table.remove(last);
            return Ok(());
        };
        let mut new = toml_value(value).with_context(|| field.join("."))?;
        match table.get_mut(last) {
            Some(Item::Value(old)) => {
                *new.decor_mut() = old.decor().clone();
                *old = new;
            }
            Some(Item::Table(old)) if new.is_inline_table() => {
                let toml_edit::Value::InlineTable(inline) = new else { unreachable!("checked") };
                let mut replaced = inline.into_table();
                *replaced.decor_mut() = old.decor().clone();
                replaced.set_position(old.position());
                *old = replaced;
            }
            Some(old) => *old = Item::Value(new),
            None => {
                table.insert(last, Item::Value(new));
            }
        }
        Ok(())
    }

    /// Make `changes` to `ant`'s `[[ant]]` table, all or none: each only if its field holds the value it expects, or
    /// already the new one, which it leaves as it is. Returns whether anything changed. Fails naming the first field
    /// holding neither, changing nothing; `name`, which names the ant, is never changed.
    pub fn change(&mut self, ant: &UserId, changes: &[FieldChange]) -> Result<bool> {
        if changes.is_empty() {
            bail!("no field to change");
        }
        let mut make = Vec::with_capacity(changes.len());
        for change in changes {
            let field = change.field.join(".");
            match change.field.first().map(String::as_str) {
                None | Some("") => bail!("a change names no field"),
                Some("name") => bail!("name names the ant: an order does not rename it"),
                Some(_) => {}
            }
            let current = self.get(ant, &change.field)?;
            if same_opt(current.as_ref(), change.value.as_ref()) {
                continue;
            }
            if !same_opt(current.as_ref(), change.expected.as_ref()) {
                bail!(
                    "{field} changed since the order was sent: it holds {}, the order expected {}",
                    shown(current.as_ref()),
                    shown(change.expected.as_ref())
                );
            }
            make.push(change);
        }
        for change in &make {
            self.set(ant, &change.field, change.value.as_ref())?;
        }
        Ok(!make.is_empty())
    }

    /// The file's text as edited.
    pub fn text(&self) -> String {
        self.doc.to_string()
    }

    /// Load the text as edited ([`super::parse`]), as the daemon would on its next reload.
    pub fn check(&self) -> Result<Loaded> {
        super::parse(&self.text(), &self.path, &self.home)
    }

    /// Write the text as edited over the file once it loads ([`Edit::check`]), beside it first ([`super::write`]).
    pub fn save(&self) -> Result<Loaded> {
        let loaded = self.check()?;
        super::write(&self.path, &self.text())?;
        Ok(loaded)
    }

    fn ant(&self, ant: &UserId) -> Result<&dyn TableLike> {
        let named = |t: &dyn TableLike| t.get("name").and_then(Item::as_str) == Some(ant.as_str());
        let found = match self.doc.get("ant") {
            Some(Item::ArrayOfTables(tables)) => tables.iter().find(|t| named(*t)).map(|t| t as &dyn TableLike),
            Some(Item::Value(toml_edit::Value::Array(items))) => items
                .iter()
                .filter_map(toml_edit::Value::as_inline_table)
                .find(|t| named(*t))
                .map(|t| t as &dyn TableLike),
            _ => None,
        };
        found.with_context(|| format!("config {} lists no ant {ant}", self.path.display()))
    }

    fn ant_mut(&mut self, ant: &UserId) -> Result<&mut dyn TableLike> {
        let named = |t: &dyn TableLike| t.get("name").and_then(Item::as_str) == Some(ant.as_str());
        let found = match self.doc.get_mut("ant") {
            Some(Item::ArrayOfTables(tables)) => tables.iter_mut().find(|t| named(*t)).map(|t| t as &mut dyn TableLike),
            Some(Item::Value(toml_edit::Value::Array(items))) => items
                .iter_mut()
                .filter_map(toml_edit::Value::as_inline_table_mut)
                .find(|t| named(*t))
                .map(|t| t as &mut dyn TableLike),
            _ => None,
        };
        found.with_context(|| format!("config {} lists no ant {ant}", self.path.display()))
    }
}

/// A value as a change names it: `"gpt-5"`, `nothing` for none.
fn shown(value: Option<&Value>) -> String {
    value.map_or_else(|| "nothing".to_owned(), Value::to_string)
}

fn same_opt(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => same(a, b),
        (a, b) => a.is_none() && b.is_none(),
    }
}

/// Whether `a` and `b` are the same value, numbers compared by what they stand for: `3` and `3.0` are the same.
fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x == y || x.as_f64().is_some_and(|x| y.as_f64() == Some(x)),
        (Value::Array(x), Value::Array(y)) => x.len() == y.len() && x.iter().zip(y).all(|(x, y)| same(x, y)),
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(key, x)| y.get(key).is_some_and(|y| same(x, y)))
        }
        _ => a == b,
    }
}

fn item_json(item: &Item) -> Value {
    match item {
        Item::None => Value::Null,
        Item::Value(value) => value_json(value),
        Item::Table(table) => table_json(table),
        Item::ArrayOfTables(tables) => Value::Array(tables.iter().map(|t| table_json(t)).collect()),
    }
}

fn table_json(table: &dyn TableLike) -> Value {
    Value::Object(table.iter().map(|(key, item)| (key.to_owned(), item_json(item))).collect::<Map<_, _>>())
}

fn value_json(value: &toml_edit::Value) -> Value {
    use toml_edit::Value as T;
    match value {
        T::String(s) => Value::String(s.value().clone()),
        T::Integer(i) => Value::from(*i.value()),
        T::Float(f) => Number::from_f64(*f.value()).map_or(Value::Null, Value::Number),
        T::Boolean(b) => Value::Bool(*b.value()),
        T::Datetime(d) => Value::String(d.value().to_string()),
        T::Array(items) => Value::Array(items.iter().map(value_json).collect()),
        T::InlineTable(table) => table_json(table),
    }
}

pub(crate) fn toml_value(value: &Value) -> Result<toml_edit::Value> {
    Ok(match value {
        Value::Null => bail!("null is no TOML value: leave the value out to remove the field"),
        Value::Bool(b) => (*b).into(),
        Value::Number(n) => match n.as_i64() {
            Some(i) => i.into(),
            None => n.as_f64().with_context(|| format!("{n} is no TOML number"))?.into(),
        },
        Value::String(s) => s.as_str().into(),
        Value::Array(items) => items.iter().map(toml_value).collect::<Result<toml_edit::Array>>()?.into(),
        Value::Object(map) => {
            let mut table = InlineTable::new();
            for (key, value) in map {
                table.insert(key, toml_value(value).with_context(|| key.clone())?);
            }
            table.into()
        }
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const FILE: &str = r#"harness = "copilot"

# The ants of this machine.
[[ant]]
name = "dev-a"
workdir = "."  # the crate
model = "gpt-5"   # keep it fast
env = { RUST_LOG = "info" }

# Docs, by hand.
[[ant]]
name = "dev-b"
workdir = "."
max_harnesses = 2
"#;

    fn edit(text: &str) -> Edit {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        Edit::of(text, &dir.join("config.toml"), dir.parent().unwrap()).unwrap()
    }

    fn f(path: &str) -> Vec<String> {
        path.split('.').map(str::to_owned).collect()
    }

    fn change(field: &str, expected: Option<Value>, value: Option<Value>) -> FieldChange {
        FieldChange { field: f(field), expected, value }
    }

    fn p(s: &str) -> UserId {
        s.parse().unwrap()
    }

    #[test]
    fn fields_change_in_place_keeping_the_text_around() {
        let mut file = edit(FILE);
        let changes = [
            change("model", Some(json!("gpt-5")), Some(json!("claude-sonnet"))),
            change("env.RUST_LOG", Some(json!("info")), None),
            change("env.TERM", None, Some(json!("dumb"))),
            change("max_harnesses", None, Some(json!(3))),
            change("caps.daily", None, None),
        ];
        assert!(file.change(&p("dev-a"), &changes).unwrap());
        let text = file.text();
        assert!(text.contains("model = \"claude-sonnet\"   # keep it fast\n"), "{text}");
        assert!(text.contains("env = { TERM = \"dumb\" }\nmax_harnesses = 3\n\n# Docs, by hand.\n"), "{text}");
        assert!(text.starts_with("harness = \"copilot\"\n\n# The ants of this machine.\n"), "{text}");
        assert!(text.ends_with("name = \"dev-b\"\nworkdir = \".\"\nmax_harnesses = 2\n"), "{text}");
        let loaded = file.check().unwrap();
        let dev_a = loaded.ants.iter().find(|a| a.name == p("dev-a")).unwrap();
        assert_eq!((dev_a.model.as_deref(), dev_a.max_harnesses), (Some("claude-sonnet"), 3));
        assert_eq!(file.get(&p("dev-a"), &f("env")).unwrap(), Some(json!({ "TERM": "dumb" })));

        assert!(!file.change(&p("dev-a"), &changes).unwrap(), "applied again, it changes nothing");
        assert_eq!(file.text(), text);
    }

    #[test]
    fn a_field_changed_since_is_named_and_nothing_changes() {
        let mut file = edit(FILE);
        let changes = [
            change("max_harnesses", Some(json!(2.0)), Some(json!(5))),
            change("model", Some(json!("gpt-4")), Some(json!("o3"))),
        ];
        let e = file.change(&p("dev-b"), &changes).unwrap_err().to_string();
        assert_eq!(e, "model changed since the order was sent: it holds nothing, the order expected \"gpt-4\"");
        assert_eq!(file.text(), FILE);

        let rename = file.change(&p("dev-b"), &[change("name", Some(json!("dev-b")), Some(json!("dev-c")))]);
        assert!(rename.unwrap_err().to_string().contains("does not rename"));
        let missing = file.change(&p("dev-z"), &[change("model", None, Some(json!("o3")))]).unwrap_err();
        assert!(missing.to_string().contains("lists no ant dev-z"), "{missing}");
        let nested = file.change(&p("dev-a"), &[change("model.id", None, Some(json!("o3")))]).unwrap_err();
        assert!(nested.to_string().contains("model is no table"), "{nested}");
        assert!(file.change(&p("dev-a"), &[change("model", Some(json!("gpt-5")), Some(Value::Null))]).is_err());
    }

    #[test]
    fn the_loader_judges_the_result() {
        let mut file = edit(FILE);
        assert!(file.change(&p("dev-b"), &[change("max_harnesses", Some(json!(2)), Some(json!("many")))]).unwrap());
        let e = format!("{:#}", file.check().unwrap_err());
        assert!(e.contains("max_harnesses"), "{e}");
    }
}
