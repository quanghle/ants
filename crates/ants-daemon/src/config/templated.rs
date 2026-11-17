//! Ants added from `[[ant_template]]` tables: the templates' checks, the settings an added ant's entry takes from its
//! template, the bounds the added ants keep to together, a manager's request to add one
//! ([`Loaded::check_request`]), and the signer's own copy of the templates ([`signer_templates`]).

use std::collections::BTreeSet;
use std::path::Path;

use ants_proto::UserId;
use anyhow::{Context as _, Result, bail};
use serde::Deserialize;

use super::{Ant, Loaded, home_dir, resolve};
use crate::harness::{self, Harnesses};
use crate::limits::caps::{Members, Pool};
use crate::profile::template::{AntTemplate, REQUEST_KEYS, Request};

/// The keys of an `[[ant]]` entry added from a template besides those of a [`Request`].
const TEMPLATED_KEYS: [&str; 7] = ["name", "template", "added_by", "creds", "nats", "js_domain", "paused"];

impl Loaded {
    /// Check that `manager` may add an ant named `name` from `template` as `request` asks: the template exists and
    /// lists the manager among its `managers`, the manager was not itself added from a template, no ant or schedule
    /// has the name, the template has room under its `max_ants`, and the request keeps within the template's bounds
    /// ([`AntTemplate::resolve`]). Returns the ant as the file would list it once its entry ([`template_entry`]) is
    /// added; the error tells the first bound broken. A relative or `~` workdir in the request is resolved as in the
    /// file.
    ///
    /// [`template_entry`]: super::template_entry
    pub fn check_request(&self, manager: &UserId, name: &UserId, template: &str, request: &Request) -> Result<Ant> {
        let t = self.templates.iter().find(|t| t.name == template).with_context(|| {
            let names: Vec<_> = self.templates.iter().map(|t| t.name.as_str()).collect();
            format!("no ant template {template} (the config file has: {})", list_or_none(&names))
        })?;
        if !t.managers.contains(manager) {
            bail!("{manager} is not among the managers of template {template}");
        }
        if self.ants.iter().any(|a| a.name == *manager && a.template.is_some()) {
            bail!("{manager} was added from a template, and added ants never add ants");
        }
        if self.ants.iter().any(|a| a.name == *name) || self.schedules.iter().any(|s| s.name == *name) {
            bail!("the config file already lists {name}");
        }
        let count = self.ants.iter().filter(|a| a.template.as_deref() == Some(template)).count();
        if count >= t.max_ants {
            bail!("template {template} has {count} ants already, its max_ants");
        }
        let request = Request {
            workdir: request.workdir.as_deref().map(|d| resolve(d, &self.base, &self.home)),
            ..request.clone()
        };
        t.resolve(manager, name, &request)
    }
}

fn list_or_none(items: &[&str]) -> String {
    if items.is_empty() { "none".to_owned() } else { items.join(", ") }
}

/// The file's `templates`, checked, each named once, their workdirs resolved and the harnesses they list defined;
/// one listing none needs the file's top-level `harness` (`default_harness`).
pub(super) fn templates(
    mut templates: Vec<AntTemplate>,
    harnesses: &Harnesses,
    default_harness: bool,
    base: &Path,
    home: &Path,
) -> Result<Vec<AntTemplate>> {
    let mut names = BTreeSet::new();
    for template in &mut templates {
        if !names.insert(template.name.clone()) {
            bail!("ant template {} is listed twice", template.name);
        }
        if template.harnesses.is_empty() && !default_harness {
            let lists = format!("ant template {} lists no harnesses", template.name);
            return Err(anyhow::Error::from(harness::Unset).context(lists));
        }
        template.validate(|dir| resolve(dir, base, home), |name| harnesses.get(name).map(drop))?;
    }
    Ok(templates)
}

/// The `[[ant]]` tables of the file `text`, as written, to tell which keys an entry sets.
pub(super) fn ant_tables(text: &str) -> Result<Vec<toml::Table>> {
    #[derive(Deserialize)]
    struct Tables {
        #[serde(default, rename = "ant")]
        ants: Vec<toml::Table>,
    }
    Ok(toml::from_str::<Tables>(text)?.ants)
}

/// `ant`, written as `table`, with the settings of its template when it names one: the template's, as the entry's
/// keys of a [`Request`] choose within the template's bounds, with the entry's own credentials and connection. Only
/// those keys may be set, so the template bounds every setting.
pub(super) fn from_template(
    ant: Ant,
    table: &toml::Table,
    templates: &[AntTemplate],
    base: &Path,
    home: &Path,
) -> Result<Ant> {
    let name = &ant.name;
    let Some(template) = &ant.template else {
        if ant.added_by.is_some() || !ant.allow_sets.is_empty() {
            bail!("ant {name}: added_by and allow_sets are for an ant added from a template; set template too");
        }
        if ant.workdir.as_os_str().is_empty() {
            bail!("ant {name}: workdir is missing");
        }
        return Ok(ant);
    };
    let t = templates
        .iter()
        .find(|t| t.name == *template)
        .with_context(|| format!("ant {name}: no [[ant_template]] named {template}"))?;
    let manager = ant.added_by.as_ref().with_context(|| {
        format!("ant {name}: added from template {template}, so added_by must name the manager ant that added it")
    })?;
    if let Some(key) =
        table.keys().find(|k| !TEMPLATED_KEYS.contains(&k.as_str()) && !REQUEST_KEYS.contains(&k.as_str()))
    {
        bail!(
            "ant {name}: {key} comes from template {template}; an ant added from a template sets only {}, and what \
             the template lets it choose: {}",
            TEMPLATED_KEYS.join(", "),
            REQUEST_KEYS.join(", ")
        );
    }
    let chosen: toml::Table =
        table.iter().filter(|(k, _)| REQUEST_KEYS.contains(&k.as_str())).map(|(k, v)| (k.clone(), v.clone())).collect();
    let mut request: Request = toml::Value::Table(chosen).try_into().with_context(|| format!("ant {name}"))?;
    request.workdir = request.workdir.map(|dir| resolve(&dir, base, home));
    let resolved = t.resolve(manager, name, &request).with_context(|| format!("ant {name}"))?;
    Ok(Ant { creds: ant.creds, nats: ant.nats, js_domain: ant.js_domain, paused: ant.paused, ..resolved })
}

/// Check the ants added from templates against what the templates allow together: no more of a template's than its
/// `max_ants`, and none a manager, as added ants never add ants. Hold those of a template with pool caps to them.
pub(super) fn check_added(ants: &mut [Ant], templates: &[AntTemplate]) -> Result<()> {
    let added: BTreeSet<UserId> = ants.iter().filter(|a| a.template.is_some()).map(|a| a.name.clone()).collect();
    for ant in ants.iter() {
        if let Some(manager) = ant.added_by.as_ref().filter(|m| added.contains(*m)) {
            bail!(
                "ant {}: added_by names {manager}, itself added from a template; added ants never add ants",
                ant.name
            );
        }
    }
    for t in templates {
        if let Some(manager) = t.managers.iter().find(|m| added.contains(*m)) {
            bail!("ant template {}: manager {manager} was added from a template; added ants never add ants", t.name);
        }
        let members: Vec<UserId> =
            ants.iter().filter(|a| a.template.as_ref() == Some(&t.name)).map(|a| a.name.clone()).collect();
        if members.len() > t.max_ants {
            bail!(
                "ant template {}: {} ants are added from it, more than its max_ants of {}",
                t.name,
                members.len(),
                t.max_ants
            );
        }
        if t.pooled() {
            let pool = Pool {
                template: t.name.clone(),
                daily_tokens: t.pool_daily_tokens,
                daily_cost: t.pool_daily_cost,
                members: Members::new(members),
            };
            for ant in ants.iter_mut().filter(|a| a.template.as_ref() == Some(&t.name)) {
                ant.pool = Some(pool.clone());
            }
        }
    }
    Ok(())
}

/// The `[[ant_template]]` tables of the config file at `path`, everything else in it ignored, each checked
/// ([`AntTemplate::check`]) and named once, with any harness taken as defined. Their workdirs are resolved as
/// [`parse`] does, and made canonical where they exist here; they need not. For the signer (`ants signer`), which
/// checks requests for ants that run on other machines against its own copy of the templates.
///
/// [`parse`]: super::parse
pub fn signer_templates(path: &Path) -> Result<Vec<AntTemplate>> {
    #[derive(Deserialize)]
    struct Templates {
        #[serde(default, rename = "ant_template")]
        templates: Vec<AntTemplate>,
    }
    let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
    let at = || format!("config {}", path.display());
    let mut templates = toml::from_str::<Templates>(&text).with_context(|| format!("parsing {}", at()))?.templates;
    let path = std::path::absolute(path)?;
    let (base, home) = (path.parent().unwrap_or(Path::new(".")), home_dir()?);
    let mut names = BTreeSet::new();
    for template in &mut templates {
        if !names.insert(template.name.clone()) {
            bail!("{}: ant template {} is listed twice", at(), template.name);
        }
        template.check(|_| Ok(())).with_context(at)?;
        for dir in &mut template.workdirs {
            let resolved = resolve(dir, base, &home);
            *dir = resolved.canonicalize().unwrap_or(resolved);
        }
    }
    Ok(templates)
}
