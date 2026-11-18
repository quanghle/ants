//! The identity section of a review packet: who the ant is, for its sage and itself to judge the work against, and to
//! judge against the work. It holds the persona and its pending proposal, the self-model, the relationship records of
//! the humans and other ants who started the threads covered, as their thread records tell ([`thread::record`]), the
//! skills by name and description, and the experience since the review's start ([`experience_since`]). Once the
//! threads are left out for room, it is cut to titles ([`Identity::titles`]).

use ants_proto::UserId;
use ants_proto::provider::Starter;

use crate::brief::{TOP_EXPERIENCE, experience_lines};
use crate::dream::Dream;
use crate::items::Values;
use crate::memory::Scope;
use crate::records;
use crate::store::{Items, StoreError};
use crate::thread::experience::experience_since;
use crate::{persona, proposal, skill, thread};

/// The identity section of a packet ([`identity`]), whole and cut to titles.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity {
    pub whole: String,
    /// The section with the persona's fields, the proposed persona and the bodies of the records and skills left out.
    pub titles: String,
}

impl Identity {
    /// Its text: cut to titles, or whole.
    pub(super) fn text(&self, titles: bool) -> &str {
        if titles { &self.titles } else { &self.whole }
    }

    /// Push `whole` onto the whole section and `titles` onto the one cut to titles.
    fn push(&mut self, whole: &str, titles: &str) {
        self.whole.push_str(whole);
        self.titles.push_str(titles);
    }

    /// Push `text` onto both.
    fn both(&mut self, text: &str) {
        self.push(text, text);
    }
}

/// The identity section of the packet of `dream`, of `ant`.
pub async fn identity(items: &dyn Items, dream: &Dream, ant: &UserId) -> Result<Identity, StoreError> {
    let mut identity = Identity { whole: String::new(), titles: "\nCut to titles for room.\n".into() };
    identity.both("\n### Persona\n\n");
    persona_part(&mut identity, items).await?;
    identity.both("\n### Self-model\n\n");
    match records::self_model(items).await? {
        Some(found) => identity.push(&format!("{}\n", found.text()), &format!("{}\n", found.head())),
        None => identity.both("No self-model yet.\n"),
    }
    identity.both("\n### Relationships with the threads' starters\n\n");
    relationships_part(&mut identity, items, dream, ant).await?;
    identity.both("\n### Skills\n\n");
    let skills = skill::skills(items).await?;
    if skills.is_empty() {
        identity.both("No skills yet.\n");
    }
    for skill in &skills {
        identity.push(&format!("- {}: {}\n", skill.name, skill.description), &format!("- {}\n", skill.name));
    }
    let since = dream.since.format("%Y-%m-%d %H:%M");
    identity.both(&format!("\n### Experience since {since} UTC\n\n"));
    match experience_since(items, ant, dream.since, TOP_EXPERIENCE).await? {
        Some(experience) => identity.both(&format!("{}\n", experience_lines(&experience))),
        None => identity.both("No thread active since then.\n"),
    }
    Ok(identity)
}

/// The persona and its pending proposal, cut to the names of their fields for titles.
async fn persona_part(identity: &mut Identity, items: &dyn Items) -> Result<(), StoreError> {
    let fields = |values: &Values| format!("Fields: {}.\n", persona::headings(values).join(", "));
    match persona::current(items).await? {
        Some(found) => identity.push(&format!("{}\n", persona::text(&found.values)), &fields(&found.values)),
        None => identity.both("No persona yet.\n"),
    }
    if let Some(pending) = proposal::pending_for(items, &persona::persona_id()).await? {
        let version = &pending.proposal.version;
        identity.both(&format!("\nProposed: {}\n", pending.line()));
        identity.push(&format!("{}\n", persona::text(version)), &fields(version));
    }
    Ok(())
}

/// The relationship records of the humans and other ants who started the threads `dream` covers, the most recently
/// active first, each once, with a line for each without one.
async fn relationships_part(
    identity: &mut Identity,
    items: &dyn Items,
    dream: &Dream,
    ant: &UserId,
) -> Result<(), StoreError> {
    let mut own = items.earlier_names().await;
    own.push(ant.clone());
    let mut people = Vec::new();
    for covered in &dream.threads {
        let Some(record) = thread::record::record(items, &covered.thread).await? else { continue };
        let scope = match record.origin.by {
            Starter::Human { user } => Scope::Human(user),
            Starter::Ant { user } if !own.contains(&user) => Scope::Ant(user),
            Starter::Ant { .. } | Starter::Webhook { .. } | Starter::Provider { .. } => continue,
        };
        if !people.contains(&scope) {
            people.push(scope);
        }
    }
    if people.is_empty() {
        identity.both("No human or other ant started the threads covered.\n");
    }
    let found = records::relationships(items, &people).await?;
    for scope in &people {
        match found.iter().find(|found| found.record.scope == *scope) {
            Some(found) => identity.push(&format!("{}\n", found.text()), &format!("{}\n", found.head())),
            None => identity.both(&format!("- {scope}: no relationship record.\n")),
        }
    }
    Ok(())
}
