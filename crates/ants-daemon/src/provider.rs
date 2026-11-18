//! Each ant's provider, what holds its identity, memories and work ([`ants_proto::provider`]), as its provider setting
//! names it ([`Setting`]): built-in sense unless the setting names an external provider of the config file or none.
//! One provider serves all the ants of a process that name it, connected on first use; each call names its ant, and
//! so does each wake request it makes of ants ([`wake`]). An external provider runs once for them all, supervised
//! ([`Supervised`]). What any provider gets is here; what is built-in sense's alone is in [`crate::sense`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use ants_proto::UserId;
use ants_proto::provider::{Capabilities, Connected, Info, InitializeParams, Provider};
use tokio::sync::OnceCell;

pub mod brief;
pub mod context;
mod definition;
pub mod events;
mod external;
mod link;
mod mcp;
pub mod origins;
mod process;
mod supervisor;
pub mod wake;

pub use definition::{Definition, NONE, Protocol, SENSE, Setting, define};
pub use supervisor::Supervised;

use crate::sense::Sense;
use crate::sync::MutexExt as _;

/// The capabilities ants declares at initialize: those it calls, and wake, which it serves ([`wake`]).
pub const USES: Capabilities = Capabilities::ALL;

/// How ants names itself at initialize.
pub fn client() -> Info {
    Info { name: "ants".to_owned(), version: env!("CARGO_PKG_VERSION").to_owned() }
}

/// Initialize `provider` and agree with it on what both sides use, its wake requests waking the ants `wakers` lists.
pub async fn connect(provider: Arc<dyn Provider>, wakers: &wake::Wakers) -> Option<Connected> {
    let params = InitializeParams::new(client(), USES);
    let host = Arc::new(wake::Ants::new(wakers.clone()));
    match Connected::connect(provider, params, host.clone()).await {
        Ok(connected) => {
            host.named(connected.name());
            tracing::debug!(provider = connected.name(), capabilities = ?connected.capabilities(), "provider connected");
            Some(connected)
        }
        Err(e) => {
            tracing::warn!(error = %e, "no provider: connecting it failed");
            None
        }
    }
}

/// The providers of a process's ants: the setting of each, the external providers running and built-in sense, with
/// what they reach of the ants: the wakes of those served here ([`wake`]) and the outbox of their events
/// ([`events`]). Made once where the process starts its ants; clones share it.
#[derive(Clone)]
pub struct Providers(Arc<Shared>);

struct Shared {
    registry: Mutex<Registry>,
    sense: Sense,
    /// Built-in sense, connected on first use.
    connected: OnceCell<Option<Connected>>,
    wakers: wake::Wakers,
    outbox: events::Outbox,
}

impl Default for Providers {
    fn default() -> Self {
        Self(Arc::new_cyclic(|shared: &Weak<Shared>| {
            let shared = shared.clone();
            let resolve: events::Resolve = Arc::new(move |ant: UserId| {
                let providers = shared.upgrade().map(Providers);
                Box::pin(async move { providers?.of(&ant).await })
            });
            let wakers = wake::Wakers::default();
            Shared {
                registry: Mutex::default(),
                sense: Sense::new(wakers.clone()),
                connected: OnceCell::new(),
                wakers,
                outbox: events::Outbox::new(resolve),
            }
        }))
    }
}

impl std::fmt::Debug for Providers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.registry.locked().settings.keys()).finish()
    }
}

impl PartialEq for Providers {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Providers {}

#[derive(Default)]
struct Registry {
    settings: HashMap<UserId, Setting>,
    running: HashMap<String, (Arc<Definition>, Arc<Supervised>)>,
}

impl Registry {
    /// The provider `definition` defines, running: the one running under its name if it has that definition, else
    /// started, the one it replaces stopped.
    fn run(&mut self, definition: &Arc<Definition>) -> Arc<Supervised> {
        if let Some((running, supervised)) = self.running.get(&definition.name)
            && running == definition
        {
            return supervised.clone();
        }
        let supervised = Arc::new(Supervised::start(definition));
        if let Some((_, replaced)) =
            self.running.insert(definition.name.clone(), (definition.clone(), supervised.clone()))
        {
            tracing::info!(provider = definition.name, "restarting the provider with its new definition");
            replaced.stop();
        }
        supervised
    }
}

impl Providers {
    /// The provider of `ant`, connected: none when its setting is [`NONE`] or when it cannot be connected, the ant then
    /// going without one. An ant never [`Providers::assign`]ed has sense.
    pub async fn of(&self, ant: &UserId) -> Option<Connected> {
        let setting = self.0.registry.locked().settings.get(ant).cloned().unwrap_or_default();
        match setting {
            Setting::Sense => {
                let sense = || connect(self.0.sense.provider(), &self.0.wakers);
                self.0.connected.get_or_init(sense).await.clone()
            }
            Setting::None => None,
            Setting::External(definition) => {
                let supervised = self.0.registry.locked().run(&definition);
                connect(supervised, &self.0.wakers).await
            }
        }
    }

    /// Serve `ant` with the provider `setting` names from now on, starting it if it is external and not running yet,
    /// or running with another definition.
    pub fn assign(&self, ant: &UserId, setting: &Setting) {
        let mut registry = self.0.registry.locked();
        if let Setting::External(definition) = setting {
            registry.run(definition);
        }
        registry.settings.insert(ant.clone(), setting.clone());
    }

    /// Serve the ants listed with the providers their settings name, as [`Providers::assign`] does, and stop the
    /// external providers none of them names any more.
    pub fn reconcile<'a>(&self, settings: impl IntoIterator<Item = (&'a UserId, &'a Setting)>) {
        let mut registry = self.0.registry.locked();
        registry.settings = settings.into_iter().map(|(ant, setting)| (ant.clone(), setting.clone())).collect();
        let named: Vec<Arc<Definition>> = registry
            .settings
            .values()
            .filter_map(|setting| match setting {
                Setting::External(definition) => Some(definition.clone()),
                _ => None,
            })
            .collect();
        for definition in &named {
            registry.run(definition);
        }
        registry.running.retain(|name, (_, supervised)| {
            let kept = named.iter().any(|definition| definition.name == *name);
            if !kept {
                tracing::info!(provider = name, "stopping the provider: no ant names it any more");
                supervised.stop();
            }
            kept
        });
    }
}

impl Providers {
    /// Built-in sense of the process.
    pub fn sense(&self) -> &Sense {
        &self.0.sense
    }

    /// Built-in sense of the process when `setting` names it.
    pub fn sense_of(&self, setting: &Setting) -> Option<&Sense> {
        matches!(setting, Setting::Sense).then_some(&self.0.sense)
    }

    /// The outbox of the events of the process's ants, delivering each to the ant's provider.
    pub fn outbox(&self) -> &events::Outbox {
        &self.0.outbox
    }

    /// The ants served here, which their providers may wake.
    pub fn wakers(&self) -> &wake::Wakers {
        &self.0.wakers
    }
}

#[cfg(test)]
mod tests {
    use ants_proto::provider::Capability;

    use super::*;

    #[tokio::test]
    async fn every_ant_gets_sense_with_its_tools_and_brief() {
        let providers = Providers::default();
        let provider = providers.of(&"dev-a".parse().unwrap()).await.expect("sense connects");
        assert_eq!(provider.name(), crate::sense::NAME);
        assert_eq!(provider.capabilities(), crate::sense::CAPABILITIES);
        assert!(provider.has(Capability::Tools) && provider.has(Capability::Context));
        let other = providers.of(&"dev-b".parse().unwrap()).await.unwrap();
        assert_eq!(other.agreed(), provider.agreed());
    }

    #[tokio::test(start_paused = true)]
    async fn ants_get_the_provider_their_setting_names_each_run_once() {
        let providers = Providers::default();
        let memo = Arc::new(Definition {
            name: "memo".into(),
            command: "/nonexistent/memo".into(),
            args: Vec::new(),
            env: std::collections::BTreeMap::new(),
            protocol: Protocol::Provider,
        });
        let (a, b, c): (UserId, UserId, UserId) =
            ("dev-a".parse().unwrap(), "dev-b".parse().unwrap(), "dev-c".parse().unwrap());
        let external = Setting::External(memo.clone());
        providers.reconcile([(&a, &external), (&b, &Setting::None)]);
        providers.assign(&c, &external);
        assert_eq!(providers.of(&a).await.expect("connected while down").name(), "memo");
        assert!(providers.of(&b).await.is_none());
        assert_eq!(providers.of(&"dev-d".parse().unwrap()).await.unwrap().name(), crate::sense::NAME);
        assert_eq!(providers.0.registry.locked().running.len(), 1, "one run for both ants");

        let moved = Setting::External(Arc::new(Definition { command: "/nonexistent/memo2".into(), ..(*memo).clone() }));
        providers.reconcile([(&a, &moved)]);
        let running = providers.0.registry.locked().running["memo"].0.clone();
        assert_eq!(running.command, std::path::Path::new("/nonexistent/memo2"));
        providers.reconcile([]);
        assert!(providers.0.registry.locked().running.is_empty());
        assert_eq!(providers.of(&a).await.unwrap().name(), crate::sense::NAME, "unlisted, back to sense");
    }
}
