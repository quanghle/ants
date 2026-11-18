//! Each ant's provider, what holds its identity, memories and work ([`ants_proto::provider`]), as its provider setting
//! names it ([`Setting`]): built-in sense unless the setting names an external provider of the config file or none.
//! One provider serves all the ants of a process that name it, connected on first use; each call names its ant, and
//! so does each wake request it makes of ants ([`wake`]). An external provider runs once for them all, supervised
//! ([`Supervised`]). What any provider gets is here; what is built-in sense's alone is in [`crate::sense`].

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

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

use crate::sync::MutexExt as _;

/// The capabilities ants declares at initialize: those it calls, and wake, which it serves ([`wake`]).
pub const USES: Capabilities = Capabilities::ALL;

/// How ants names itself at initialize.
pub fn client() -> Info {
    Info { name: "ants".to_owned(), version: env!("CARGO_PKG_VERSION").to_owned() }
}

static SENSE_PROVIDER: OnceCell<Option<Connected>> = OnceCell::const_new();
static PROVIDERS: LazyLock<Providers> = LazyLock::new(Providers::default);

/// The provider of `ant`, connected: none when its setting is [`NONE`] or when it cannot be connected, the ant then
/// going without one. An ant never [`assign`]ed has sense.
pub async fn of(ant: &UserId) -> Option<Connected> {
    PROVIDERS.of(ant).await
}

/// Serve `ant` with the provider `setting` names from now on, starting it if it is external and not running yet, or
/// running with another definition.
pub fn assign(ant: &UserId, setting: &Setting) {
    PROVIDERS.assign(ant, setting);
}

/// Serve the ants listed with the providers their settings name, as [`assign`] does, and stop the external providers
/// none of them names any more.
pub fn reconcile<'a>(settings: impl IntoIterator<Item = (&'a UserId, &'a Setting)>) {
    PROVIDERS.reconcile(settings);
}

/// Initialize `provider` and agree with it on what both sides use.
pub async fn connect(provider: Arc<dyn Provider>) -> Option<Connected> {
    let params = InitializeParams::new(client(), USES);
    let host = Arc::new(wake::Ants::default());
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

/// The providers of a process's ants: the setting of each, and the external providers running.
#[derive(Default)]
struct Providers(Mutex<Registry>);

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
    async fn of(&self, ant: &UserId) -> Option<Connected> {
        let setting = self.0.locked().settings.get(ant).cloned().unwrap_or_default();
        match setting {
            Setting::Sense => SENSE_PROVIDER.get_or_init(|| connect(crate::sense::built_in().provider())).await.clone(),
            Setting::None => None,
            Setting::External(definition) => {
                let supervised = self.0.locked().run(&definition);
                connect(supervised).await
            }
        }
    }

    fn assign(&self, ant: &UserId, setting: &Setting) {
        let mut registry = self.0.locked();
        if let Setting::External(definition) = setting {
            registry.run(definition);
        }
        registry.settings.insert(ant.clone(), setting.clone());
    }

    fn reconcile<'a>(&self, settings: impl IntoIterator<Item = (&'a UserId, &'a Setting)>) {
        let mut registry = self.0.locked();
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

#[cfg(test)]
mod tests {
    use ants_proto::provider::Capability;

    use super::*;

    #[tokio::test]
    async fn every_ant_gets_sense_with_its_tools_and_brief() {
        let provider = of(&"dev-a".parse().unwrap()).await.expect("sense connects");
        assert_eq!(provider.name(), crate::sense::NAME);
        assert_eq!(provider.capabilities(), crate::sense::CAPABILITIES);
        assert!(provider.has(Capability::Tools) && provider.has(Capability::Context));
        let other = of(&"dev-b".parse().unwrap()).await.unwrap();
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
        assert_eq!(providers.0.locked().running.len(), 1, "one run for both ants");

        let moved = Setting::External(Arc::new(Definition { command: "/nonexistent/memo2".into(), ..(*memo).clone() }));
        providers.reconcile([(&a, &moved)]);
        let running = providers.0.locked().running["memo"].0.clone();
        assert_eq!(running.command, std::path::Path::new("/nonexistent/memo2"));
        providers.reconcile([]);
        assert!(providers.0.locked().running.is_empty());
        assert_eq!(providers.of(&a).await.unwrap().name(), crate::sense::NAME, "unlisted, back to sense");
    }
}
