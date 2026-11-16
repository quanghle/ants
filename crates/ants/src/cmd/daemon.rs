//! `ants daemon`: serve the ants of the config file.

use std::path::Path;

use ants_daemon::admin::ConfigFile;
use anyhow::Result;

use crate::{Connection, manager, reload, scheduler, serve, upgrade};

pub(crate) async fn run(args: &serve::DaemonArgs, conn: &Connection<'_>) -> Result<()> {
    let domain = conn.js_domain;
    let default_path = ants_daemon::config::default_path()?;
    let config = std::path::absolute(serve::config_file(args, &default_path)?)?;
    let dropins = ants_daemon::config::dropin_dir(&config);
    let mut triggers = reload::Triggers::new(config.clone(), Some(dropins), reload::CHECK_EVERY)?;
    let edited = ConfigFile::new(config, triggers.edits());
    triggers.watch_shared(edited.shared.subscribe());
    let hostname = ants_daemon::colony::machine().await;
    let served = || {
        let listed = serve::ants(args, &default_path, &hostname)?;
        edited.shared.listed(&listed.ants);
        let mut configs = serve::configs(
            listed.ants,
            &listed.harnesses,
            conn.nats,
            conn.creds.map(Path::to_path_buf),
            conn.js_domain.cloned(),
            Some(&edited.shared),
        )?;
        for config in &configs {
            config.pause.credit(edited.pausers.take(&config.ant));
        }
        if let Some((managers, path)) = &listed.managers {
            for config in configs.iter_mut().filter(|c| managers.contains(&c.ant)) {
                let ants_daemon::daemon::Connection { nats_url, creds, js_domain } = config.connection.clone();
                let manager = manager::Manager::new(path.clone(), nats_url, creds, js_domain)?;
                config.requester = ants_daemon::mcp::Requester(Some(manager.requester()));
            }
        }
        let schedules = scheduler::resolve(listed.schedules, conn.nats, domain)?;
        let host = serve::host(listed.host, &configs, conn.nats, domain, Some(edited.clone()))
            .map(|(host, ants)| (ants_daemon::host::Host { leaf: serve::leaf_node(), ..host }, ants));
        let shared = Some(edited.shared.clone());
        Ok(serve::Served { configs, schedules, metrics: listed.metrics, host, shared, machine: listed.machine })
    };
    match serve::run(served()?, served, triggers, upgrade::replaced(args.upgrade.upgrade_check)).await? {
        serve::Ended::Stopped => Ok(()),
        serve::Ended::Replaced => upgrade::exit(),
    }
}
