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
    let served = || {
        let listed = serve::ants(args, &default_path)?;
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
                let (nats, creds, js_domain) =
                    (config.nats_url.clone(), config.creds.clone(), config.js_domain.clone());
                let manager = manager::Manager::new(path.clone(), nats, creds, js_domain)?;
                config.requester = ants_daemon::mcp::Requester(Some(manager.requester()));
            }
        }
        let schedules = scheduler::resolve(listed.schedules, conn.nats, domain)?;
        let host = serve::host(listed.host, &configs, conn.nats, domain, Some(edited.clone()));
        Ok(serve::Served { configs, schedules, metrics: listed.metrics, host })
    };
    match serve::run(served()?, served, triggers, upgrade::replaced(args.upgrade.upgrade_check)).await? {
        serve::Ended::Stopped => Ok(()),
        serve::Ended::Replaced => upgrade::exit(),
    }
}
