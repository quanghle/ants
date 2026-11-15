//! `ants daemon`: serve the ants of the config file.

use std::path::Path;

use anyhow::Result;

use crate::{Connection, manager, reload, scheduler, serve, upgrade};

pub(crate) async fn run(args: &serve::DaemonArgs, conn: &Connection<'_>) -> Result<()> {
    let domain = conn.js_domain;
    let default_path = ants_daemon::config::default_path()?;
    let served = || {
        let listed = serve::ants(args, &default_path)?;
        let mut configs = serve::configs(
            listed.ants,
            &listed.harnesses,
            conn.nats,
            conn.creds.map(Path::to_path_buf),
            conn.js_domain.cloned(),
        )?;
        if let Some((managers, path)) = &listed.managers {
            for config in configs.iter_mut().filter(|c| managers.contains(&c.ant)) {
                let (nats, creds, js_domain) =
                    (config.nats_url.clone(), config.creds.clone(), config.js_domain.clone());
                let manager = manager::Manager::new(path.clone(), nats, creds, js_domain)?;
                config.requester = ants_daemon::mcp::Requester(Some(manager.requester()));
            }
        }
        let schedules = scheduler::resolve(listed.schedules, conn.nats, domain)?;
        Ok(serve::Served { configs, schedules, metrics: listed.metrics })
    };
    let config = serve::config_file(args, &default_path)?;
    let dropins = ants_daemon::config::dropin_dir(&std::path::absolute(&config)?);
    let triggers = reload::Triggers::new(config, Some(dropins), reload::CHECK_EVERY)?;
    match serve::run(served()?, served, triggers, upgrade::replaced(args.upgrade.upgrade_check)).await? {
        serve::Ended::Stopped => Ok(()),
        serve::Ended::Replaced => upgrade::exit(),
    }
}
