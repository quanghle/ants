//! `ants daemon`: serve the ants of the config file.

use ants_daemon::admin::ConfigFile;
use anyhow::Result;

use crate::connection::Connection;
use crate::node::natsd;
use crate::release::{update, upgrade};
use crate::signing::manager;
use crate::{reload, scheduler, serve};

pub(crate) async fn run(conn: &Connection<'_>, args: serve::DaemonArgs) -> Result<()> {
    let args = &args;
    let domain = conn.js_domain;
    let default_path = ants_daemon::config::default_path()?;
    let config = std::path::absolute(serve::config_file(args, &default_path)?)?;
    let dropins = ants_daemon::config::dropin_dir(&config);
    let mut triggers = reload::Triggers::new(config.clone(), Some(dropins), reload::CHECK_EVERY)?;
    let edited = ConfigFile::new(config.clone(), triggers.edits());
    triggers.watch_shared(edited.shared.subscribe());
    triggers.watch_creds(ants_daemon::config::builtins::creds_files()?);
    let host_creds = triggers.host_creds();
    let hostname = ants_daemon::colony::machine().await;
    let startup = || serve::ants(args, &default_path, &hostname, conn.creds);
    let listed = startup()?;
    ants_daemon::limits::files::raise_for(ants_daemon::limits::files::needed(
        listed.ants.iter().map(|a| a.max_harnesses),
    ));
    // The ants connect to the file's top-level nats, where it sets one: the address the managed server must answer.
    let local_url = listed.host.as_ref().and_then(|host| host.nats.clone()).unwrap_or_else(|| conn.nats.to_owned());
    let nats_server = listed.nats_server;
    let managed = nats_server.as_ref().map(|_| serve::outage::Managed { url: local_url.clone(), config });
    let _outages = serve::outage::watch(managed);
    let served = |listed: serve::Listed| {
        if listed.nats_server != nats_server {
            tracing::warn!("the [nats_server] table changed; it takes effect on a restart");
        }
        edited.shared.listed(&listed.ants);
        let mut configs =
            serve::configs(listed.ants, &listed.harnesses, conn.nats, conn.js_domain.cloned(), Some(&edited.shared))?;
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
        let settings = listed.host.clone().or(listed.sense_link);
        host_creds.watch(settings.as_ref().map_or_default(|s| s.creds_candidates(&hostname)));
        let host = serve::host(listed.host, &configs, conn.nats, domain, Some(edited.clone()))
            .map(|(host, ants)| (ants_daemon::host::Host { leaf: serve::leaf_node(), ..host }, ants));
        let sense = serve::sense_link(host.as_ref().map(|(h, _)| h), settings, &configs, conn.nats, domain, &hostname);
        let shared = Some(edited.shared.clone());
        Ok(serve::Served { configs, schedules, metrics: listed.metrics, host, sense, shared, machine: listed.machine })
    };
    // One listener for the managed nats-server's start and the serving, so a signal between them is not missed.
    let stop = serve::stop_signal();
    tokio::pin!(stop);
    let managed = match &nats_server {
        Some(settings) => match natsd::start(settings, &local_url, natsd::Policy::DEFAULT, stop.as_mut()).await? {
            Some(managed) => Some(managed),
            None => return Ok(()),
        },
        None => None,
    };
    let every = args.upgrade.upgrade_check;
    let auto_update = listed.auto_update;
    let replaced = async {
        tokio::select! {
            () = upgrade::replaced(every) => {}
            () = Box::pin(update::auto(auto_update, every)) => {}
        }
    };
    let reload = || serve::ants_on_reload(args, &default_path, &hostname, conn.creds).and_then(&served);
    // A daemon managing the nats-server goes on serving it, and the machine's admin orders, with no ants left.
    let idle = if managed.is_some() { serve::Idle::Wait } else { serve::Idle::Exit };
    let serving = async { serve::run(served(startup()?)?, reload, triggers, replaced, stop, idle).await };
    let ended = match managed {
        Some(managed) => natsd::run_serving(managed, serving).await?,
        None => serving.await?,
    };
    match ended {
        serve::Ended::Stopped => Ok(()),
        serve::Ended::Replaced => upgrade::exit(),
    }
}
