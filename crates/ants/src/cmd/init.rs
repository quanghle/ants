//! `ants init`: create what the hub lacks, as [`hub()`] does, and say what was done.

use ants_daemon::hub;
use anyhow::{Context as _, Result};

use crate::connection::Nats;
use crate::people::{devices, humans};

pub(crate) async fn run(nats: &Nats<'_>, identities_restored: bool) -> Result<()> {
    for line in self::hub(nats, identities_restored).await? {
        println!("{line}");
    }
    Ok(())
}

/// `ants init`: create the hub's stream and buckets that are missing, register the human of the credentials and put
/// the user JWT of its device in the directory of encryption keys.
/// Returns what was done, a line each.
pub(crate) async fn hub(nats: &Nats<'_>, identities_restored: bool) -> Result<Vec<String>> {
    let (_, js) = nats.connect_only("ants-cli").await?;
    let fixed = hub::init(&js, |subject| nats.rights.may_publish(subject)).await;
    let fixed = fixed.context("`ants init` needs a human's credentials (`ants auth user NAME --human`)")?;
    let mut lines: Vec<String> = fixed.iter().map(hub::Finding::fixed).collect();
    humans::register_or_warn(&js, &nats.me).await;
    if let Some(own) = &nats.own_device {
        devices::publish_or_warn(&js, own).await;
    }
    lines.push(format!(
        "stream {}, buckets {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} and {}, and \
         store {} ready",
        ants_proto::STREAM,
        ants_proto::COLONY_BUCKET,
        ants_proto::SESSIONS_BUCKET,
        ants_proto::ATTEMPTS_BUCKET,
        ants_proto::CHANNELS_BUCKET,
        ants_proto::HUMANS_BUCKET,
        ants_proto::CHAINS_BUCKET,
        ants_proto::LINKS_BUCKET,
        ants_proto::OWNERS_BUCKET,
        ants_proto::WEBHOOKS_BUCKET,
        ants_proto::REACTIONS_BUCKET,
        ants_proto::THREADS_BUCKET,
        ants_proto::READ_BUCKET,
        ants_proto::PRESENCE_BUCKET,
        ants_proto::NOTIFY_BUCKET,
        ants_proto::DEVICES_BUCKET,
        ants_proto::MACHINES_BUCKET,
        ants_proto::IDENTITIES_BUCKET,
        ants_proto::TRAILS_BUCKET,
        ants_proto::TRAIL_RUNS_BUCKET,
        ants_proto::TRAIL_TIPS_BUCKET,
        ants_proto::TRAIL_RATINGS_BUCKET,
        ants_proto::TEAMS_BUCKET,
        ants_proto::FILES_BUCKET
    ));
    if identities_restored {
        hub::new_incarnation(&js, nats.domain).await?;
        lines.push(format!(
            "bucket {} in a new incarnation: no shared settings record signed before verifies; each daemon holding an \
             ant's credentials signs the ant's settings anew within {} s",
            ants_proto::IDENTITIES_BUCKET,
            ants_daemon::profile::shared::POLL_EVERY.as_secs()
        ));
    }
    Ok(lines)
}
