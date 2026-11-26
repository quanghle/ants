//! The teams as a human writes them, from `ants team` and `ants chat`: a team created ([`create`]) or deleted
//! ([`delete`]), and a change of one ([`change`]), each checked and applied as ants-client's
//! ([`ants_client::teams::change`]) checks and applies it, before anything is read of the bus, and written by
//! compare-and-set; [`tell`] then tells observers.

use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::teams::modify;
use ants_proto::{Humans, JsDomain, Team, TeamChange, UserId};
use anyhow::Result;
use async_nats::jetstream;

pub use ants_client::teams::change::{Change, Changed, Create, check_not_human, check_owner};

/// Create the team of `create` in the directory of `kv` as `by`, after [`Create::check`]: refused when a team, an ant
/// or a human holds its name, or a member is neither an ant of the bus nor a team. The team, and the lines saying what
/// its creation did.
pub async fn create(
    create: &Create,
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    kv: &DirectKv,
    by: &UserId,
    humans: &Humans,
) -> Result<(Team, Vec<String>)> {
    let created = create.check(by, humans)?;
    let known = super::known_ants(js, js_domain, &created.members.iter().collect::<Vec<_>>()).await?;
    super::check_team_name(js, js_domain, humans, &created.team).await?;
    let lines = modify(kv, |directory| ants_client::teams::change::create(directory, &created, &known)).await?;
    Ok((created, lines))
}

/// Delete `team` from the directory of `kv`: it leaves the team holding it, and its members are on no team. The lines
/// saying what it did.
pub async fn delete(kv: &DirectKv, team: &UserId) -> Result<Vec<String>> {
    modify(kv, |directory| directory.delete(team)).await
}

/// Tell observers what befell the teams of `changes`, which `by` wrote in the directory of `kv`, on `by`'s status, the
/// directory read back ([`ants_daemon::teams::tell`]): best effort, as all status is.
pub async fn tell(client: &async_nats::Client, by: &UserId, kv: &DirectKv, changes: &[(UserId, TeamChange)]) {
    if let Some(directory) = ants_daemon::teams::read_quickly(kv).await {
        ants_daemon::teams::tell(client, by, &directory, changes);
        if let Err(e) = client.flush().await {
            tracing::debug!(error = %e, "telling the team change");
        }
    }
}

/// Write `change` to `team` in the directory of `kv`, after [`Change::check`]: the members to add each an ant of the
/// bus or a team ([`Change::apply`]).
pub async fn change(
    change: &Change,
    js: &jetstream::Context,
    js_domain: Option<&JsDomain>,
    kv: &DirectKv,
    team: &UserId,
    humans: &Humans,
) -> Result<Changed> {
    change.check(team, humans)?;
    let known = super::known_ants(js, js_domain, &change.add.iter().collect::<Vec<_>>()).await?;
    modify(kv, |directory| change.apply(directory, team, &known)).await
}
