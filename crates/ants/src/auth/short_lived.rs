//! Short-lived and narrow credentials: an ant's enrollment, the bus's public bootstrap, and the approver's, signer's
//! and revoker's for one task.

use std::fs;
use std::path::Path;
use std::time::Duration;

use ants_proto::{JsDomain, Permissions, UserId};
use anyhow::{Context as _, Result, bail};
use nkeys::KeyPair;
use serde_json::json;

use super::claims::{now, user_jwt};
use super::files::{creds, load, seed, write_secret};
use super::uplink::merge;
use super::{ACCOUNT_SEED, check_not_reserved};
use crate::credentials::issued::{self, Issued, Kind};

/// Issue the enrollment credentials of `ant`, valid for `ttl` (zero: with no expiry), and write their `.creds` file
/// to `out`: they may only ask for the ant's credentials and hear the answer ([`Permissions::for_enrollment`]).
/// Returns when they expire, in seconds since the epoch.
pub fn issue_enrollment(dir: &Path, ant: &UserId, ttl: Duration, out: &Path) -> Result<Option<u64>> {
    check_not_reserved(ant)?;
    if !ttl.is_zero() && ttl < Duration::from_secs(1) {
        bail!("a ttl of {ttl:?} is under a second: give at least 1s, or 0 for no expiry");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = if ttl.is_zero() { None } else { Some(now()? + ttl.as_secs()) };
    let name = ants_proto::enrollment_user(ant);
    let jwt = user_jwt(&account, &user.public_key(), &name, Permissions::for_enrollment(ant), json!({}), exp)?;
    write_secret(out, &creds(&jwt, &seed(&user)?))?;
    issued::record(dir, &Issued::new(&user.public_key(), &name, Kind::Enrollment, exp)?)?;
    Ok(exp)
}

/// Issue the bus's public bootstrap credentials, valid for `ttl` (zero: with no expiry), and write their `.creds`
/// file to `out`, readable by all as they are meant to be shared: they may only ask `ants signer` to register a
/// human's device and hear its answers ([`Permissions::for_bootstrap`]), with messages of at most
/// [`ants_proto::BOOTSTRAP_MAX_PAYLOAD`] bytes. Returns their public key and when they expire, in seconds since the
/// epoch.
pub fn issue_bootstrap(dir: &Path, ttl: Duration, out: &Path) -> Result<(String, Option<u64>)> {
    if !ttl.is_zero() && ttl < Duration::from_secs(1) {
        bail!("a ttl of {ttl:?} is under a second: give at least 1s, or 0 for no expiry");
    }
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = if ttl.is_zero() { None } else { Some(now()? + ttl.as_secs()) };
    let name = ants_proto::BOOTSTRAP_USER;
    let limits = json!({ "payload": ants_proto::BOOTSTRAP_MAX_PAYLOAD });
    let jwt = user_jwt(&account, &user.public_key(), name, Permissions::for_bootstrap(), limits, exp)?;
    fs::write(out, creds(&jwt, &seed(&user)?)).with_context(|| format!("writing {}", out.display()))?;
    #[cfg(unix)]
    fs::set_permissions(out, std::os::unix::fs::PermissionsExt::from_mode(0o644))?;
    issued::record(dir, &Issued::new(&user.public_key(), name, Kind::Bootstrap, exp)?)?;
    Ok((user.public_key(), exp))
}

/// The text of short-lived credentials for `ants enroll` and `ants signer`, which hear enrollment requests and answer
/// them ([`Permissions::for_enroll_approver`]), for the hub's JetStream domain if it has one, issued afresh from the
/// account key of each run.
pub fn approver_creds(dir: &Path, js_domain: Option<&JsDomain>) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_enroll_approver())
}

/// [`approver_creds`] for `ants signer`, which also answers ants' reactions requests
/// ([`Permissions::for_reactions_responder`]), trail saves ([`Permissions::for_trail_keeper`]) and team changes
/// ([`Permissions::for_team_keeper`]), and humans' registration requests ([`Permissions::for_registrar`]), and keeps
/// the buckets held in memory ([`Permissions::for_bucket_keeper`]).
pub fn signer_creds(dir: &Path, js_domain: Option<&JsDomain>) -> Result<String> {
    let mut permissions = signer_permissions();
    // Registration and keeping the buckets happen on the hub only: a leaf uplink with signer_permissions never carries
    // them.
    merge(&mut permissions, Permissions::for_registrar());
    merge(&mut permissions, Permissions::for_bucket_keeper());
    short_lived_creds(dir, js_domain, permissions)
}

/// What `ants enroll` and `ants signer` need: [`Permissions::for_enroll_approver`],
/// [`Permissions::for_reactions_responder`], [`Permissions::for_trail_keeper`], [`Permissions::for_team_keeper`] and
/// [`Permissions::for_account_holder`].
pub(super) fn signer_permissions() -> Permissions {
    let mut permissions = Permissions::for_enroll_approver();
    merge(&mut permissions, Permissions::for_reactions_responder());
    merge(&mut permissions, Permissions::for_trail_keeper());
    merge(&mut permissions, Permissions::for_team_keeper());
    merge(&mut permissions, Permissions::for_account_holder());
    permissions
}

/// The text of short-lived credentials signed with the account key in `dir`, for the hub's JetStream domain if it has
/// one, that put the revocation records of humans' keys ([`Permissions::for_device_revoker`]).
pub fn revoker_creds(dir: &Path, js_domain: Option<&JsDomain>) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_device_revoker())
}

/// Short-lived credentials deleting the device entries `entries` of revoked keys from the directory of encryption keys
/// ([`Permissions::for_device_entry_remover`]).
pub(crate) fn device_entry_remover_creds(
    dir: &Path,
    js_domain: Option<&JsDomain>,
    entries: &[String],
) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_device_entry_remover(entries))
}

/// Short-lived credentials that read and write the team directory ([`Permissions::for_team_keeper`]): `ants auth
/// revoke` takes a revoked ant off its team with them, and `ants auth user` reads which names teams hold.
pub(crate) fn team_keeper_creds(dir: &Path, js_domain: Option<&JsDomain>) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_team_keeper())
}

/// Short-lived credentials taking one human revoked whole out of the humans bucket
/// ([`Permissions::for_human_offboarder`]).
pub(crate) fn human_offboarder_creds(dir: &Path, js_domain: Option<&JsDomain>, human: &UserId) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_human_offboarder(human))
}

/// Short-lived credentials for checking and cleaning up one retired ant ([`Permissions::for_retired_consumers`]).
pub(crate) fn retired_consumer_creds(dir: &Path, js_domain: Option<&JsDomain>, ant: &UserId) -> Result<String> {
    short_lived_creds(dir, js_domain, Permissions::for_retired_consumers(ant))
}

fn short_lived_creds(dir: &Path, js_domain: Option<&JsDomain>, permissions: Permissions) -> Result<String> {
    let account = load(&dir.join(ACCOUNT_SEED))?;
    let user = KeyPair::new_user();
    let exp = now()? + APPROVER_TTL.as_secs();
    let permissions = permissions.with_js_domain(js_domain);
    let jwt = user_jwt(&account, &user.public_key(), "enroll", permissions, json!({}), Some(exp))?;
    Ok(creds(&jwt, &seed(&user)?))
}

pub(super) const APPROVER_TTL: Duration = Duration::from_mins(10);
