//! The credentials checks: the CLI's own, their keys in the keystore, and the backup of the team keys.

use std::path::Path;

use ants_daemon::secrets::creds::{self, Expiry, Kind, Seed, VersionStatus};
use ants_daemon::secrets::keystore::{self, Store};
use ants_proto::PERMISSIONS_VERSION;

use super::{Check, Identity, Setup, Status};
use crate::auth;
use crate::credentials::backup;
use crate::people::humans::{Me, Rights};

pub(super) fn check_cli_creds(setup: &Setup, now: u64, checks: &mut Vec<Check>) -> Option<Identity> {
    let check = "credentials";
    let Some(path) = &setup.creds else {
        checks.push(Check::skip(check, "none set (--creds, ANTS_CREDS or `creds` in [cli]): connecting as no one"));
        let me = Me::from_creds(None).ok()?;
        return Some(Identity { me, rights: Rights::from_creds(None).ok()?, finding: None });
    };
    let read = creds::check(path).and_then(|finding| {
        Ok(Identity {
            me: Me::from_creds(Some(path))?,
            rights: Rights::from_creds(Some(path))?,
            finding: Some(finding),
        })
    });
    match read {
        Ok(identity) => {
            let finding = identity.finding.as_ref()?;
            checks.push(check_creds(check, path, finding, now));
            checks.extend(check_key("credentials key", path, finding.renew(), keystore::os()));
            checks.extend(check_xkey("credentials xkey", path, finding, keystore::os()));
            Some(identity)
        }
        Err(e) => {
            let fix = format!(
                "ants auth user HUMAN --human on the machine holding the team keys, then name the file in `creds` of \
                 [cli] in {}",
                setup.config.display()
            );
            checks.push(Check::fail(check, format!("{e:#}"), vec![fix]));
            None
        }
    }
}

/// The check of credentials at `path`: their holder, permissions version and expiry ([`creds::Finding`]), failing
/// once expired and warning as [`creds::Finding::warnings`] does, with the commands renewing them.
pub(super) fn check_creds(check: &str, path: &Path, finding: &creds::Finding, now: u64) -> Check {
    let version = format!("permissions version {}", finding.version);
    let at = |exp: Option<u64>| exp.map_or_default(creds::time);
    let mut problems = Vec::new();
    match finding.version_status() {
        VersionStatus::Current => {}
        VersionStatus::Outdated => problems.push(format!(
            "{version}, older than version {PERMISSIONS_VERSION} of this ants: requests their role allows now may be \
             refused"
        )),
        VersionStatus::Newer => {
            let detail = format!("{version}, newer than version {PERMISSIONS_VERSION} of this ants");
            return Check::warn(
                check,
                format!("{} ({}): {detail}", finding.holder(), path.display()),
                vec!["upgrade ants".to_owned()],
            );
        }
    }
    let expiry = finding.expiry(now);
    match expiry {
        Expiry::Expired => problems.push(format!("expired at {}", at(finding.expires_at))),
        Expiry::Soon if !matches!(finding.kind, Kind::Enrollment { .. }) => {
            problems.push(format!("expire at {}", at(finding.expires_at)));
        }
        _ => {}
    }
    let holder = format!("{} ({})", finding.holder(), path.display());
    if problems.is_empty() {
        let expires = match finding.expires_at {
            Some(exp) => format!("expiring at {}", creds::time(exp)),
            None => "never expiring".to_owned(),
        };
        return Check::ok(check, format!("{holder}: {version}, {expires}"));
    }
    let when = if finding.version_status() == VersionStatus::Outdated { finding.renew_when() } else { "" };
    let status = if expiry == Expiry::Expired { Status::Fail } else { Status::Warn };
    if finding.renewable(now) {
        let detail = format!(
            "{holder}: {}; renew them over NATS, approved on the machine holding the account key{when}",
            problems.join("; ")
        );
        return Check::new(check, status, detail, finding.renew_over_nats(path));
    }
    let detail = format!("{holder}: {}; renew them on the machine holding the team keys{when}", problems.join("; "));
    Check::new(check, status, detail, finding.renew())
}

/// The check of the seed that the credentials at `path` keep in `store` in their place, when they name a stored key:
/// that the store answers, holds it, and that it is the key of their user JWT. `issue` has them issued again. Never
/// shows the seed.
pub(super) fn check_key(check: &str, path: &Path, issue: Vec<String>, store: &dyn Store) -> Option<Check> {
    let text = std::fs::read_to_string(path).ok()?;
    let Some(Seed::Stored(key)) = creds::seed_of(&text) else { return None };
    Some(match creds::load(path, store) {
        Ok(_) => Check::ok(check, format!("{key} ({}): its seed is in the OS credential store", path.display())),
        Err(e) => unloaded(check, &e, issue),
    })
}

/// The check of the encryption key of a human's credentials at `path`, of `finding`: a user JWT binding none warns,
/// with `ants renew` to add one; a seed missing, unreadable or of another key than the JWT binds fails, as in
/// [`check_key`]. None for others' credentials. Never shows the seed.
pub(super) fn check_xkey(check: &str, path: &Path, finding: &creds::Finding, store: &dyn Store) -> Option<Check> {
    if finding.kind != Kind::Human {
        return None;
    }
    let Some(bound) = &finding.xkey else {
        let detail = format!(
            "{} ({}) bind no encryption key, so no message can be sealed for this device",
            finding.holder(),
            path.display()
        );
        return Some(Check::warn(check, detail, finding.renew_over_nats(path)));
    };
    let stored = std::fs::read_to_string(path).is_ok_and(|text| matches!(creds::xkey_of(&text), Some(Seed::Stored(_))));
    let held = if stored { "the OS credential store" } else { "the file" };
    Some(match creds::load_xkey(path, store) {
        Ok(_) => Check::ok(check, format!("{bound} ({}): its seed is in {held}", path.display())),
        Err(e) => unloaded(check, &e, finding.renew()),
    })
}

/// A failed check of a seed that did not load: unlocking the OS credential store when it did not answer, else
/// restoring the seed or `issue` again.
fn unloaded(check: &str, e: &anyhow::Error, issue: Vec<String>) -> Check {
    if keystore::is_unavailable(e) {
        let fix = "unlock the OS credential store (log in), then check again; services started before then wait for it";
        return Check::fail(check, format!("{e:#}"), vec![fix.to_owned()]);
    }
    let restore = "restore the seed from a backup of the OS credential store, or issue the credentials again:";
    Check::fail(check, format!("{e:#}"), std::iter::once(restore.to_owned()).chain(issue).collect())
}

/// Whether a backup of `ants auth backup` holds the team keys of `team` and every key issued and revoked since
/// ([`backup::state`]); none without team keys.
pub(super) fn check_backup(team: &Path) -> Option<Check> {
    if !auth::holds_team(team) {
        return None;
    }
    let check = "team keys backup";
    let dir = match auth::default_dir() {
        Ok(default) if default == team => String::new(),
        _ => format!(" --dir {}", team.display()),
    };
    let fix = vec![format!("ants auth backup FILE{dir}, then keep FILE off this machine")];
    let last = |record: &backup::Record| format!("{} ({})", creds::time(record.at), record.file.display());
    Some(match backup::state(team) {
        Err(e) => Check::warn(check, format!("{e:#}"), fix),
        Ok(backup::State::None) => Check::warn(
            check,
            format!(
                "no backup of the team keys in {}: without them no credentials can be issued, renewed or revoked \
                 again",
                team.display()
            ),
            fix,
        ),
        Ok(backup::State::Current(record)) => Check::ok(check, format!("backed up {}", last(&record))),
        Ok(backup::State::Behind { record, issued, revoked }) => {
            let mut since = Vec::new();
            match issued {
                0 => {}
                1 => since.push("1 issued.jsonl entry (a key issued, renewed or relabelled)".to_owned()),
                2.. => since.push(format!("{issued} issued.jsonl entries (keys issued, renewed or relabelled)")),
                _ => since.push("issued.jsonl changes".to_owned()),
            }
            if revoked {
                since.push("revocations".to_owned());
            }
            Check::warn(check, format!("the last backup, {}, predates {}", last(&record), since.join(" and ")), fix)
        }
    })
}
