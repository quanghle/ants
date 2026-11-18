//! `ants ant sense-id`: the sense id an ant's home keeps against the one its identity record names, which its daemon
//! refuses to open sense under when they differ, and settling them by taking one side.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use ants_daemon::config;
use ants_daemon::records::direct_kv::DirectKv;
use ants_daemon::sense::{self, HomeSense};
use ants_proto::{AntIdentity, IDENTITIES_BUCKET, SeedSource, SenseId, UserId};
use anyhow::{Context as _, Result, bail};

use crate::{avatars, export};

/// Which side `ants ant sense-id --take` keeps.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Take {
    /// Name the home's sense id in the identity record.
    Home,
    /// Have the home keep the sense id the identity record names.
    Record,
}

/// `ants ant sense-id`: print the sense ids of `ant`'s home on this machine and of its identity record, after settling
/// them by `take` when given.
pub(crate) async fn run(
    nats: &crate::connection::Nats<'_>,
    ant: &UserId,
    take: Option<Take>,
    config: Option<PathBuf>,
) -> Result<()> {
    if !nats.me.role.is_human() {
        bail!("the credentials of {} are an ant's; settling a sense id runs as a human", nats.me.id);
    }
    let path = match config {
        Some(path) => path,
        None => config::default_path()?,
    };
    let loaded = config::load(&path)?;
    let listed = loaded.ants.iter().find(|a| a.name == *ant).with_context(|| {
        format!("{} lists no ant {ant}: run this on the machine whose config file lists it", path.display())
    })?;
    let home = export::home_of(listed)?;
    let (_, js) = nats.connect("ants-ant-sense-id").await?;
    let kv = DirectKv::whole(&js, nats.domain, IDENTITIES_BUCKET).await?;
    let (record, _) = export::read(&kv, ant).await?;
    let kept = sense::home_sense(&home)?;
    let Some(take) = take else {
        print!("{}", describe(ant, &home, &kept, record.as_ref()));
        return Ok(());
    };
    let (homes, records) = settling(ant, &home, &kept, record.as_ref(), take)?;
    match take {
        Take::Home => {
            let stored = avatars::change(&js, nats.domain, ant, |record| {
                if record.sense.as_ref() != Some(&records) {
                    bail!("the identity record of {ant} changed meanwhile: run this again");
                }
                record.sense = Some(homes.clone());
                Ok(())
            })
            .await?;
            println!("{}", took_home(ant, &homes, &records, &stored));
        }
        Take::Record => {
            sense::follow_record(&home, &records)
                .with_context(|| format!("having the home of {ant} keep sense identity {records}"))?;
            println!(
                "the home of {ant} keeps sense identity {records}, its identity record's, in place of {homes}, whose \
                 data stay in the log; its daemon opens sense under {records} when next used"
            );
        }
    }
    Ok(())
}

/// The sense ids of `ant`'s home at `home` and of its identity `record`, whether they agree and how to settle them.
fn describe(ant: &UserId, home: &Path, kept: &HomeSense, record: Option<&AntIdentity>) -> String {
    let mut out = String::new();
    let id = |id: Option<&SenseId>| id.map_or_else(|| "none".to_owned(), SenseId::to_string);
    let _ = writeln!(out, "home    {}  ({})", id(kept.id.as_ref()), home.display());
    match record {
        Some(record) => {
            let seed = if record.seed_source() == SeedSource::Sense { "  (the avatar's seed)" } else { "" };
            let _ = writeln!(out, "record  {}{seed}", id(record.sense.as_ref()));
        }
        None => out.push_str("record  none: the daemon of the ant creates it when it starts\n"),
    }
    match (&kept.id, record.and_then(|r| r.sense.as_ref())) {
        (Some(homes), Some(records)) if homes != records => {
            let _ = writeln!(
                out,
                "they differ, and the daemon of {ant} refuses to open sense until settled: `ants ant sense-id {ant} \
                 --take home` names {homes} in the record, `--take record` has the home keep {records}"
            );
        }
        (Some(_), Some(_)) => out.push_str("they agree\n"),
        _ => out.push_str(
            "nothing to settle: the daemon of the ant names the one side's in the other when it opens sense\n",
        ),
    }
    out
}

/// The sense ids `take` settles, the home's and the record's: refused, saying why, when there is nothing to settle or
/// settling would lose data.
fn settling(
    ant: &UserId,
    home: &Path,
    kept: &HomeSense,
    record: Option<&AntIdentity>,
    take: Take,
) -> Result<(SenseId, SenseId)> {
    let record =
        record.with_context(|| format!("{ant} has no identity record: its daemon creates one when it starts"))?;
    if let Some(importing) = &kept.importing {
        bail!(
            "the sense data of {ant} await their import into its home: `ants ant import FILE --provider-only` imports \
             them, removing {} starts afresh",
            importing.display()
        );
    }
    let (homes, records) = match (&kept.id, &record.sense) {
        (None, _) => bail!(
            "nothing to settle: the home of {ant} ({}) keeps no sense id, and its daemon has it keep the record's",
            home.display()
        ),
        (_, None) => {
            bail!("nothing to settle: the identity record of {ant} names no sense id, and its daemon names the home's")
        }
        (Some(homes), Some(records)) if homes == records => {
            bail!("nothing to settle: the home of {ant} and its identity record name sense identity {homes}")
        }
        (Some(homes), Some(records)) => (homes.clone(), records.clone()),
    };
    if take == Take::Record
        && let Some(pending) = &kept.pending
    {
        bail!(
            "writes of {ant} to sense identity {homes} wait in its home to be appended to the log, which taking the \
             record's would lose: `--take home` keeps them, removing {} drops them",
            pending.display()
        );
    }
    Ok((homes, records))
}

/// What `--take home` did: `ant`'s identity record, now `stored`, names `homes` in place of `records`.
fn took_home(ant: &UserId, homes: &SenseId, records: &SenseId, stored: &AntIdentity) -> String {
    let mut out = format!(
        "the identity record of {ant} names sense identity {homes}, its home's, in place of {records}, whose data stay \
         in the log; a home elsewhere keeping {records} is refused until `ants ant sense-id {ant} --take record` there"
    );
    if stored.seed_source() == SeedSource::Sense {
        out.push_str("; the avatar, drawn from the sense id, changes with it");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> SenseId {
        SenseId::new()
    }

    fn record(sense: Option<&SenseId>) -> AntIdentity {
        let mut record = AntIdentity::new("dev-a".parse().unwrap(), "a-id".to_owned(), chrono::Utc::now());
        record.sense = sense.cloned();
        record
    }

    #[test]
    fn settling_takes_differing_ids_only_where_no_data_would_be_lost() {
        let (ant, home): (UserId, _) = ("dev-a".parse().unwrap(), Path::new("/homes/dev-a"));
        let (homes, records) = (id(), id());
        let kept = HomeSense { id: Some(homes.clone()), ..HomeSense::default() };
        let differing = record(Some(&records));
        for take in [Take::Home, Take::Record] {
            let settled = settling(&ant, home, &kept, Some(&differing), take).unwrap();
            assert_eq!(settled, (homes.clone(), records.clone()));
        }

        let pending = HomeSense { pending: Some(PathBuf::from("/homes/dev-a/.sense/pending")), ..kept.clone() };
        let lost = settling(&ant, home, &pending, Some(&differing), Take::Record).unwrap_err().to_string();
        assert!(lost.contains("would lose") && lost.contains("--take home"), "{lost}");
        assert!(settling(&ant, home, &pending, Some(&differing), Take::Home).is_ok(), "the home's writes are kept");

        let importing = HomeSense { importing: Some(PathBuf::from("/homes/dev-a/.sense/importing")), ..kept.clone() };
        for take in [Take::Home, Take::Record] {
            let refused = settling(&ant, home, &importing, Some(&differing), take).unwrap_err().to_string();
            assert!(refused.contains("await their import"), "{refused}");
        }

        let nothing = |kept: &HomeSense, record: Option<&AntIdentity>| {
            settling(&ant, home, kept, record, Take::Home).unwrap_err().to_string()
        };
        assert!(nothing(&kept, Some(&record(Some(&homes)))).starts_with("nothing to settle"), "the same id");
        assert!(nothing(&kept, Some(&record(None))).starts_with("nothing to settle"), "the daemon names the home's");
        assert!(
            nothing(&HomeSense::default(), Some(&differing)).starts_with("nothing to settle"),
            "taken by the daemon"
        );
        assert!(nothing(&kept, None).contains("no identity record"));
    }

    #[test]
    fn differing_ids_are_described_with_how_to_settle_them() {
        let ant: UserId = "dev-a".parse().unwrap();
        let (homes, records) = (id(), id());
        let kept = HomeSense { id: Some(homes.clone()), ..HomeSense::default() };
        let text = describe(&ant, Path::new("/homes/dev-a"), &kept, Some(&record(Some(&records))));
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines[0], format!("home    {homes}  (/homes/dev-a)"));
        assert_eq!(lines[1], format!("record  {records}  (the avatar's seed)"));
        assert!(lines[2].contains("`ants ant sense-id dev-a --take home`"), "{text}");
        let agreeing = describe(&ant, Path::new("/homes/dev-a"), &kept, Some(&record(Some(&homes))));
        assert!(agreeing.ends_with("they agree\n"), "{agreeing}");
    }
}
