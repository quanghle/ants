//! Which of the built-in ants a file serves, the messenger ([`super::builtin_messenger`]) and the sage
//! ([`super::builtin_sage`]), the machine serves: on a server taking credentials, only those whose own are on the
//! machine, each other one being left to another machine ([`Elsewhere`]); and whose credentials `ants daemon --creds`
//! stands for ([`Loaded::credit`]).

use std::path::{Path, PathBuf};

use ants_proto::UserId;
use anyhow::{Result, bail};

use super::ant::builtin_creds;
use super::{Ant, HostSettings, Loaded, home_dir};

/// A built-in ant the file serves but the machine leaves to another, the server taking credentials and the ant having
/// none on the machine.
#[derive(Debug, Clone, PartialEq)]
pub struct Elsewhere {
    /// The ant as the machine would serve it, without credentials.
    pub ant: Ant,
    /// Where its credentials belong: `~/.config/ants/auth/<name>.creds`.
    pub creds: PathBuf,
}

/// Where the credentials of each built-in ant belong, which the machine serving it or not depends on.
pub fn creds_files() -> Result<Vec<PathBuf>> {
    let home = home_dir()?;
    Ok([UserId::messenger(), UserId::sage()].iter().map(|name| builtin_creds(name, &home)).collect())
}

/// Whether the server takes credentials, as the machine's `host` credentials or those of one of `ants` tell: only
/// `ants auth` issues them, and a server without authentication has its ants connect as no one, as the built-in ants
/// then do too.
fn takes_credentials(ants: &[Ant], host: &HostSettings) -> bool {
    host.creds.is_some() || ants.iter().any(|ant| ant.creds.is_some())
}

/// Take out of `ants` the built-in ones the machine leaves to another, those without credentials on a server taking
/// some ([`takes_credentials`]), `home` standing for `~`.
pub(super) fn leave(ants: &mut Vec<Ant>, host: &HostSettings, home: &Path) -> Vec<Elsewhere> {
    if !takes_credentials(ants, host) {
        return Vec::new();
    }
    ants.extract_if(.., |ant| ant.builtin && ant.creds.is_none())
        .map(|ant| Elsewhere { creds: builtin_creds(&ant.name, home), ant })
        .collect()
}

impl Loaded {
    /// Have `creds` (`ants daemon --creds`) stand for the credentials of the ant `named` (`--ant`), else of the file's
    /// one `[[ant]]`, where it has none of its own: never for a built-in ant's unless named, one the machine leaves to
    /// another then served here with them. With them the server takes credentials, so of the whole file's ants the
    /// built-in ones without their own are left to another machine. Fails where none is named and the file lists
    /// several `[[ant]]` entries, or none.
    pub fn credit(&mut self, named: Option<&UserId>, creds: &Path) -> Result<()> {
        let creds = Some(creds.to_owned());
        if let Some(name) = named {
            if let Some(i) = self.elsewhere.iter().position(|left| left.ant.name == *name) {
                let ant = self.elsewhere.remove(i).ant;
                self.ants.push(Ant { creds, ..ant });
            } else if let Some(ant) = self.ants.iter_mut().find(|ant| ant.name == *name && ant.creds.is_none()) {
                ant.creds = creds;
            }
            return Ok(());
        }
        let mut listed = self.ants.iter_mut().filter(|ant| !ant.builtin);
        match (listed.next(), listed.count()) {
            (Some(ant), 0) => {
                if ant.creds.is_none() {
                    ant.creds = creds;
                }
            }
            (Some(_), more) => bail!(
                "--creds (ANTS_CREDS) stands for one ant's credentials, and the file lists {} [[ant]] entries: set \
                 creds in each, or serve one with --ant",
                more + 1
            ),
            (None, _) => bail!(
                "--creds (ANTS_CREDS) stands for one ant's credentials, and the file lists no [[ant]]: a built-in \
                 ant takes them from ~/.config/ants/auth/<name>.creds, or from --creds with --ant naming it"
            ),
        }
        self.elsewhere.extend(leave(&mut self.ants, &self.host, &self.home));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse;

    fn ant(name: &str, more: &str) -> String {
        format!("[[ant]]\nname = \"{name}\"\nworkdir = \".\"\n{more}\n")
    }

    /// A throwaway home with its auth directory, and a loader of config texts under `harness = "copilot"` there.
    struct Home {
        dir: PathBuf,
        auth: PathBuf,
    }

    impl Home {
        fn new(name: &str) -> Self {
            let dir = ants_test_support::scratch_dir(name).canonicalize().unwrap();
            let auth = dir.join(".config/ants/auth");
            std::fs::create_dir_all(&auth).unwrap();
            Self { dir, auth }
        }

        fn load(&self, text: &str) -> Loaded {
            let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.toml");
            parse(&format!("harness = \"copilot\"\n{text}"), &path, &self.dir).unwrap()
        }

        fn issue(&self, name: &str) {
            std::fs::write(self.auth.join(format!("{name}.creds")), "").unwrap();
        }
    }

    impl Drop for Home {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn names(ants: &[Ant]) -> Vec<String> {
        ants.iter().map(|ant| ant.name.to_string()).collect()
    }

    fn left(loaded: &Loaded) -> Vec<String> {
        loaded.elsewhere.iter().map(|left| left.ant.name.to_string()).collect()
    }

    #[test]
    fn on_a_server_taking_credentials_a_builtin_ant_is_served_only_where_its_own_are() {
        let home = Home::new("builtins-served");
        let bare = ant("dev-b", "");
        let open = home.load(&bare);
        assert_eq!(
            names(&open.ants),
            ["dev-b", "messenger", "sage"],
            "without credentials anywhere all connect as no one"
        );
        assert!(open.elsewhere.is_empty());

        let leaf = ant("dev-b", "creds = \"~/.config/ants/auth/dev-b.creds\"");
        let elsewhere = home.load(&leaf);
        assert_eq!(names(&elsewhere.ants), ["dev-b"], "dev-b's credentials tell the server takes some");
        assert_eq!(left(&elsewhere), ["messenger", "sage"]);
        let creds: Vec<&Path> = elsewhere.elsewhere.iter().map(|left| left.creds.as_path()).collect();
        assert_eq!(creds, [home.auth.join("messenger.creds"), home.auth.join("sage.creds")]);
        assert_eq!(elsewhere.ants[0].reviewed_by(), Some(UserId::sage()), "dev-b names the sage served elsewhere");
        assert!(elsewhere.elsewhere[1].ant.is_sage, "the sage as the machine would serve it");

        home.issue("messenger");
        let messenger = home.load(&bare);
        assert_eq!(names(&messenger.ants), ["dev-b", "messenger"], "so do the built-in messenger's");
        assert_eq!(messenger.ants[1].creds, Some(home.auth.join("messenger.creds")));
        assert_eq!(left(&messenger), ["sage"]);
        std::fs::remove_file(home.auth.join("messenger.creds")).unwrap();
        home.issue("host");
        assert_eq!(names(&home.load(&bare).ants), ["dev-b"], "and the machine's host credentials");
        let alone = home.load("");
        assert_eq!((names(&alone.ants), left(&alone)), (vec![], vec!["messenger".to_owned()]), "nothing served here");

        home.issue("sage");
        let served = home.load(&leaf);
        assert_eq!(names(&served.ants), ["dev-b", "sage"]);
        assert_eq!(served.ants[1].creds, Some(home.auth.join("sage.creds")));
        let listed = home.load(&format!("{leaf}{}", ant("messenger", "")));
        assert_eq!(names(&listed.ants), ["dev-b", "messenger", "sage"], "a listed messenger is no built-in one");
        assert!(listed.elsewhere.is_empty());
        let off = home.load(&format!("messenger = false\n{leaf}"));
        assert_eq!((names(&off.ants), left(&off)), (vec!["dev-b".to_owned(), "sage".to_owned()], vec![]));
    }

    #[test]
    fn creds_given_stand_for_the_named_ant_or_the_files_one_never_a_builtin_ant_unnamed() {
        let home = Home::new("builtins-credit");
        let creds = Path::new("/keys/given.creds");
        let credited = |text: &str, named: Option<&str>| {
            let mut loaded = home.load(text);
            loaded.credit(named.map(|name| name.parse().unwrap()).as_ref(), creds).map(|()| loaded)
        };
        let (bare, two) = (ant("dev-a", ""), format!("{}{}", ant("dev-a", ""), ant("dev-b", "")));

        let one = credited(&bare, None).unwrap();
        assert_eq!(names(&one.ants), ["dev-a"], "with them the server takes credentials, the built-in ants none here");
        assert_eq!((one.ants[0].creds.as_deref(), left(&one)), (Some(creds), vec!["messenger".into(), "sage".into()]));

        let named = credited(&bare, Some("sage")).unwrap();
        let sage = named.ants.iter().find(|ant| ant.name == UserId::sage()).unwrap();
        assert_eq!(sage.creds.as_deref(), Some(creds), "the sage named");
        let leaf = ant("dev-a", "creds = \"~/.config/ants/auth/dev-a.creds\"");
        let named = credited(&leaf, Some("sage")).unwrap();
        assert_eq!(names(&named.ants), ["dev-a", "sage"]);
        assert_eq!(named.ants[1].creds.as_deref(), Some(creds), "the sage left to another machine, served here");
        assert_eq!(left(&named), ["messenger"]);
        let own = credited(&leaf, None).unwrap();
        assert_eq!(own.ants[0].creds, Some(home.auth.join("dev-a.creds")), "the file's own come first");
        let named = credited(&two, Some("dev-b")).unwrap();
        assert_eq!((named.ants[0].creds.as_deref(), named.ants[1].creds.as_deref()), (None, Some(creds)));
        let messenger = credited("", Some("messenger")).unwrap();
        assert_eq!(messenger.ants[0].creds.as_deref(), Some(creds), "the messenger named");

        let e = format!("{:#}", credited(&two, None).unwrap_err());
        assert!(e.contains("the file lists 2 [[ant]] entries: set creds in each, or serve one with --ant"), "{e}");
        let e = format!("{:#}", credited("", None).unwrap_err());
        assert!(e.contains("the file lists no [[ant]]: a built-in ant takes them from"), "{e}");
    }
}
