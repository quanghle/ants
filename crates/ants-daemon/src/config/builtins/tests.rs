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
        ["dev-b", "system-messenger", "system-sage"],
        "without credentials anywhere all connect as no one"
    );
    assert!(open.elsewhere.is_empty());

    let leaf = ant("dev-b", "creds = \"~/.config/ants/auth/dev-b.creds\"");
    let elsewhere = home.load(&leaf);
    assert_eq!(names(&elsewhere.ants), ["dev-b"], "dev-b's credentials tell the server takes some");
    assert_eq!(left(&elsewhere), ["system-messenger", "system-sage"]);
    let creds: Vec<&Path> = elsewhere.elsewhere.iter().map(|left| left.creds.as_path()).collect();
    assert_eq!(creds, [home.auth.join("system-messenger.creds"), home.auth.join("system-sage.creds")]);
    assert_eq!(elsewhere.ants[0].reviewed_by(), Some(UserId::sage()), "dev-b names the sage served elsewhere");
    assert!(elsewhere.elsewhere[1].ant.is_sage, "the sage as the machine would serve it");

    home.issue("system-messenger");
    let messenger = home.load(&bare);
    assert_eq!(names(&messenger.ants), ["dev-b", "system-messenger"], "so do the built-in messenger's");
    assert_eq!(messenger.ants[1].creds, Some(home.auth.join("system-messenger.creds")));
    assert_eq!(left(&messenger), ["system-sage"]);
    std::fs::remove_file(home.auth.join("system-messenger.creds")).unwrap();
    home.issue("host");
    assert_eq!(names(&home.load(&bare).ants), ["dev-b"], "and the machine's host credentials");
    let alone = home.load("");
    assert_eq!((names(&alone.ants), left(&alone)), (vec![], vec!["system-messenger".to_owned()]));

    home.issue("system-sage");
    let served = home.load(&leaf);
    assert_eq!(names(&served.ants), ["dev-b", "system-sage"]);
    assert_eq!(served.ants[1].creds, Some(home.auth.join("system-sage.creds")));
    let listed = home.load(&format!("{leaf}{}", ant("messenger", "")));
    assert_eq!(names(&listed.ants), ["dev-b", "messenger", "system-sage"], "a listed messenger is ordinary");
    assert_eq!(left(&listed), ["system-messenger"]);
    let off = home.load(&format!("messenger = false\n{leaf}"));
    assert_eq!((names(&off.ants), left(&off)), (vec!["dev-b".to_owned(), "system-sage".to_owned()], vec![]));
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
    assert_eq!(
        (one.ants[0].creds.as_deref(), left(&one)),
        (Some(creds), vec!["system-messenger".into(), "system-sage".into()])
    );

    let named = credited(&bare, Some("system-sage")).unwrap();
    let sage = named.ants.iter().find(|ant| ant.name == UserId::sage()).unwrap();
    assert_eq!(sage.creds.as_deref(), Some(creds), "the sage named");
    let leaf = ant("dev-a", "creds = \"~/.config/ants/auth/dev-a.creds\"");
    let named = credited(&leaf, Some("system-sage")).unwrap();
    assert_eq!(names(&named.ants), ["dev-a", "system-sage"]);
    assert_eq!(named.ants[1].creds.as_deref(), Some(creds), "the sage left to another machine, served here");
    assert_eq!(left(&named), ["system-messenger"]);
    let own = credited(&leaf, None).unwrap();
    assert_eq!(own.ants[0].creds, Some(home.auth.join("dev-a.creds")), "the file's own come first");
    let named = credited(&two, Some("dev-b")).unwrap();
    assert_eq!((named.ants[0].creds.as_deref(), named.ants[1].creds.as_deref()), (None, Some(creds)));
    let messenger = credited("", Some("system-messenger")).unwrap();
    assert_eq!(messenger.ants[0].creds.as_deref(), Some(creds), "the messenger named");

    let e = format!("{:#}", credited(&two, None).unwrap_err());
    assert!(e.contains("the file lists 2 [[ant]] entries: set creds in each, or serve one with --ant"), "{e}");
    let e = format!("{:#}", credited("", None).unwrap_err());
    assert!(e.contains("the file lists no [[ant]]: a built-in ant takes them from"), "{e}");
}
