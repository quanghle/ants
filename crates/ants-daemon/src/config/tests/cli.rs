//! The CLI section: the connection `ants login` sets, and what the section overrides of the top level.

use super::*;

#[test]
fn ants_login_sets_the_cli_connection_where_cli_names_no_credentials() {
    let (path, home) = (Path::new("/team/config.toml"), Path::new("/home/alice"));
    let creds = Path::new("/home/alice/.config/ants/auth/alice.creds");
    let set = |text: &str| cli_login_text(text, path, home, "wss://hub.example/nats", creds);
    let (fresh, nats) = set("").unwrap().unwrap();
    assert!(nats);
    assert_eq!(
        fresh,
        "[cli]\ncreds = \"/home/alice/.config/ants/auth/alice.creds\"\nnats = \"wss://hub.example/nats\"\n"
    );

    let top = "nats = \"nats://127.0.0.1:4222\" # the leaf node\n";
    let (appended, nats) = set(top).unwrap().unwrap();
    assert!(nats, "the top-level server is the ants'");
    assert!(
        appended.starts_with(top)
            && appended.ends_with(
                "\n\n[cli]\ncreds = \"/home/alice/.config/ants/auth/alice.creds\"\nnats = \"wss://hub.example/nats\"\n"
            ),
        "{appended}"
    );

    let own = "# mine\n[cli] # the CLI\nnats = \"nats://hub:4222\"\n\n[[ant]]\nname = \"dev-a\"\n";
    let (inserted, nats) = set(own).unwrap().unwrap();
    assert!(!nats, "[cli] names a server");
    assert_eq!(
        inserted,
        "# mine\n[cli] # the CLI\ncreds = \"/home/alice/.config/ants/auth/alice.creds\"\nnats = \"nats://hub:4222\"\n\n[[ant]]\nname = \"dev-a\"\n"
    );
    assert_eq!(set("[cli]\ncreds = \"bob.creds\"\n").unwrap(), None, "the CLI's credentials stay");
    assert!(set("cli.js_domain = \"hub\"\n").is_err(), "no [cli] header to put the keys under");
    assert!(set("[cli\n").is_err());
}

#[test]
fn the_cli_section_overrides_the_top_level_server() {
    let (path, home) = (Path::new("/team/config.toml"), Path::new("/home/alice"));
    let cli = |text: &str| CliDefaults::parse(text, path, home);
    assert_eq!(cli("").unwrap(), CliDefaults::default());
    let top = "nats = \"wss://hub.example/nats\"\njs_domain = \"hub\"\n\
                   [[ant]]\nname = \"dev-a\"\nworkdir = \"/nowhere\"\ncreds = \"dev-a.creds\"\nbogus = 1\n";
    let defaults = cli(top).unwrap();
    assert_eq!(defaults.nats.as_deref(), Some("wss://hub.example/nats"));
    assert_eq!(defaults.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(defaults.creds, None, "an ant's credentials are not the CLI's");

    let own = cli(&format!("{top}[cli]\nnats = \"nats://127.0.0.1:4222\"\ncreds = \"~/alice.creds\"\n")).unwrap();
    assert_eq!(own.nats.as_deref(), Some("nats://127.0.0.1:4222"));
    assert_eq!(own.js_domain.as_ref().map(JsDomain::as_str), Some("hub"));
    assert_eq!(own.creds.as_deref(), Some(Path::new("/home/alice/alice.creds")));
    let relative = cli("[cli]\ncreds = \"auth/alice.creds\"\njs_domain = \"own\"").unwrap();
    assert_eq!(relative.creds.as_deref(), Some(Path::new("/team/auth/alice.creds")));
    assert_eq!(relative.js_domain.as_ref().map(JsDomain::as_str), Some("own"));
    assert_eq!(relative.auto_archive, None, "never by default");
    assert_eq!(cli("[cli]\nauto_archive = \"14d\"").unwrap().auto_archive, Some(Duration::from_hours(14 * 24)));
    assert_eq!(cli("[cli]\nauto_archive = \"0\"").unwrap().auto_archive, None);
    assert!(!relative.inline_images, "off by default");
    assert!(cli("[cli]\ninline_images = true").unwrap().inline_images);
    assert!(parse_all("[cli]\ninline_images = true").is_ok(), "the daemon accepts it");
    assert_eq!((relative.theme, relative.animate), (Theme::Auto, true), "asked of the terminal, moving");
    let still = cli("[cli]\ntheme = \"light\"\nanimate = false").unwrap();
    assert_eq!((still.theme, still.animate), (Theme::Light, false));
    assert!(cli("[cli]\ntheme = \"blue\"").is_err());
    assert!(relative.mouse, "mouse reporting is enabled by default");
    assert_eq!(relative.notify, Notify::Bell);
    let quiet = cli("[cli]\nmouse = false\nnotify = \"off\"").unwrap();
    assert!(!quiet.mouse);
    assert_eq!(quiet.notify, Notify::Off);
    assert_eq!(cli("[cli]\nnotify = \"terminal\"").unwrap().notify, Notify::Terminal);
    assert!(parse_all("[cli]\nmouse = false\nnotify = \"terminal\"").is_ok());
    assert!(cli("[cli]\nnotify = \"desktop\"").is_err());
    assert!(cli("[cli]\nmouse = \"off\"").is_err());

    assert_eq!(relative.summaries, SummaryConfig::default(), "on demand, by the messenger");
    let ahead = cli("[cli]\nsummaries = \"ahead\"\nsummary_daily_tokens = 50000\nsummary_ant = \"scribe\"").unwrap();
    let scribe = "scribe".parse().unwrap();
    assert_eq!(ahead.summaries, SummaryConfig { mode: Summaries::Ahead, ant: scribe, daily_tokens: 50000 });
    let unbounded = format!("{:#}", cli("[cli]\nsummaries = \"ahead\"").unwrap_err());
    assert!(unbounded.contains("needs summary_daily_tokens above 0"), "{unbounded}");
    assert!(parse_all("[cli]\nsummaries = \"ahead\"").is_err(), "the daemon refuses it too");

    for bad in [
        "[cli]\ncred = \"a.creds\"",
        "[cli]\nauto_archive = 14",
        "[cli]\nsummaries = \"eager\"",
        "nats = 4222",
        "nats = \"nats://u:secret@hub\"",
        "[cli",
    ] {
        let err = format!("{:#}", cli(bad).unwrap_err());
        assert!(err.contains("/team/config.toml") && !err.contains("secret"), "{bad}: {err}");
    }
    assert!(parse_all("[cli]\nnats = \"nats://hub:4222\"\ncreds = \"alice.creds\"").is_ok(), "daemon accepts it");
    assert!(parse_all("[cli]\nharness = \"claude\"").is_err());
}
