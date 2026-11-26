//! The WebSocket listeners of the machine's nats-server configurations: which pages each takes. A browser lets any
//! page open in it connect to a listener on the machine, so one naming no origin, on a server asking for no
//! credentials, lets any site read and send as the human; on a server asking for credentials, as a hub does, it still
//! takes pages of any site.

use std::path::Path;

use super::{Check, Setup, same_file};
use crate::node::web::{self, Listener};

const CHECK: &str = "web listener";

/// The WebSocket listener of each nats-server configuration of the machine with one: the leaf node's, the local
/// hub's when there is no leaf node, `managed`, the one `[nats_server]` of the config file names, and the one
/// `--nats-conf` names, checked even without a listener.
pub(super) fn check_web_listeners(setup: &Setup, managed: Option<&Path>) -> Vec<Check> {
    let leaf = setup.leaf_config.as_path();
    let hub = (!leaf.exists()).then_some(setup.hub_config.as_path());
    let other = managed.filter(|m| !same_file(m, leaf) && !same_file(m, &setup.hub_config));
    let mut checks: Vec<Check> = [Some(leaf), hub, other].into_iter().flatten().filter_map(check_listener).collect();
    let named =
        setup.nats_conf.as_deref().filter(|n| [Some(leaf), hub, other].into_iter().flatten().all(|c| !same_file(n, c)));
    checks.extend(named.map(check_named));
    checks
}

/// The check of the nats-server configuration `--nats-conf` names: its listener's, else that it has none or is
/// unreadable.
fn check_named(conf: &Path) -> Check {
    let shown = conf.display();
    match std::fs::read_to_string(conf) {
        Err(e) => Check::fail(
            CHECK,
            format!("reading {shown}: {e}"),
            vec!["give --nats-conf the path of the nats-server configuration, readable by this user".to_owned()],
        ),
        Ok(_) => check_listener(conf)
            .unwrap_or_else(|| Check::ok(CHECK, format!("{shown} has no websocket listener: no page connects to it"))),
    }
}

/// The WebSocket listener of the nats-server configuration at `conf`; None without one.
fn check_listener(conf: &Path) -> Option<Check> {
    let listener = Listener::read(&std::fs::read_to_string(conf).ok()?)?;
    let shown = conf.display();
    let at = match listener.listen.as_str() {
        "" => String::new(),
        listen => format!(" on {listen}"),
    };
    if !listener.origins.is_empty() {
        let detail =
            format!("the websocket listener{at} of {shown} takes pages of {} only", listener.origins.join(", "));
        return Some(Check::ok(CHECK, detail));
    }
    if web::authenticated(conf) {
        let detail = format!(
            "the websocket listener{at} of {shown} names no origin: its server asks every connection for credentials, \
             but takes pages of any site"
        );
        let fix = format!(
            "add `allowed_origins: [\"ORIGIN\"]`, ORIGIN the one the page is served on (the reverse proxy serving \
             `ants web export` on a hub, {} for `ants web`), to the websocket block of {shown} and restart \
             nats-server, which reloads no change of that block",
            web::origin()
        );
        return Some(Check::warn(CHECK, detail, vec![fix]));
    }
    let detail = format!(
        "the websocket listener{at} of {shown} names no origin, and its server asks for no credentials: any page \
         open in a browser here connects as the human"
    );
    let fix = format!(
        "add `allowed_origins: [\"{}\"]`, the origin `ants web` serves the page on, to the websocket block of \
         {shown} and restart nats-server (`ants setup --web` writes such a listener), or remove the block",
        web::origin()
    );
    Some(Check::fail(CHECK, detail, vec![fix]))
}

#[cfg(test)]
mod tests {
    //! Tests of the WebSocket listener check: a listener naming no origin fails on a server asking for no
    //! credentials and warns on one asking for them, one naming the page's origin passes, and the configuration
    //! `--nats-conf` names is checked beside the machine's.

    use ants_test_support::scratch_dir;

    use super::*;
    use crate::doctor::Status;

    #[test]
    fn an_unrestricted_listener_fails_without_authentication_and_a_restricted_one_passes() {
        let dir = scratch_dir("doctor-web").canonicalize().unwrap();
        let conf = dir.join("nats.conf");
        let plain = "listen: \"127.0.0.1:4222\"\njetstream {\n  store_dir: \"/js\"\n}\n";
        let checked = |text: &str| {
            std::fs::write(&conf, text).unwrap();
            check_listener(&conf)
        };
        assert_eq!(checked(plain), None, "no listener, no check");

        let open =
            checked(&format!("{plain}websocket {{\n  listen: \"127.0.0.1:4281\"\n  no_tls: true\n}}\n")).unwrap();
        assert_eq!(open.status, Status::Fail);
        assert_eq!(
            open.detail,
            format!(
                "the websocket listener on 127.0.0.1:4281 of {} names no origin, and its server asks for no \
                 credentials: any page open in a browser here connects as the human",
                conf.display()
            )
        );
        assert_eq!(
            open.fix,
            [format!(
                "add `allowed_origins: [\"http://127.0.0.1:4280\"]`, the origin `ants web` serves the page on, to \
                 the websocket block of {} and restart nats-server (`ants setup --web` writes such a listener), or \
                 remove the block",
                conf.display()
            )]
        );
        let same_origin = checked(&format!("{plain}websocket {{ port: 8080, no_tls: true, same_origin: true }}\n"));
        assert_eq!(same_origin.unwrap().status, Status::Fail, "same_origin names none");

        let at = web::listen("127.0.0.1:4222").unwrap();
        let web::Added::New(restricted) = web::add(plain, at, &web::origin()) else { panic!("not added") };
        let restricted = checked(&restricted).unwrap();
        assert_eq!(restricted.status, Status::Ok);
        assert_eq!(
            restricted.detail,
            format!(
                "the websocket listener on 127.0.0.1:4281 of {} takes pages of http://127.0.0.1:4280 only",
                conf.display()
            )
        );

        std::fs::write(dir.join("server.conf"), "operator: \"eyJ0\"\n").unwrap();
        let hub = checked(&format!("{plain}websocket {{\n  port: 8080\n  no_tls: true\n}}\ninclude \"server.conf\"\n"));
        let hub = hub.unwrap();
        assert_eq!(hub.status, Status::Warn, "every connection signs in, but from pages of any site");
        assert_eq!(
            hub.detail,
            format!(
                "the websocket listener of {} names no origin: its server asks every connection for credentials, but \
                 takes pages of any site",
                conf.display()
            )
        );
        assert!(hub.fix[0].starts_with("add `allowed_origins: [\"ORIGIN\"]`"), "{:?}", hub.fix);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_configuration_nats_conf_names_is_checked_beside_the_machine_s() {
        let dir = scratch_dir("doctor-web-named").canonicalize().unwrap();
        let (leaf, hub, named) = (dir.join("leaf.conf"), dir.join("nats.conf"), dir.join("etc-nats.conf"));
        let open = "listen: \"127.0.0.1:4222\"\nwebsocket {\n  port: 8080\n  no_tls: true\n}\n";
        std::fs::write(&hub, open).unwrap();
        let setup = |nats_conf: Option<&Path>| Setup {
            nats: "nats://127.0.0.1:4222".to_owned(),
            creds: None,
            js_domain: None,
            human: None,
            config: dir.join("config.toml"),
            leaf_config: leaf.clone(),
            hub_config: hub.clone(),
            nats_conf: nats_conf.map(Path::to_path_buf),
            harnesses: false,
            team: dir.join("auth"),
            webhooks: dir.join("webhooks.toml"),
            home: None,
            worker: None,
        };
        let statuses = |checks: Vec<Check>| checks.into_iter().map(|c| (c.status, c.detail)).collect::<Vec<_>>();
        assert_eq!(check_web_listeners(&setup(None), None).len(), 1, "the local hub's");
        assert_eq!(check_web_listeners(&setup(Some(&hub)), None).len(), 1, "named once");

        let missing = statuses(check_web_listeners(&setup(Some(&named)), None));
        assert_eq!(missing[1].0, Status::Fail);
        assert!(missing[1].1.starts_with(&format!("reading {}", named.display())), "{}", missing[1].1);

        std::fs::write(&named, "listen: \"0.0.0.0:4222\"\n").unwrap();
        let without = statuses(check_web_listeners(&setup(Some(&named)), None));
        assert_eq!(
            without[1],
            (Status::Ok, format!("{} has no websocket listener: no page connects to it", named.display()))
        );

        std::fs::write(&named, format!("operator: \"eyJ0\"\n{}", &open[22..])).unwrap();
        let checks = check_web_listeners(&setup(Some(&named)), None);
        assert_eq!(checks.iter().map(|c| c.status).collect::<Vec<_>>(), [Status::Fail, Status::Warn]);
        assert!(checks[1].detail.contains(&named.display().to_string()));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
