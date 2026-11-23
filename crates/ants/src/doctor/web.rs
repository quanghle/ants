//! The WebSocket listeners of the machine's nats-server configurations: which pages each takes. A browser lets any
//! page open in it connect to a listener on the machine, so one naming no origin, on a server asking for no
//! credentials, lets any site read and send as the human.

use std::path::Path;

use super::{Check, Setup, same_file};
use crate::node::web::{self, Listener};

/// The WebSocket listener of each nats-server configuration of the machine with one: the leaf node's, the local
/// hub's when there is no leaf node, and `managed`, the one `[nats_server]` of the config file names.
pub(super) fn check_web_listeners(setup: &Setup, managed: Option<&Path>) -> Vec<Check> {
    let leaf = setup.leaf_config.as_path();
    let hub = (!leaf.exists()).then_some(setup.hub_config.as_path());
    let other = managed.filter(|m| !same_file(m, leaf) && !same_file(m, &setup.hub_config));
    [Some(leaf), hub, other].into_iter().flatten().filter_map(check_listener).collect()
}

/// The WebSocket listener of the nats-server configuration at `conf`; None without one.
fn check_listener(conf: &Path) -> Option<Check> {
    let check = "web listener";
    let listener = Listener::read(&std::fs::read_to_string(conf).ok()?)?;
    let shown = conf.display();
    let at = match listener.listen.as_str() {
        "" => String::new(),
        listen => format!(" on {listen}"),
    };
    if !listener.origins.is_empty() {
        let detail =
            format!("the websocket listener{at} of {shown} takes pages of {} only", listener.origins.join(", "));
        return Some(Check::ok(check, detail));
    }
    if web::authenticated(conf) {
        let detail = format!(
            "the websocket listener{at} of {shown} names no origin; its server asks every connection for credentials"
        );
        return Some(Check::ok(check, detail));
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
    Some(Check::fail(check, detail, vec![fix]))
}

#[cfg(test)]
mod tests {
    //! Tests of the WebSocket listener check: a listener naming no origin fails on a server asking for no
    //! credentials, and one naming the page's origin passes.

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
        assert_eq!(hub.unwrap().status, Status::Ok, "every connection signs in");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
