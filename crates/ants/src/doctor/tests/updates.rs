//! Tests of the doctor's update check: a newer release, where the machine stands to the version the bus runs, and
//! what `auto_update = "install"` would take.

use ants_daemon::config::AutoUpdate;

use super::super::updates::{Bus, findings};
use super::*;
use crate::release::update::Version;

fn v(text: &str) -> Version {
    Version::parse(text).unwrap()
}

fn bus(version: &str) -> Result<Option<Bus>, String> {
    Ok(Some(Bus { version: v(version), machine: "hub-box".parse().unwrap() }))
}

fn shown(check: &Check) -> (Status, &str, Vec<&str>) {
    (check.status, check.detail.as_str(), check.fix.iter().map(String::as_str).collect())
}

#[test]
fn the_update_check_names_the_bus_s_version_and_where_the_machine_stands_to_it() {
    let (v3, v4, v5) = (v("0.3.0"), v("0.4.0"), v("0.5.0"));
    let behind = findings(AutoUpdate::Install, &v3, Ok(v5.clone()), bus("0.4.0"));
    assert_eq!(
        shown(&behind),
        (
            Status::Warn,
            "ants v0.5.0 is available (running v0.3.0; auto_update = \"install\"); the bus runs ants v0.4.0 (the version \
             `ants signer` runs, on hub-box): this machine runs behind it; auto_update = \"install\" takes ants v0.4.0",
            vec!["ants update --version v0.4.0"],
        )
    );
    let ahead = findings(AutoUpdate::Check, &v5, Ok(v5.clone()), bus("0.4.0"));
    assert_eq!(
        shown(&ahead),
        (
            Status::Warn,
            "ants v0.5.0 is the latest release; the bus runs ants v0.4.0 (the version `ants signer` runs, on hub-box): \
             this machine runs ahead of it, and a release before 1.0 may change the wire",
            vec!["ants update --version v0.5.0 on hub-box, where `ants signer` runs"],
        )
    );
    let level = findings(AutoUpdate::Install, &v4, Ok(v5), bus("0.4.0"));
    assert_eq!(level.status, Status::Warn, "{level}");
    assert!(level.detail.ends_with("on hub-box), as this machine does"), "takes nothing past the bus: {level}");
    assert_eq!(
        level.fix,
        ["ants update on hub-box, where `ants signer` runs, first; auto_update = \"install\" then takes it here"]
    );
    let current = findings(AutoUpdate::Check, &v4, Ok(v4.clone()), bus("0.4.0"));
    assert_eq!(
        shown(&current),
        (
            Status::Ok,
            "ants v0.4.0 is the latest release; the bus runs ants v0.4.0 (the version `ants signer` runs, on hub-box), \
             as this machine does",
            vec![],
        )
    );
}

#[test]
fn without_the_bus_s_version_a_newer_release_still_warns_and_install_takes_none() {
    let (v4, v5) = (v("0.4.0"), v("0.5.0"));
    let unknown = findings(AutoUpdate::Install, &v4, Ok(v5), Ok(None));
    assert_eq!(
        shown(&unknown),
        (
            Status::Warn,
            "ants v0.5.0 is available (running v0.4.0; auto_update = \"install\"); the version the bus runs is unknown: \
             no `ants signer` recorded one in ants_machines; auto_update = \"install\" takes none while the version the \
             bus runs is unknown",
            vec!["ants update"],
        )
    );
    let unread = findings(AutoUpdate::Check, &v4, Ok(v4.clone()), Err("the hub is not reached".to_owned()));
    assert_eq!(unread.status, Status::Ok, "{unread}");
    assert!(unread.detail.ends_with("the version the bus runs is unread: the hub is not reached"), "{unread}");
    let nothing = findings(AutoUpdate::Check, &v4, Err("GitHub did not answer in time".to_owned()), Ok(None));
    assert_eq!(nothing.status, Status::Skip, "{nothing}");
    let github_down = findings(AutoUpdate::Check, &v4, Err("GitHub did not answer in time".to_owned()), bus("0.4.1"));
    assert_eq!(github_down.status, Status::Warn, "the bus's version tells all the same: {github_down}");
    assert_eq!(github_down.fix, ["ants update --version v0.4.1"]);
}
