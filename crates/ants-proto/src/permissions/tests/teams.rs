//! Teams: humans write the team directory, ants and assistants read it and ask `ants signer` to change it, each on
//! its own subject, and the signer reads the humans' entries that a new team's name must not be.

use super::*;

#[test]
fn humans_write_the_team_directory_and_ants_ask_the_keeper() {
    let may = |perms: &Permissions, s: &str| perms.publish.iter().any(|have| covers(have, s));
    let hears = |perms: &Permissions, s: &str| perms.subscribe.iter().any(|have| covers(have, s));
    let human = Permissions::for_user(&p("alice"), Role::Human);
    let ant = Permissions::for_user(&p("dev-a"), Role::Ant);
    let assistant = Permissions::for_assistant(&p("pa"), &p("alice"));
    for perms in [&human, &ant, &assistant] {
        assert!(may(perms, "$JS.API.DIRECT.GET.KV_ants_teams"));
    }
    for s in ["$KV.ants_teams.directory", "$JS.API.STREAM.CREATE.KV_ants_teams"] {
        assert!(may(&human, s), "{s} is a human's");
        assert!(!may(&ant, s) && !may(&assistant, s), "{s} is no ant's");
    }
    assert!(!may(&human, "$KV.ants_teams.other"));
    assert!(may(&ant, "ants.teamadm.dev-a") && !may(&ant, "ants.teamadm.dev-b"));
    assert!(hears(&ant, "_INBOX.dev-a.teamadm") && !hears(&ant, "_INBOX.dev-b.teamadm"));
    assert!(may(&assistant, "ants.teamadm.pa"));
    assert!(!may(&human, "_INBOX.dev-a.teamadm"), "a human answers no team change");
    let keeper = Permissions::for_team_keeper();
    for s in ["$KV.ants_teams.directory", "_INBOX.dev-a.teamadm", "$JS.API.DIRECT.GET.KV_ants_teams"] {
        assert!(may(&keeper, s), "the keeper may not publish {s}");
    }
    assert!(hears(&keeper, "ants.teamadm.dev-a"));
    assert!(!may(&keeper, "ants.teamadm.dev-a"), "the keeper asks nothing");
    let entry = "$JS.API.DIRECT.GET.KV_ants_humans.$KV.ants_humans.alice";
    assert!(may(&keeper, entry), "the keeper reads a human's entry, which a new team's name must not be");
    assert!(!may(&keeper, "$JS.API.DIRECT.GET.KV_ants_humans.$KV.ants_humans.alice.preallow"));
    assert!(!may(&keeper, "$JS.API.DIRECT.GET.KV_ants_humans"), "nor the rest of the bucket");
}
