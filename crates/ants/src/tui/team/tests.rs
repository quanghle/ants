//! Tests of the teams in `ants chat`: the nav's teams as a forest, a team's view with its header and members tree,
//! its keys, `@` completing teams and a message to a team going to its lead, a thread started through a team showing
//! it, a team deleted once confirmed, and a bus without teams.

use ants_proto::{Team, TeamDirectory, UserId};

use super::*;
use crate::test_support::colony::entry;
use crate::test_support::live::{at, message, user};
use crate::test_support::render::rendered;
use crate::tui::command::Tone;
use crate::tui::exec::Job;
use crate::tui::model::{Action, Focus, Page as View};
use crate::tui::reads::Read;
use crate::tui::test_support::{keys, model, screen, typed};

/// eng (lead `boss`): boss, backend; backend (lead `dev-a`, "The API and its storage."): dev-a, dev-b; web (lead
/// `dev-c`): dev-c.
fn forest() -> TeamDirectory {
    let mut directory = TeamDirectory::default();
    directory.create(Team::new(user("eng"), user("boss"), user("alice")), None).unwrap();
    let mut backend = Team::new(user("backend"), user("dev-a"), user("alice"));
    backend.members.push(user("dev-b"));
    backend.description = Some("The API and its storage.".to_owned());
    directory.create(backend, Some(&user("eng"))).unwrap();
    directory.create(Team::new(user("web"), user("dev-c"), user("alice")), None).unwrap();
    directory
}

/// Alice's model with the teams of [`forest`] read and boss, dev-a to dev-d in the colony, showing `team`.
fn showing(team: &str) -> Model {
    let mut m = model();
    m.read(Read::Teams(Ok(Some(forest()))), at(0));
    let colony = ["boss", "dev-a", "dev-b", "dev-c", "dev-d"].map(|a| entry(a, 60, 5, at(0))).to_vec();
    m.data.live.colony(colony, at(0));
    m.select(nav::Item::Team(user(team)));
    m.focus = Focus::View;
    m
}

/// Alice's model showing team backend.
pub(super) fn backend() -> Model {
    showing("backend")
}

/// The team page `m` shows, drawn on a terminal `width` by `height` at second 1.
pub(super) fn team_screen(m: &Model, width: u16, height: u16) -> String {
    let View::Team(page) = m.page() else { panic!("not the team") };
    rendered(width, height, |f| page.draw(f, f.area(), m, at(1), true))
}

/// The nav of `m` drawn, each row trimmed of its borders.
fn nav_rows(m: &Model) -> Vec<String> {
    let shown = rendered(30, 30, |f| nav::draw(f, f.area(), &m.data, &m.ui, &m.nav, at(1), false));
    shown.lines().map(|l| l.trim_matches(|c: char| c == '│' || c == ' ').to_owned()).collect()
}

#[test]
fn the_nav_lists_the_teams_as_a_forest_each_with_its_ants() {
    let m = backend();
    let items = nav::items(&m.data, &nav::Item::Home, at(1));
    let teams: Vec<&nav::Item> = items.iter().filter(|i| matches!(i, nav::Item::Team(_))).collect();
    let want = ["eng", "backend", "web"].map(|t| nav::Item::Team(user(t)));
    assert_eq!(teams, want.iter().collect::<Vec<_>>(), "each team under the one holding it");
    let first_ant = items.iter().position(|i| matches!(i, nav::Item::Ant(_))).unwrap();
    assert!(items.iter().rposition(|i| matches!(i, nav::Item::Team(_))).unwrap() < first_ant, "before the ants");
    let rows = nav_rows(&m);
    let row = rows.iter().position(|r| r == "TEAMS").expect("a TEAMS section");
    assert_eq!(rows[row + 1..row + 5], ["eng 3", "backend 2", "web 1", "+ new team"]);
    let shown = rendered(30, 30, |f| nav::draw(f, f.area(), &m.data, &m.ui, &m.nav, at(1), false));
    assert!(shown.contains("│  backend 2"), "indented under eng: {shown}");
}

#[test]
fn a_bus_without_teams_shows_no_teams_section() {
    let mut m = backend();
    m.read(Read::Teams(Ok(None)), at(1));
    let items = nav::items(&m.data, &nav::Item::Home, at(1));
    assert!(!items.iter().any(|i| matches!(i, nav::Item::Team(_) | nav::Item::NewTeam)));
    assert!(!nav_rows(&m).iter().any(|r| r == "TEAMS"));
    assert!(team_screen(&m, 100, 30).contains("no team backend"), "the view says so");
    m.ui.bar.focused = true;
    typed(&mut m, "@dev-a hi");
    let job = Job::Ask { to: user("dev-a"), thread: None, text: "hi".into(), quote: None, team: None };
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(job, "@dev-a hi".into())), "sends go on as before");
}

#[test]
fn the_view_shows_the_team_above_its_members_tree_with_faces_states_and_the_leads() {
    let m = backend();
    let shown = team_screen(&m, 100, 20);
    let lines: Vec<&str> = shown.lines().collect();
    assert!(lines[0].contains("team backend · within eng") && lines[3].contains("M manage · a ask"), "{shown}");
    assert!(lines[1].contains("The API and its storage."), "{shown}");
    assert!(lines[2].contains("lead dev-a · owner none"), "{shown}");
    assert!(lines[4].contains("members · 2 ants"), "{shown}");
    let face = m.data.looks.face(&user("dev-a")).content.to_string();
    assert!(lines[5].contains(&format!("● {face} dev-a ★ lead  live")), "{shown}");
    assert!(lines[6].contains("dev-b  live") && !lines[6].contains("lead"), "{shown}");

    let eng = showing("eng");
    let shown = team_screen(&eng, 100, 20);
    let lines: Vec<&str> = shown.lines().collect();
    assert!(lines[2].contains("lead boss · owner none · 1 team beneath"), "{shown}");
    assert!(lines[1].contains("no description"), "{shown}");
    assert!(lines[4].contains("members · 3 ants"), "{shown}");
    assert!(lines[5].contains("boss ★ lead"), "{shown}");
    assert!(lines[6].contains("│team backend  lead dev-a"), "{shown}");
    assert!(lines[7].contains("│  ● ") && lines[7].contains("dev-a ★ lead"), "under its team: {shown}");
}

#[test]
fn a_team_without_a_lead_takes_the_one_above_and_one_without_any_refuses_a_send() {
    let mut m = backend();
    let mut directory = forest();
    directory.teams.iter_mut().find(|t| t.team == user("backend")).unwrap().lead = None;
    directory.teams.iter_mut().find(|t| t.team == user("web")).unwrap().lead = None;
    m.read(Read::Teams(Ok(Some(directory))), at(1));
    let shown = team_screen(&m, 120, 20);
    assert!(shown.contains("lead boss (from team eng)") && !shown.contains("no lead"), "{shown}");
    assert!(nav_rows(&m).iter().any(|r| r == "backend 2"), "{:?}", nav_rows(&m));
    assert!(nav_rows(&m).iter().any(|r| r == "web 1 ✕ no lead"), "{:?}", nav_rows(&m));
    m.ui.bar.focused = true;
    typed(&mut m, "@backend deploy");
    assert_eq!(m.bar_title(), "to team backend (lead boss, from team eng) on a new thread");
    m.ui.bar.clear();

    typed(&mut m, "@web deploy");
    assert_eq!(m.bar_title(), "to team web, which has no lead on a new thread");
    assert_eq!(m.key(Key::Enter, at(1)), None);
    let status = m.ui.bar.status.as_ref().unwrap();
    assert!(status.text.starts_with("team web has no lead, nor has any team above it"), "{}", status.text);
    assert_eq!((status.tone, m.ui.bar.text()), (Tone::Error, "@web deploy".to_owned()), "kept to send later");
}

#[test]
fn enter_opens_the_member_selected_and_a_writes_to_the_team() {
    let mut m = showing("eng");
    keys(&mut m, [Key::Down, Key::Enter]);
    let View::Team(page) = m.page() else { panic!("not a team: {}", m.page().name()) };
    assert_eq!(page.team, Some(user("backend")), "the member team opens over the view");
    keys(&mut m, [Key::Enter]);
    let View::Ant(page) = m.page() else { panic!("not an ant: {}", m.page().name()) };
    assert_eq!(page.ant, user("dev-a"));
    keys(&mut m, [Key::Esc, Key::Esc]);
    assert!(matches!(m.page(), View::Team(page) if page.team == Some(user("eng"))), "back to eng");

    keys(&mut m, [Key::Char('a')]);
    assert!(m.ui.bar.focused);
    assert_eq!(m.ui.bar.text(), "@eng ");
}

#[test]
fn a_message_to_a_team_goes_to_its_lead_carrying_the_team() {
    let mut m = backend();
    m.ui.bar.focused = true;
    typed(&mut m, "@ba");
    let candidates = m.completions();
    let backend = candidates.iter().find(|c| c.word == "@backend").expect("teams complete");
    assert_eq!(backend.detail, "team · lead dev-a");
    keys(&mut m, [Key::Tab]);
    typed(&mut m, "deploy");
    assert_eq!(m.bar_title(), "to team backend (lead dev-a) on a new thread");
    let job =
        Job::Ask { to: user("dev-a"), thread: None, text: "deploy".into(), quote: None, team: Some(user("backend")) };
    assert_eq!(m.key(Key::Enter, at(1)), Some(Action::Run(job, "@backend deploy".into())));
}

#[test]
fn a_thread_started_through_a_team_shows_it_where_the_title_shows_its_channels() {
    let mut m = backend();
    let thread = ants_proto::ThreadId::generate();
    let mut asked = message(1, &thread, "alice", "dev-a", "deploy the API");
    asked.team = Some(user("backend"));
    let mut forged = message(2, &thread, "dev-b", "alice", "done");
    (forged.team, forged.forged) = (Some(user("web")), true);
    m.stored(vec![asked, forged], at(2));
    m.push(View::Thread(crate::tui::thread::Page::new(thread)));
    let shown = screen(&m, 140, 30, at(3));
    assert!(shown.contains("deploy the API · via team backend"), "{shown}");
    assert!(
        !shown.contains("via team web") && !shown.contains("backend, web"),
        "a forged message's team is not taken: {shown}"
    );
}

#[test]
fn every_name_of_the_forest_is_a_member_of_its_tree() {
    let m = showing("eng");
    let View::Team(page) = m.page() else { panic!() };
    let names: Vec<UserId> = page.ids(&m.data, at(1));
    assert_eq!(names, ["boss", "backend", "dev-a", "dev-b"].map(user));
}

#[test]
fn d_deletes_the_team_once_y_confirms() {
    let mut m = backend();
    assert_eq!(keys(&mut m, [Key::Char('D')]), None);
    let View::Team(page) = m.page() else { panic!() };
    assert_eq!(page.prompt(), Some(Page::DELETE_KEYS));
    assert!(m.page().armed());
    assert_eq!(keys(&mut m, [Key::Char('x')]), None, "any other key keeps it");
    assert!(!m.page().armed());
    keys(&mut m, [Key::Char('D'), Key::Esc]);
    assert!(!m.page().armed() && matches!(m.page(), View::Team(_)), "Esc drops the confirmation first");
    let delete = Job::TeamDelete { team: user("backend") };
    assert_eq!(keys(&mut m, [Key::Char('D'), Key::Char('y')]), Some(Action::Run(delete, String::new())));
}
