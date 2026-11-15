use ants_daemon::identity::Identity;
use chrono::{TimeZone as _, Utc};

use super::*;
use crate::auth::tests::Server;

fn p(name: &str) -> UserId {
    name.parse().unwrap()
}

#[test]
fn a_record_s_avatar_is_described_with_its_growth() {
    let (dev_a, dev_b) = (p("dev-a"), p("dev-b"));
    let born = Utc.with_ymd_and_hms(2025, 1, 2, 3, 4, 5).unwrap();
    let mut record = AntIdentity::new(dev_a.clone(), "00112233445566778899aabbccddeeff".to_owned(), born);
    record.prompts = 42;
    record.color = Some("blue".to_owned());
    let faces = Faces::new(Identities::from([(dev_a.clone(), record.clone())]), Depth::Plain, Theme::Dark);
    let text = describe(&faces, &dev_a, born + chrono::Duration::days(3));
    let avatar = Avatar::of(&dev_a, Some(&record));
    let head = avatar.head(Stage::Worker, &Frame::default()).to_string();
    let [antennae, ..] = avatar.parts();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[..ants_proto::HEAD_ROWS].join("\n") + "\n", head, "the ASCII head without colour");
    assert_eq!(lines[ants_proto::HEAD_ROWS], format!("{} dev-a  blue (set)", avatar.face()));
    assert!(lines[ants_proto::HEAD_ROWS + 1].starts_with(&format!("{antennae} antennae, ")), "{text}");
    assert_eq!(lines[ants_proto::HEAD_ROWS + 2], "born 2025-01-02 03:04:05Z (3d ago)");
    assert_eq!(lines[ants_proto::HEAD_ROWS + 3], "worker, 42 prompts; veteran at 100");

    let unborn = describe(&faces, &dev_b, born);
    let named = Avatar::derive("dev-b");
    assert!(unborn.contains(&format!("{} dev-b  {}\n", named.face(), named.hue_name())), "{unborn}");
    assert!(unborn.contains("no identity record yet"), "{unborn}");
}

#[test]
fn ants_sharing_face_and_colour_are_warned_of() {
    let mut identities = Identities::new();
    let born = Utc::now();
    for (name, seed) in [("dev-a", "same"), ("dev-b", "same"), ("dev-c", "other")] {
        let mut record = AntIdentity::new(p(name), format!("{name}-id"), born);
        record.avatar = Some(seed.to_owned());
        identities.insert(p(name), record);
    }
    let faces = Faces::new(identities, Depth::Plain, Theme::Dark);
    let ants = [p("dev-a"), p("dev-b"), p("dev-c"), p("dev-a")];
    let warnings = faces.collisions(&ants);
    let face = Avatar::derive("same").face();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert!(warnings[0].starts_with(&format!("dev-a and dev-b share the face {face} in ")), "{warnings:?}");
    assert!(warnings[0].contains("`ants ant avatar dev-b --seed WORD`"), "{warnings:?}");
    assert!(Faces::plain().collisions(&[p("dev-a"), p("dev-b")]).is_empty(), "their names differ");
}

#[test]
fn colours_follow_the_terminal() {
    assert_eq!(Depth::of_colorterm(Some("truecolor")), Depth::True);
    assert_eq!(Depth::of_colorterm(None), Depth::Palette);
    let green = Rgb(10, 200, 30);
    assert_eq!(paint(Depth::Plain, "(ovo)", green), "(ovo)");
    assert_eq!(paint(Depth::True, "(ovo)", green), "\x1b[38;2;10;200;30m(ovo)\x1b[0m");
    assert_eq!(paint(Depth::Palette, "x", green), format!("\x1b[38;5;{}mx\x1b[0m", green.ansi256()));

    let white = parse_background(b"\x1b]11;rgb:ffff/ffff/ffff\x07").unwrap();
    assert_eq!(luminance_theme(white.0, white.1, white.2), Theme::Light);
    let black = parse_background(b"\x1b]11;rgb:00/00/00\x1b\\").unwrap();
    assert_eq!(luminance_theme(black.0, black.1, black.2), Theme::Dark);
    assert_eq!(parse_background(b"garbage"), None);
    assert_eq!(colorfgbg_theme(Some("0;15")), Theme::Light);
    assert_eq!(colorfgbg_theme(Some("15;0")), Theme::Dark);
    assert_eq!(colorfgbg_theme(None), Theme::Dark);
}

#[tokio::test]
async fn an_ant_keeps_its_own_identity_and_humans_restyle_it() {
    let Some(server) = Server::start() else { return };
    let (human, dev_a, dev_b) = (UserId::human(), p("dev-a"), p("dev-b"));
    let (_h, h_js) = server.connect(&human).await;
    let (_a, a_js) = server.connect(&dev_a).await;
    assert!(a_js.create_key_value(ants_daemon::hub::identities_config()).await.is_err(), "only a human creates it");
    h_js.create_key_value(ants_daemon::hub::identities_config()).await.unwrap();

    let own = Identity::new(&a_js, None, &dev_a).await.unwrap();
    let born = own.ensure().await.unwrap().expect("the ant's daemon creates its record");
    assert_eq!(born.id.len(), 32, "128 bits in hex");
    assert_eq!(own.count_prompt().await.unwrap().unwrap().prompts, 1);
    let as_ant = DirectKv::whole(&a_js, None, IDENTITIES_BUCKET).await.unwrap();
    assert!(as_ant.put("dev-b", b"{}".to_vec()).await.is_err(), "an ant writes only its own record");
    assert!(as_ant.get("dev-a").await.unwrap().is_some(), "and reads its own");

    let read = read(&h_js).await.unwrap();
    assert_eq!(read.keys().collect::<Vec<_>>(), [&dev_a], "a human reads every record");
    let styled =
        update(&h_js, None, &dev_a, Some(Some("pebble".to_owned())), Some(Some("Blue".to_owned()))).await.unwrap();
    assert_eq!((styled.avatar.as_deref(), styled.color.as_deref()), (Some("pebble"), Some("blue")));
    let counted = own.count_prompt().await.unwrap().unwrap();
    assert_eq!((counted.prompts, counted.avatar.as_deref(), counted.id), (2, Some("pebble"), born.id));
    let reset = update(&h_js, None, &dev_a, Some(None), Some(None)).await.unwrap();
    assert_eq!((reset.avatar, reset.color, reset.prompts), (None, None, 2), "back to the id's avatar");
    let unknown = update(&h_js, None, &dev_a, None, Some(Some("plaid".to_owned()))).await.unwrap_err();
    assert!(format!("{unknown:#}").contains("no colour plaid"), "{unknown:#}");
    let unborn = update(&h_js, None, &dev_b, Some(Some("x".to_owned())), None).await.unwrap_err();
    assert!(format!("{unborn:#}").contains("dev-b has no identity record"), "{unborn:#}");
}
