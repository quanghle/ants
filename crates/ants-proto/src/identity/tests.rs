use std::collections::HashSet;

use super::*;

fn grid(seed: &str, stage: Stage, frame: Frame) -> String {
    Avatar::derive(seed).head(stage, &frame).to_string()
}

fn lines(rows: &[&str]) -> String {
    rows.iter().map(|r| format!("{r}\n")).collect()
}

// Golden values from the prototype (avatars.py in ants-tw2yh): changing any redraws every ant.
#[test]
fn derivation_matches_the_prototype() {
    let cases = [
        ("messenger", 0xf244_18b3_ab8b_a404, [4, 0, 0, 2, 1, 10], "[.=.]"),
        ("dev-a", 0x8597_2c76_092a_8026, [6, 0, 6, 4, 1, 4], "{.~.}"),
        ("lead", 0x24ad_82ad_a202_056f, [7, 1, 2, 0, 1, 11], "[OuO]"),
        ("nightly", 0x7e30_8d76_80db_5f8a, [2, 3, 5, 3, 2, 6], "[OwO]"),
        ("reviewer", 0x9fd5_b952_4736_781a, [2, 1, 3, 1, 2, 1], "(-x-)"),
        ("lead-docs", 0xdc92_8f29_70f8_73f1, [1, 4, 7, 1, 1, 10], "(*=*)"),
    ];
    for (seed, hash, parts, face) in cases {
        assert_eq!(fnv1a64(seed.as_bytes()), hash, "{seed}");
        let a = Avatar::derive(seed);
        assert_eq!([a.antennae, a.head, a.eyes, a.mandibles, a.markings, a.hue], parts, "{seed}");
        assert_eq!(a.face(), face, "{seed}");
        assert_eq!(Avatar::derive(seed), a);
    }
}

#[test]
fn still_heads_match_the_prototype() {
    let still = Frame::default();
    assert_eq!(
        grid("dev-a", Stage::Worker, still),
        lines(&[
            "..............",
            "..#........#..",
            ".#..........#.",
            "..#........#..",
            "...##....##...",
            "....BBBBBB....",
            "...BBHBBHBB...",
            "..BBBBPPBBBB..",
            "..WKKBPPBKKW..",
            "...BBBBBBBB...",
            "....BBBBBB....",
            "....M....M....",
            "....M....M....",
            "..............",
        ])
    );
    assert_eq!(
        grid("lead", Stage::Callow, still),
        lines(&[
            "..............",
            "..............",
            "..............",
            "...#......#...",
            "....#....#....",
            "...BBB..BBB...",
            "..BBWHBBHWBB..",
            ".BBBKBPPBKBBB.",
            "..BBBBPPBBBB..",
            "...BBBBBBBB...",
            ".....BBBB.....",
            "....M....M....",
            ".....M..M.....",
            "..............",
        ])
    );
}

#[test]
fn twitching_and_closed_heads_match_the_prototype() {
    let still = Frame::default();
    assert_eq!(
        grid("nightly", Stage::Veteran, Frame { twitch: Some(Side::Left), chew: true, ..still }),
        lines(&[
            "..............",
            "..##.......##.",
            "..##.......##.",
            "...#......#...",
            "....#....#....",
            "....BBBBBB....",
            "....WKPPKW....",
            "...KKBPPBKK...",
            "...BBBPPBSB...",
            "...BBBPPSBB...",
            "....BBBBBB....",
            "...MM....MM...",
            "......MM......",
            "..............",
        ])
    );
    assert_eq!(
        grid("dev-a", Stage::Elder, Frame { turn: 1, closed: true, ..still }),
        lines(&[
            "..............",
            "...#........#.",
            "..#..........#",
            "...#.G..G...#.",
            "...##GGGG##...",
            "....BBBBBB....",
            "...BBBHBBHB...",
            "..BBBBBPPBBS..",
            "..B###BPPB##..",
            "...BBBBBBSB...",
            "....BBBBBB....",
            ".....M....M...",
            ".....M....M...",
            "..............",
        ])
    );
}

#[test]
fn turned_heads_match_the_prototype() {
    let still = Frame::default();
    assert_eq!(
        grid("reviewer", Stage::Worker, Frame { glint: Some(4), bob: true, turn: -1, ..still }),
        lines(&[
            "..............",
            "..............",
            "##........##..",
            "##........##..",
            "..#......#....",
            "....#....#....",
            "...BBB..BBB...",
            "..BBHPPBBBBB..",
            ".WBBBPPBBBWBB.",
            "..KBBPPBBBKB..",
            "...KBPPBBKB...",
            ".....BBBB.....",
            "..M......M....",
            "...MM..MM.....",
        ])
    );
    assert_eq!(
        grid("lead-docs", Stage::Elder, Frame { twitch: Some(Side::Right), turn: -1, ..still }),
        lines(&[
            "..............",
            "#.........#...",
            "#.........#...",
            ".##..G..G##...",
            "....#GGGG#....",
            "...BBBBBBBB...",
            "..BBHBBHBBBB..",
            "..BBBPPBBSBB..",
            "..BBBPPBSBBB..",
            "..KBBBBSBKBB..",
            "...BBBBBBBB...",
            "..M......M....",
            "...MM..MM.....",
            "..............",
        ])
    );
}

#[test]
fn palettes_match_the_prototype() {
    let p = Palette::new(7, Theme::Dark, Stage::Worker, false, false);
    assert_eq!(
        [p.body, p.highlight, p.marking, p.outline, p.mandible],
        [Rgb(45, 127, 210), Rgb(116, 173, 231), Rgb(34, 97, 160), Rgb(42, 76, 111), Rgb(42, 76, 111)]
    );
    assert_eq!([p.crown, p.scar], [Rgb(235, 185, 40), Rgb(240, 225, 215)]);
    let p = Palette::new(7, Theme::Light, Stage::Callow, false, true);
    assert_eq!(
        [p.body, p.highlight, p.marking, p.outline, p.mandible],
        [Rgb(124, 158, 192), Rgb(165, 189, 212), Rgb(90, 133, 175), Rgb(200, 20, 20), Rgb(70, 107, 145)]
    );
    let p = Palette::new(3, Theme::Dark, Stage::Veteran, true, false);
    assert_eq!(
        [p.body, p.highlight, p.marking, p.outline],
        [Rgb(89, 89, 89), Rgb(115, 115, 115), Rgb(71, 71, 71), Rgb(64, 64, 64)]
    );
    let p = Palette::new(0, Theme::Light, Stage::Worker, false, false);
    assert_eq!(
        [p.body, p.highlight, p.marking, p.outline],
        [Rgb(182, 32, 32), Rgb(228, 68, 68), Rgb(130, 23, 23), Rgb(77, 25, 25)]
    );
}

#[test]
fn every_head_shape_is_distinct() {
    let mut shapes = HashSet::new();
    for antennae in 0..ANTENNAE.len() {
        for head in 0..HEADS.len() {
            for eyes in 0..EYES.len() {
                for mandibles in 0..MANDIBLES.len() {
                    for markings in 0..MARKINGS.len() {
                        let a = Avatar { antennae, head, eyes, mandibles, markings, hue: 0, face: (0, 0, 0) };
                        shapes.insert(a.head(Stage::Worker, &Frame::default()));
                    }
                }
            }
        }
    }
    assert_eq!(shapes.len(), 9216);
}

#[test]
fn the_record_seeds_and_colours_the_avatar() {
    let ant = crate::p("dev-a");
    assert_eq!(Avatar::of(&ant, None), Avatar::derive("dev-a"));
    let born = "2026-11-15T10:00:00Z".parse().unwrap();
    let mut record = AntIdentity::new(ant.clone(), "0123456789abcdef0123456789abcdef".into(), born);
    assert_eq!(Avatar::of(&ant, Some(&record)), Avatar::derive("0123456789abcdef0123456789abcdef"));
    record.avatar = Some("lead".into());
    record.color = Some("Azure".into());
    let avatar = Avatar::of(&ant, Some(&record));
    assert_eq!(avatar.face(), "[OuO]");
    assert_eq!(avatar.hue_name(), "azure");
    record.color = Some("plaid".into());
    assert_eq!(Avatar::of(&ant, Some(&record)).hue_name(), "rose");
}

#[test]
fn a_record_round_trips_and_names_its_key() {
    let ant = crate::p("dev-a");
    let born = "2026-11-15T10:00:00Z".parse().unwrap();
    let record = AntIdentity::new(ant, "0123456789abcdef0123456789abcdef".into(), born);
    let json = serde_json::to_string(&record).unwrap();
    assert_eq!(
        json,
        r#"{"ant":"dev-a","id":"0123456789abcdef0123456789abcdef","born":"2026-11-15T10:00:00Z","prompts":0}"#
    );
    assert_eq!(serde_json::from_str::<AntIdentity>(&json).unwrap(), record);
    assert_eq!(record.check_key("dev-a"), Ok(()));
    assert!(record.check_key("dev-b").is_err());
}

#[test]
fn stages_grow_with_prompts() {
    let stages = [0, 9, 10, 99, 100, 999, 1000].map(Stage::of);
    use Stage::*;
    assert_eq!(stages, [Callow, Callow, Worker, Worker, Veteran, Veteran, Elder]);
}

#[test]
fn colours_fall_back_to_the_256_palette() {
    assert_eq!(Rgb(0, 0, 0).ansi256(), 16);
    assert_eq!(Rgb(255, 255, 255).ansi256(), 231);
    assert_eq!(Rgb(255, 0, 0).ansi256(), 196);
    assert_eq!(Rgb(128, 128, 128).ansi256(), 244);
}

#[test]
fn motion_follows_the_mood_the_same_everywhere() {
    let frames = |mood| {
        let mut motion = Motion::new("dev-a", mood, 100.0);
        (0..400).map(|i| motion.frame(100.0 + f64::from(i) * 0.05)).collect::<Vec<_>>()
    };
    assert_eq!(frames(Mood::Idle), frames(Mood::Idle), "seeded, not random");
    let idle = frames(Mood::Idle);
    assert!(idle.iter().any(|f| f.closed), "it blinks");
    assert!(idle.iter().any(|f| f.twitch.is_some()), "it twitches");
    assert!(idle.iter().any(|f| f.turn != 0), "it looks around");
    assert!(idle.iter().all(|f| !f.chew && !f.grey && !f.alarm && !f.bob && f.glint.is_none()));
    assert!(frames(Mood::Thinking).iter().all(|f| f.turn == 0), "only an idle ant looks around");
    let glints: HashSet<_> = frames(Mood::Thinking).iter().filter_map(|f| f.glint).collect();
    assert!(glints.len() > 5, "a shine sweeps the brow: {glints:?}");
    assert!(frames(Mood::Tool).iter().any(|f| f.chew));
    let waving = frames(Mood::Waiting);
    assert!(
        waving.iter().any(|f| f.twitch == Some(Side::Left)) && waving.iter().any(|f| f.twitch == Some(Side::Right))
    );
    assert!(frames(Mood::Stuck).iter().any(|f| f.alarm));
    assert!(frames(Mood::Message).iter().any(|f| f.bob));
    let paused = frames(Mood::Paused);
    assert!(paused.iter().all(|f| f.closed && f.twitch.is_none() && !f.grey && f.turn == 0), "eyes shut, still");
    assert!(frames(Mood::Offline).iter().all(|f| f.closed && f.grey), "greyed");
}
