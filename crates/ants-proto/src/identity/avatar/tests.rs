//! Tests of avatars: derivation and every head pose matching the prototype, distinct head shapes, and the record
//! seeding and colouring the avatar.

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
    let sense: crate::SenseId = "01JD8X5G3T7Q9R2M4N6P8S0V1W".parse().unwrap();
    record.sense = Some(sense.clone());
    assert_eq!(Avatar::of(&ant, Some(&record)), Avatar::derive(sense.as_str()), "the sense id before the record's id");
    let again = AntIdentity { id: "fedcba9876543210fedcba9876543210".into(), ..record.clone() };
    assert_eq!(Avatar::of(&ant, Some(&again)), Avatar::of(&ant, Some(&record)), "one face for one sense identity");
    record.avatar = Some("lead".into());
    record.color = Some("Azure".into());
    let avatar = Avatar::of(&ant, Some(&record));
    assert_eq!(avatar.face(), "[OuO]");
    assert_eq!(avatar.hue_name(), "azure");
    record.color = Some("plaid".into());
    assert_eq!(Avatar::of(&ant, Some(&record)).hue_name(), "rose");
}
