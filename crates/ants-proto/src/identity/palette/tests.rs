use super::*;

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
fn colours_fall_back_to_the_256_palette() {
    assert_eq!(Rgb(0, 0, 0).ansi256(), 16);
    assert_eq!(Rgb(255, 255, 255).ansi256(), 231);
    assert_eq!(Rgb(255, 0, 0).ansi256(), 196);
    assert_eq!(Rgb(128, 128, 128).ansi256(), 244);
}
