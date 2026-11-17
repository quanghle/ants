//! The colours an avatar is drawn in: its [`Palette`] on the terminal's [`Theme`], as [`Rgb`] colours.

use super::{HUE_NAMES, Pixel, Stage};

/// A 24-bit colour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rgb(pub u8, pub u8, pub u8);

impl Rgb {
    /// The nearest colour of the 256-colour palette's 6x6x6 cube or grey ramp.
    pub fn ansi256(self) -> u8 {
        let level = |v: u8| -> (u8, u8) {
            const STEPS: [u8; 6] = [0, 95, 135, 175, 215, 255];
            let i = STEPS
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| (i32::from(**s) - i32::from(v)).abs())
                .map_or(0, |(i, _)| i);
            (i as u8, STEPS[i])
        };
        let ((r, rv), (g, gv), (b, bv)) = (level(self.0), level(self.1), level(self.2));
        let distance = |x: (u8, u8, u8)| {
            let d = |a: u8, b: u8| (i32::from(a) - i32::from(b)).pow(2);
            d(x.0, self.0) + d(x.1, self.1) + d(x.2, self.2)
        };
        let cube = 16 + 36 * r + 6 * g + b;
        let mean = (u16::from(self.0) + u16::from(self.1) + u16::from(self.2)) / 3;
        let grey = if mean < 8 { 0 } else { ((mean - 8) / 10).min(23) as u8 };
        let grey_value = 8 + 10 * grey;
        if distance((grey_value, grey_value, grey_value)) < distance((rv, gv, bv)) { 232 + grey } else { cube }
    }
}

/// The terminal's background, which the palette suits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Theme {
    #[default]
    Dark,
    Light,
}

/// The colours of a head's pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Palette {
    pub body: Rgb,
    pub highlight: Rgb,
    pub marking: Rgb,
    pub outline: Rgb,
    pub mandible: Rgb,
    pub pupil: Rgb,
    pub glint: Rgb,
    pub crown: Rgb,
    pub scar: Rgb,
}

struct ThemeValues {
    body: f64,
    highlight: f64,
    marking: f64,
    dark: f64,
    saturation: f64,
    callow: (f64, f64),
    grey: (f64, f64, f64, f64),
    alarm: Rgb,
    gold: Rgb,
    scar: Rgb,
}

const DARK: ThemeValues = ThemeValues {
    body: 0.50,
    highlight: 0.68,
    marking: 0.38,
    dark: 0.30,
    saturation: 0.65,
    callow: (0.72, 0.30),
    grey: (0.35, 0.45, 0.28, 0.25),
    alarm: Rgb(225, 45, 45),
    gold: Rgb(235, 185, 40),
    scar: Rgb(240, 225, 215),
};
const LIGHT: ThemeValues = ThemeValues {
    body: 0.42,
    highlight: 0.58,
    marking: 0.30,
    dark: 0.20,
    saturation: 0.70,
    callow: (0.62, 0.35),
    grey: (0.62, 0.72, 0.52, 0.45),
    alarm: Rgb(200, 20, 20),
    gold: Rgb(190, 140, 0),
    scar: Rgb(250, 240, 232),
};

impl Palette {
    /// The colours of hue `hue` (an index into [`HUE_NAMES`]) on `theme` at `stage`, drained when `grey`, the antennae
    /// red when `alarm`.
    pub fn new(hue: usize, theme: Theme, stage: Stage, grey: bool, alarm: bool) -> Self {
        let t = match theme {
            Theme::Dark => &DARK,
            Theme::Light => &LIGHT,
        };
        let h = hue as f64 / HUE_NAMES.len() as f64;
        let (body, highlight, marking, outline) = if grey {
            let (b, hl, p, d) = t.grey;
            (hls(0.0, b, 0.0), hls(0.0, hl, 0.0), hls(0.0, p, 0.0), hls(0.0, d, 0.0))
        } else if stage == Stage::Callow {
            let (l, s) = t.callow;
            (hls(h, l, s), hls(h, (l + 0.12).min(0.9), s), hls(h, l - 0.10, s), hls(h, l - 0.2, s))
        } else {
            let s = t.saturation;
            (hls(h, t.body, s), hls(h, t.highlight, s + 0.05), hls(h, t.marking, s), hls(h, t.dark, s - 0.2))
        };
        Self {
            body,
            highlight,
            marking,
            outline: if alarm { t.alarm } else { outline },
            mandible: outline,
            pupil: Rgb(20, 20, 24),
            glint: Rgb(240, 240, 240),
            crown: t.gold,
            scar: t.scar,
        }
    }

    /// The colour of `px`; None for a blank.
    pub fn of(&self, px: Pixel) -> Option<Rgb> {
        Some(match px {
            Pixel::Blank => return None,
            Pixel::Outline => self.outline,
            Pixel::Body => self.body,
            Pixel::Highlight => self.highlight,
            Pixel::Marking => self.marking,
            Pixel::Mandible => self.mandible,
            Pixel::Pupil => self.pupil,
            Pixel::Glint => self.glint,
            Pixel::Crown => self.crown,
            Pixel::Scar => self.scar,
        })
    }
}

/// Hue, lightness and saturation, each 0 to 1, as RGB, as Python's colorsys converts them.
fn hls(h: f64, l: f64, s: f64) -> Rgb {
    let (r, g, b) = if s == 0.0 {
        (l, l, l)
    } else {
        let m2 = if l <= 0.5 { l * (1.0 + s) } else { l + s - l * s };
        let m1 = 2.0 * l - m2;
        let v = |hue: f64| {
            let hue = hue.rem_euclid(1.0);
            if hue < 1.0 / 6.0 {
                m1 + (m2 - m1) * hue * 6.0
            } else if hue < 0.5 {
                m2
            } else if hue < 2.0 / 3.0 {
                m1 + (m2 - m1) * (2.0 / 3.0 - hue) * 6.0
            } else {
                m1
            }
        };
        (v(h + 1.0 / 3.0), v(h), v(h - 1.0 / 3.0))
    };
    let byte = |c: f64| (c * 255.0).round_ties_even() as u8;
    Rgb(byte(r), byte(g), byte(b))
}

#[cfg(test)]
mod tests;
