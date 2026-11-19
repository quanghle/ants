//! Seeded draws from Beta distributions, the same on every machine and release: SplitMix64 for the uniform bits,
//! Box–Muller for normals, Marsaglia–Tsang for gammas, and a Beta as the share of one gamma in the sum of two.

use std::f64::consts::TAU;

/// The SplitMix64 generator.
#[derive(Debug, Clone)]
pub(super) struct SplitMix64(u64);

impl SplitMix64 {
    pub(super) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(super) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A uniform draw in (0, 1].
    fn uniform(&mut self) -> f64 {
        ((self.next_u64() >> 11) + 1) as f64 / (1u64 << 53) as f64
    }

    /// A standard normal draw.
    fn normal(&mut self) -> f64 {
        let (r, angle) = (self.uniform(), self.uniform());
        (-2.0 * r.ln()).sqrt() * (TAU * angle).cos()
    }

    /// A draw from Gamma(`shape`, 1), for a `shape` of 1 or more.
    pub(super) fn gamma(&mut self, shape: f64) -> f64 {
        let d = shape - 1.0 / 3.0;
        let c = 1.0 / (9.0 * d).sqrt();
        loop {
            let x = self.normal();
            let v = (1.0 + c * x).powi(3);
            if v <= 0.0 {
                continue;
            }
            if self.uniform().ln() < 0.5 * x * x + d - d * v + d * v.ln() {
                return d * v;
            }
        }
    }

    /// A draw from Beta(`alpha`, `beta`), both 1 or more.
    pub(super) fn beta(&mut self, alpha: f64, beta: f64) -> f64 {
        let x = self.gamma(alpha);
        x / (x + self.gamma(beta))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_gives_its_reference_outputs() {
        let mut rng = SplitMix64::new(1234567);
        let outputs: Vec<_> = (0..3).map(|_| rng.next_u64()).collect();
        assert_eq!(outputs, [6457827717110365317, 3203168211198807973, 9817491932198370423]);
    }

    #[test]
    fn uniforms_stay_in_the_unit_interval() {
        let mut rng = SplitMix64::new(7);
        assert!((0..10_000).map(|_| rng.uniform()).all(|u| u > 0.0 && u <= 1.0));
    }

    #[test]
    fn draws_have_the_means_of_their_distributions() {
        let mut rng = SplitMix64::new(42);
        let n = 20_000;
        for shape in [1.0, 2.5, 10.0] {
            let mean = (0..n).map(|_| rng.gamma(shape)).sum::<f64>() / f64::from(n);
            assert!((mean - shape).abs() < 0.05 * shape, "gamma({shape}) mean {mean}");
        }
        let mean = (0..n).map(|_| rng.beta(3.0, 7.0)).sum::<f64>() / f64::from(n);
        assert!((mean - 0.3).abs() < 0.01, "beta(3, 7) mean {mean}");
        assert!((0..n).map(|_| rng.beta(1.0, 1.0)).all(|b| (0.0..=1.0).contains(&b)));
    }
}
