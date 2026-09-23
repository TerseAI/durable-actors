#[derive(Default)]
pub(super) struct History {
    pub demand: Vec<(u32, u32)>,
    pub startup_ms: Vec<u64>,
}

pub(super) struct Policy {
    pub minimum: u32,
    pub maximum: u32,
    pub shrink_after_seconds: u32,
}

#[derive(Clone, Copy)]
pub(super) struct Target {
    pub size: u32,
    pub shrink_at_ms: Option<i64>,
}

pub(super) struct Plan {
    pub target: Target,
    pub horizon_seconds: u32,
}

impl Policy {
    pub fn plan(&self, history: &History, current: Target, now_ms: i64) -> Plan {
        let horizon_seconds = history.refill_seconds() + 1;
        let desired = if self.minimum == 0 {
            0
        } else {
            history
                .capacity(horizon_seconds)
                .clamp(self.minimum, self.maximum)
        };
        Plan {
            target: self.adjust(current, desired, now_ms),
            horizon_seconds,
        }
    }

    fn adjust(&self, current: Target, desired: u32, now_ms: i64) -> Target {
        let size = current.size.min(self.maximum);
        if self.minimum == 0 || desired >= size {
            return Target {
                size: desired,
                shrink_at_ms: None,
            };
        }
        let shrink_at_ms = current
            .shrink_at_ms
            .unwrap_or(now_ms + i64::from(self.shrink_after_seconds) * 1000);
        if now_ms < shrink_at_ms {
            return Target {
                size,
                shrink_at_ms: Some(shrink_at_ms),
            };
        }
        let size = size.saturating_sub(size.div_ceil(10)).max(desired);
        Target {
            size,
            shrink_at_ms: (size > desired).then_some(now_ms + 60_000),
        }
    }
}

impl History {
    fn refill_seconds(&self) -> u32 {
        if self.startup_ms.is_empty() {
            return 10;
        }
        let mut samples = self.startup_ms.clone();
        samples.sort_unstable();
        let p95 = samples[(samples.len() * 95).div_ceil(100) - 1];
        p95.div_ceil(1000).clamp(1, 120) as u32
    }

    fn capacity(&self, horizon: u32) -> u32 {
        let rate = self.rate(10).max(self.rate(60));
        let expected = rate * f64::from(horizon);
        let forecast = (expected + 2.0 * expected.sqrt() + 1.0).ceil() as u32;
        forecast.max(self.peak(horizon).saturating_add(1))
    }

    fn rate(&self, seconds: u32) -> f64 {
        let count: u64 = self
            .demand
            .iter()
            .filter(|(age, _)| *age < seconds)
            .map(|(_, count)| u64::from(*count))
            .sum();
        count as f64 / f64::from(seconds)
    }

    fn peak(&self, horizon: u32) -> u32 {
        (0..60)
            .map(|start| {
                self.demand
                    .iter()
                    .filter(|(age, _)| *age >= start && *age < start + horizon && *age < 60)
                    .fold(0u32, |sum, (_, count)| sum.saturating_add(*count))
            })
            .max()
            .unwrap_or(0)
    }
}
