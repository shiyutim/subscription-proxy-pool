use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant},
};

use crate::{Error, Result};

/// Passive health feedback is enabled by default. Active probes require an
/// explicit URL, so a reusable library never depends on a particular website.
#[derive(Clone, Debug)]
pub struct HealthPolicy {
    /// Optional URL fetched through each proxy; 2xx/3xx passes the probe.
    pub check_url: Option<String>,
    /// Maximum duration of an active probe.
    pub timeout: Duration,
    /// Maximum simultaneous active probes.
    pub concurrency: NonZeroUsize,
    /// Consecutive transport failures that open a node's circuit.
    pub failure_threshold: NonZeroU32,
    /// First cooldown. Repeated failed recoveries double this delay.
    pub cooldown: Duration,
    /// Cap for exponential cooldowns.
    pub max_cooldown: Duration,
    /// Random delay variation in [0, 0.5]; zero gives deterministic delays.
    pub cooldown_jitter: f64,
    /// Probe new nodes before normal use. Failed nodes enter cooldown and
    /// can receive one recovery attempt after it expires.
    /// Requires `check_url` to be set.
    pub check_on_build: bool,
}

impl Default for HealthPolicy {
    fn default() -> Self {
        Self {
            check_url: None,
            timeout: Duration::from_secs(5),
            concurrency: NonZeroUsize::new(16).unwrap(),
            failure_threshold: NonZeroU32::new(2).unwrap(),
            cooldown: Duration::from_secs(30),
            max_cooldown: Duration::from_secs(300),
            cooldown_jitter: 0.2,
            check_on_build: false,
        }
    }
}

impl HealthPolicy {
    /// Enable initial and periodic probes against a caller-chosen HTTP(S) URL.
    pub fn active(url: impl Into<String>) -> Result<Self> {
        let policy = Self {
            check_url: Some(url.into()),
            check_on_build: true,
            ..Self::default()
        };
        policy.validate()?;
        Ok(policy)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.check_on_build && self.check_url.is_none() {
            return Err(Error::Config("initial health checks require a check URL"));
        }
        if let Some(value) = &self.check_url {
            let url = reqwest::Url::parse(value)
                .map_err(|_| Error::Config("invalid health check URL"))?;
            if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
                return Err(Error::Config("health check URL must use HTTP or HTTPS"));
            }
        }
        if self.cooldown.is_zero()
            || self.max_cooldown < self.cooldown
            || Instant::now().checked_add(self.max_cooldown).is_none()
        {
            return Err(Error::Config(
                "cooldown must be positive and its maximum must be representable and at least the initial delay",
            ));
        }
        if !self.cooldown_jitter.is_finite() || !(0.0..=0.5).contains(&self.cooldown_jitter) {
            return Err(Error::Config("cooldown jitter must be between 0 and 0.5"));
        }
        Ok(())
    }

    pub(crate) fn cooldown_for(&self, failures: u32) -> Duration {
        let nanos = 1_u128
            .checked_shl(failures.saturating_sub(1))
            .and_then(|factor| self.cooldown.as_nanos().checked_mul(factor))
            .unwrap_or(self.max_cooldown.as_nanos())
            .min(self.max_cooldown.as_nanos());
        let base = Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        );
        if self.cooldown_jitter == 0.0 {
            return base;
        }
        let factor =
            rand::random_range((1.0 - self.cooldown_jitter)..=(1.0 + self.cooldown_jitter));
        base.mul_f64(factor)
            .min(self.max_cooldown)
            .max(Duration::from_nanos(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_failures_back_off_and_stop_at_the_cap() {
        let policy = HealthPolicy {
            cooldown: Duration::from_secs(2),
            max_cooldown: Duration::from_secs(10),
            cooldown_jitter: 0.0,
            ..HealthPolicy::default()
        };
        let delays: Vec<_> = [1, 2, 3, 4, u32::MAX]
            .map(|n| policy.cooldown_for(n).as_secs())
            .into();
        assert_eq!(delays, [2, 4, 8, 10, 10]);
        let small = HealthPolicy {
            cooldown: Duration::from_nanos(1),
            ..policy
        };
        assert_eq!(small.cooldown_for(33), Duration::from_nanos(1_u64 << 32));
        assert_eq!(small.cooldown_for(u32::MAX), small.max_cooldown);
    }
    #[test]
    fn defaults_need_no_external_probe_and_bad_policies_are_rejected() {
        assert!(HealthPolicy::default().check_url.is_none());
        assert!(HealthPolicy::default().validate().is_ok());
        assert!(
            HealthPolicy {
                check_on_build: true,
                ..HealthPolicy::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            HealthPolicy {
                cooldown_jitter: f64::NAN,
                ..HealthPolicy::default()
            }
            .validate()
            .is_err()
        );
    }
}
