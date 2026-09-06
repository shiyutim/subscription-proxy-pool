#![doc = include_str!("../README.md")]

mod cache;
mod diagnostics;
mod error;
mod health;
mod node;
mod pool;
mod rotation;
mod subscription;

pub use cache::CachePolicy;
pub use diagnostics::{RefreshReport, SourceOutcome, SourceReport};
pub use error::{Error, Result};
pub use health::HealthPolicy;
pub use node::{ProxyKind, ProxyNode};
pub use pool::{MaintenanceTask, PoolBuilder, PoolStats, ProxyLease, ProxyPool, ProxySession};
/// The HTTP client types used by this crate, avoiding a separate version selection.
pub use reqwest;
pub use rotation::{RotationPolicy, SelectionStrategy};
pub use subscription::{
    DEFAULT_MAX_SUBSCRIPTION_BYTES, ParseOptions, ParseReport, SubscriptionSource,
    SubscriptionUpdate, SubscriptionValidators, parse_subscription,
    parse_subscription_with_options,
};
