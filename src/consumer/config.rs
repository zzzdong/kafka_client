//! Consumer configuration types.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration & enums
// ---------------------------------------------------------------------------

/// Kafka consumer configuration.
///
/// # Construction
///
/// Use `ConsumerConfig::new()` for a direct-mode (non-group) consumer, or
/// chain `.with_group_id(...)` to enable consumer group coordination:
///
/// ```
/// use kafka_client::{ConsumerConfig, AutoOffsetReset};
/// use std::time::Duration;
///
/// // Direct mode — no consumer group
/// let config = ConsumerConfig::new();
///
/// // Group mode — with consumer group coordination
/// let config = ConsumerConfig::new()
///     .with_group_id("my-group")
///     .with_earliest()
///     .with_max_poll_records(1000);
/// ```
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group ID. `None` means direct (non-group) consumer mode.
    /// `Some(...)` enables consumer group coordination (join/sync/heartbeat/commit).
    pub group_id: Option<String>,
    /// If true, offsets are committed automatically at `auto_commit_interval`.
    pub auto_commit: bool,
    /// Interval between automatic offset commits.
    pub auto_commit_interval: Duration,
    /// What to do when there is no committed offset or the offset is out of range.
    pub auto_offset_reset: AutoOffsetReset,
    /// Minimum bytes of data to return in a single Fetch response.
    /// The broker waits until at least this much data is available (up to `max_wait`).
    pub min_bytes: i32,
    /// Maximum bytes of data to return in a single Fetch response.
    pub max_bytes: i32,
    /// Maximum bytes per partition in a single Fetch response.
    pub partition_max_bytes: i32,
    /// Maximum time the broker will wait to satisfy `min_bytes`.
    pub max_wait: Duration,
    /// Maximum number of records returned by a single `poll()` call.
    pub max_poll_records: usize,
    /// Group session timeout. If the coordinator receives no heartbeat within this
    /// window, the member is considered dead and its partitions are reassigned.
    pub session_timeout: Duration,
    /// Maximum time a rebalance is allowed to complete.
    pub rebalance_timeout: Duration,
    /// Interval between heartbeats sent to the group coordinator.
    pub heartbeat_interval: Duration,
    /// Partition assignment strategy used during group rebalance.
    pub partition_assignment_strategy: PartitionAssignmentStrategy,
    /// If true, committed offsets track the last record **delivered to `poll()`**
    /// rather than the last record **fetched from the broker**.
    ///
    /// This provides **at-least-once** delivery semantics: on restart,
    /// the consumer resumes from the last delivered position, which may
    /// cause some messages to be re-processed.
    ///
    /// Default: `false` (at-most-once, same as before).
    pub enable_at_least_once: bool,
    /// Maximum number of retries for retriable offset commit failures.
    pub retries: i32,
    /// Initial backoff delay between retries (doubles each attempt).
    pub retry_backoff: Duration,
}

impl ConsumerConfig {
    /// Create a new consumer config with sensible defaults in **direct mode**
    /// (no consumer group coordination).
    ///
    /// Call [`with_group_id`](Self::with_group_id) to enable consumer group mode.
    pub fn new() -> Self {
        Self {
            group_id: None,
            auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            auto_offset_reset: AutoOffsetReset::Latest,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            partition_max_bytes: 1024 * 1024,
            max_wait: Duration::from_millis(500),
            max_poll_records: 500,
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
            partition_assignment_strategy: PartitionAssignmentStrategy::Range,
            enable_at_least_once: false,
            retries: 3,
            retry_backoff: Duration::from_millis(200),
        }
    }

    // ------------------------------------------------------------------
    // Builder methods
    // ------------------------------------------------------------------

    /// Set the consumer group ID, enabling group coordination.
    pub fn with_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.group_id = Some(group_id.into());
        self
    }

    /// Enable or disable automatic offset commit.
    pub fn with_auto_commit(mut self, auto_commit: bool) -> Self {
        self.auto_commit = auto_commit;
        self
    }

    /// Set the interval between automatic offset commits.
    pub fn with_auto_commit_interval(mut self, interval: Duration) -> Self {
        self.auto_commit_interval = interval;
        self
    }

    /// Set what to do when there is no committed offset or the offset is out of range.
    pub fn with_auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Convenience: reset offset to the earliest available (beginning of partition).
    pub fn with_earliest(mut self) -> Self {
        self.auto_offset_reset = AutoOffsetReset::Earliest;
        self
    }

    /// Convenience: reset offset to the latest available (end of partition).
    pub fn with_latest(mut self) -> Self {
        self.auto_offset_reset = AutoOffsetReset::Latest;
        self
    }

    /// Set minimum bytes of data to return in a single Fetch response.
    pub fn with_min_bytes(mut self, min_bytes: i32) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    /// Set maximum bytes of data to return in a single Fetch response.
    pub fn with_max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Set maximum bytes per partition in a single Fetch response.
    pub fn with_partition_max_bytes(mut self, partition_max_bytes: i32) -> Self {
        self.partition_max_bytes = partition_max_bytes;
        self
    }

    /// Set the maximum time the broker will wait to satisfy `min_bytes`.
    pub fn with_max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// Set the maximum number of records returned by a single `poll()` call.
    pub fn with_max_poll_records(mut self, max_poll_records: usize) -> Self {
        self.max_poll_records = max_poll_records;
        self
    }

    /// Set group session timeout.
    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    /// Set maximum time a rebalance is allowed to complete.
    pub fn with_rebalance_timeout(mut self, timeout: Duration) -> Self {
        self.rebalance_timeout = timeout;
        self
    }

    /// Set the interval between heartbeats sent to the group coordinator.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set the partition assignment strategy.
    pub fn with_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.partition_assignment_strategy = strategy;
        self
    }

    /// Enable at-least-once delivery semantics.
    ///
    /// When enabled, committed offsets track the last record delivered to
    /// `poll()` rather than the last record fetched from the broker.
    /// On restart, the consumer resumes from the last delivered position.
    pub fn with_at_least_once(mut self) -> Self {
        self.enable_at_least_once = true;
        self
    }

    /// Set the maximum number of retries for retriable offset commit failures.
    pub fn with_retries(mut self, retries: i32) -> Self {
        self.retries = retries;
        self
    }

    /// Set the initial backoff delay between retries (doubles each attempt).
    pub fn with_retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_backoff = backoff;
        self
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_config_defaults_are_direct_mode() {
        let config = ConsumerConfig::new();
        assert_eq!(config.group_id, None, "default is direct (non-group) mode");
        assert!(config.auto_commit);
        assert_eq!(config.auto_commit_interval, Duration::from_secs(5));
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Latest);
        assert_eq!(config.max_poll_records, 500);
        assert_eq!(config.max_wait, Duration::from_millis(500));
        assert!(!config.enable_at_least_once);
        assert_eq!(config.retries, 3);
    }

    #[test]
    fn with_group_id_enables_group_mode() {
        let config = ConsumerConfig::new().with_group_id("my-group").with_earliest();
        assert_eq!(config.group_id.as_deref(), Some("my-group"));
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
    }

    #[test]
    fn with_at_least_once_flips_delivery_semantics() {
        let config = ConsumerConfig::new().with_at_least_once();
        assert!(config.enable_at_least_once);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAssignmentStrategy {
    /// Range assignment — each topic's partitions are divided contiguously
    /// among members. May cause uneven distribution when topics have
    /// different partition counts.
    Range,
    /// Round-robin assignment — partitions are distributed evenly across
    /// members in cyclic order.
    RoundRobin,
    /// Cooperative sticky assignment (simplified).
    ///
    /// This is a **simplified** implementation that distributes partitions
    /// using a round-robin-like algorithm. A full CooperativeSticky
    /// implementation would need to track each member's previous assignment
    /// across rebalances and minimize partition movement.
    CooperativeSticky,
}
