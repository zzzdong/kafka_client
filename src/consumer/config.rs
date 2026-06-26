//! Consumer configuration types.

use std::time::Duration;

// ---------------------------------------------------------------------------
// Configuration & enums
// ---------------------------------------------------------------------------

/// Kafka consumer configuration.
///
/// # Construction
///
/// Use `ConsumerConfig::new(...)` for sensible defaults, then chain builder
/// methods to customize:
///
/// ```
/// use kafka_client::{ConsumerConfig, AutoOffsetReset};
/// use std::time::Duration;
///
/// // Quick start — all defaults
/// let config = ConsumerConfig::new("my-group");
///
/// // Builder pattern — chain with_* calls
/// let config = ConsumerConfig::new("my-group")
///     .with_earliest()
///     .with_max_poll_records(1000)
///     .with_max_wait(Duration::from_secs(5));
/// ```
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer group ID. Members with the same group_id share partition assignments.
    /// Empty string means simple (non-group) consumer mode.
    pub group_id: String,
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
}

impl ConsumerConfig {
    /// Create a new consumer config with sensible defaults.
    ///
    /// Only the consumer `group_id` is required; all other parameters
    /// use Kafka-recommended defaults.
    ///
    /// # Simple (non-group) consumer
    ///
    /// Pass an empty string to create a simple consumer that fetches directly
    /// without consumer group coordination:
    ///
    /// ```
    /// use kafka_client::ConsumerConfig;
    /// let config = ConsumerConfig::new("");
    /// ```
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
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
        }
    }

    // ------------------------------------------------------------------
    // Builder methods
    // ------------------------------------------------------------------

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
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self::new(format!("{}-consumer", crate::NAME))
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
    Range,
    RoundRobin,
    CooperativeSticky,
}
