//! Internal consumer types — shared between the unified background task
//! and the public Consumer facade.

use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::protocol::FetchRequest;
use kafka_client_protocol::RecordBatch;

// ---------------------------------------------------------------------------
// Modes
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConsumerMode {
    /// No consumer group — fetch all assigned partitions directly
    Direct,
    /// Consumer group — join/sync + heartbeat + commit
    Group,
}

// ---------------------------------------------------------------------------
// Commands sent via mpsc to the background task
// ---------------------------------------------------------------------------
pub(crate) enum ConsumerCommand {
    /// Subscribe to topics (group mode: join group; direct mode: get all partitions)
    /// `reply` is resolved when partition assignment and offset initialization complete.
    Subscribe {
        topics: Vec<String>,
        reply: Option<oneshot::Sender<Result<()>>>,
    },
    /// Manually assign specific partitions (direct mode only)
    /// `reply` is resolved when offset initialization completes.
    Assign {
        topic: String,
        partitions: Vec<i32>,
        reply: Option<oneshot::Sender<Result<()>>>,
    },
    /// Manually set offset for a partition
    Seek {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Commit current offsets (group mode only)
    Commit { reply: oneshot::Sender<Result<()>> },
    /// Get current offset for a partition
    GetOffset {
        topic: String,
        partition: i32,
        reply: oneshot::Sender<Option<i64>>,
    },
    /// Set offset for a partition (internal use)
    SetOffset {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Send a heartbeat (group mode only)
    Heartbeat { reply: oneshot::Sender<Result<()>> },
    /// Signal that the consumer has started polling (triggers fetching to begin).
    /// Fetches are NOT started automatically on partition assignment — they wait
    /// for this command to avoid buffering records before any receiver is ready.
    StartPolling,
    /// Leave the consumer group
    Leave { reply: oneshot::Sender<Result<()>> },
    /// Get current partition assignment
    GetAssignment {
        reply: oneshot::Sender<HashMap<String, Vec<i32>>>,
    },
    /// Get the group's current generation and member id (for transactional
    /// offset commits).
    GetGroupMetadata {
        reply: oneshot::Sender<(i32, String)>,
    },
    /// Shutdown the background task
    Shutdown,
    /// Unsubscribe from all topics (direct mode: clear assignment;
    /// group mode: also leave the group)
    Unsubscribe { reply: oneshot::Sender<Result<()>> },
}

// ---------------------------------------------------------------------------
// Fetch parameters
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub(crate) struct FetchParams {
    pub timeout_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub partition_max_bytes: i32,
}

// ---------------------------------------------------------------------------
// Record types
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Bytes,
}

#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub headers: Vec<Header>,
}

// ---------------------------------------------------------------------------
// Pipelining types
// ---------------------------------------------------------------------------

/// Cursor over a single RecordBatch, consuming records one at a time.
pub(crate) struct RecordBatchCursor {
    topic: String,
    partition: i32,
    base_offset: i64,
    first_timestamp: i64,
    records: std::vec::IntoIter<kafka_client_protocol::Record>,
}

impl RecordBatchCursor {
    pub(crate) fn new(topic: String, partition: i32, batch: RecordBatch) -> Self {
        Self {
            base_offset: batch.base_offset,
            first_timestamp: batch.first_timestamp,
            records: batch.records.into_iter(),
            topic,
            partition,
        }
    }

    pub(crate) fn next(&mut self) -> Option<ConsumerRecord> {
        let rec = self.records.next()?;
        let offset = self.base_offset + rec.offset_delta as i64;
        Some(ConsumerRecord {
            topic: self.topic.clone(),
            partition: self.partition,
            offset,
            timestamp: self.first_timestamp + rec.timestamp_delta,
            key: rec.key,
            value: rec.value.unwrap_or_default(),
            headers: rec
                .headers
                .into_iter()
                .map(|h| Header {
                    key: h.key,
                    value: h.value.unwrap_or_default(),
                })
                .collect(),
        })
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.records.as_slice().is_empty()
    }
}

/// Result of a single partition's fetch from a background task.
pub(crate) struct CompletedFetch {
    pub(crate) topic: String,
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: i32,
    pub(crate) error_code: i16,
    pub(crate) records: Option<RecordBatch>,
}

/// A fetch-request task sent to the background fetch manager.
pub(crate) struct FetchRequestTask {
    pub(crate) broker_addr: SocketAddr,
    pub(crate) request: FetchRequest,
    /// The partitions included in this request (for error reporting).
    pub(crate) partitions: Vec<(String, i32)>,
}
