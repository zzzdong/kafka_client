//! Internal consumer types — shared between consumer variants and the reactor.

use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::sync::oneshot;

use crate::error::Result;
use crate::protocol::FetchRequest;
use kafka_client_protocol::RecordBatch;

// ---------------------------------------------------------------------------
// Reactor states
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReactorState {
    Init,
    Joining,
    Fetching,
    Rebalancing,
    /// Consumer has left the group. Waits for a new Subscribe command.
    Stopped,
}

// ---------------------------------------------------------------------------
// Commands sent via mpsc to the reactor
// ---------------------------------------------------------------------------
pub(crate) enum ConsumerCommand {
    Subscribe {
        topics: Vec<String>,
    },
    Commit {
        reply: oneshot::Sender<Result<()>>,
    },
    GetOffset {
        topic: String,
        partition: i32,
        reply: oneshot::Sender<Option<i64>>,
    },
    SetOffset {
        topic: String,
        partition: i32,
        offset: i64,
    },
    Heartbeat {
        reply: oneshot::Sender<Result<()>>,
    },
    Leave {
        reply: oneshot::Sender<Result<()>>,
    },
    GetAssignment {
        reply: oneshot::Sender<HashMap<String, Vec<i32>>>,
    },
    /// Trigger try_send_fetches() — used after poll() to prime pipeline
    TryFetch,
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
