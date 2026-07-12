//! Producer - high-level Kafka message producer
//!
//! Architecture:
//!
//! ```text
//! Producer::send(record)
//!   → ProducerCommand::Buffer { record, oneshot }
//!     → Background event loop: buffer_records() — batches by (topic, partition)
//!     → Linger tick or batch_size threshold → flush_buffer()
//!       → Group by leader broker → send to each broker in parallel
//!       → Resolve per-record oneshots with metadata
//! ```
//!
//! All sends go through the buffer. `send()` returns a `Future` that resolves
//! when the batch is actually flushed, not when the record is enqueued.
//! This is the same batching model as Java's KafkaProducer.

mod router;

pub use router::{PartitionRouter, PartitionRouting};

use bytes::Bytes;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::cluster::ClusterClient;
use crate::error::{KafkaError, Result};
use crate::protocol::{
    PartitionProduceData, ProduceRequest, ProduceResponse, Record, RecordBatch, TopicProduceData,
};
use kafka_client_protocol::KafkaErrorCode;
use kafka_client_protocol::init_producer_id_request::InitProducerIdRequest;
use kafka_client_protocol::init_producer_id_response::InitProducerIdResponse;

// ---------------------------------------------------------------------------
// Buffered entry — a group of records for the same (topic, partition)
// ---------------------------------------------------------------------------
struct BufferedEntry {
    records: Vec<ProducerRecord>,
    /// Per-record oneshots (in insertion order). Only `send()` creates these;
    /// `SendBatch` records do not have tracking.
    pending: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    estimated_bytes: usize,
}

impl BufferedEntry {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            pending: Vec::new(),
            estimated_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------
enum ProducerCommand {
    /// Buffer a single record — oneshot resolves after batch is flushed
    Send {
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    },
    /// Buffer records for batched sending (no per-record tracking)
    SendBatch {
        records: Vec<ProducerRecord>,
        result_tx: oneshot::Sender<Result<usize>>,
    },
    /// Send a record directly, bypassing the buffer
    SendDirect {
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    },
    /// Force flush the buffer
    Flush {
        barrier: oneshot::Sender<Result<()>>,
    },
    /// Shut down the background loop (best-effort flush, no ack)
    Shutdown,
    /// Shut down the background loop and wait for flush to complete
    ShutdownWithAck { done: oneshot::Sender<Result<()>> },
}

/// Message header
#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Bytes,
}

/// Producer record
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: Option<i64>,
    pub headers: Vec<Header>,
}

impl ProducerRecord {
    pub fn new(topic: impl Into<String>, value: Bytes) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value,
            timestamp: None,
            headers: Vec::new(),
        }
    }

    pub fn with_key(mut self, key: Bytes) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn with_headers(mut self, headers: Vec<Header>) -> Self {
        self.headers = headers;
        self
    }
}

/// Send metadata
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub acks: i16,
    pub timeout_ms: i32,
    pub routing: PartitionRouting,
    pub retries: u32,
    /// Maximum total time (ms) for a send operation, including retries.
    ///
    /// If the total time exceeds this limit, the send fails with the last
    /// error.  Default: 120_000 (2 minutes).
    pub delivery_timeout_ms: u64,
    pub batch_size: usize,
    pub linger_ms: u64,
    /// Maximum byte size of a single ProduceRequest per partition.
    /// Records exceeding this are split into multiple requests.
    /// Default matches Kafka's default max.message.bytes (1MB).
    pub max_batch_bytes: usize,
    /// Enable idempotent producer.
    ///
    /// When enabled, the producer automatically gets a producer ID and
    /// epoch from the broker, and attaches per-partition sequence numbers
    /// to every batch. The broker deduplicates on (PID, partition, sequence),
    /// preventing duplicate messages caused by retries.
    ///
    /// This also forces `acks = -1` (all) and sets a reasonable delivery
    /// timeout for indefinite retries.
    ///
    /// Default: `false`.
    pub enable_idempotence: bool,
}

impl ProducerConfig {
    pub fn new() -> Self {
        Self {
            acks: 1,
            timeout_ms: 5000,
            routing: PartitionRouting::HashKey,
            retries: 5,
            delivery_timeout_ms: 120_000,
            batch_size: 16384,
            linger_ms: 100,
            max_batch_bytes: 1_048_576,
            enable_idempotence: false,
        }
    }

    pub fn with_acks(mut self, acks: i16) -> Self {
        self.acks = acks;
        self
    }

    pub fn with_timeout(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_routing(mut self, routing: PartitionRouting) -> Self {
        self.routing = routing;
        self
    }

    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set the delivery timeout in milliseconds.
    ///
    /// This bounds the total time spent retrying a single send operation.
    /// Default: 120_000 (2 minutes).
    pub fn with_delivery_timeout(mut self, timeout_ms: u64) -> Self {
        self.delivery_timeout_ms = timeout_ms;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_linger(mut self, linger_ms: u64) -> Self {
        self.linger_ms = linger_ms;
        self
    }

    pub fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes;
        self
    }

    /// Enable idempotent producer.
    ///
    /// When idempotence is enabled, the producer attaches a producer ID,
    /// epoch, and per-partition sequence numbers to every batch, so the
    /// broker can deduplicate messages produced by retries.
    ///
    /// This automatically sets `acks = -1` (all) and sets a large
    /// delivery timeout (5 minutes) for indefinite retries.
    pub fn with_enable_idempotence(mut self) -> Self {
        self.enable_idempotence = true;
        self.acks = -1;
        self.retries = i32::MAX as u32;
        self.delivery_timeout_ms = 300_000; // 5 minutes
        self
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Internal types
// ===========================================================================

/// A chunk of records for a single partition, ready to send in one
/// ProduceRequest.  The pending senders correspond 1:1 with the records
/// (in order) for the Send API; they are empty for the SendBatch API.
struct PartitionChunk {
    topic: String,
    partition: i32,
    records: Vec<ProducerRecord>,
    pending: Vec<oneshot::Sender<Result<RecordMetadata>>>,
}

// ===========================================================================
// Internal state
// ===========================================================================

struct ProducerState {
    cluster: Arc<ClusterClient>,
    router: PartitionRouter,
    config: ProducerConfig,
    /// Buffered records by (topic, partition)
    buffer: HashMap<(String, i32), BufferedEntry>,
    /// Total buffered bytes (for batch_size check)
    total_buffered_bytes: usize,
    /// Last buffer flush time
    last_send: Instant,
    // Idempotent producer state
    producer_id: i64,
    producer_epoch: i16,
    /// Whether InitProducerIdRequest has been completed.
    producer_id_initialized: bool,
    /// Per-partition sequence numbers (for idempotent producer).
    /// Accessed via joint_all futures from send_records_to_partition,
    /// so interior mutability through `&self` is needed.
    sequence_numbers: Mutex<HashMap<(String, i32), i32>>,
}

impl ProducerState {
    async fn buffer_single(
        &mut self,
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    ) {
        let partition = self.select_partition(&record).await;
        match partition {
            Ok(partition) => {
                let entry = self
                    .buffer
                    .entry((record.topic.clone(), partition))
                    .or_insert_with(BufferedEntry::new);
                let estimated =
                    record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0) + 64;
                self.total_buffered_bytes += estimated;
                entry.estimated_bytes += estimated;
                entry.pending.push(result_tx);
                entry.records.push(record);
            }
            Err(e) => {
                let _ = result_tx.send(Err(e));
            }
        }
    }

    async fn buffer_records(&mut self, records: Vec<ProducerRecord>) -> Result<usize> {
        let count = records.len();
        for record in records {
            let partition = self.select_partition(&record).await?;
            let entry = self
                .buffer
                .entry((record.topic.clone(), partition))
                .or_insert_with(BufferedEntry::new);
            let estimated =
                record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0) + 64;
            self.total_buffered_bytes += estimated;
            entry.estimated_bytes += estimated;
            entry.records.push(record);
        }
        Ok(count)
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        debug!("flush_buffer: {} entries", self.buffer.len());
        let buffer = std::mem::take(&mut self.buffer);
        self.total_buffered_bytes = 0;
        self.last_send = Instant::now();

        // Lazy init: send InitProducerIdRequest on first flush when idempotence
        // is enabled.  Deferring this to flush time keeps the constructor
        // infallible.
        if self.config.enable_idempotence && !self.producer_id_initialized {
            let request = InitProducerIdRequest {
                transactional_id: None,
                transaction_timeout_ms: 0,
                producer_id: -1,
                producer_epoch: -1,
                enable2_pc: false,
                keep_prepared_txn: false,
            };

            let deadline = Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms);
            let mut init_error = None;
            let mut backoff = Duration::from_millis(100);
            for attempt in 0..self.config.retries.max(1) {
                if Instant::now() >= deadline {
                    init_error = Some(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));
                    break;
                }
                let response: Result<InitProducerIdResponse> =
                    self.cluster.send_to_any_broker(&request).await;
                match response {
                    Ok(resp) if resp.error_code == 0 => {
                        self.producer_id = resp.producer_id;
                        self.producer_epoch = resp.producer_epoch;
                        self.producer_id_initialized = true;
                        debug!(
                            "Obtained producer_id={}, epoch={} (attempt {})",
                            resp.producer_id,
                            resp.producer_epoch,
                            attempt + 1
                        );
                        break;
                    }
                    Ok(resp) => {
                        let code = KafkaErrorCode::from_i16(resp.error_code);
                        let err = KafkaError::ProduceError(code);
                        let retryable = code.is_retriable();
                        warn!(
                            "InitProducerId failed with {} (attempt {})",
                            err,
                            attempt + 1
                        );
                        init_error = Some(err);
                        if !retryable || attempt + 1 >= self.config.retries.max(1) {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Failed to init producer id (attempt {}): {}",
                            attempt + 1,
                            e
                        );
                        let retryable = match &e {
                            KafkaError::Io(_) | KafkaError::ConnectionClosed => true,
                            KafkaError::ProduceError(code) => code.is_retriable(),
                            _ => false,
                        };
                        init_error = Some(e);
                        if !retryable || attempt + 1 >= self.config.retries.max(1) {
                            break;
                        }
                    }
                }
                tokio::time::sleep(backoff).await;
                backoff = backoff.mul_f32(2.0).min(Duration::from_secs(10));
            }

            if !self.producer_id_initialized {
                let err =
                    init_error.unwrap_or(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));
                for (_, entry) in buffer {
                    for tx in entry.pending {
                        let _ = tx.send(Err(err.clone()));
                    }
                }
                return Err(err);
            }
        }

        // Step 1: Resolve all partition leaders (retry with backoff if missing)
        let mut resolved: Vec<((String, i32), BufferedEntry, SocketAddr)> = Vec::new();
        let mut unresolvable: Vec<((String, i32), BufferedEntry)> = Vec::new();

        for ((topic, partition), entry) in buffer {
            let leader = self
                .cluster
                .metadata()
                .get_partition_leader(&topic, partition)
                .await;
            if let Some(addr) = leader {
                resolved.push(((topic, partition), entry, addr));
            } else {
                unresolvable.push(((topic, partition), entry));
            }
        }

        // Retry unresolvable with metadata refresh + backoff (leader may not be
        // assigned immediately after topic creation, leader_id == -1)
        let mut pending = unresolvable;
        for retry in 0..5 {
            if pending.is_empty() {
                break;
            }
            let _ = self.cluster.refresh_metadata().await;
            let mut still_missing = Vec::new();
            let batch = std::mem::take(&mut pending);
            for ((topic, partition), entry) in batch {
                let leader = self
                    .cluster
                    .metadata()
                    .get_partition_leader(&topic, partition)
                    .await;
                if let Some(addr) = leader {
                    resolved.push(((topic, partition), entry, addr));
                } else {
                    still_missing.push(((topic, partition), entry));
                }
            }
            pending = still_missing;
            if retry < 4 {
                tokio::time::sleep(Duration::from_millis(200 * (retry as u64 + 1))).await;
            }
        }
        for ((topic, partition), entry) in pending {
            warn!("No leader for {}/{} after retries", topic, partition);
            for tx in entry.pending {
                let _ = tx.send(Err(KafkaError::PartitionNotFound(topic.clone(), partition)));
            }
        }

        // Step 2: Group resolved entries by leader broker
        let mut by_broker: HashMap<SocketAddr, HashMap<(String, i32), BufferedEntry>> =
            HashMap::new();
        for ((topic, partition), entry, leader) in resolved {
            by_broker
                .entry(leader)
                .or_default()
                .insert((topic, partition), entry);
        }

        // Step 3: Send to all brokers in parallel (each broker's partitions
        // are also sent concurrently via join_all inside send_to_broker_batch)
        use futures::future::join_all;
        let handles: Vec<_> = by_broker
            .into_iter()
            .map(|(broker, entries)| self.send_to_broker_batch(broker, entries))
            .collect();
        debug!("flush_buffer: {} brokers", handles.len());
        let results: Vec<Result<()>> = join_all(handles).await;

        // Collect the first error if any, so flush() can propagate it upward.
        for r in results {
            r?;
        }
        Ok(())
    }

    async fn send_to_broker_batch(
        &self,
        _broker: SocketAddr,
        entries: HashMap<(String, i32), BufferedEntry>,
    ) -> Result<()> {
        debug!("send_to_broker_batch: {} partitions", entries.len());
        if entries.is_empty() {
            return Ok(());
        }

        let max_bytes = self.config.max_batch_bytes;
        let per_record_overhead = 64; // conservative per-record wire-format overhead
        let idempotent = self.config.enable_idempotence;

        // Split each partition's records into chunks that fit within
        // max_batch_bytes. This prevents MESSAGE_TOO_LARGE errors when
        // a single ProduceRequest exceeds Kafka's max.message.bytes.
        //
        // When idempotent producer is enabled, we do NOT split chunks
        // because per-partition sequence numbers must be strictly
        // increasing. Splitting would send chunks for the same partition
        // in parallel, potentially causing OUT_OF_ORDER_SEQUENCE_NUMBER.
        let mut chunks: Vec<PartitionChunk> = Vec::new();

        for ((topic, partition), entry) in entries {
            let mut chunk_records: Vec<ProducerRecord> = Vec::new();
            let mut chunk_pending: Vec<oneshot::Sender<Result<RecordMetadata>>> = Vec::new();
            let mut chunk_bytes: usize = 0;
            let mut pending_iter = entry.pending.into_iter();

            for record in entry.records {
                let record_bytes = record.value.len()
                    + record.key.as_ref().map(|k| k.len()).unwrap_or(0)
                    + per_record_overhead;

                // Start a new chunk if this record would exceed the limit
                // (only if chunk already has records — don't create empty chunks)
                // Skip splitting for idempotent producer.
                if !idempotent
                    && !chunk_records.is_empty()
                    && chunk_bytes + record_bytes > max_bytes
                {
                    chunks.push(PartitionChunk {
                        topic: topic.clone(),
                        partition,
                        records: std::mem::take(&mut chunk_records),
                        pending: std::mem::take(&mut chunk_pending),
                    });
                    chunk_bytes = 0;
                }

                chunk_bytes += record_bytes;
                chunk_records.push(record);
                if let Some(tx) = pending_iter.next() {
                    chunk_pending.push(tx);
                }
            }

            if !chunk_records.is_empty() {
                chunks.push(PartitionChunk {
                    topic,
                    partition,
                    records: chunk_records,
                    pending: chunk_pending,
                });
            }
        }

        // Fire all chunk sends in parallel
        let futs: Vec<_> = chunks
            .iter()
            .map(|chunk| {
                self.send_records_to_partition(&chunk.topic, chunk.partition, chunk.records.clone())
            })
            .collect();
        let results = futures::future::join_all(futs).await;

        let mut first_error = None;
        let mut remaining: Vec<PartitionChunk> = Vec::new();

        for (result, chunk) in results.into_iter().zip(chunks) {
            match result {
                Ok(metadata) => {
                    for (idx, tx) in chunk.pending.into_iter().enumerate() {
                        let per_record_meta = RecordMetadata {
                            offset: metadata.offset + idx as i64,
                            ..metadata.clone()
                        };
                        let _ = tx.send(Ok(per_record_meta));
                    }
                }
                Err(KafkaError::ProduceError(code))
                    if code == KafkaErrorCode::MESSAGE_TOO_LARGE && chunk.records.len() > 1 =>
                {
                    // MESSAGE_TOO_LARGE: binary-split and retry each half.
                    warn!(
                        "MESSAGE_TOO_LARGE for {}/{} ({} records), splitting and retrying",
                        chunk.topic,
                        chunk.partition,
                        chunk.records.len()
                    );
                    let mid = chunk.records.len() / 2;
                    let (r1, r2) =
                        chunk
                            .records
                            .into_iter()
                            .enumerate()
                            .partition::<Vec<(usize, ProducerRecord)>, _>(|(i, _)| *i < mid);
                    let records1: Vec<ProducerRecord> = r1.into_iter().map(|(_, r)| r).collect();
                    let records2: Vec<ProducerRecord> = r2.into_iter().map(|(_, r)| r).collect();
                    let (p1, p2) = chunk.pending.into_iter().enumerate().partition::<Vec<(
                        usize,
                        oneshot::Sender<Result<RecordMetadata>>,
                    )>, _>(
                        |(i, _)| *i < mid
                    );
                    let pending1: Vec<_> = p1.into_iter().map(|(_, tx)| tx).collect();
                    let pending2: Vec<_> = p2.into_iter().map(|(_, tx)| tx).collect();
                    if !records1.is_empty() {
                        remaining.push(PartitionChunk {
                            topic: chunk.topic.clone(),
                            partition: chunk.partition,
                            records: records1,
                            pending: pending1,
                        });
                    }
                    if !records2.is_empty() {
                        remaining.push(PartitionChunk {
                            topic: chunk.topic,
                            partition: chunk.partition,
                            records: records2,
                            pending: pending2,
                        });
                    }
                }
                Err(e) => {
                    warn!("Send to partition failed: {}", e);
                    let err = e.clone();
                    for tx in chunk.pending {
                        let _ = tx.send(Err(e.clone()));
                    }
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        // Retry remaining (split) chunks — each individual record that fails
        // again with MESSAGE_TOO_LARGE truly exceeds the broker limit.
        while !remaining.is_empty() {
            let batch = std::mem::take(&mut remaining);
            let futs: Vec<_> = batch
                .iter()
                .map(|chunk| {
                    self.send_records_to_partition(
                        &chunk.topic,
                        chunk.partition,
                        chunk.records.clone(),
                    )
                })
                .collect();
            let results = futures::future::join_all(futs).await;
            for (result, chunk) in results.into_iter().zip(batch) {
                match result {
                    Ok(metadata) => {
                        for (idx, tx) in chunk.pending.into_iter().enumerate() {
                            let per_record_meta = RecordMetadata {
                                offset: metadata.offset + idx as i64,
                                ..metadata.clone()
                            };
                            let _ = tx.send(Ok(per_record_meta));
                        }
                    }
                    Err(KafkaError::ProduceError(code))
                        if code == KafkaErrorCode::MESSAGE_TOO_LARGE =>
                    {
                        // Single record that's still too large — cannot split further.
                        let err = KafkaError::ProduceError(KafkaErrorCode::MESSAGE_TOO_LARGE);
                        warn!("Record exceeds broker max.message.bytes: {}", err);
                        for tx in chunk.pending {
                            let _ = tx.send(Err(err.clone()));
                        }
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                    Err(e) => {
                        warn!("Send to partition failed: {}", e);
                        let err = e.clone();
                        for tx in chunk.pending {
                            let _ = tx.send(Err(e.clone()));
                        }
                        if first_error.is_none() {
                            first_error = Some(err);
                        }
                    }
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn send_records_to_partition(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<ProducerRecord>,
    ) -> Result<RecordMetadata> {
        // Pre-assign sequence number for idempotent producer (used for all
        // retry attempts so the broker deduplicates correctly).
        let base_sequence = if self.config.enable_idempotence {
            let mut seq_map = self.sequence_numbers.lock().unwrap();
            let entry = seq_map.entry((topic.to_string(), partition)).or_insert(0);
            let seq = *entry;
            *entry += records.len() as i32;
            seq
        } else {
            0
        };

        let mut last_error = None;
        let deadline = Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms);
        for attempt in 0..self.config.retries.max(1) {
            if Instant::now() >= deadline {
                break;
            }

            let request = self
                .build_request(topic, partition, &records, base_sequence)
                .await?;
            let response: Result<ProduceResponse> = self
                .cluster
                .send_to_partition(topic, partition, &request)
                .await;

            debug!(
                "send_records: topic={}, partition={}, attempt={}",
                topic,
                partition,
                attempt + 1,
            );

            match response {
                Ok(resp) => {
                    // DUPLICATE_SEQUENCE_NUMBER means the broker already
                    // accepted this batch — treat as success.
                    let has_dup = resp
                        .responses
                        .iter()
                        .flat_map(|tr| &tr.partition_responses)
                        .any(|pr| {
                            KafkaErrorCode::from_i16(pr.error_code)
                                == KafkaErrorCode::DUPLICATE_SEQUENCE_NUMBER
                        });
                    if has_dup {
                        return Ok(RecordMetadata {
                            topic: topic.to_string(),
                            partition,
                            offset: -1,
                            timestamp: 0,
                        });
                    }
                    match self.parse_response(topic, partition, resp).await {
                        Ok(meta) => return Ok(meta),
                        Err(e) => {
                            let is_retryable = match &e {
                                KafkaError::ProduceError(code) => code.is_retriable(),
                                _ => false,
                            };
                            debug!(
                                "send_records response error: topic={}, partition={}, error={}, retryable={}",
                                topic, partition, e, is_retryable,
                            );
                            last_error = Some(e);
                            if is_retryable {
                                let _ = self.cluster.refresh_metadata().await;
                            } else {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    let is_retryable = match &e {
                        KafkaError::ProduceError(code) => code.is_retriable(),
                        KafkaError::TopicNotFound(_) | KafkaError::PartitionNotFound(_, _) => true,
                        _ => false,
                    };
                    debug!(
                        "send_records error: topic={}, partition={}, error={}, retryable={}",
                        topic, partition, e, is_retryable,
                    );
                    last_error = Some(e);
                    if is_retryable {
                        let _ = self.cluster.refresh_metadata().await;
                    } else {
                        break;
                    }
                }
            }

            // Exponential backoff with jitter, capped by delivery timeout
            if attempt + 1 < self.config.retries {
                let base = 100u64 * (1u64 << attempt.min(6)); // 100, 200, 400, ... up to 6400
                let jitter: u64 = rand::random_range(0..base);
                let delay = Duration::from_millis(base + jitter);
                let remaining = deadline.saturating_duration_since(Instant::now());
                tokio::time::sleep(delay.min(remaining)).await;
            }
        }
        Err(last_error.unwrap_or(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1))))
    }

    async fn build_request(
        &self,
        topic: &str,
        partition: i32,
        records: &[ProducerRecord],
        base_sequence: i32,
    ) -> Result<ProduceRequest> {
        let batch = self.build_record_batch(records, base_sequence)?;

        let topic_meta = self.cluster.metadata().get_topic(topic).await;
        let topic_id = topic_meta
            .as_ref()
            .map(|t| t.topic_id)
            .unwrap_or_else(uuid::Uuid::nil);

        debug!(
            "build_request: topic={}, partition={}, topic_id={}, records={}",
            topic,
            partition,
            topic_id,
            records.len(),
        );

        Ok(ProduceRequest {
            transactional_id: None,
            acks: self.config.acks,
            timeout_ms: self.config.timeout_ms,
            topic_data: vec![TopicProduceData {
                name: topic.to_string(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: partition,
                    records: Some(batch),
                }],
            }],
        })
    }

    fn build_record_batch(
        &self,
        records: &[ProducerRecord],
        base_sequence: i32,
    ) -> Result<RecordBatch> {
        let refs: Vec<&ProducerRecord> = records.iter().collect();
        let mut batch = Self::build_record_batch_inner(&refs)?;
        if self.config.enable_idempotence {
            batch.producer_id = self.producer_id;
            batch.producer_epoch = self.producer_epoch;
            batch.base_sequence = base_sequence;
        }
        Ok(batch)
    }

    async fn select_partition(&self, record: &ProducerRecord) -> Result<i32> {
        if let Some(p) = record.partition {
            return Ok(p);
        }

        let partition_count = self
            .cluster
            .metadata()
            .get_partition_count(&record.topic)
            .await
            .ok_or_else(|| KafkaError::TopicNotFound(record.topic.clone()))?;

        let key = record.key.as_deref();
        Ok(self.router.select_partition(key, partition_count))
    }

    async fn parse_response(
        &self,
        topic: &str,
        partition: i32,
        response: ProduceResponse,
    ) -> Result<RecordMetadata> {
        let topic_id = self
            .cluster
            .metadata()
            .get_topic(topic)
            .await
            .map(|t| t.topic_id);

        for topic_response in &response.responses {
            let name_matches = !topic_response.name.is_empty() && topic_response.name == topic;
            let id_matches = topic_id
                .map(|id| !id.is_nil() && topic_response.topic_id == id)
                .unwrap_or(false);

            if !name_matches && !id_matches {
                continue;
            }

            for partition_response in &topic_response.partition_responses {
                if partition_response.index == partition {
                    if partition_response.error_code != 0 {
                        return Err(KafkaError::ProduceError(KafkaErrorCode::from_i16(
                            partition_response.error_code,
                        )));
                    }
                    return Ok(RecordMetadata {
                        topic: topic.to_string(),
                        partition,
                        offset: partition_response.base_offset,
                        timestamp: partition_response.log_append_time_ms,
                    });
                }
            }
        }
        Err(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)))
    }

    /// Send a single record directly and wait for ack — no buffer.
    /// Designed to be spawned as a standalone task so the background
    /// event loop is not blocked by network I/O.
    async fn send_direct_to_partition(
        cluster: Arc<ClusterClient>,
        config: &ProducerConfig,
        router: &PartitionRouter,
        record: ProducerRecord,
    ) -> Result<RecordMetadata> {
        let partition = if let Some(p) = record.partition {
            p
        } else {
            let topic = &record.topic;
            let partition_count = cluster
                .metadata()
                .get_partition_count(topic)
                .await
                .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;
            let key = record.key.as_deref();
            router.select_partition(key, partition_count)
        };
        let batch = ProducerState::build_record_batch_inner(&[&record])?;
        let topic = record.topic.clone();

        let mut last_error = None;
        for attempt in 0..config.retries {
            let topic_meta = cluster.metadata().get_topic(&topic).await;
            let topic_id = topic_meta
                .as_ref()
                .map(|t| t.topic_id)
                .unwrap_or_else(uuid::Uuid::nil);
            let request = ProduceRequest {
                transactional_id: None,
                acks: config.acks,
                timeout_ms: config.timeout_ms,
                topic_data: vec![TopicProduceData {
                    name: topic.clone(),
                    topic_id,
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(batch.clone()),
                    }],
                }],
            };
            let result = match cluster
                .send_to_partition::<_, ProduceResponse>(&topic, partition, &request)
                .await
            {
                Ok(response) => {
                    let mut found = false;
                    let mut parsed = Err(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));
                    for topic_response in &response.responses {
                        let name_matches =
                            !topic_response.name.is_empty() && topic_response.name == topic;
                        let id_matches =
                            topic_id != uuid::Uuid::nil() && topic_response.topic_id == topic_id;
                        if !name_matches && !id_matches {
                            continue;
                        }
                        for partition_response in &topic_response.partition_responses {
                            if partition_response.index == partition {
                                found = true;
                                if partition_response.error_code != 0 {
                                    parsed = Err(KafkaError::ProduceError(
                                        KafkaErrorCode::from_i16(partition_response.error_code),
                                    ));
                                } else {
                                    parsed = Ok(RecordMetadata {
                                        topic: topic.clone(),
                                        partition,
                                        offset: partition_response.base_offset,
                                        timestamp: partition_response.log_append_time_ms,
                                    });
                                }
                            }
                        }
                    }
                    if !found {
                        parsed = Err(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));
                    }
                    parsed
                }
                Err(e) => Err(e),
            };

            match result {
                Ok(meta) => return Ok(meta),
                Err(e) => {
                    let is_retryable = match &e {
                        KafkaError::ProduceError(code) => code.is_retriable(),
                        KafkaError::TopicNotFound(_) | KafkaError::PartitionNotFound(_, _) => true,
                        _ => false,
                    };
                    last_error = Some(e);
                    if is_retryable {
                        let _ = cluster.refresh_metadata().await;
                    } else {
                        break;
                    }
                }
            }

            if attempt + 1 < config.retries {
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
            }
        }
        Err(last_error.unwrap_or(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1))))
    }

    /// Helper to build a record batch without &self (for send_direct).
    fn build_record_batch_inner(records: &[&ProducerRecord]) -> Result<RecordBatch> {
        if records.is_empty() {
            return Err(KafkaError::InvalidConfiguration("Empty batch".to_string()));
        }
        let timestamps: Vec<i64> = records
            .iter()
            .map(|r| {
                r.timestamp.unwrap_or_else(|| {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                })
            })
            .collect();
        let first_timestamp = *timestamps.iter().min().unwrap_or(&0);
        let max_timestamp = *timestamps.iter().max().unwrap_or(&0);
        let mut batch = RecordBatch::new(0);
        batch.first_timestamp = first_timestamp;
        batch.max_timestamp = max_timestamp;
        for (idx, (record, timestamp)) in records.iter().zip(timestamps).enumerate() {
            let mut rec = Record::new(idx as i32, timestamp - first_timestamp)
                .with_value(record.value.clone());
            if let Some(ref key) = record.key {
                rec = rec.with_key(key.clone());
            }
            for header in &record.headers {
                rec = rec.with_header(header.key.clone(), header.value.clone());
            }
            batch.add_record(rec);
        }
        Ok(batch)
    }
}

// ===========================================================================
// Producer — public facade
// ===========================================================================

/// High-level Kafka Producer.
///
/// All messages go through an internal buffer. Automatic flush is triggered
/// by `batch_size` or `linger_ms`. Use [`flush`](Self::flush) to force-send
/// all buffered messages, and [`close`](Self::close) to clean up.
pub struct Producer {
    command_tx: tokio::sync::mpsc::UnboundedSender<ProducerCommand>,
}

impl Producer {
    /// Create Producer and start the background batch-sending task.
    pub(crate) async fn new(cluster: Arc<ClusterClient>, mut config: ProducerConfig) -> Self {
        // Dynamically discover broker's max.message.bytes to set max_batch_bytes.
        // The reactive MESSAGE_TOO_LARGE binary-split fallback handles cases where
        // the query fails or the actual limit differs (e.g. per-topic overrides).
        if let Some(server_max) = cluster.query_broker_config("max.message.bytes").await
            && server_max > 0
            && server_max < config.max_batch_bytes
        {
            debug!(
                "Using broker max.message.bytes={} for max_batch_bytes (was {})",
                server_max, config.max_batch_bytes
            );
            config.max_batch_bytes = server_max;
        }

        let state = ProducerState {
            router: PartitionRouter::new(config.routing),
            config: config.clone(),
            cluster,
            buffer: HashMap::new(),
            total_buffered_bytes: 0,
            last_send: Instant::now(),
            producer_id: -1,
            producer_epoch: -1,
            producer_id_initialized: false,
            sequence_numbers: Mutex::new(HashMap::new()),
        };

        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let linger = Duration::from_millis(config.linger_ms);

        tokio::spawn(async move {
            let mut state = state;
            let mut interval = tokio::time::interval(linger);

            loop {
                tokio::select! {
                    biased; // Shutdown first

                    cmd = command_rx.recv() => {
                        match cmd {
                            Some(ProducerCommand::Shutdown) => {
                                let _ = state.flush_buffer().await;
                                break;
                            }
                            Some(ProducerCommand::ShutdownWithAck { done }) => {
                                let result = state.flush_buffer().await;
                                if let Err(ref e) = result {
                                    warn!("Final flush during shutdown failed: {}", e);
                                }
                                let _ = done.send(result);
                                break;
                            }
                            Some(ProducerCommand::Send { record, result_tx }) => {
                                state.buffer_single(record, result_tx).await;
                                if state.total_buffered_bytes >= state.config.batch_size
                                    && let Err(e) = state.flush_buffer().await {
                                        warn!("Auto-flush after Send failed: {}", e);
                                    }
                            }
                            Some(ProducerCommand::SendBatch { records, result_tx }) => {
                                let result = state.buffer_records(records).await;
                                // NOTE: No auto-flush here. If flush fails, SendBatch's
                                // caller has no pending oneshot to receive the error
                                // (buffer_records does not create pending channels).
                                // Instead, rely on explicit flush() or the linger timer.
                                let _ = result_tx.send(result);
                            }
                            Some(ProducerCommand::SendDirect { record, result_tx }) => {
                                if state.config.enable_idempotence {
                                    // Idempotent producer requires per-partition
                                    // sequence ordering, so fall back to buffered send.
                                    state.buffer_single(record, result_tx).await;
                                    if state.total_buffered_bytes >= state.config.batch_size
                                        && let Err(e) = state.flush_buffer().await {
                                            warn!("Auto-flush after direct send (idempotent fallback) failed: {}", e);
                                        }
                                } else {
                                    // Spawn a separate task so the event loop is not
                                    // blocked by the network RTT for each message.
                                    let cluster = Arc::clone(&state.cluster);
                                    let config = state.config.clone();
                                    let router = state.router.clone();
                                    tokio::spawn(async move {
                                        let result = ProducerState::send_direct_to_partition(
                                            cluster, &config, &router, record,
                                        )
                                        .await;
                                        let _ = result_tx.send(result);
                                    });
                                }
                            }
                            Some(ProducerCommand::Flush { barrier }) => {
                                let result = state.flush_buffer().await;
                                let _ = barrier.send(result);
                            }
                            None => break,
                        }
                    }

                    // Linger timer: periodic flush
                    _ = interval.tick() => {
                        if !state.buffer.is_empty()
                            && let Err(e) = state.flush_buffer().await {
                                warn!("Linger-triggered flush failed: {}", e);
                            }
                    }
                }
            }

            debug!("Producer background task exited");
        });

        Self { command_tx }
    }

    /// Send a single message and return its metadata.
    ///
    /// The message is **buffered** internally and flushed when either
    /// `batch_size` bytes have accumulated or `linger_ms` has elapsed.
    /// The returned `Future` resolves when the batch is actually sent.
    ///
    /// Call [`flush`](Self::flush) to force an immediate send.
    pub async fn send(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::Send {
                record,
                result_tx: tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Send a single record directly to the broker and wait for ack.
    ///
    /// The record is sent immediately — no batching, no retries, no linger.
    /// Each call is a separate network round-trip. Returns when the broker
    /// acknowledges receipt.
    ///
    /// For buffered sends, use [`send`](Self::send) or
    /// [`send_batch`](Self::send_batch).
    pub async fn send_direct(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::SendDirect {
                record,
                result_tx: tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Buffer messages for batched sending.
    ///
    /// Unlike [`send`](Self::send), this does not return per-record metadata.
    /// Returns the number of records successfully buffered.
    ///
    /// Call [`flush`](Self::flush) to force-send all buffered messages.
    pub async fn send_batch(&self, records: Vec<ProducerRecord>) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::SendBatch {
                records,
                result_tx: tx,
            })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Force flush the buffer.
    ///
    /// Waits for all buffered messages to be sent to Kafka.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::Flush { barrier: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Close the producer.
    ///
    /// Flushes all remaining buffered messages, waits for all in-flight
    /// sends to complete, and shuts down the background task.
    pub async fn close(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::ShutdownWithAck { done: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        let _ = self.command_tx.send(ProducerCommand::Shutdown);
    }
}
