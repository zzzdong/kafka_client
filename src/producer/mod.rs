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
use std::collections::{HashMap, HashSet};
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
use kafka_client_protocol::add_partitions_to_txn_request::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
};
use kafka_client_protocol::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
use kafka_client_protocol::end_txn_request::EndTxnRequest;
use kafka_client_protocol::end_txn_response::EndTxnResponse;
use kafka_client_protocol::find_coordinator_request::FindCoordinatorRequest;
use kafka_client_protocol::find_coordinator_response::FindCoordinatorResponse;
use kafka_client_protocol::init_producer_id_request::InitProducerIdRequest;
use kafka_client_protocol::init_producer_id_response::InitProducerIdResponse;
use kafka_client_protocol::txn_offset_commit_request::{
    TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_client_protocol::txn_offset_commit_response::TxnOffsetCommitResponse;

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
    /// Initialize the transactional producer (find the transaction
    /// coordinator and obtain a producer id/epoch).
    InitTransactions {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Begin a new transaction.
    BeginTransaction {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Commit the current transaction (EndTxn commit).
    CommitTransaction {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Abort the current transaction (EndTxn abort).
    AbortTransaction {
        reply: oneshot::Sender<Result<()>>,
    },
    /// Commit consumer offsets as part of the current transaction
    /// (TxnOffsetCommit, the consume-process-produce EOS bridge).
    SendOffsetsToTransaction {
        group_id: String,
        generation_id: i32,
        member_id: String,
        offsets: HashMap<String, HashMap<i32, i64>>,
        reply: oneshot::Sender<Result<()>>,
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
    /// Default: `true` (idempotence is on by default, like modern Kafka
    /// clients). Disable it explicitly by setting the field to `false` if you
    /// need `acks = 0` or an older broker.
    pub enable_idempotence: bool,
    /// Transactional id (Kafka EOS).
    ///
    /// When set, the producer becomes transactional: messages can only be
    /// sent inside
    /// [`begin_transaction`](crate::Producer::begin_transaction) ...
    /// [`commit_transaction`](crate::Producer::commit_transaction) /
    /// [`abort_transaction`](crate::Producer::abort_transaction) blocks, and
    /// the broker guarantees exactly-once semantics for the transaction.
    /// Idempotence is implied.
    pub transactional_id: Option<String>,
    /// Transaction timeout in milliseconds (used by InitProducerId).
    pub transaction_timeout_ms: i32,
}

impl ProducerConfig {
    pub fn new() -> Self {
        Self {
            // Idempotence is enabled by default: acks must be -1 and retries
            // are effectively unbounded (bounded by delivery_timeout_ms).
            acks: -1,
            timeout_ms: 5000,
            routing: PartitionRouting::HashKey,
            retries: i32::MAX as u32,
            delivery_timeout_ms: 120_000,
            batch_size: 16384,
            linger_ms: 100,
            max_batch_bytes: 1_048_576,
            enable_idempotence: true,
            transactional_id: None,
            transaction_timeout_ms: 60_000,
        }
    }

    pub fn with_acks(mut self, acks: i16) -> Self {
        self.acks = acks;
        self
    }

    /// Enable/disable the idempotent producer (default: enabled).
    ///
    /// Disable it only if you need `acks = 0/1` or target a broker older than
    /// Kafka 0.11. Idempotence implies `acks = -1` and effectively unbounded
    /// retries (bounded by the delivery timeout).
    pub fn with_idempotence(mut self, enabled: bool) -> Self {
        self.enable_idempotence = enabled;
        if enabled {
            self.acks = -1;
            self.retries = i32::MAX as u32;
        }
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

    /// Configure the producer as transactional (Kafka EOS).
    ///
    /// Implies idempotence (`acks = -1`, unbounded retries). Messages may
    /// only be sent between
    /// [`begin_transaction`](crate::Producer::begin_transaction) and
    /// [`commit_transaction`](crate::Producer::commit_transaction) /
    /// [`abort_transaction`](crate::Producer::abort_transaction).
    pub fn with_transactional_id(mut self, transactional_id: impl Into<String>) -> Self {
        self.transactional_id = Some(transactional_id.into());
        self.enable_idempotence = true;
        self.acks = -1;
        self.retries = i32::MAX as u32;
        self
    }

    /// Set the transaction timeout in milliseconds (default 60000).
    pub fn with_transaction_timeout(mut self, timeout_ms: i32) -> Self {
        self.transaction_timeout_ms = timeout_ms;
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

/// Transaction state machine (Kafka EOS, KIP-98).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    /// No transaction in progress; the producer can begin one.
    Ready,
    /// A transaction is open; sends are part of it until commit/abort.
    InTransaction,
}

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
    /// Transaction state (EOS).
    txn_state: TxnState,
    /// Transaction coordinator address (lazily resolved).
    txn_coordinator: Option<SocketAddr>,
    /// Partitions already added to the current transaction.
    txn_partitions: HashSet<(String, i32)>,
    /// Per-partition sequence numbers at transaction start. The broker rolls
    /// back the sequence state of an *aborted* transaction, so the local
    /// counters must be restored to this snapshot on abort to stay in sync.
    txn_start_sequences: HashMap<(String, i32), i32>,
}

impl ProducerState {
    async fn buffer_single(
        &mut self,
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    ) {
        if self.is_transactional() && self.txn_state != TxnState::InTransaction {
            let _ = result_tx.send(Err(KafkaError::InvalidTransactionState(
                "transactional producer can only send inside begin_transaction()/commit_transaction()"
                    .into(),
            )));
            return;
        }
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
        if self.is_transactional() && self.txn_state != TxnState::InTransaction {
            return Err(KafkaError::InvalidTransactionState(
                "transactional producer can only send inside begin_transaction()/commit_transaction()"
                    .into(),
            ));
        }
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

    fn is_transactional(&self) -> bool {
        self.config.transactional_id.is_some()
    }

    /// Obtain (or refresh) a producer id + epoch.
    ///
    /// For transactional producers the InitProducerId request goes through
    /// the transaction coordinator and carries the transactional id. A fresh
    /// epoch invalidates all per-partition sequence numbers, so they are
    /// cleared here.
    async fn ensure_producer_id(&mut self) -> Result<()> {
        if self.producer_id_initialized {
            return Ok(());
        }
        let txn_id = self.config.transactional_id.clone();
        let coordinator = if txn_id.is_some() {
            Some(
                find_coordinator(&self.cluster, txn_id.as_deref().unwrap(), 1).await?,
            )
        } else {
            None
        };

        let deadline = Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms);
        let mut backoff = Duration::from_millis(100);
        let mut last_error = None;
        for attempt in 0..self.config.retries.max(1) {
            if Instant::now() >= deadline {
                last_error = Some(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));
                break;
            }

            let request = InitProducerIdRequest {
                transactional_id: txn_id.clone(),
                transaction_timeout_ms: self.config.transaction_timeout_ms,
                producer_id: -1,
                producer_epoch: -1,
                enable2_pc: false,
                keep_prepared_txn: false,
            };
            let response: Result<InitProducerIdResponse> = match coordinator {
                Some(addr) => self.cluster.send_to_broker(addr, &request).await,
                None => self.cluster.send_to_any_broker(&request).await,
            };

            match response {
                Ok(resp) if resp.error_code == 0 => {
                    self.producer_id = resp.producer_id;
                    self.producer_epoch = resp.producer_epoch;
                    self.producer_id_initialized = true;
                    self.txn_coordinator = coordinator;
                    // A new PID/epoch resets broker-side sequence state.
                    self.sequence_numbers.lock().unwrap().clear();
                    debug!(
                        "Obtained producer_id={}, epoch={} (attempt {})",
                        resp.producer_id,
                        resp.producer_epoch,
                        attempt + 1
                    );
                    return Ok(());
                }
                Ok(resp) => {
                    let code = KafkaErrorCode::from_i16(resp.error_code);
                    let err = KafkaError::ProduceError(code);
                    warn!(
                        "InitProducerId failed with {} (attempt {})",
                        err,
                        attempt + 1
                    );
                    last_error = Some(err);
                    if !code.is_retriable() || attempt + 1 >= self.config.retries.max(1) {
                        break;
                    }
                }
                Err(e) => {
                    let retryable = match &e {
                        KafkaError::Io(_) | KafkaError::ConnectionClosed => true,
                        KafkaError::ProduceError(code) | KafkaError::TransactionError(code) => {
                            code.is_retriable()
                        }
                        _ => false,
                    };
                    warn!(
                        "Failed to init producer id (attempt {}): {}",
                        attempt + 1,
                        e
                    );
                    last_error = Some(e);
                    if !retryable || attempt + 1 >= self.config.retries.max(1) {
                        break;
                    }
                }
            }

            tokio::time::sleep(backoff).await;
            backoff = backoff.mul_f32(2.0).min(Duration::from_secs(10));
        }
        Err(last_error.unwrap_or(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1))))
    }

    /// Begin a new transaction. Automatically initializes the producer id if
    /// needed (equivalent to calling [`init_transactions`] first).
    async fn begin_transaction(&mut self) -> Result<()> {
        if !self.is_transactional() {
            return Err(KafkaError::InvalidConfiguration(
                "Producer is not configured with a transactional_id".into(),
            ));
        }
        self.ensure_producer_id().await?;
        if self.txn_state != TxnState::Ready {
            return Err(KafkaError::InvalidTransactionState(
                "a transaction is already in progress".into(),
            ));
        }
        self.txn_state = TxnState::InTransaction;
        self.txn_partitions.clear();
        self.txn_start_sequences = self.sequence_numbers.lock().unwrap().clone();
        debug!("Transaction begun");
        Ok(())
    }

    /// Commit the current transaction.
    pub(crate) async fn commit_transaction(&mut self) -> Result<()> {
        self.end_transaction(true).await
    }

    /// Abort the current transaction.
    pub(crate) async fn abort_transaction(&mut self) -> Result<()> {
        self.end_transaction(false).await
    }

    async fn end_transaction(&mut self, committed: bool) -> Result<()> {
        if self.txn_state != TxnState::InTransaction {
            return Err(KafkaError::InvalidTransactionState(
                "no transaction in progress".into(),
            ));
        }
        let txn_id = self.config.transactional_id.clone().unwrap();
        let producer_id = self.producer_id;
        let producer_epoch = self.producer_epoch;
        let mut coordinator = match self.txn_coordinator {
            Some(addr) => addr,
            None => {
                let addr = find_coordinator(&self.cluster, &txn_id, 1).await?;
                self.txn_coordinator = Some(addr);
                addr
            }
        };

        let deadline = Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms);
        let mut backoff = Duration::from_millis(100);
        let mut last_error = None;
        for attempt in 0..self.config.retries.max(1) {
            if Instant::now() >= deadline {
                last_error = Some(KafkaError::TransactionError(KafkaErrorCode::from_i16(-1)));
                break;
            }

            let request = EndTxnRequest {
                transactional_id: txn_id.clone(),
                producer_id,
                producer_epoch,
                committed,
            };
            let response: Result<EndTxnResponse> =
                self.cluster.send_to_broker(coordinator, &request).await;

            match response {
                Ok(resp) => {
                    let code = KafkaErrorCode::from_i16(resp.error_code);
                    if code.is_ok() {
                        // KIP-890 (EndTxn v5+): the broker may return the
                        // current producer id/epoch — adopt it so a fenced or
                        // recovered producer keeps working.
                        if resp.producer_id != -1 && resp.producer_epoch != -1 {
                            self.producer_id = resp.producer_id;
                            self.producer_epoch = resp.producer_epoch;
                        }
                        if !committed {
                            // The broker reverts per-partition sequence numbers
                            // for aborted transactions; mirror that locally so
                            // the next transaction restarts from the same
                            // sequence the broker expects.
                            *self.sequence_numbers.lock().unwrap() =
                                self.txn_start_sequences.clone();
                        }
                        self.txn_start_sequences.clear();
                        self.txn_state = TxnState::Ready;
                        self.txn_partitions.clear();
                        debug!("Transaction {}", if committed { "committed" } else { "aborted" });
                        return Ok(());
                    }
                    let err = KafkaError::TransactionError(code);
                    if code.is_retriable() {
                        if code == KafkaErrorCode::NOT_COORDINATOR
                            && let Ok(addr) = find_coordinator(&self.cluster, &txn_id, 1).await
                        {
                            self.txn_coordinator = Some(addr);
                            coordinator = addr;
                        }
                        warn!(
                            "EndTxn(committed={}) retriable error {} (attempt {})",
                            committed,
                            code,
                            attempt + 1
                        );
                        last_error = Some(err);
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                        continue;
                    }
                    self.handle_fatal_txn_error(code);
                    return Err(err);
                }
                Err(e) => {
                    let retryable = match &e {
                        KafkaError::Io(_) | KafkaError::ConnectionClosed => true,
                        KafkaError::TransactionError(code) => code.is_retriable(),
                        _ => false,
                    };
                    last_error = Some(e.clone());
                    if !retryable {
                        return Err(e);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                }
            }
        }
        Err(last_error.unwrap_or(KafkaError::TransactionError(KafkaErrorCode::from_i16(
            -1,
        ))))
    }

    /// Lazily register partitions with the transaction coordinator before
    /// producing to them (once per partition per transaction).
    async fn add_partitions_to_txn(&mut self, partitions: &[(String, i32)]) -> Result<()> {
        let pending: Vec<(String, i32)> = partitions
            .iter()
            .filter(|(topic, partition)| !self.txn_partitions.contains(&(topic.clone(), *partition)))
            .cloned()
            .collect();
        if pending.is_empty() {
            return Ok(());
        }

        let txn_id = self.config.transactional_id.clone().unwrap();
        let mut coordinator = self.txn_coordinator.ok_or(KafkaError::NoCoordinator)?;

        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partition) in &pending {
            by_topic.entry(topic.clone()).or_default().push(*partition);
        }

        let deadline = Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms);
        let mut backoff = Duration::from_millis(100);
        let mut last_error = None;
        for attempt in 0..self.config.retries.max(1) {
            if Instant::now() >= deadline {
                last_error = Some(KafkaError::TransactionError(KafkaErrorCode::from_i16(-1)));
                break;
            }
            let producer_id = self.producer_id;
            let producer_epoch = self.producer_epoch;

            let topics: Vec<AddPartitionsToTxnTopic> = by_topic
                .iter()
                .map(|(name, partitions)| AddPartitionsToTxnTopic {
                    name: name.clone(),
                    partitions: partitions.clone(),
                })
                .collect();
            let request = AddPartitionsToTxnRequest {
                transactions: vec![AddPartitionsToTxnTransaction {
                    transactional_id: txn_id.clone(),
                    producer_id,
                    producer_epoch,
                    verify_only: false,
                    topics: topics.clone(),
                }],
                v3_and_below_transactional_id: txn_id.clone(),
                v3_and_below_producer_id: producer_id,
                v3_and_below_producer_epoch: producer_epoch,
                v3_and_below_topics: topics,
            };
            let response: Result<AddPartitionsToTxnResponse> =
                self.cluster.send_to_broker(coordinator, &request).await;

            match response {
                Ok(resp) => {
                    let mut first_error = None;
                    if !resp.results_by_transaction.is_empty() {
                        if resp.error_code != 0 {
                            first_error = Some(KafkaErrorCode::from_i16(resp.error_code));
                        }
                        for tr in &resp.results_by_transaction {
                            for t in &tr.topic_results {
                                for p in &t.results_by_partition {
                                    if p.partition_error_code != 0 {
                                        first_error =
                                            Some(KafkaErrorCode::from_i16(p.partition_error_code));
                                    }
                                }
                            }
                        }
                    } else {
                        for t in &resp.results_by_topic_v3_and_below {
                            for p in &t.results_by_partition {
                                if p.partition_error_code != 0 {
                                    first_error =
                                        Some(KafkaErrorCode::from_i16(p.partition_error_code));
                                }
                            }
                        }
                    }

                    match first_error {
                        None => {
                            for (topic, partition) in &pending {
                                self.txn_partitions.insert((topic.clone(), *partition));
                            }
                            debug!("Added {} partitions to transaction", pending.len());
                            return Ok(());
                        }
                        Some(code) => {
                            let err = KafkaError::TransactionError(code);
                            if code.is_retriable() {
                                if code == KafkaErrorCode::NOT_COORDINATOR
                                    && let Ok(addr) =
                                        find_coordinator(&self.cluster, &txn_id, 1).await
                                {
                                    self.txn_coordinator = Some(addr);
                                    coordinator = addr;
                                }
                                warn!(
                                    "AddPartitionsToTxn retriable error {} (attempt {})",
                                    code,
                                    attempt + 1
                                );
                                last_error = Some(err);
                                tokio::time::sleep(backoff).await;
                                backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                                continue;
                            }
                            // The broker fenced or forgot our PID (e.g. after an
                            // abort the coordinator may require a fresh epoch).
                            // Re-initialize (epoch bump) and retry once — the
                            // standard client recovery for these errors.
                            if attempt == 0
                                && (code == KafkaErrorCode::PRODUCER_FENCED
                                    || code == KafkaErrorCode::INVALID_PRODUCER_EPOCH
                                    || code == KafkaErrorCode::UNKNOWN_PRODUCER_ID)
                            {
                                warn!(
                                    "AddPartitionsToTxn {} — re-initializing producer id and retrying",
                                    code
                                );
                                self.producer_id_initialized = false;
                                self.sequence_numbers.lock().unwrap().clear();
                                self.txn_start_sequences.clear();
                                self.txn_coordinator = None;
                                match self.ensure_producer_id().await {
                                    Ok(()) => {
                                        coordinator =
                                            self.txn_coordinator.ok_or(KafkaError::NoCoordinator)?;
                                        last_error = Some(err);
                                        tokio::time::sleep(backoff).await;
                                        backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                                        continue;
                                    }
                                    Err(e) => {
                                        self.handle_fatal_txn_error(code);
                                        return Err(e);
                                    }
                                }
                            }
                            self.handle_fatal_txn_error(code);
                            return Err(err);
                        }
                    }
                }
                Err(e) => {
                    let retryable = match &e {
                        KafkaError::Io(_) | KafkaError::ConnectionClosed => true,
                        KafkaError::TransactionError(code) => code.is_retriable(),
                        _ => false,
                    };
                    last_error = Some(e.clone());
                    if !retryable {
                        return Err(e);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                }
            }
        }
        Err(last_error.unwrap_or(KafkaError::TransactionError(KafkaErrorCode::from_i16(
            -1,
        ))))
    }

    /// Commit consumer offsets as part of the current transaction
    /// (TxnOffsetCommit), the consume-process-produce EOS bridge.
    async fn send_offsets_to_transaction(
        &mut self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        offsets: HashMap<String, HashMap<i32, i64>>,
    ) -> Result<()> {
        if self.txn_state != TxnState::InTransaction {
            return Err(KafkaError::InvalidTransactionState(
                "offsets can only be committed inside a transaction".into(),
            ));
        }
        let txn_id = self.config.transactional_id.clone().unwrap();
        let producer_id = self.producer_id;
        let producer_epoch = self.producer_epoch;
        // TxnOffsetCommit is handled by the *group* coordinator.
        let group_coordinator = find_coordinator(&self.cluster, group_id, 0).await?;

        let topics: Vec<TxnOffsetCommitRequestTopic> = offsets
            .into_iter()
            .map(|(name, partitions)| TxnOffsetCommitRequestTopic {
                name,
                partitions: partitions
                    .into_iter()
                    .map(|(partition_index, committed_offset)| {
                        TxnOffsetCommitRequestPartition {
                            partition_index,
                            committed_offset,
                            committed_leader_epoch: -1,
                            committed_metadata: None,
                        }
                    })
                    .collect(),
            })
            .collect();

        let deadline = Instant::now() + Duration::from_millis(self.config.delivery_timeout_ms);
        let mut backoff = Duration::from_millis(100);
        let mut last_error = None;
        for _attempt in 0..self.config.retries.max(1) {
            if Instant::now() >= deadline {
                last_error = Some(KafkaError::OffsetCommitError(KafkaErrorCode::from_i16(-1)));
                break;
            }
            let request = TxnOffsetCommitRequest {
                transactional_id: txn_id.clone(),
                group_id: group_id.to_string(),
                producer_id,
                producer_epoch,
                generation_id,
                member_id: member_id.to_string(),
                group_instance_id: None,
                topics: topics.clone(),
            };
            let response: Result<TxnOffsetCommitResponse> =
                self.cluster.send_to_broker(group_coordinator, &request).await;
            match response {
                Ok(resp) => {
                    let mut first_error = None;
                    for t in &resp.topics {
                        for p in &t.partitions {
                            if p.error_code != 0 {
                                first_error = Some(KafkaErrorCode::from_i16(p.error_code));
                            }
                        }
                    }
                    match first_error {
                        None => {
                            debug!("Committed offsets in transaction for group {}", group_id);
                            return Ok(());
                        }
                        Some(code) => {
                            let err = KafkaError::OffsetCommitError(code);
                            if code.is_retriable() {
                                last_error = Some(err);
                                tokio::time::sleep(backoff).await;
                                backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                                continue;
                            }
                            return Err(err);
                        }
                    }
                }
                Err(e) => {
                    let retryable = match &e {
                        KafkaError::Io(_) | KafkaError::ConnectionClosed => true,
                        KafkaError::OffsetCommitError(code) => code.is_retriable(),
                        _ => false,
                    };
                    last_error = Some(e.clone());
                    if !retryable {
                        return Err(e);
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.mul_f32(2.0).min(Duration::from_secs(5));
                }
            }
        }
        Err(last_error.unwrap_or(KafkaError::OffsetCommitError(KafkaErrorCode::from_i16(
            -1,
        ))))
    }

    /// On fatal transaction errors the current PID/epoch can no longer be
    /// used. Force a re-init (epoch bump) so the next transaction recovers
    /// (the "if we maybe should abort, abort" policy).
    fn handle_fatal_txn_error(&mut self, code: KafkaErrorCode) {
        warn!(
            "Fatal transaction error {}, producer id will be re-initialized",
            code
        );
        self.producer_id_initialized = false;
        self.sequence_numbers.lock().unwrap().clear();
        self.txn_start_sequences.clear();
        self.txn_state = TxnState::Ready;
        self.txn_partitions.clear();
        self.txn_coordinator = None;
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
        // is enabled. Deferring this to flush time keeps the constructor
        // infallible. Transactional producers are initialized by
        // begin_transaction() before any send is accepted.
        if self.config.enable_idempotence
            && !self.producer_id_initialized
            && let Err(err) = self.ensure_producer_id().await
        {
            for (_, entry) in buffer {
                for tx in entry.pending {
                    let _ = tx.send(Err(err.clone()));
                }
            }
            return Err(err);
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

        // Transactional flush: every partition must be registered with the
        // transaction coordinator before producing (lazily, once per txn).
        if self.is_transactional() && self.txn_state == TxnState::InTransaction {
            let partitions: Vec<(String, i32)> = resolved
                .iter()
                .map(|((topic, partition), _, _)| (topic.clone(), *partition))
                .collect();
            if let Err(e) = self.add_partitions_to_txn(&partitions).await {
                for (_, entry, _) in resolved {
                    for tx in entry.pending {
                        let _ = tx.send(Err(e.clone()));
                    }
                }
                return Err(e);
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

        // Retry remaining (split) chunks. Multi-record chunks that are still
        // too large are split again, until every record is sent individually
        // (a single record exceeding max.message.bytes truly fails).
        while !remaining.is_empty() {
            let batch = std::mem::take(&mut remaining);
            let mut next_remaining: Vec<PartitionChunk> = Vec::new();
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
                        if code == KafkaErrorCode::MESSAGE_TOO_LARGE && chunk.records.len() > 1 =>
                    {
                        // Still too large as a group — binary-split and retry.
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
                        let records1: Vec<ProducerRecord> =
                            r1.into_iter().map(|(_, r)| r).collect();
                        let records2: Vec<ProducerRecord> =
                            r2.into_iter().map(|(_, r)| r).collect();
                        let (p1, p2) = chunk.pending.into_iter().enumerate().partition::<Vec<(
                            usize,
                            oneshot::Sender<Result<RecordMetadata>>,
                        )>, _>(
                            |(i, _)| *i < mid
                        );
                        let pending1: Vec<_> = p1.into_iter().map(|(_, tx)| tx).collect();
                        let pending2: Vec<_> = p2.into_iter().map(|(_, tx)| tx).collect();
                        if !records1.is_empty() {
                            next_remaining.push(PartitionChunk {
                                topic: chunk.topic.clone(),
                                partition: chunk.partition,
                                records: records1,
                                pending: pending1,
                            });
                        }
                        if !records2.is_empty() {
                            next_remaining.push(PartitionChunk {
                                topic: chunk.topic,
                                partition: chunk.partition,
                                records: records2,
                                pending: pending2,
                            });
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
            remaining = next_remaining;
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
        let err = last_error.unwrap_or(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));

        if self.config.enable_idempotence {
            // The batch failed and will not be retried. Roll the partition's
            // sequence number back to the base so the next batch reuses it:
            // if the broker never accepted this batch, the retry appends
            // cleanly; if it did (lost response), the broker deduplicates.
            //
            // OUT_OF_ORDER_SEQUENCE_NUMBER is the exception: the broker's
            // expected sequence is ahead of ours, so keep the advanced value
            // instead of going backwards.
            let is_out_of_order = matches!(
                &err,
                KafkaError::ProduceError(code)
                    if *code == KafkaErrorCode::OUT_OF_ORDER_SEQUENCE_NUMBER
            );
            if !is_out_of_order {
                let mut seq_map = self.sequence_numbers.lock().unwrap();
                seq_map.insert((topic.to_string(), partition), base_sequence);
            }
        }

        Err(err)
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
            transactional_id: if self.is_transactional()
                && self.txn_state == TxnState::InTransaction
            {
                self.config.transactional_id.clone()
            } else {
                None
            },
            // Idempotent/transactional producers require acks=-1.
            acks: if self.config.enable_idempotence {
                -1
            } else {
                self.config.acks
            },
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

        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(config.delivery_timeout_ms);
        let mut last_error = None;
        for attempt in 0..config.retries {
            if tokio::time::Instant::now() >= deadline {
                last_error = Some(KafkaError::ProduceError(KafkaErrorCode::from_i16(-1)));
                break;
            }
            let topic_meta = cluster.metadata().get_topic(&topic).await;
            let topic_id = topic_meta
                .as_ref()
                .map(|t| t.topic_id)
                .unwrap_or_else(uuid::Uuid::nil);
            let request = ProduceRequest {
                transactional_id: None,
                acks: if config.enable_idempotence {
                    -1
                } else {
                    config.acks
                },
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
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::sleep(
                    Duration::from_millis(100 * (attempt as u64 + 1)).min(remaining),
                )
                .await;
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
            txn_state: TxnState::Ready,
            txn_coordinator: None,
            txn_partitions: HashSet::new(),
            txn_start_sequences: HashMap::new(),
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
                            Some(ProducerCommand::InitTransactions { reply }) => {
                                let result = if state.config.transactional_id.is_some() {
                                    state.ensure_producer_id().await
                                } else {
                                    Err(KafkaError::InvalidConfiguration(
                                        "transactional_id is not configured".into(),
                                    ))
                                };
                                let _ = reply.send(result);
                            }
                            Some(ProducerCommand::BeginTransaction { reply }) => {
                                let _ = reply.send(state.begin_transaction().await);
                            }
                            Some(ProducerCommand::CommitTransaction { reply }) => {
                                let _ = reply.send(state.commit_transaction().await);
                            }
                            Some(ProducerCommand::AbortTransaction { reply }) => {
                                let _ = reply.send(state.abort_transaction().await);
                            }
                            Some(ProducerCommand::SendOffsetsToTransaction {
                                group_id,
                                generation_id,
                                member_id,
                                offsets,
                                reply,
                            }) => {
                                let _ = reply.send(
                                    state
                                        .send_offsets_to_transaction(
                                            &group_id,
                                            generation_id,
                                            &member_id,
                                            offsets,
                                        )
                                        .await,
                                );
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

    /// Initialize the transactional producer.
    ///
    /// Finds the transaction coordinator and obtains a producer id/epoch for
    /// the configured `transactional_id`. Automatically invoked by
    /// [`begin_transaction`](Self::begin_transaction) when needed; calling it
    /// explicitly surfaces coordinator/authentication errors early.
    ///
    /// # Example
    /// ```ignore
    /// let producer = client.producer(
    ///     ProducerConfig::new().with_transactional_id("txn-1")
    /// ).await;
    /// producer.init_transactions().await?;
    /// ```
    pub async fn init_transactions(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::InitTransactions { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Begin a new transaction (Kafka EOS).
    ///
    /// All messages sent after this call and before
    /// [`commit_transaction`](Self::commit_transaction) /
    /// [`abort_transaction`](Self::abort_transaction) belong to the
    /// transaction and are only visible to consumers with
    /// `isolation.level=read_committed` once it commits.
    ///
    /// Requires the producer to be configured with
    /// [`ProducerConfig::with_transactional_id`].
    ///
    /// # Example
    /// ```ignore
    /// producer.begin_transaction().await?;
    /// producer.send(ProducerRecord::new("orders", b"hello".into())).await?;
    /// producer.commit_transaction().await?;
    /// ```
    pub async fn begin_transaction(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::BeginTransaction { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Commit the current transaction, making its messages visible to
    /// read-committed consumers atomically.
    pub async fn commit_transaction(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::CommitTransaction { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Abort the current transaction; its messages are discarded (marked
    /// aborted) by the broker.
    pub async fn abort_transaction(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::AbortTransaction { reply: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }

    /// Commit consumer offsets as part of the current transaction
    /// (`TxnOffsetCommit`) — the consume-process-produce EOS bridge.
    ///
    /// The offsets are committed atomically with the transaction: they only
    /// become visible when [`commit_transaction`](Self::commit_transaction)
    /// succeeds. Pass the consumer group's generation and member id when
    /// using a real consumer (`-1` / `""` for simple commits).
    ///
    /// # Example
    /// ```ignore
    /// let offsets = HashMap::from([(
    ///     "orders".to_string(),
    ///     HashMap::from([(0, 42i64)]),
    /// )]);
    /// producer.send_offsets_to_transaction("my-group", -1, "", offsets).await?;
    /// producer.commit_transaction().await?;
    /// ```
    pub async fn send_offsets_to_transaction(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        offsets: HashMap<String, HashMap<i32, i64>>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::SendOffsetsToTransaction {
                group_id: group_id.to_string(),
                generation_id,
                member_id: member_id.to_string(),
                offsets,
                reply: tx,
            })
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

/// Resolve the socket address of a group (`key_type = 0`) or transaction
/// (`key_type = 1`) coordinator, retrying while the coordinator is not yet
/// available.
async fn find_coordinator(
    cluster: &Arc<ClusterClient>,
    key: &str,
    key_type: i8,
) -> Result<SocketAddr> {
    const MAX_ATTEMPTS: u32 = 10;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let request = FindCoordinatorRequest {
            key: key.to_string(),
            key_type,
            coordinator_keys: vec![key.to_string()],
        };
        let response: FindCoordinatorResponse = cluster.send_to_any_broker(&request).await?;

        // error_code 15 = COORDINATOR_NOT_AVAILABLE (retryable), e.g. while
        // __consumer_offsets / __transaction_state is being created.
        let retryable = response.error_code == 15
            || response
                .coordinators
                .first()
                .map(|c| c.error_code == 15)
                .unwrap_or(false);
        if retryable && attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        if response.error_code != 0 {
            return Err(KafkaError::NoCoordinator);
        }
        let (host, port) = if !response.host.is_empty() {
            (response.host.clone(), response.port)
        } else if let Some(coord) = response.coordinators.first() {
            if coord.error_code != 0 {
                return Err(KafkaError::NoCoordinator);
            }
            (coord.host.clone(), coord.port)
        } else {
            return Err(KafkaError::NoCoordinator);
        };
        return tokio::net::lookup_host(format!("{}:{}", host, port))
            .await
            .map_err(|_| KafkaError::NoCoordinator)?
            .next()
            .ok_or(KafkaError::NoCoordinator);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn producer_config_defaults_to_idempotent() {
        let config = ProducerConfig::new();
        assert!(config.enable_idempotence, "idempotence should be on by default");
        assert_eq!(config.acks, -1, "idempotence requires acks=-1");
        assert_eq!(config.retries, i32::MAX as u32);
        assert_eq!(config.transactional_id, None);
        assert_eq!(config.transaction_timeout_ms, 60_000);
    }

    #[test]
    fn with_transactional_id_implies_idempotence() {
        let config = ProducerConfig::new().with_transactional_id("txn-1");
        assert_eq!(config.transactional_id.as_deref(), Some("txn-1"));
        assert!(config.enable_idempotence);
        assert_eq!(config.acks, -1);
    }

    #[test]
    fn with_idempotence_can_be_disabled_and_re_enabled() {
        let disabled = ProducerConfig::new().with_idempotence(false).with_acks(1);
        assert!(!disabled.enable_idempotence);
        assert_eq!(disabled.acks, 1);

        let reenabled = disabled.with_idempotence(true);
        assert!(reenabled.enable_idempotence);
        assert_eq!(reenabled.acks, -1);
    }

    #[test]
    fn build_record_batch_empty_errors() {
        let err = ProducerState::build_record_batch_inner(&[]).unwrap_err();
        assert!(matches!(err, KafkaError::InvalidConfiguration(_)));
    }

    #[test]
    fn build_record_batch_preserves_timestamps_keys_and_headers() {
        let records = [
            ProducerRecord {
                topic: "t".into(),
                partition: None,
                key: Some(Bytes::from_static(b"k1")),
                value: Bytes::from_static(b"v1"),
                timestamp: Some(1000),
                headers: vec![Header {
                    key: "h1".into(),
                    value: Bytes::from_static(b"hv1"),
                }],
            },
            ProducerRecord {
                topic: "t".into(),
                partition: None,
                key: None,
                value: Bytes::from_static(b"v2"),
                timestamp: Some(2000),
                headers: vec![],
            },
        ];
        let refs: Vec<&ProducerRecord> = records.iter().collect();
        let batch = ProducerState::build_record_batch_inner(&refs).unwrap();

        assert_eq!(batch.first_timestamp, 1000);
        assert_eq!(batch.max_timestamp, 2000);
        assert_eq!(batch.records.len(), 2);

        let r0 = &batch.records[0];
        assert_eq!(r0.offset_delta, 0);
        assert_eq!(r0.timestamp_delta, 0);
        assert_eq!(r0.key.as_deref(), Some(&b"k1"[..]));
        assert_eq!(r0.value.as_deref(), Some(&b"v1"[..]));
        assert_eq!(r0.headers[0].key, "h1");
        assert_eq!(r0.headers[0].value.as_deref(), Some(&b"hv1"[..]));

        let r1 = &batch.records[1];
        assert_eq!(r1.offset_delta, 1);
        assert_eq!(r1.timestamp_delta, 1000);
        assert_eq!(r1.key, None);
        assert_eq!(r1.value.as_deref(), Some(&b"v2"[..]));
    }
}
