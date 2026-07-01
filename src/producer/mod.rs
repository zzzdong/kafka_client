//! Producer - high-level Kafka message producer
//!
//! Uses a consistent internal architecture: all messages go through a shared
//! buffer, with automatic flush based on `batch_size` and `linger_ms`.
//! [`Producer`] provides the public facade with `send`, `flush`, and `close`.

mod router;

pub use router::{PartitionRouter, PartitionRouting};

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::cluster::ClusterClient;
use crate::error::{KafkaError, Result};
use crate::protocol::{
    PartitionProduceData, ProduceRequest, ProduceResponse, Record, RecordBatch, TopicProduceData,
};

/// Command sent to the background event loop.
enum ProducerCommand {
    /// Send a single record (buffer + immediate flush).
    Send {
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    },
    /// Buffer records for batched sending.
    SendBatch {
        records: Vec<ProducerRecord>,
        result_tx: oneshot::Sender<Result<usize>>,
    },
    /// Force flush the buffer.
    Flush {
        barrier: oneshot::Sender<Result<()>>,
    },
    /// Shut down the background loop.
    Shutdown,
}

/// Message header
#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Bytes,
}

/// Producer record
///
/// A message to be sent to Kafka with optional metadata.
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

/// Producer configuration
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub acks: i16,
    pub timeout_ms: i32,
    pub routing: PartitionRouting,
    pub retries: u32,
    pub batch_size: usize,
    pub linger_ms: u64,
}

impl ProducerConfig {
    /// Create a new producer config with sensible defaults.
    ///
    /// ```ignore
    /// use kafka_client::ProducerConfig;
    ///
    /// // Builder pattern
    /// let config = ProducerConfig::new()
    ///     .with_acks(-1)
    ///     .with_retries(5);
    /// ```
    pub fn new() -> Self {
        Self {
            acks: 1,
            timeout_ms: 5000,
            routing: PartitionRouting::HashKey,
            retries: 5,
            batch_size: 16384,
            linger_ms: 100,
        }
    }

    // ------------------------------------------------------------------
    // Builder methods
    // ------------------------------------------------------------------

    /// Set the required acks (-1 = all, 0 = none, 1 = leader).
    pub fn with_acks(mut self, acks: i16) -> Self {
        self.acks = acks;
        self
    }

    /// Set the request timeout in milliseconds.
    pub fn with_timeout(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Set the partition routing strategy.
    pub fn with_routing(mut self, routing: PartitionRouting) -> Self {
        self.routing = routing;
        self
    }

    /// Set the number of retries on transient errors.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set the batch size in bytes for batching messages.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the linger time in milliseconds before flushing a batch.
    pub fn with_linger(mut self, linger_ms: u64) -> Self {
        self.linger_ms = linger_ms;
        self
    }
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Internal state — shared between background task and Producer
// ===========================================================================

struct ProducerState {
    cluster: Arc<ClusterClient>,
    router: PartitionRouter,
    config: ProducerConfig,
    /// Buffered records by (topic, partition)
    buffer: HashMap<(String, i32), Vec<ProducerRecord>>,
    /// Approximate buffered bytes (for batch_size check)
    buffered_bytes: usize,
    /// Last buffer flush time
    last_send: Instant,
}

impl ProducerState {
    /// Buffer records, auto-flush if batch_size or linger_ms triggers.
    async fn buffer_records(&mut self, records: Vec<ProducerRecord>) -> Result<usize> {
        let count = records.len();
        for record in records {
            let partition = self.select_partition(&record).await?;
            let estimated_size =
                record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0) + 64;
            self.buffered_bytes += estimated_size;
            self.buffer
                .entry((record.topic.clone(), partition))
                .or_default()
                .push(record);
        }
        Ok(count)
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let records: HashMap<(String, i32), Vec<ProducerRecord>> = std::mem::take(&mut self.buffer);
        self.buffered_bytes = 0;
        self.last_send = Instant::now();

        for ((topic, partition), recs) in records {
            debug!("Flushing {} records to {}/{}", recs.len(), topic, partition);
            if let Err(e) = self.send_to_broker(&topic, partition, recs).await {
                warn!("Flush to {}/{} failed: {}", topic, partition, e);
            }
        }
        Ok(())
    }

    async fn send_single(&mut self, record: ProducerRecord) -> Result<RecordMetadata> {
        let mut last_error = None;
        for attempt in 0..self.config.retries {
            // select_partition can fail with TopicNotFound on brand-new topics
            match self.select_partition(&record).await {
                Ok(partition) => {
                    let topic = record.topic.clone();
                    match self
                        .send_to_broker(&topic, partition, vec![record.clone()])
                        .await
                    {
                        Ok(mut v) => return v.pop().ok_or(KafkaError::ProduceError(-1)),
                        Err(e) => {
                            let is_retryable = matches!(
                                &e,
                                KafkaError::ProduceError(6)
                                    | KafkaError::TopicNotFound(_)
                                    | KafkaError::PartitionNotFound(_, _)
                            );
                            last_error = Some(e);
                            if is_retryable {
                                let _ = self.cluster.refresh_metadata().await;
                            }
                        }
                    }
                }
                Err(e) => {
                    let is_retryable = matches!(&e, KafkaError::TopicNotFound(_));
                    last_error = Some(e);
                    if is_retryable {
                        let _ = self.cluster.refresh_metadata().await;
                    }
                }
            }
            if attempt + 1 < self.config.retries {
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
            }
        }
        Err(last_error.unwrap_or(KafkaError::ProduceError(-1)))
    }

    async fn send_to_broker(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<ProducerRecord>,
    ) -> Result<Vec<RecordMetadata>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let request = self.build_request(topic, partition, records).await?;
        let response = self
            .cluster
            .send_to_partition(topic, partition, &request)
            .await?;
        let metadata = self.parse_response(topic, partition, response).await?;

        Ok(vec![metadata])
    }

    async fn build_request(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<ProducerRecord>,
    ) -> Result<ProduceRequest> {
        let batch = self.build_record_batch(records)?;

        // Look up topic UUID from metadata for v13+ compatibility
        let topic_id = self
            .cluster
            .metadata()
            .get_topic(topic)
            .await
            .map(|t| t.topic_id)
            .unwrap_or_else(uuid::Uuid::nil);

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

    fn build_record_batch(&self, records: Vec<ProducerRecord>) -> Result<RecordBatch> {
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

        for (idx, (record, timestamp)) in records.into_iter().zip(timestamps).enumerate() {
            let mut rec =
                Record::new(idx as i32, timestamp - first_timestamp).with_value(record.value);
            if let Some(key) = record.key {
                rec = rec.with_key(key);
            }
            for header in record.headers {
                rec = rec.with_header(header.key, header.value);
            }
            batch.add_record(rec);
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
                        return Err(KafkaError::ProduceError(partition_response.error_code));
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
        Err(KafkaError::ProduceError(-1))
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
    pub(crate) async fn new(cluster: Arc<ClusterClient>, config: ProducerConfig) -> Self {
        let mut state = ProducerState {
            router: PartitionRouter::new(config.routing),
            config: config.clone(),
            cluster,
            buffer: HashMap::new(),
            buffered_bytes: 0,
            last_send: Instant::now(),
        };

        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let linger = Duration::from_millis(config.linger_ms);

        tokio::spawn(async move {
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
                            Some(ProducerCommand::Send { record, result_tx }) => {
                                let result = state.send_single(record).await;
                                let _ = result_tx.send(result);
                            }
                            Some(ProducerCommand::SendBatch { records, result_tx }) => {
                                let result = state.buffer_records(records).await;
                                let should_flush = result.is_ok() && (
                                    state.buffered_bytes >= state.config.batch_size
                                    || state.last_send.elapsed() >= linger
                                );
                                if should_flush && let Err(e) = state.flush_buffer().await {
                                    warn!("Auto-flush failed: {}", e);
                                }
                                let _ = result_tx.send(result);
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
                        if !state.buffer.is_empty() && let Err(e) = state.flush_buffer().await {
                            warn!("Linger flush failed: {}", e);
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
    /// The message is added to the buffer and flushed immediately,
    /// ensuring the caller gets back partition/offset metadata.
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

    /// Buffer messages for batched sending.
    ///
    /// Messages are added to the internal buffer. Automatic flush is
    /// triggered when `batch_size` or `linger_ms` thresholds are met.
    /// Returns the number of records buffered.
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
    /// Flushes any remaining buffered messages and shuts down the
    /// background task.
    pub async fn close(&self) -> Result<()> {
        // Send shutdown signal — the background task will flush then exit.
        let _ = self.command_tx.send(ProducerCommand::Shutdown);
        Ok(())
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        // Best-effort flush on drop. If the runtime is still active the
        // Shutdown command will handle it; otherwise buffered data is lost.
        let _ = self.command_tx.send(ProducerCommand::Shutdown);
    }
}
