//! Producer - high-level Kafka message producer

mod router;

pub use router::{PartitionRouting, PartitionRouter};

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

/// Command sent to Producer background event loop
enum ProducerCommand {
    /// Send single message, immediately send and return result
    Send {
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    },
    /// Batch send messages, add to buffer and return count
    SendBatch {
        records: Vec<ProducerRecord>,
        result_tx: oneshot::Sender<Result<usize>>,
    },
    /// Force flush buffer, notify caller when done
    Flush {
        barrier: oneshot::Sender<Result<()>>,
    },
}

/// Message header
///
/// Key-value pair attached to a Kafka message.
#[derive(Debug, Clone)]
pub struct Header {
    /// Header key name
    pub key: String,
    /// Header value data
    pub value: Bytes,
}

/// Producer record
///
/// A message to be sent to Kafka with optional metadata.
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// Topic name to send the message to
    pub topic: String,
    /// Optional explicit partition number (auto-selected if None)
    pub partition: Option<i32>,
    /// Optional message key (used for partition routing)
    pub key: Option<Bytes>,
    /// Message value data
    pub value: Bytes,
    /// Optional message timestamp (auto-generated if None)
    pub timestamp: Option<i64>,
    /// Message headers
    pub headers: Vec<Header>,
}

impl ProducerRecord {
    /// Create a new producer record with topic and value
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

    /// Set message key for partition routing
    pub fn with_key(mut self, key: Bytes) -> Self {
        self.key = Some(key);
        self
    }

    /// Set explicit partition number
    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set explicit message timestamp
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set message headers
    pub fn with_headers(mut self, headers: Vec<Header>) -> Self {
        self.headers = headers;
        self
    }
}

/// Send metadata
///
/// Metadata returned after a successful message send.
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic name where the message was sent
    pub topic: String,
    /// Partition number where the message was stored
    pub partition: i32,
    /// Offset of the message within the partition
    pub offset: i64,
    /// Timestamp assigned to the message by the broker
    pub timestamp: i64,
}

/// Producer configuration
///
/// Controls producer behavior including acknowledgment mode,
/// batching, and retry settings.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Number of acknowledgments required (0=none, 1=leader, -1=all replicas)
    pub acks: i16,
    /// Request timeout in milliseconds
    pub timeout_ms: i32,
    /// Partition routing strategy
    pub routing: PartitionRouting,
    /// Number of retry attempts on failure
    pub retries: u32,
    /// Maximum batch size in bytes
    pub batch_size: usize,
    /// Time to wait before sending batch (linger time)
    pub linger_ms: u64,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            acks: 1,
            timeout_ms: 5000,
            routing: PartitionRouting::default(),
            retries: 3,
            batch_size: 16384,
            linger_ms: 100,
        }
    }
}

/// Producer internal state
struct ProducerInner {
    cluster: Arc<ClusterClient>,
    router: PartitionRouter,
    config: ProducerConfig,
    /// Buffered records by (topic, partition)
    buffer: HashMap<(String, i32), Vec<ProducerRecord>>,
    /// Approximate buffered bytes (for batch_size check)
    buffered_bytes: usize,
    /// Last send time
    last_send: Instant,
}

impl ProducerInner {
    async fn flush_buffer(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let records: HashMap<(String, i32), Vec<ProducerRecord>> = std::mem::take(&mut self.buffer);
        self.buffered_bytes = 0;
        self.last_send = Instant::now();

        for ((topic, partition), recs) in records {
            debug!("Flushing {} records to {}/{}", recs.len(), topic, partition);
            self.send_batch_inner(&topic, partition, recs).await?;
            debug!("Successfully sent batch to {}/{}", topic, partition);
        }
        Ok(())
    }

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

        let should_flush = self.buffered_bytes >= self.config.batch_size
            || self.last_send.elapsed() >= Duration::from_millis(self.config.linger_ms);

        if should_flush {
            self.flush_buffer().await?;
        }

        Ok(count)
    }

    async fn send_batch_inner(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<ProducerRecord>,
    ) -> Result<Vec<RecordMetadata>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let request = self.build_request(topic, partition, records)?;
        let response = self.send_with_retry(topic, partition, &request).await?;
        let metadata = self.parse_response(topic, partition, response)?;
        Ok(vec![metadata])
    }

    fn build_request(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<ProducerRecord>,
    ) -> Result<ProduceRequest> {
        let batch = self.build_record_batch(records)?;

        Ok(ProduceRequest {
            transactional_id: None,
            acks: self.config.acks,
            timeout_ms: self.config.timeout_ms,
            topic_data: vec![TopicProduceData {
                name: Some(topic.to_string()),
                topic_id: None,
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
        let partition = self.router.select_partition(key, partition_count);
        Ok(partition)
    }

    async fn send_with_retry(
        &self,
        topic: &str,
        partition: i32,
        request: &ProduceRequest,
    ) -> Result<ProduceResponse> {
        let mut attempts = 0u64;
        let mut last_error = None;

        while attempts < self.config.retries as u64 {
            match self.cluster.send_to_partition(topic, partition, request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    let is_not_leader = matches!(e, KafkaError::ProduceError(6));
                    last_error = Some(e);
                    attempts += 1;
                    if attempts < self.config.retries as u64 {
                        let delay_ms = if is_not_leader {
                            500
                        } else {
                            100 * attempts
                        };
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        let _ = self.cluster.refresh_metadata().await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or(KafkaError::ProduceError(-1)))
    }

    fn parse_response(
        &self,
        topic: &str,
        partition: i32,
        response: ProduceResponse,
    ) -> Result<RecordMetadata> {
        for topic_response in response.responses {
            if topic_response.name.as_deref() == Some(topic) {
                for partition_response in topic_response.partition_responses {
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
        }
        Err(KafkaError::ProduceError(-1))
    }
}

/// High-level Kafka Producer
///
/// Provides message batching and automatic partition routing.
/// Uses a background event loop to handle batch sending.
pub struct Producer {
    command_tx: tokio::sync::mpsc::UnboundedSender<ProducerCommand>,
}

impl Producer {
    /// Create Producer and start background batch sending task
    pub(crate) async fn new(cluster: Arc<ClusterClient>, config: ProducerConfig) -> Self {
        let mut inner = ProducerInner {
            cluster,
            router: PartitionRouter::new(config.routing),
            config: config.clone(),
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
                    _ = interval.tick() => {
                        if !inner.buffer.is_empty() {
                            if let Err(e) = inner.flush_buffer().await {
                                warn!("Linger flush failed: {}", e);
                            }
                        }
                    }
                    cmd = command_rx.recv() => {
                        match cmd {
                            Some(ProducerCommand::Send { record, result_tx }) => {
                                let result = async {
                                    let partition = inner.select_partition(&record).await?;
                                    let topic = record.topic.clone();
                                    let metadatas = inner
                                        .send_batch_inner(&topic, partition, vec![record])
                                        .await?;
                                    metadatas.into_iter().next().ok_or(KafkaError::ProduceError(-1))
                                }
                                .await;
                                let _ = result_tx.send(result);
                            }
                            Some(ProducerCommand::SendBatch { records, result_tx }) => {
                                let result = inner.buffer_records(records).await;
                                let _ = result_tx.send(result);
                            }
                            Some(ProducerCommand::Flush { barrier }) => {
                                let result = inner.flush_buffer().await;
                                let _ = barrier.send(result);
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        Self { command_tx }
    }

    /// Send single message and return its metadata
    ///
    /// Immediately sends the message without batching.
    /// Returns metadata including partition, offset, and timestamp.
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

    /// Batch send messages
    ///
    /// Messages are added to buffer first. Based on linger_ms and batch_size,
    /// automatically decides when to actually send to Kafka.
    /// Returns count of records added to buffer.
    /// Call flush() to ensure all messages are sent.
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

    /// Force flush buffer
    ///
    /// Waits for all buffered messages to be sent to Kafka.
    /// Useful for ensuring all messages are delivered before shutdown.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::Flush { barrier: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
}