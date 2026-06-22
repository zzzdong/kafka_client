use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::client::core::KafkaClient;
use crate::client::partition_router::{PartitionRouter, PartitionRouting};
use crate::error::{KafkaError, Result};
use crate::protocol::{
    PartitionProduceData, ProduceRequest, ProduceResponse, Record, RecordBatch, TopicProduceData,
};

/// 发送给 Producer 后台事件循环的命令
enum ProducerCommand {
    /// 发送单条消息，要求后台任务立即发送并返回结果
    Send {
        record: ProducerRecord,
        result_tx: oneshot::Sender<Result<RecordMetadata>>,
    },
    /// 批量发送消息，将记录加入缓冲区后返回已加入数量
    SendBatch {
        records: Vec<ProducerRecord>,
        result_tx: oneshot::Sender<Result<usize>>,
    },
    /// 强制刷新缓冲区，完成后通知调用方
    Flush {
        barrier: oneshot::Sender<Result<()>>,
    },
}

/// 消息头
#[derive(Debug, Clone)]
pub struct Header {
    pub key: String,
    pub value: Bytes,
}

/// 生产者记录
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

/// 发送元数据
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

/// 生产者配置
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    pub acks: i16,
    pub timeout_ms: i32,
    pub routing: PartitionRouting,
    pub retries: u32,
    pub batch_size: usize,
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

/// 生产者内部状态
struct ProducerInner {
    client: Arc<KafkaClient>,
    router: PartitionRouter,
    config: ProducerConfig,
    /// 按 (topic, partition) 缓冲的记录
    buffer: HashMap<(String, i32), Vec<ProducerRecord>>,
    /// 缓冲的近似字节数（用于 batch_size 判断）
    buffered_bytes: usize,
    /// 上次发送时间
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

        // 对每个 (topic, partition) 分批发送，第一次失败即返回
        for ((topic, partition), recs) in records {
            debug!("Flushing {} records to {}/{}", recs.len(), topic, partition);
            self.send_batch_inner(&topic, partition, recs).await?;
            debug!("Successfully sent batch to {}/{}", topic, partition);
        }
        Ok(())
    }

    /// 将记录加入缓冲区，若达到 batch_size 或 linger 已超时则触发 flush。
    /// 返回实际加入缓冲区的记录数量。
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
            .client
            .metadata()
            .get_partition_count(&record.topic)
            .await
            .ok_or_else(|| KafkaError::TopicNotFound(record.topic.clone()))?;

        let key = record.key.as_deref();
        let partition = self.router.select_partition(key, partition_count);
        info!(topic = %record.topic, partition_count, partition, key = ?key.map(|k| String::from_utf8_lossy(k).to_string()), "selected partition for record");
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
            info!(?request, topic, partition, "sending produce request");
            match self
                .client
                .send_to_partition(topic, partition, 0, request)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    let is_not_leader = matches!(e, KafkaError::ProduceError(6));
                    last_error = Some(e);
                    attempts += 1;
                    if attempts < self.config.retries as u64 {
                        // NOT_LEADER_OR_FOLLOWER usually means KRaft leader
                        // election is still settling.  Always back off briefly
                        // so the controller has time to finalise leadership.
                        let delay_ms = if is_not_leader {
                            500 // give KRaft a moment to elect
                        } else {
                            100 * attempts
                        };
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        // 刷新元数据以获取最新的 leader 信息
                        let _ = self.client.refresh_metadata().await;
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
        info!(
            ?response,
            topic, partition, "produce response in parse_response"
        );
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

/// 高级生产者
pub struct Producer {
    /// 发送命令到后台事件循环
    command_tx: tokio::sync::mpsc::UnboundedSender<ProducerCommand>,
}

impl Producer {
    /// 创建 Producer 并启动后台批量发送任务
    pub async fn new(client: Arc<KafkaClient>, config: ProducerConfig) -> Self {
        let mut inner = ProducerInner {
            client,
            router: PartitionRouter::new(config.routing),
            config: config.clone(),
            buffer: HashMap::new(),
            buffered_bytes: 0,
            last_send: Instant::now(),
        };

        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        let linger = Duration::from_millis(config.linger_ms);

        // 后台事件循环：单一任务独占 ProducerInner，避免锁竞争
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

    /// 发送单条消息并返回其元数据（同步发送，不进入批量缓冲）
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

    /// 批量发送消息
    ///
    /// 消息会先进入缓冲区，根据 linger_ms 和 batch_size
    /// 自动决定何时真正发送到 Kafka。返回已加入缓冲区的记录数量。
    /// 如需确认所有消息发送完成，请显式调用 flush()。
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

    /// 强制刷新缓冲区，等待所有缓冲消息发送完成
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(ProducerCommand::Flush { barrier: tx })
            .map_err(|_| KafkaError::ConnectionClosed)?;
        rx.await.map_err(|_| KafkaError::ConnectionClosed)?
    }
}
