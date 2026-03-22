use bytes::{Bytes, BytesMut, BufMut};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::client::low_level::KafkaClient;
use crate::client::high_level::partition_router::{PartitionRouter, PartitionRouting};
use crate::protocol::api::produce::{ProduceRequest, ProduceResponse, TopicProduceData, PartitionProduceData};
use crate::error::{Result, KafkaError};

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

/// 高级生产者
pub struct Producer {
    client: Arc<Mutex<KafkaClient>>,
    router: PartitionRouter,
    config: ProducerConfig,
}

impl Producer {
    pub async fn new(client: Arc<Mutex<KafkaClient>>, config: ProducerConfig) -> Self {
        Self {
            client,
            router: PartitionRouter::new(config.routing),
            config,
        }
    }

    pub async fn send(&mut self, record: ProducerRecord) -> Result<RecordMetadata> {
        // 1. 确定分区
        let partition = self.select_partition(&record).await?;

        // 2. 构建请求
        let request = self.build_request(&record, partition)?;

        // 3. 发送（带重试）
        let response = self.send_with_retry(&record.topic, partition, &request).await?;

        // 4. 解析响应
        self.parse_response(&record.topic, partition, response)
    }

    async fn select_partition(&self, record: &ProducerRecord) -> Result<i32> {
        if let Some(p) = record.partition {
            return Ok(p);
        }

        let client = self.client.lock().await;
        let partition_count = client.metadata().get_partition_count(&record.topic).await
            .ok_or_else(|| KafkaError::TopicNotFound(record.topic.clone()))?;

        let key = record.key.as_deref();
        Ok(self.router.select_partition(key, partition_count))
    }

    async fn send_with_retry(
        &self,
        topic: &str,
        partition: i32,
        request: &ProduceRequest,
    ) -> Result<ProduceResponse> {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < self.config.retries {
            let mut client = self.client.lock().await;
            match client.send_to_partition(topic, partition, 0, request).await {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    last_error = Some(e);
                    attempts += 1;
                    if attempts < self.config.retries {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * attempts as u64)).await;
                        // 刷新元数据
                        let _ = client.refresh_metadata().await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| KafkaError::ProduceError(-1)))
    }

    fn build_request(&self, record: &ProducerRecord, partition: i32) -> Result<ProduceRequest> {
        // 构建 RecordBatch（简化实现）
        let records = self.build_record_batch(record)?;

        Ok(ProduceRequest {
            transactional_id: None,
            acks: self.config.acks,
            timeout_ms: self.config.timeout_ms,
            topics: vec![
                TopicProduceData {
                    name: record.topic.clone(),
                    partitions: vec![
                        PartitionProduceData {
                            index: partition,
                            records,
                        }
                    ],
                }
            ],
        })
    }

    fn build_record_batch(&self, record: &ProducerRecord) -> Result<Bytes> {
        // 简化实现：构建一个简单的消息格式
        // 实际实现应该使用 Kafka 的 RecordBatch 格式
        let mut buf = BytesMut::new();

        // 写入消息长度
        let key_len = record.key.as_ref().map(|k| k.len()).unwrap_or(0);
        let value_len = record.value.len();

        buf.put_i32((key_len + value_len + 8) as i32);

        // 写入 key 长度和 key
        if let Some(key) = &record.key {
            buf.put_i32(key.len() as i32);
            buf.extend_from_slice(key);
        } else {
            buf.put_i32(-1);
        }

        // 写入 value 长度和 value
        buf.put_i32(value_len as i32);
        buf.extend_from_slice(&record.value);

        Ok(buf.freeze())
    }

    fn parse_response(
        &self,
        topic: &str,
        partition: i32,
        response: ProduceResponse,
    ) -> Result<RecordMetadata> {
        for topic_response in response.responses {
            if topic_response.name == topic {
                for partition_response in topic_response.partitions {
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
