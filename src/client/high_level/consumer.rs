use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::client::low_level::KafkaClient;
use crate::protocol::api::fetch::{FetchRequest, FetchResponse, TopicFetchData, PartitionFetchData, ConsumerRecord};
use crate::error::{Result, KafkaError};

/// 消费者配置
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub group_id: String,
    pub auto_commit: bool,
    pub auto_offset_reset: AutoOffsetReset,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub partition_max_bytes: i32,
    pub max_wait_ms: i32,
}

#[derive(Debug, Clone)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
    None,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: "rust-consumer".to_string(),
            auto_commit: true,
            auto_offset_reset: AutoOffsetReset::Latest,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            partition_max_bytes: 1024 * 1024,
            max_wait_ms: 500,
        }
    }
}

/// 高级消费者
pub struct Consumer {
    client: Arc<Mutex<KafkaClient>>,
    subscriptions: HashMap<String, Vec<i32>>,
    offsets: HashMap<String, HashMap<i32, i64>>,
    config: ConsumerConfig,
}

impl Consumer {
    pub async fn new(client: Arc<Mutex<KafkaClient>>, config: ConsumerConfig) -> Self {
        Self {
            client,
            subscriptions: HashMap::new(),
            offsets: HashMap::new(),
            config,
        }
    }

    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        let client = self.client.lock().await;

        for topic in &topics {
            let partitions = client.metadata().get_partitions(topic).await
                .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;

            self.subscriptions.insert(topic.clone(), partitions.clone());

            let topic_offsets = self.offsets.entry(topic.clone()).or_insert_with(HashMap::new);
            for partition in &partitions {
                if !topic_offsets.contains_key(partition) {
                    // 简化实现：从最新偏移量开始
                    topic_offsets.insert(*partition, -1);
                }
            }
        }

        Ok(())
    }

    pub async fn poll(&mut self, timeout_ms: i32) -> Result<Vec<ConsumerRecord>> {
        let mut all_records = Vec::new();

        for (topic, partitions) in &self.subscriptions.clone() {
            for partition in partitions {
                let offset = self.offsets.get(topic)
                    .and_then(|t| t.get(partition))
                    .copied()
                    .unwrap_or(-1);

                let records = self.fetch_partition(topic, *partition, offset, timeout_ms).await?;

                for record in records {
                    // 更新偏移量
                    if let Some(topic_offsets) = self.offsets.get_mut(topic) {
                        topic_offsets.insert(*partition, record.offset + 1);
                    }

                    all_records.push(record);
                }
            }
        }

        Ok(all_records)
    }

    async fn fetch_partition(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        timeout_ms: i32,
    ) -> Result<Vec<ConsumerRecord>> {
        let mut client = self.client.lock().await;
        let leader_addr = client.metadata().get_partition_leader(topic, partition).await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: timeout_ms,
            min_bytes: self.config.min_bytes,
            max_bytes: self.config.max_bytes,
            topics: vec![
                TopicFetchData {
                    name: topic.to_string(),
                    partitions: vec![
                        PartitionFetchData {
                            partition,
                            fetch_offset: offset,
                            log_start_offset: -1,
                            max_bytes: self.config.partition_max_bytes,
                        }
                    ],
                }
            ],
        };

        let response: FetchResponse = client.send_request(leader_addr, 1, &request).await?;
        Self::parse_fetch_response(response, topic)
    }

    fn parse_fetch_response(response: FetchResponse, topic_name: &str) -> Result<Vec<ConsumerRecord>> {
        let records = Vec::new();

        for topic_response in response.responses {
            if topic_response.name == topic_name {
                for partition_response in topic_response.partitions {
                    if partition_response.error_code != 0 {
                        warn!("Fetch error for partition {}: {}",
                            partition_response.partition_index, partition_response.error_code);
                        continue;
                    }

                    // 简化实现：解析 records 数据
                    // 实际实现应该使用 Kafka 的 RecordBatch 解析
                    let _records_data = partition_response.records;
                    // TODO: 实现完整的 RecordBatch 解析
                }
            }
        }

        Ok(records)
    }

    /// 提交偏移量（简化实现）
    pub async fn commit(&mut self) -> Result<()> {
        if !self.config.auto_commit {
            // 手动提交逻辑
            debug!("Manual commit not fully implemented");
        }
        Ok(())
    }

    /// 获取当前偏移量
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets.get(topic)?.get(&partition).copied()
    }

    /// 设置偏移量
    pub fn set_offset(&mut self, topic: &str, partition: i32, offset: i64) {
        self.offsets
            .entry(topic.to_string())
            .or_insert_with(HashMap::new)
            .insert(partition, offset);
    }
}
