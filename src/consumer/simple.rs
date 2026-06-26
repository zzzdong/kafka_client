use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

use crate::cluster::ClusterClient;
use crate::consumer::config::{AutoOffsetReset, ConsumerConfig};
use crate::consumer::types::{ConsumerRecord, FetchParams};
use crate::consumer::util::{fetch_partition_simple, list_offset_for};
use crate::error::{KafkaError, Result};

pub struct SimpleConsumer {
    pub(crate) cluster: Arc<ClusterClient>,
    pub(crate) config: ConsumerConfig,
    pub(crate) subscribed_topics: Vec<String>,
    pub(crate) offsets: HashMap<String, HashMap<i32, i64>>,
}

impl SimpleConsumer {
    pub(crate) fn new(cluster: Arc<ClusterClient>, config: ConsumerConfig) -> Self {
        Self {
            cluster,
            config,
            subscribed_topics: Vec::new(),
            offsets: HashMap::new(),
        }
    }

    pub async fn subscribe(&mut self, topics: Vec<String>) -> Result<()> {
        self.subscribed_topics = topics.clone();
        self.init_offsets(topics).await?;
        Ok(())
    }

    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        self.poll_inner(self.config.max_wait).await
    }

    pub async fn poll_timeout(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        self.poll_inner(timeout).await
    }

    pub async fn try_poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        self.poll_inner(Duration::ZERO).await
    }

    pub async fn close(&self) -> Result<()> {
        Ok(())
    }

    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.offsets
            .get(topic)
            .and_then(|m| m.get(&partition))
            .copied()
    }

    pub fn set_offset(&mut self, topic: &str, partition: i32, offset: i64) {
        self.offsets
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }

    async fn init_offsets(&mut self, topics: Vec<String>) -> Result<()> {
        for topic in &topics {
            let partitions = self
                .cluster
                .metadata()
                .get_partitions(topic)
                .await
                .ok_or_else(|| KafkaError::TopicNotFound(topic.clone()))?;
            for partition in partitions {
                if !self
                    .offsets
                    .get(topic)
                    .is_some_and(|m| m.contains_key(&partition))
                {
                    self.offsets
                        .entry(topic.clone())
                        .or_default()
                        .insert(partition, -1);
                }
            }
        }
        let to_resolve: Vec<(String, i32)> = self
            .offsets
            .iter()
            .flat_map(|(t, m)| {
                m.iter()
                    .filter(|&(_, &v)| v < 0)
                    .map(|(p, _)| (t.clone(), *p))
            })
            .collect();
        for (topic, partition) in to_resolve {
            let ts = match self.config.auto_offset_reset {
                AutoOffsetReset::Latest => -1i64,
                AutoOffsetReset::Earliest => -2i64,
                AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
            };
            match list_offset_for(&self.cluster, &topic, partition, ts).await {
                Ok(off) => {
                    self.offsets
                        .entry(topic)
                        .or_default()
                        .insert(partition, off);
                }
                Err(e) => warn!("Failed to init offset for {}@{}: {}", topic, partition, e),
            }
        }
        Ok(())
    }

    async fn poll_inner(&mut self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let fetch_params = FetchParams {
            timeout_ms: timeout.as_millis() as i32,
            min_bytes: self.config.min_bytes,
            max_bytes: self.config.max_bytes,
            partition_max_bytes: self.config.partition_max_bytes,
        };
        let mut all_records = Vec::new();

        for topic in self.subscribed_topics.clone() {
            let Some(partitions) = self.cluster.metadata().get_partitions(&topic).await else {
                continue;
            };
            for partition in partitions {
                let offset = self
                    .offsets
                    .get(&topic)
                    .and_then(|m| m.get(&partition))
                    .copied()
                    .unwrap_or(-1);
                if offset < 0 {
                    continue;
                }
                match fetch_partition_simple(
                    &self.cluster,
                    &topic,
                    partition,
                    offset,
                    &fetch_params,
                    self.config.auto_offset_reset,
                )
                .await
                {
                    Ok(records) => {
                        if let Some(last) = records.last() {
                            self.offsets
                                .entry(last.topic.clone())
                                .or_default()
                                .insert(last.partition, last.offset + 1);
                        }
                        all_records.extend(records);
                    }
                    Err(e) => warn!("Failed to fetch {}/{}: {}", topic, partition, e),
                }
            }
        }
        Ok(all_records)
    }
}
