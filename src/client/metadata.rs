use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;
use std::net::SocketAddr;

use crate::protocol::api::metadata::{Broker, Topic, Partition, MetadataResponse};
use crate::error::Result;

/// 元数据缓存
#[derive(Debug, Clone)]
pub struct MetadataCache {
    inner: Arc<RwLock<CachedMetadata>>,
    ttl: Duration,
}

#[derive(Debug, Clone)]
struct CachedMetadata {
    fetched_at: Option<Instant>,
    cluster_id: Option<String>,
    controller_id: Option<i32>,
    brokers: HashMap<i32, Broker>,
    topics: HashMap<String, Topic>,
    broker_addresses: HashMap<String, i32>,
}

impl CachedMetadata {
    fn new() -> Self {
        Self {
            fetched_at: None,
            cluster_id: None,
            controller_id: None,
            brokers: HashMap::new(),
            topics: HashMap::new(),
            broker_addresses: HashMap::new(),
        }
    }

    fn update(&mut self, response: MetadataResponse) {
        self.fetched_at = Some(Instant::now());
        self.cluster_id = response.cluster_id;
        self.controller_id = Some(response.controller_id);

        // Update brokers
        self.brokers.clear();
        self.broker_addresses.clear();
        for broker in response.brokers {
            let addr = format!("{}:{}", broker.host, broker.port);
            self.broker_addresses.insert(addr, broker.node_id);
            self.brokers.insert(broker.node_id, broker);
        }

        // Update topics
        self.topics.clear();
        for topic in response.topics {
            self.topics.insert(topic.name.clone(), topic);
        }

        debug!("Metadata cache updated: {} brokers, {} topics", self.brokers.len(), self.topics.len());
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        match self.fetched_at {
            None => true,
            Some(fetched_at) => fetched_at.elapsed() > ttl,
        }
    }

    fn get_partition_leader(&self, topic: &str, partition: i32) -> Option<SocketAddr> {
        let topic = self.topics.get(topic)?;
        let partition = topic.partitions.iter().find(|p| p.partition_index == partition)?;
        let broker = self.brokers.get(&partition.leader_id)?;
        broker.socket_addr()
    }

    fn get_partition_count(&self, topic: &str) -> Option<usize> {
        let topic = self.topics.get(topic)?;
        Some(topic.partitions.len())
    }

    fn get_partitions(&self, topic: &str) -> Option<Vec<i32>> {
        let topic = self.topics.get(topic)?;
        Some(topic.partitions.iter().map(|p| p.partition_index).collect())
    }
}

impl MetadataCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CachedMetadata::new())),
            ttl,
        }
    }

    pub fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }

    pub async fn update(&self, response: MetadataResponse) {
        let mut inner = self.inner.write().await;
        inner.update(response);
    }

    pub async fn is_expired(&self) -> bool {
        let inner = self.inner.read().await;
        inner.is_expired(self.ttl)
    }

    pub async fn get_partition_leader(&self, topic: &str, partition: i32) -> Option<SocketAddr> {
        let inner = self.inner.read().await;
        inner.get_partition_leader(topic, partition)
    }

    pub async fn get_partition_count(&self, topic: &str) -> Option<usize> {
        let inner = self.inner.read().await;
        inner.get_partition_count(topic)
    }

    pub async fn get_partitions(&self, topic: &str) -> Option<Vec<i32>> {
        let inner = self.inner.read().await;
        inner.get_partitions(topic)
    }

    pub async fn get_broker(&self, node_id: i32) -> Option<Broker> {
        let inner = self.inner.read().await;
        inner.brokers.get(&node_id).cloned()
    }

    pub async fn get_topic(&self, name: &str) -> Option<Topic> {
        let inner = self.inner.read().await;
        inner.topics.get(name).cloned()
    }

    pub async fn get_all_brokers(&self) -> Vec<Broker> {
        let inner = self.inner.read().await;
        inner.brokers.values().cloned().collect()
    }

    pub async fn get_broker_by_address(&self, addr: &SocketAddr) -> Option<i32> {
        let inner = self.inner.read().await;
        let addr_str = format!("{}:{}", addr.ip(), addr.port());
        inner.broker_addresses.get(&addr_str).copied()
    }
}
