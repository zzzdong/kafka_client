//! Metadata cache - cluster topology information

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::debug;

use kafka_client_protocol::{MetadataResponse, MetadataResponseBroker, MetadataResponseTopic};

/// Metadata cache with TTL-based expiry and O(1) partition leader lookup.
///
/// Uses `RwLock` for concurrent reads and a separate `Mutex` to serialise
/// refresh operations so that multiple callers don't trigger redundant
/// Metadata RPCs.
pub struct MetadataCache {
    inner: RwLock<CachedMetadata>,
    ttl: Duration,
    /// Lock to prevent concurrent refresh operations
    refresh_lock: Mutex<()>,
}

#[derive(Debug, Clone)]
struct CachedMetadata {
    fetched_at: Option<Instant>,
    cluster_id: Option<String>,
    controller_id: Option<i32>,
    brokers: HashMap<i32, MetadataResponseBroker>,
    topics: HashMap<String, MetadataResponseTopic>,
    topic_ids: HashMap<uuid::Uuid, String>, // topic_id → topic_name
    broker_addresses: HashMap<String, i32>,
    broker_sockets: HashMap<i32, SocketAddr>,
    /// (topic_name, partition_index) → leader SocketAddr — O(1) lookup
    partition_leader_sockets: HashMap<(String, i32), SocketAddr>,
}

/// Pre-resolved broker address info (DNS resolved outside the write lock).
struct ResolvedBroker {
    node_id: i32,
    broker: MetadataResponseBroker,
    socket: Option<SocketAddr>,
}

impl CachedMetadata {
    fn new() -> Self {
        Self {
            fetched_at: None,
            cluster_id: None,
            controller_id: None,
            brokers: HashMap::new(),
            topics: HashMap::new(),
            topic_ids: HashMap::new(),
            broker_addresses: HashMap::new(),
            broker_sockets: HashMap::new(),
            partition_leader_sockets: HashMap::new(),
        }
    }

    /// Pre-resolve broker addresses (DNS outside any lock).
    /// Takes a reference so the caller can still use `response` afterwards.
    fn resolve_brokers(brokers: &[MetadataResponseBroker]) -> Vec<ResolvedBroker> {
        let mut resolved = Vec::with_capacity(brokers.len());
        for broker in brokers {
            let addr_str = format!("{}:{}", broker.host, broker.port);
            let socket = addr_str.to_socket_addrs().ok().and_then(|mut a| a.next());
            resolved.push(ResolvedBroker {
                node_id: broker.node_id,
                broker: broker.clone(),
                socket,
            });
        }
        resolved
    }

    fn update(&mut self, response: &MetadataResponse, resolved: &[ResolvedBroker]) {
        self.fetched_at = Some(Instant::now());
        self.cluster_id = response.cluster_id.clone();
        self.controller_id = Some(response.controller_id);

        // --- Brokers (DNS already resolved) ---
        let new_brokers: HashMap<i32, MetadataResponseBroker> = resolved
            .iter()
            .map(|r| (r.node_id, r.broker.clone()))
            .collect();
        let new_broker_sockets: HashMap<i32, SocketAddr> = resolved
            .iter()
            .filter_map(|r| r.socket.map(|s| (r.node_id, s)))
            .collect();
        let new_broker_addresses: HashMap<String, i32> = resolved
            .iter()
            .map(|r| (format!("{}:{}", r.broker.host, r.broker.port), r.node_id))
            .collect();

        self.brokers = new_brokers;
        self.broker_sockets = new_broker_sockets;
        self.broker_addresses = new_broker_addresses;

        // --- Topics & partition leader index ---
        let mut new_topics = HashMap::with_capacity(response.topics.len());
        let mut new_topic_ids = HashMap::new();
        let mut new_partition_leader_sockets = HashMap::new();

        for topic in &response.topics {
            let topic_id = topic.topic_id;
            if let Some(ref name) = topic.name {
                // Build O(1) partition → leader index
                for p in &topic.partitions {
                    if let Some(&socket) = self.broker_sockets.get(&p.leader_id) {
                        new_partition_leader_sockets
                            .insert((name.clone(), p.partition_index), socket);
                    }
                }
                new_topics.insert(name.clone(), topic.clone());
            }
            if !topic_id.is_nil() {
                new_topic_ids.insert(topic_id, topic.name.clone().unwrap_or_default());
            }
        }

        self.topics = new_topics;
        self.topic_ids = new_topic_ids;
        self.partition_leader_sockets = new_partition_leader_sockets;

        debug!(
            "Metadata cache updated: {} brokers, {} topics",
            self.brokers.len(),
            self.topics.len()
        );
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        match self.fetched_at {
            None => true,
            Some(fetched_at) => fetched_at.elapsed() > ttl,
        }
    }

    // ------------------------------------------------------------------
    // O(1) partition leader lookup via pre-built index
    // ------------------------------------------------------------------

    fn get_partition_leader(&self, topic: &str, partition: i32) -> Option<SocketAddr> {
        self.partition_leader_sockets
            .get(&(topic.to_string(), partition))
            .copied()
    }

    fn get_broker_address(&self, node_id: i32) -> Option<SocketAddr> {
        self.broker_sockets.get(&node_id).copied()
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
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            inner: RwLock::new(CachedMetadata::new()),
            ttl,
            refresh_lock: Mutex::new(()),
        }
    }

    /// Create default metadata cache
    #[allow(dead_code)]
    pub(crate) fn default() -> Self {
        Self::new(Duration::from_secs(300))
    }

    /// Replace the cached metadata atomically.
    ///
    /// DNS resolution for broker addresses is performed **before** acquiring
    /// the write lock, so readers are never blocked by network I/O.
    pub(crate) async fn update(&self, response: MetadataResponse) {
        let resolved = CachedMetadata::resolve_brokers(&response.brokers);
        let mut inner = self.inner.write().await;
        inner.update(&response, &resolved);
    }

    pub(crate) async fn is_expired(&self) -> bool {
        let inner = self.inner.read().await;
        inner.is_expired(self.ttl)
    }

    /// Acquire refresh lock to prevent concurrent refresh operations
    pub(crate) async fn acquire_refresh_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.refresh_lock.lock().await
    }

    // ================================================================
    // Public query methods
    // ================================================================

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

    pub async fn get_broker(&self, node_id: i32) -> Option<MetadataResponseBroker> {
        let inner = self.inner.read().await;
        inner.brokers.get(&node_id).cloned()
    }

    pub async fn get_broker_address(&self, node_id: i32) -> Option<SocketAddr> {
        let inner = self.inner.read().await;
        inner.get_broker_address(node_id)
    }

    pub async fn get_topic(&self, name: &str) -> Option<MetadataResponseTopic> {
        let inner = self.inner.read().await;
        inner.topics.get(name).cloned()
    }

    pub async fn get_topic_name_by_id(&self, topic_id: uuid::Uuid) -> Option<String> {
        let inner = self.inner.read().await;
        inner.topic_ids.get(&topic_id).cloned()
    }

    pub async fn get_all_brokers(&self) -> Vec<MetadataResponseBroker> {
        let inner = self.inner.read().await;
        inner.brokers.values().cloned().collect()
    }

    pub async fn get_all_topics(&self) -> Vec<MetadataResponseTopic> {
        let inner = self.inner.read().await;
        inner.topics.values().cloned().collect()
    }

    pub async fn get_broker_by_address(&self, addr: &SocketAddr) -> Option<i32> {
        let inner = self.inner.read().await;
        let addr_str = format!("{}:{}", addr.ip(), addr.port());
        inner.broker_addresses.get(&addr_str).copied()
    }
}
