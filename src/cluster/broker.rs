//! Broker manager - connection pool for all cluster brokers

use dashmap::DashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::connection::{Builder, Connection};
use crate::error::{KafkaError, Result};
use crate::transport::SecurityProtocol;
use kafka_client_protocol::{ApiVersionsRequest, MetadataResponseBroker};

/// Broker connection entry
struct BrokerEntry {
    addr: SocketAddr,
    conn: Arc<Mutex<Connection>>,
    healthy: bool,
}

/// Manage connections to all brokers in the cluster
///
/// Uses `DashMap` for per-broker locking, supporting concurrent requests
/// to different brokers.
pub struct BrokerManager {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: SecurityProtocol,
    client_id: String,
    client_name: String,
    client_version: String,
    brokers: DashMap<i32, BrokerEntry>,
    addr_to_node: DashMap<SocketAddr, i32>,
    next_unknown_node_id: AtomicI32,
}

impl BrokerManager {
    pub fn new(
        bootstrap_servers: Vec<SocketAddr>,
        security_protocol: SecurityProtocol,
        client_id: String,
        client_name: String,
        client_version: String,
    ) -> Self {
        Self {
            bootstrap_servers,
            security_protocol,
            client_id,
            client_name,
            client_version,
            brokers: DashMap::new(),
            addr_to_node: DashMap::new(),
            next_unknown_node_id: AtomicI32::new(i32::MIN),
        }
    }

    async fn connect_to_broker(&self, addr: SocketAddr) -> Result<Connection> {
        let builder = Builder::new(
            addr,
            self.security_protocol.clone(),
            self.client_name.clone(),
            self.client_version.clone(),
        )
        .with_client_id(self.client_id.clone());

        builder.build().await
    }

    /// Try to connect to a bootstrap broker
    pub async fn bootstrap(&self) -> Result<SocketAddr> {
        let addrs: Vec<SocketAddr> = self.bootstrap_servers.clone();
        for addr in addrs {
            match self.connect_to_broker(addr).await {
                Ok(conn) => {
                    let node_id = self.next_unknown_node_id.fetch_sub(1, Ordering::SeqCst);
                    self.register_broker(node_id, addr, conn).await;
                    debug!("Connected to bootstrap broker {}", addr);
                    return Ok(addr);
                }
                Err(e) => {
                    warn!("Failed to connect to bootstrap broker {}: {}", addr, e);
                    continue;
                }
            }
        }
        Err(KafkaError::NoBootstrapBrokerAvailable)
    }

    /// Register a broker connection
    pub async fn register_broker(&self, node_id: i32, addr: SocketAddr, conn: Connection) {
        if let Some(old_node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if old_node_id != node_id {
                self.brokers.remove(&old_node_id);
            }
        }

        self.addr_to_node.insert(addr, node_id);
        self.brokers.insert(
            node_id,
            BrokerEntry {
                addr,
                conn: Arc::new(Mutex::new(conn)),
                healthy: true,
            },
        );
    }

    /// Get or create connection for a broker by node_id and host:port
    /// 预留功能，用于动态连接管理
    #[allow(dead_code)]
    pub async fn get_or_connect(
        &self,
        node_id: i32,
        host: &str,
        port: i32,
    ) -> Result<Arc<Mutex<Connection>>> {
        if let Some(entry) = self.brokers.get(&node_id) {
            if entry.healthy {
                return Ok(entry.conn.clone());
            }
            // Try reconnect
            match self.connect_to_broker(entry.addr).await {
                Ok(new_conn) => {
                    drop(entry);
                    if let Some(mut entry) = self.brokers.get_mut(&node_id) {
                        entry.conn = Arc::new(Mutex::new(new_conn));
                        entry.healthy = true;
                        return Ok(entry.conn.clone());
                    }
                }
                Err(e) => {
                    warn!(
                        "Reconnect to broker {} at {} failed: {}",
                        node_id, entry.addr, e
                    );
                }
            }
        }

        let addr = resolve_broker_address(host, port)?;
        let conn = self.connect_to_broker(addr).await?;
        self.register_broker(node_id, addr, conn).await;
        self.brokers
            .get(&node_id)
            .map(|e| e.conn.clone())
            .ok_or_else(|| {
                KafkaError::InvalidConfiguration("Failed to register broker connection".to_string())
            })
    }

    /// Get connection for a broker by address
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<Arc<Mutex<Connection>>> {
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if let Some(entry) = self.brokers.get(&node_id) {
                if entry.healthy {
                    return Ok(entry.conn.clone());
                }
                match self.connect_to_broker(addr).await {
                    Ok(new_conn) => {
                        drop(entry);
                        if let Some(mut entry) = self.brokers.get_mut(&node_id) {
                            entry.conn = Arc::new(Mutex::new(new_conn));
                            entry.healthy = true;
                            return Ok(entry.conn.clone());
                        }
                    }
                    Err(e) => {
                        warn!("Reconnect to broker at {} failed: {}", addr, e);
                    }
                }
            }
        }

        let conn = self.connect_to_broker(addr).await?;
        let node_id = self.next_unknown_node_id.fetch_sub(1, Ordering::SeqCst);
        self.register_broker(node_id, addr, conn).await;
        self.brokers
            .get(&node_id)
            .map(|e| e.conn.clone())
            .ok_or_else(|| {
                KafkaError::InvalidConfiguration(
                    "Failed to register new broker connection".to_string(),
                )
            })
    }

    /// Get any healthy broker connection
    pub fn get_any_healthy_broker(&self) -> Option<(SocketAddr, Arc<Mutex<Connection>>)> {
        self.brokers
            .iter()
            .find(|e| e.healthy)
            .map(|e| (e.addr, e.conn.clone()))
    }

    /// Get all known broker addresses
    pub fn all_broker_addresses(&self) -> Vec<SocketAddr> {
        self.brokers.iter().map(|e| e.addr).collect()
    }

    /// Refresh broker list from metadata response
    pub async fn refresh_from_metadata(
        &self,
        brokers: Vec<MetadataResponseBroker>,
    ) -> Result<()> {
        for broker in brokers {
            let addr = resolve_broker_address(&broker.host, broker.port)?;
            let node_id = broker.node_id;

            // Reuse healthy connection if address unchanged
            if let Some(entry) = self.brokers.get(&node_id) {
                if entry.addr == addr && entry.healthy {
                    continue;
                }
            }

            // Try new connection
            match self.connect_to_broker(addr).await {
                Ok(conn) => {
                    self.register_broker(node_id, addr, conn).await;
                    debug!("Registered/updated broker {} at {}", node_id, addr);
                }
                Err(e) => {
                    warn!("Could not connect to broker {} at {}: {}", node_id, addr, e);
                }
            }
        }
        Ok(())
    }

    /// Mark a broker as unhealthy
    pub fn mark_unhealthy(&self, addr: SocketAddr) {
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if let Some(mut entry) = self.brokers.get_mut(&node_id) {
                entry.healthy = false;
                warn!("Marked broker {} at {} as unhealthy", node_id, addr);
            }
        }
    }

    /// Close all connections
    pub async fn close(&self) -> Result<()> {
        let node_ids: Vec<i32> = self.brokers.iter().map(|e| *e.key()).collect();
        for node_id in node_ids {
            if let Some((_, entry)) = self.brokers.remove(&node_id) {
                if let Ok(conn) = Arc::try_unwrap(entry.conn) {
                    let conn = conn.into_inner();
                    if let Err(e) = conn.close().await {
                        warn!(
                            "Error closing connection to broker {} at {}: {}",
                            node_id, entry.addr, e
                        );
                    }
                }
            }
        }
        self.addr_to_node.clear();
        Ok(())
    }

    /// Spawn background health check task
    /// 预留功能，用于健康检查
    #[allow(dead_code)]
    pub fn spawn_health_check(this: &Arc<Self>, check_interval: Duration) {
        let this = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                debug!("Starting broker health check");

                let addrs: Vec<(i32, SocketAddr, bool)> = this
                    .brokers
                    .iter()
                    .map(|e| (*e.key(), e.addr, e.healthy))
                    .collect();

                for (node_id, addr, healthy) in &addrs {
                    if *healthy {
                        let conn = this.brokers.get(node_id).map(|e| e.conn.clone());
                        if let Some(conn) = conn {
                            let mut guard = conn.lock().await;
                            let request = ApiVersionsRequest {
                                client_software_name: None,
                                client_software_version: None,
                            };
                            match guard
                                .send_request::<_, crate::protocol::ApiVersionsResponse>(&request)
                                .await
                            {
                                Ok(_) => continue,
                                Err(e) => {
                                    warn!(
                                        "Health check failed for broker {} at {}: {}",
                                        node_id, addr, e
                                    );
                                }
                            }
                        }
                        this.mark_unhealthy(*addr);
                    }

                    // Try reconnect
                    match this.connect_to_broker(*addr).await {
                        Ok(new_conn) => {
                            let addr = *addr;
                            if let Some(mut entry) = this.brokers.get_mut(node_id) {
                                entry.conn = Arc::new(Mutex::new(new_conn));
                                entry.healthy = true;
                                debug!("Reconnected broker {} at {}", node_id, addr);
                            } else {
                                this.register_broker(*node_id, addr, new_conn).await;
                            }
                        }
                        Err(e) => {
                            warn!("Reconnect failed for broker {} at {}: {}", node_id, addr, e);
                        }
                    }
                }
            }
        });
    }
}

fn resolve_broker_address(host: &str, port: i32) -> Result<SocketAddr> {
    let addr_str = format!("{}:{}", host, port);
    addr_str
        .to_socket_addrs()
        .map_err(|_| {
            KafkaError::InvalidConfiguration(format!("Invalid broker address: {}", addr_str))
        })?
        .next()
        .ok_or_else(|| {
            KafkaError::InvalidConfiguration(format!("No address resolved for: {}", addr_str))
        })
}