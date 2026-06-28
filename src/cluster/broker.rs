//! Broker manager - connection pool for all cluster brokers
//!
//! Uses `ArcSwap` for `BrokerEntry.conn` to allow atomic connection hot-swap
//! without tearing down entries that concurrent tasks hold references to.

use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;
use tracing::{debug, warn};

use crate::connection::{Builder, ConnectionHandle};
use crate::error::{KafkaError, Result};
use crate::sasl::SaslCredentials;
use crate::transport::SecurityProtocol;
use kafka_client_protocol::{ApiVersionsRequest, MetadataResponseBroker};

/// Broker connection entry.
///
/// `conn` uses `ArcSwap` so that a corrupted connection handle can be
/// atomically replaced without breaking references held by callers that
/// already hold a clone of the handle.
struct BrokerEntry {
    addr: SocketAddr,
    conn: ArcSwap<ConnectionHandle>,
    healthy: AtomicBool,
}

impl BrokerEntry {
    fn new(addr: SocketAddr, conn: ConnectionHandle) -> Self {
        Self {
            addr,
            conn: ArcSwap::new(Arc::new(conn)),
            healthy: AtomicBool::new(true),
        }
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    fn mark_unhealthy(&self) {
        self.healthy.store(false, Ordering::Relaxed);
    }

    fn mark_healthy(&self) {
        self.healthy.store(true, Ordering::Relaxed);
    }

    fn load_conn(&self) -> Arc<ConnectionHandle> {
        self.conn.load_full()
    }

    /// Atomically replace the connection handle and mark healthy.
    fn swap_conn(&self, new_conn: ConnectionHandle) {
        self.conn.store(Arc::new(new_conn));
        self.mark_healthy();
    }
}

/// Manage connections to all brokers in the cluster.
///
/// Each broker has a single [`ConnectionHandle`] stored behind an
/// [`ArcSwap`] so that:
/// - Callers can hold a clone of the handle without a lock.
/// - A corrupted handle can be atomically replaced.
/// - Multiple requests can be in-flight concurrently (handles are lock-free).
pub struct BrokerManager {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: SecurityProtocol,
    client_id: String,
    client_name: String,
    client_version: String,
    sasl: Option<SaslCredentials>,
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
        sasl: Option<SaslCredentials>,
    ) -> Self {
        Self {
            bootstrap_servers,
            security_protocol,
            client_id,
            client_name,
            client_version,
            sasl,
            brokers: DashMap::new(),
            addr_to_node: DashMap::new(),
            next_unknown_node_id: AtomicI32::new(i32::MIN),
        }
    }

    async fn connect_to_broker(&self, addr: SocketAddr) -> Result<ConnectionHandle> {
        let mut builder = Builder::new(
            addr,
            self.security_protocol.clone(),
            self.client_name.clone(),
            self.client_version.clone(),
        )
        .with_client_id(self.client_id.clone());

        if let Some(ref sasl) = self.sasl {
            builder = builder.with_sasl(sasl.mechanism, sasl.clone());
        }

        builder.build().await
    }

    /// Try to connect to a bootstrap broker.
    pub async fn bootstrap(&self) -> Result<SocketAddr> {
        let addrs: Vec<SocketAddr> = self.bootstrap_servers.clone();
        let mut errors: Vec<String> = Vec::new();
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
                    errors.push(format!("{}: {}", addr, e));
                    continue;
                }
            }
        }
        Err(KafkaError::NoBootstrapBrokerAvailable(errors.join("; ")))
    }

    /// Register a broker connection.
    async fn register_broker(&self, node_id: i32, addr: SocketAddr, conn: ConnectionHandle) {
        if let Some(old_node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if old_node_id != node_id {
                self.brokers.remove(&old_node_id);
            }
        }
        self.addr_to_node.insert(addr, node_id);
        self.brokers.insert(node_id, BrokerEntry::new(addr, conn));
    }

    /// Get a connection handle for a broker by address.
    ///
    /// Returns a handle to a healthy connection. Unhealthy entries trigger an
    /// automatic reconnect.
    pub async fn get_connection(&self, addr: SocketAddr) -> Result<ConnectionHandle> {
        // Fast path: find existing entry and return healthy connection
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if let Some(entry) = self.brokers.get(&node_id) {
                if entry.is_healthy() {
                    return Ok(entry.load_conn().as_ref().clone());
                }
                // Unhealthy — try to reconnect inline
                drop(entry);
                match self.try_swap_connection(node_id, addr).await {
                    Some(conn) => return Ok(conn),
                    None => {
                        // Entry was removed between checks — fall through
                    }
                }
            }
        }

        // No existing entry — create a new one
        let conn = self.connect_to_broker(addr).await?;
        let node_id = self.next_unknown_node_id.fetch_sub(1, Ordering::SeqCst);
        self.register_broker(node_id, addr, conn).await;
        self.brokers
            .get(&node_id)
            .map(|e| e.load_conn().as_ref().clone())
            .ok_or_else(|| {
                KafkaError::InvalidConfiguration(
                    "Failed to register new broker connection".to_string(),
                )
            })
    }

    /// Try to reconnect and atomically swap the connection for a node.
    /// Returns the new connection handle on success, `None` if the entry was removed.
    async fn try_swap_connection(
        &self,
        node_id: i32,
        addr: SocketAddr,
    ) -> Option<ConnectionHandle> {
        match self.connect_to_broker(addr).await {
            Ok(new_conn) => {
                if let Some(entry) = self.brokers.get(&node_id) {
                    entry.swap_conn(new_conn);
                    debug!("Reconnected broker {} at {}", node_id, addr);
                    return Some(entry.load_conn().as_ref().clone());
                }
                None
            }
            Err(e) => {
                warn!("Reconnect to broker {} at {} failed: {}", node_id, addr, e);
                None
            }
        }
    }

    /// Get any healthy broker connection handle.
    pub fn get_any_healthy_broker(&self) -> Option<(SocketAddr, ConnectionHandle)> {
        self.brokers
            .iter()
            .find(|e| e.is_healthy())
            .map(|e| (e.addr, e.load_conn().as_ref().clone()))
    }

    /// Get all known broker addresses.
    pub fn all_broker_addresses(&self) -> Vec<SocketAddr> {
        self.brokers.iter().map(|e| e.addr).collect()
    }

    /// Refresh broker list from metadata response.
    pub async fn refresh_from_metadata(&self, brokers: Vec<MetadataResponseBroker>) -> Result<()> {
        for broker in brokers {
            let addr = resolve_broker_address(&broker.host, broker.port)?;
            let node_id = broker.node_id;

            // Reuse healthy connection if address unchanged
            if let Some(entry) = self.brokers.get(&node_id) {
                if entry.addr == addr && entry.is_healthy() {
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

    /// Mark a broker as unhealthy.
    ///
    /// Does *not* remove the entry — callers that already hold a
    /// `ConnectionHandle` clone can still use it, but future
    /// [`get_connection`](Self::get_connection) calls will trigger a reconnect.
    pub fn mark_unhealthy(&self, addr: SocketAddr) {
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if let Some(entry) = self.brokers.get(&node_id) {
                entry.mark_unhealthy();
                warn!("Marked broker {} at {} as unhealthy", node_id, addr);
            }
        }
    }

    /// Force-close a corrupted connection by atomically replacing it.
    ///
    /// Creates a fresh connection and swaps it in. The old handle drops,
    /// which closes the channel to its reactor, causing the old reactor
    /// task to exit cleanly.
    pub async fn force_close_connection(&self, addr: SocketAddr) {
        let node_id = match self.addr_to_node.get(&addr).map(|e| *e) {
            Some(id) => id,
            None => return,
        };

        match self.connect_to_broker(addr).await {
            Ok(new_conn) => {
                if let Some(entry) = self.brokers.get(&node_id) {
                    entry.swap_conn(new_conn);
                    warn!(
                        "Force-closed (and replaced) connection to broker {} at {}",
                        node_id, addr
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Force-close for broker {} at {} failed to reconnect: {}",
                    node_id, addr, e
                );
                self.brokers.remove(&node_id);
                self.addr_to_node.remove(&addr);
            }
        }
    }

    /// Close all connections.
    pub async fn close(&self) -> Result<()> {
        let node_ids: Vec<i32> = self.brokers.iter().map(|e| *e.key()).collect();
        for node_id in node_ids {
            self.brokers.remove(&node_id);
            self.addr_to_node.retain(|_, v| *v != node_id);
        }
        // ConnectionHandle's Drop will close the channel to the reactor,
        // which causes the reactor to exit and clean up the TCP socket.
        Ok(())
    }

    /// Spawn background health check task.
    #[allow(dead_code)]
    pub fn spawn_health_check(this: &Arc<Self>, check_interval: Duration) {
        let this = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            interval.tick().await;
            loop {
                interval.tick().await;
                debug!("Starting broker health check");

                let entries: Vec<(i32, SocketAddr)> =
                    this.brokers.iter().map(|e| (*e.key(), e.addr)).collect();

                for (node_id, addr) in &entries {
                    let healthy = this
                        .brokers
                        .get(node_id)
                        .map(|e| e.is_healthy())
                        .unwrap_or(false);

                    if healthy {
                        let conn = this.brokers.get(node_id).map(|e| e.load_conn());
                        if let Some(conn) = conn {
                            let handle = conn.as_ref().clone();
                            let request = ApiVersionsRequest {
                                client_software_name: String::new(),
                                client_software_version: String::new(),
                            };
                            match handle
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

                    // Try reconnect via force-close (which atomically replaces)
                    this.force_close_connection(*addr).await;
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
