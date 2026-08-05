//! Broker manager - connection pool for all cluster brokers
//!
//! Uses `ArcSwap` for `BrokerEntry.conn` to allow atomic connection hot-swap
//! without tearing down entries that concurrent tasks hold references to.

use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::collections::HashSet;
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
use krb5_gss::KerberosCredentials;

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
pub(crate) struct BrokerManager {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: SecurityProtocol,
    client_id: String,
    client_name: String,
    client_version: String,
    sasl: Option<SaslCredentials>,
    kerberos: Option<KerberosCredentials>,
    kdc_host: Option<String>,
    kdc_port: u16,
    broker_hostname: Option<String>,
    request_timeout: Duration,
    brokers: DashMap<i32, BrokerEntry>,
    addr_to_node: DashMap<SocketAddr, i32>,
    /// Broker hostname as advertised in metadata, keyed by resolved socket
    /// address. Used to keep the hostname (Kerberos service principal, etc.)
    /// across reconnects — without it a reconnect would silently fall back to
    /// the IP address.
    addr_hostnames: DashMap<SocketAddr, String>,
    next_unknown_node_id: AtomicI32,
}

impl BrokerManager {
    pub(crate) fn new(
        bootstrap_servers: Vec<SocketAddr>,
        security_protocol: SecurityProtocol,
        client_id: String,
        client_name: String,
        client_version: String,
        sasl: Option<SaslCredentials>,
        kerberos: Option<KerberosCredentials>,
    ) -> Self {
        Self {
            bootstrap_servers,
            security_protocol,
            client_id,
            client_name,
            client_version,
            sasl,
            kerberos,
            kdc_host: None,
            kdc_port: 88,
            broker_hostname: None,
            request_timeout: Duration::from_secs(60),
            brokers: DashMap::new(),
            addr_to_node: DashMap::new(),
            addr_hostnames: DashMap::new(),
            next_unknown_node_id: AtomicI32::new(i32::MIN),
        }
    }

    /// 设置 KDC 地址 (供内部 Builder 传递)。
    pub(crate) fn with_kdc(mut self, host: Option<String>, port: u16) -> Self {
        self.kdc_host = host;
        self.kdc_port = port;
        self
    }

    /// 设置 broker 主机名 (Kerberos 服务 principal 用)。
    pub(crate) fn with_broker_hostname(mut self, host: Option<String>) -> Self {
        self.broker_hostname = host;
        self
    }

    /// Set the per-request timeout for every broker connection.
    pub(crate) fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    async fn connect_to_broker(
        &self,
        addr: SocketAddr,
        broker_hostname: Option<&str>,
    ) -> Result<ConnectionHandle> {
        let mut builder = Builder::new(
            addr,
            self.security_protocol.clone(),
            self.client_name.clone(),
            self.client_version.clone(),
        )
        .with_client_id(self.client_id.clone())
        .with_kdc(self.kdc_host.clone().unwrap_or_default(), self.kdc_port)
        .with_request_timeout(self.request_timeout);

        if let Some(host) = broker_hostname {
            builder = builder.with_broker_hostname(host);
        }

        if let Some(ref sasl) = self.sasl {
            builder = builder.with_sasl(sasl.mechanism(), sasl.clone());
        }

        if let Some(ref krb) = self.kerberos {
            builder = builder.with_kerberos(krb.clone());
        }

        builder.build().await
    }

    /// Try to connect to a bootstrap broker.
    pub(crate) async fn bootstrap(&self) -> Result<SocketAddr> {
        let addrs: Vec<SocketAddr> = self.bootstrap_servers.clone();
        let mut errors: Vec<crate::error::BrokerConnError> = Vec::new();
        for addr in addrs {
            match self
                .connect_to_broker(addr, self.broker_hostname.as_deref())
                .await
            {
                Ok(conn) => {
                    let node_id = self.next_unknown_node_id.fetch_sub(1, Ordering::SeqCst);
                    let hostname = self
                        .broker_hostname
                        .clone()
                        .unwrap_or_else(|| addr.ip().to_string());
                    self.register_broker(node_id, addr, hostname, conn).await;
                    debug!("Connected to bootstrap broker {}", addr);
                    return Ok(addr);
                }
                Err(e) => {
                    warn!("Failed to connect to bootstrap broker {}: {}", addr, e);
                    errors.push(crate::error::BrokerConnError {
                        addr: addr.to_string(),
                        error: e,
                    });
                    continue;
                }
            }
        }
        Err(KafkaError::NoBootstrapBrokerAvailable(
            crate::error::BrokerErrors(errors),
        ))
    }

    /// Register a broker connection.
    async fn register_broker(
        &self,
        node_id: i32,
        addr: SocketAddr,
        hostname: String,
        conn: ConnectionHandle,
    ) {
        if let Some(old_node_id) = self.addr_to_node.get(&addr).map(|e| *e)
            && old_node_id != node_id
        {
            self.brokers.remove(&old_node_id);
        }
        self.addr_to_node.insert(addr, node_id);
        self.addr_hostnames.insert(addr, hostname);
        self.brokers.insert(node_id, BrokerEntry::new(addr, conn));
    }

    /// Get a connection handle for a broker by address.
    ///
    /// Returns a handle to a healthy connection. Unhealthy entries trigger an
    /// automatic reconnect.
    pub(crate) async fn get_connection(&self, addr: SocketAddr) -> Result<ConnectionHandle> {
        // Fast path: find existing entry and return healthy connection
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e)
            && let Some(entry) = self.brokers.get(&node_id)
        {
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

        // No existing entry — create a new one
        let hostname = self
            .addr_hostnames
            .get(&addr)
            .map(|e| e.value().clone())
            .unwrap_or_else(|| addr.ip().to_string());
        let conn = self.connect_to_broker(addr, Some(&hostname)).await?;
        let node_id = self.next_unknown_node_id.fetch_sub(1, Ordering::SeqCst);
        self.register_broker(node_id, addr, hostname, conn).await;
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
        let hostname = self
            .addr_hostnames
            .get(&addr)
            .map(|e| e.value().clone())
            .unwrap_or_else(|| addr.ip().to_string());
        match self.connect_to_broker(addr, Some(&hostname)).await {
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
    pub(crate) fn get_any_healthy_broker(&self) -> Option<(SocketAddr, ConnectionHandle)> {
        self.brokers
            .iter()
            .find(|e| e.is_healthy())
            .map(|e| (e.addr, e.load_conn().as_ref().clone()))
    }

    /// Get all known broker addresses.
    pub(crate) fn all_broker_addresses(&self) -> Vec<SocketAddr> {
        self.brokers.iter().map(|e| e.addr).collect()
    }

    /// Refresh broker list from metadata response.
    pub(crate) async fn refresh_from_metadata(
        &self,
        brokers: Vec<MetadataResponseBroker>,
    ) -> Result<()> {
        let mut new_node_ids: HashSet<i32> = HashSet::with_capacity(brokers.len());
        let mut new_addrs: HashSet<SocketAddr> = HashSet::with_capacity(brokers.len());

        for broker in brokers {
            let addr = resolve_broker_address(&broker.host, broker.port)?;
            let node_id = broker.node_id;
            new_node_ids.insert(node_id);
            new_addrs.insert(addr);

            // Remember the advertised hostname for reconnect purposes.
            self.addr_hostnames.insert(addr, broker.host.clone());

            // Reuse healthy connection if address unchanged
            if let Some(entry) = self.brokers.get(&node_id)
                && entry.addr == addr
                && entry.is_healthy()
            {
                continue;
            }

            // Try new connection
            match self.connect_to_broker(addr, Some(&broker.host)).await {
                Ok(conn) => {
                    self.register_broker(node_id, addr, broker.host.clone(), conn)
                        .await;
                    debug!("Registered/updated broker {} at {}", node_id, addr);
                }
                Err(e) => {
                    warn!("Could not connect to broker {} at {}: {}", node_id, addr, e);
                }
            }
        }

        // Prune brokers that disappeared from the metadata response. Keep
        // bootstrap entries (e.g. a load balancer that never appears as a
        // broker node) so the client can still reach the cluster through them.
        let stale: Vec<i32> = self
            .brokers
            .iter()
            .filter(|e| {
                !new_node_ids.contains(e.key())
                    && !self.bootstrap_servers.contains(&e.addr)
                    && !new_addrs.contains(&e.addr)
            })
            .map(|e| *e.key())
            .collect();
        for node_id in stale {
            debug!(
                "Removing broker {} (no longer present in metadata)",
                node_id
            );
            self.brokers.remove(&node_id);
            self.addr_to_node.retain(|_, v| *v != node_id);
        }
        // Drop hostname entries for addresses no longer in the cluster.
        let stale_addrs: Vec<SocketAddr> = self
            .addr_hostnames
            .iter()
            .filter(|e| !new_addrs.contains(e.key()) && !self.bootstrap_servers.contains(e.key()))
            .map(|e| *e.key())
            .collect();
        for addr in stale_addrs {
            self.addr_hostnames.remove(&addr);
        }
        Ok(())
    }

    /// Mark a broker as unhealthy.
    ///
    /// Does *not* remove the entry — callers that already hold a
    /// `ConnectionHandle` clone can still use it, but future
    /// [`get_connection`](Self::get_connection) calls will trigger a reconnect.
    pub(crate) fn mark_unhealthy(&self, addr: SocketAddr) {
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e)
            && let Some(entry) = self.brokers.get(&node_id)
        {
            entry.mark_unhealthy();
            warn!("Marked broker {} at {} as unhealthy", node_id, addr);
        }
    }

    /// Force-close a corrupted connection by atomically replacing it.
    ///
    /// Creates a fresh connection and swaps it in. The old handle drops,
    /// which closes the channel to its reactor, causing the old reactor
    /// task to exit cleanly.
    pub(crate) async fn force_close_connection(&self, addr: SocketAddr) {
        let node_id = match self.addr_to_node.get(&addr).map(|e| *e) {
            Some(id) => id,
            None => return,
        };

        let hostname = self
            .addr_hostnames
            .get(&addr)
            .map(|e| e.value().clone())
            .unwrap_or_else(|| addr.ip().to_string());
        match self.connect_to_broker(addr, Some(&hostname)).await {
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
                self.addr_hostnames.remove(&addr);
            }
        }
    }

    /// Close all connections.
    pub(crate) async fn close(&self) -> Result<()> {
        let node_ids: Vec<i32> = self.brokers.iter().map(|e| *e.key()).collect();
        for node_id in node_ids {
            self.brokers.remove(&node_id);
            self.addr_to_node.retain(|_, v| *v != node_id);
        }
        self.addr_hostnames.clear();
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
