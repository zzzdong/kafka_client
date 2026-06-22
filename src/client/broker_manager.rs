use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::connection::{Builder as ConnectionBuilder, Connection};
use crate::error::{KafkaError, Result};
use crate::protocol::{ApiVersionsRequest, MetadataResponseBroker};
use crate::transport::SecurityProtocol;

/// Broker 连接条目
struct BrokerEntry {
    addr: SocketAddr,
    conn: Arc<Mutex<Connection>>,
    healthy: bool,
}

/// 管理集群中所有 broker 的发现、连接与连接池
pub struct BrokerManager {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: SecurityProtocol,
    client_id: String,
    client_name: String,
    client_version: String,
    /// node_id -> broker entry
    brokers: HashMap<i32, BrokerEntry>,
    /// address -> node_id
    addr_to_node: HashMap<SocketAddr, i32>,
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
            brokers: HashMap::new(),
            addr_to_node: HashMap::new(),
        }
    }

    async fn connect_to_broker(&self, addr: SocketAddr) -> Result<Connection> {
        let builder = ConnectionBuilder::new(
            addr,
            self.security_protocol.clone(),
            self.client_name.clone(),
            self.client_version.clone(),
        )
        .with_client_id(self.client_id.clone());

        builder.build().await
    }

    /// 尝试连接一个 bootstrap broker，成功即返回
    pub async fn bootstrap(&mut self) -> Result<SocketAddr> {
        let addrs: Vec<SocketAddr> = self.bootstrap_servers.clone();
        for addr in addrs {
            match self.connect_to_broker(addr).await {
                Ok(conn) => {
                    self.register_broker(-1, addr, conn);
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

    /// 注册一个 broker（通常来自 metadata 响应）
    pub fn register_broker(&mut self, node_id: i32, addr: SocketAddr, conn: Connection) {
        // 如果地址已被其他 node_id 占用，先清理旧映射
        if let Some(old_node_id) = self.addr_to_node.get(&addr).copied() {
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

    /// 获取指定 node_id 的连接，如果不存在则按 host:port 新建
    pub async fn get_or_connect(
        &mut self,
        node_id: i32,
        host: &str,
        port: i32,
    ) -> Result<Arc<Mutex<Connection>>> {
        let existing_addr = self
            .brokers
            .get(&node_id)
            .map(|e| (e.addr, e.healthy, e.conn.clone()));
        if let Some((addr, healthy, conn)) = existing_addr {
            if healthy {
                return Ok(conn);
            }
            // 尝试重连不健康的 broker
            match self.connect_to_broker(addr).await {
                Ok(new_conn) => {
                    if let Some(entry) = self.brokers.get_mut(&node_id) {
                        entry.conn = Arc::new(Mutex::new(new_conn));
                        entry.healthy = true;
                        return Ok(entry.conn.clone());
                    }
                }
                Err(e) => {
                    warn!("Reconnect to broker {} at {} failed: {}", node_id, addr, e);
                }
            }
        }

        let addr = resolve_broker_address(host, port)?;
        let conn = self.connect_to_broker(addr).await?;
        self.register_broker(node_id, addr, conn);
        self.brokers
            .get(&node_id)
            .map(|e| e.conn.clone())
            .ok_or_else(|| {
                KafkaError::InvalidConfiguration("Failed to register broker connection".to_string())
            })
    }

    /// 获取指定地址的连接，如果不存在则新建
    pub async fn get_connection(&mut self, addr: SocketAddr) -> Result<Arc<Mutex<Connection>>> {
        let existing = self.addr_to_node.get(&addr).copied().and_then(|node_id| {
            self.brokers
                .get(&node_id)
                .map(|e| (node_id, e.healthy, e.conn.clone()))
        });

        if let Some((node_id, healthy, conn)) = existing {
            if healthy {
                return Ok(conn);
            }
            match self.connect_to_broker(addr).await {
                Ok(new_conn) => {
                    if let Some(entry) = self.brokers.get_mut(&node_id) {
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

        let conn = self.connect_to_broker(addr).await?;
        let node_id = -1;
        self.register_broker(node_id, addr, conn);
        self.brokers
            .get(&node_id)
            .map(|e| e.conn.clone())
            .ok_or_else(|| {
                KafkaError::InvalidConfiguration(
                    "Failed to register new broker connection".to_string(),
                )
            })
    }

    /// 获取任意一个健康的 broker 连接（优先已连接且健康的 broker）
    pub fn get_any_healthy_broker(&self) -> Option<(SocketAddr, Arc<Mutex<Connection>>)> {
        self.brokers
            .values()
            .find(|e| e.healthy)
            .map(|e| (e.addr, e.conn.clone()))
    }

    /// 返回所有已知 broker 地址
    pub fn all_broker_addresses(&self) -> Vec<SocketAddr> {
        self.brokers.values().map(|e| e.addr).collect()
    }

    /// 根据 metadata 响应刷新 broker 列表
    pub async fn refresh_from_metadata(
        &mut self,
        brokers: Vec<MetadataResponseBroker>,
    ) -> Result<()> {
        for broker in brokers {
            let addr = resolve_broker_address(&broker.host, broker.port)?;
            let node_id = broker.node_id;

            // 若已存在同一 node_id 的健康连接且地址未变，则复用
            if let Some(entry) = self.brokers.get(&node_id) {
                if entry.addr == addr && entry.healthy {
                    continue;
                }
            }

            // 否则尝试建立新连接
            match self.connect_to_broker(addr).await {
                Ok(conn) => {
                    self.register_broker(node_id, addr, conn);
                    debug!("Registered/updated broker {} at {}", node_id, addr);
                }
                Err(e) => {
                    warn!("Could not connect to broker {} at {}: {}", node_id, addr, e);
                    // 仍保留旧的 broker 信息，以便后续重连
                }
            }
        }
        Ok(())
    }

    /// 标记某个 broker 为不健康
    pub fn mark_unhealthy(&mut self, addr: SocketAddr) {
        if let Some(node_id) = self.addr_to_node.get(&addr).copied() {
            if let Some(entry) = self.brokers.get_mut(&node_id) {
                entry.healthy = false;
                warn!("Marked broker {} at {} as unhealthy", node_id, addr);
            }
        }
    }

    /// 关闭并清理所有连接
    pub async fn close(&mut self) -> Result<()> {
        for (node_id, entry) in self.brokers.drain() {
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
        self.addr_to_node.clear();
        Ok(())
    }

    /// 启动后台健康检查任务，定期对已知 broker 进行探活和重连
    ///
    /// # Arguments
    /// * `check_interval` - 健康检查间隔（默认建议 30 秒）
    pub fn spawn_health_check(this: &Arc<Mutex<Self>>, check_interval: Duration) {
        let this = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            interval.tick().await; // 跳过立即执行
            loop {
                interval.tick().await;
                debug!("Starting broker health check");

                let addrs: Vec<(i32, SocketAddr, bool)> = {
                    let mgr = this.lock().await;
                    mgr.brokers
                        .iter()
                        .map(|(id, e)| (*id, e.addr, e.healthy))
                        .collect()
                };

                for (node_id, addr, healthy) in &addrs {
                    if *healthy {
                        // 对健康节点发送轻量探活请求
                        let result = {
                            let mgr = this.lock().await;

                            mgr.brokers.get(node_id).map(|e| e.conn.clone())
                        };

                        if let Some(conn) = result {
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

                        // 探活失败，标记不健康
                        let mut mgr = this.lock().await;
                        mgr.mark_unhealthy(*addr);
                    }

                    // 尝试重连不健康节点
                    let mut mgr = this.lock().await;
                    match mgr.connect_to_broker(*addr).await {
                        Ok(new_conn) => {
                            let addr = *addr;
                            if let Some(entry) = mgr.brokers.get_mut(node_id) {
                                entry.conn = Arc::new(Mutex::new(new_conn));
                                entry.healthy = true;
                                debug!("Reconnected broker {} at {}", node_id, addr);
                            } else {
                                mgr.register_broker(*node_id, addr, new_conn);
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
