use dashmap::DashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
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
///
/// 内部使用 `DashMap` 实现按 broker 拆分锁，方法签名以 `&self` 为主，
/// 支持多个并发请求同时获取不同 broker 的连接。
pub struct BrokerManager {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: SecurityProtocol,
    client_id: String,
    client_name: String,
    client_version: String,
    /// node_id -> broker entry
    brokers: DashMap<i32, BrokerEntry>,
    /// address -> node_id
    addr_to_node: DashMap<SocketAddr, i32>,
    /// 为未知地址（无 metadata node_id）生成唯一临时 node_id 的计数器
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
            // 从 i32::MIN 开始，避免与 Kafka 正常非负 node_id 冲突
            next_unknown_node_id: AtomicI32::new(i32::MIN),
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

    /// 注册一个 broker（通常来自 metadata 响应）
    pub async fn register_broker(&self, node_id: i32, addr: SocketAddr, conn: Connection) {
        // 固定更新顺序：先清理 addr_to_node 中旧映射，再插入新映射
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

    /// 获取指定 node_id 的连接，如果不存在则按 host:port 新建
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
            // 尝试重连不健康的 broker
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

    /// 获取指定地址的连接，如果不存在则新建
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

    /// 获取任意一个健康的 broker 连接（优先已连接且健康的 broker）
    pub fn get_any_healthy_broker(&self) -> Option<(SocketAddr, Arc<Mutex<Connection>>)> {
        self.brokers
            .iter()
            .find(|e| e.healthy)
            .map(|e| (e.addr, e.conn.clone()))
    }

    /// 返回所有已知 broker 地址
    pub fn all_broker_addresses(&self) -> Vec<SocketAddr> {
        self.brokers.iter().map(|e| e.addr).collect()
    }

    /// 根据 metadata 响应刷新 broker 列表
    pub async fn refresh_from_metadata(&self, brokers: Vec<MetadataResponseBroker>) -> Result<()> {
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
                    self.register_broker(node_id, addr, conn).await;
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
    pub fn mark_unhealthy(&self, addr: SocketAddr) {
        if let Some(node_id) = self.addr_to_node.get(&addr).map(|e| *e) {
            if let Some(mut entry) = self.brokers.get_mut(&node_id) {
                entry.healthy = false;
                warn!("Marked broker {} at {} as unhealthy", node_id, addr);
            }
        }
    }

    /// 关闭并清理所有连接
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

    /// 启动后台健康检查任务，定期对已知 broker 进行探活和重连
    ///
    /// # Arguments
    /// * `check_interval` - 健康检查间隔（默认建议 30 秒）
    pub fn spawn_health_check(this: &Arc<Self>, check_interval: Duration) {
        let this = this.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_interval);
            interval.tick().await; // 跳过立即执行
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
                        // 对健康节点发送轻量探活请求
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

                        // 探活失败，标记不健康
                        this.mark_unhealthy(*addr);
                    }

                    // 尝试重连不健康节点
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
