//! Cluster layer - broker management and metadata caching

mod broker;
mod metadata;

pub(crate) use broker::BrokerManager;
pub use metadata::MetadataCache;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use crate::error::{KafkaError, Result};
use crate::sasl::SaslCredentials;
use crate::transport::SecurityProtocol;
use kafka_client_protocol::{self as protocol, Request, Response};
use krb5_gss::KerberosCredentials;

/// Cluster connection configuration (crate-internal)
#[derive(Debug, Clone)]
pub(crate) struct ClusterConfig {
    pub bootstrap_servers: Vec<SocketAddr>,
    pub security_protocol: SecurityProtocol,
    pub client_id: String,
    pub metadata_ttl: Duration,
    /// SASL authentication credentials (None = no authentication)
    pub sasl: Option<SaslCredentials>,
    /// SASL/GSSAPI (Kerberos) credentials.
    pub kerberos: Option<KerberosCredentials>,
    /// KDC 地址: host. 为 None 时回退为 realm 域名或 localhost。
    pub kdc_host: Option<String>,
    /// KDC 端口。
    pub kdc_port: u16,
    /// Broker 主机名 (Kerberos 服务 principal 用)。
    pub broker_hostname: Option<String>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: vec![],
            security_protocol: SecurityProtocol::Plaintext,
            client_id: "rust-kafka-client".to_string(),
            metadata_ttl: Duration::from_secs(300),
            sasl: None,
            kerberos: None,
            kdc_host: None,
            kdc_port: 88,
            broker_hostname: None,
        }
    }
}

/// Cluster client — shared core for Producer and Consumer.
///
/// **Crate-internal only.** External users interact via [`Client`](crate::Client).
///
/// Responsibilities:
/// - Manage connection pool to all brokers
/// - Cache and refresh cluster metadata
/// - Provide request routing to partition leaders (with retry + metadata refresh)
pub(crate) struct ClusterClient {
    broker_manager: Arc<BrokerManager>,
    metadata: Arc<MetadataCache>,
}

impl ClusterClient {
    /// Connect to cluster: bootstrap → ApiVersions negotiation → refresh metadata
    pub(crate) async fn connect(config: ClusterConfig) -> Result<Self> {
        // 把用户配置的 broker_hostname 注入 kerberos credentials,
        // 确保跨所有连接使用一致的服务 principal hostname.
        let mut kerberos = config.kerberos.clone();
        if let Some(ref krb) = kerberos
            && krb.broker_hostname.is_none()
            && let Some(ref host) = config.broker_hostname
        {
            kerberos = Some(krb.clone().with_broker_hostname(host.clone()));
        }

        let broker_manager = Arc::new(
            BrokerManager::new(
                config.bootstrap_servers.clone(),
                config.security_protocol.clone(),
                config.client_id.clone(),
                crate::NAME.to_string(),
                crate::VERSION.to_string(),
                config.sasl.clone(),
                kerberos,
            )
            .with_kdc(config.kdc_host.clone(), config.kdc_port)
            .with_broker_hostname(config.broker_hostname.clone()),
        );

        broker_manager.bootstrap().await.map_err(|e| {
            debug!(error = ?e, "bootstrap failed");
            e
        })?;

        let client = Self {
            broker_manager,
            metadata: Arc::new(MetadataCache::new(config.metadata_ttl)),
        };

        client.refresh_metadata().await.map_err(|e| {
            debug!(error = ?e, "refresh metadata failed");
            e
        })?;

        Ok(client)
    }

    /// Close all broker connections
    pub(crate) async fn close(&self) -> Result<()> {
        self.broker_manager.close().await
    }

    // ================================================================
    // Request routing
    // ================================================================

    /// Send request to partition leader — pure routing, no retry.
    ///
    /// Refreshes expired metadata, then sends the request to the current
    /// partition leader. Does **not** retry on failure — retry logic
    /// belongs to the caller (Producer/Consumer), which can classify
    /// errors and apply appropriate backoff.
    pub(crate) async fn send_to_partition<Req, Resp>(
        &self,
        topic: &str,
        partition: i32,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        // Opportunistic cache maintenance — refresh stale metadata,
        // but don't fail the request if refresh itself fails.
        if self.metadata.is_expired().await {
            debug!("Metadata expired, refreshing before routing");
            if let Err(e) = self.refresh_metadata().await {
                warn!("Failed to refresh expired metadata: {}", e);
            }
        }

        let leader_addr = self
            .metadata
            .get_partition_leader(topic, partition)
            .await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        self.send_to_broker(leader_addr, request).await
    }

    /// Send request to specific broker
    ///
    /// On `CorrelationIdMismatch` or `Protocol` decode errors, force-closes
    /// the broken connection and retries once. Marks broker as unhealthy on
    /// other failures.
    pub(crate) async fn send_to_broker<Req, Resp>(
        &self,
        broker_addr: SocketAddr,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let handle = self.broker_manager.get_connection(broker_addr).await?;
        let result = handle.send_request(request).await;

        match result {
            Ok(resp) => Ok(resp),
            Err(e @ KafkaError::CorrelationIdMismatch { .. })
            | Err(e @ KafkaError::Protocol(_)) => {
                warn!(
                    "Broker {} returned protocol error ({}), force-closing connection",
                    broker_addr, e
                );
                self.broker_manager.mark_unhealthy(broker_addr);
                self.broker_manager
                    .force_close_connection(broker_addr)
                    .await;

                let _ = self.refresh_metadata().await;

                let fresh_handle = self.broker_manager.get_connection(broker_addr).await?;
                let retry_result = fresh_handle.send_request(request).await;
                if let Err(ref e) = retry_result {
                    warn!(
                        "Retry to broker {} also failed: {}, api_key={}",
                        broker_addr,
                        e,
                        request.api_key(),
                    );
                }
                retry_result
            }
            Err(e) => {
                self.broker_manager.mark_unhealthy(broker_addr);
                Err(e)
            }
        }
    }

    /// Send request to any available broker
    ///
    /// Used for Metadata, FindCoordinator, CreateTopics, etc.
    /// Prefers healthy connected brokers, falls back to all known addresses.
    pub(crate) async fn send_to_any_broker<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        let mut errors: Vec<crate::error::BrokerConnError> = Vec::new();

        // Try healthy broker first
        if let Some((addr, handle)) = self.broker_manager.get_any_healthy_broker() {
            match handle.send_request(request).await {
                Ok(resp) => return Ok(resp),
                Err(e @ KafkaError::CorrelationIdMismatch { .. })
                | Err(e @ KafkaError::Protocol(_)) => {
                    warn!("Broker {} protocol error ({}), force-closing", addr, e);
                    self.broker_manager.mark_unhealthy(addr);
                    self.broker_manager.force_close_connection(addr).await;
                    errors.push(crate::error::BrokerConnError {
                        addr: addr.to_string(),
                        error: e,
                    });
                }
                Err(e) => {
                    warn!("Request to healthy broker {} failed: {}", addr, e);
                    self.broker_manager.mark_unhealthy(addr);
                    errors.push(crate::error::BrokerConnError {
                        addr: addr.to_string(),
                        error: e,
                    });
                }
            }
        }

        // Try all known broker addresses
        let addresses: Vec<SocketAddr> = self.broker_manager.all_broker_addresses();

        for addr in addresses {
            let handle = self.broker_manager.get_connection(addr).await?;
            match handle.send_request(request).await {
                Ok(resp) => return Ok(resp),
                Err(e @ KafkaError::CorrelationIdMismatch { .. })
                | Err(e @ KafkaError::Protocol(_)) => {
                    warn!(
                        "Broker {} api_key {} protocol error ({}), force-closing",
                        addr,
                        request.api_key(),
                        e
                    );
                    self.broker_manager.mark_unhealthy(addr);
                    self.broker_manager.force_close_connection(addr).await;
                    errors.push(crate::error::BrokerConnError {
                        addr: addr.to_string(),
                        error: e,
                    });
                }
                Err(e) => {
                    warn!(
                        "Request api_key: {} to broker {} failed: {}",
                        request.api_key(),
                        addr,
                        e
                    );
                    self.broker_manager.mark_unhealthy(addr);
                    errors.push(crate::error::BrokerConnError {
                        addr: addr.to_string(),
                        error: e,
                    });
                }
            }
        }

        Err(KafkaError::NoBrokerAvailable(crate::error::BrokerErrors(
            errors,
        )))
    }

    /// Query a broker configuration value (e.g. "max.message.bytes").
    ///
    /// Uses the DescribeConfigs API (broker resource type 4, empty resource name
    /// = the target broker itself). Returns `None` if the config key is unknown
    /// or the broker doesn't support DescribeConfigs.
    pub(crate) async fn query_broker_config(&self, key: &str) -> Option<usize> {
        use protocol::describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource};
        use protocol::describe_configs_response::DescribeConfigsResponse;

        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: 4,             // broker
                resource_name: String::new(), // empty = target broker itself
                configuration_keys: Some(vec![key.to_string()]),
            }],
            include_synonyms: false,
            include_documentation: false,
        };

        let response: Result<DescribeConfigsResponse> = self.send_to_any_broker(&request).await;
        match response {
            Ok(resp) => {
                for result in resp.results {
                    if result.error_code == 0 {
                        for config in result.configs {
                            if config.name == key
                                && let Some(ref val) = config.value
                            {
                                debug!("Broker config {} = {}", key, val);
                                return val.parse::<usize>().ok();
                            }
                        }
                    }
                }
                None
            }
            Err(e) => {
                warn!("Failed to query broker config '{}': {}", key, e);
                None
            }
        }
    }

    // ================================================================
    // Metadata
    // ================================================================

    /// Get metadata cache reference
    pub(crate) fn metadata(&self) -> &Arc<MetadataCache> {
        &self.metadata
    }

    /// Get any broker address (for admin operations)
    #[allow(dead_code)]
    pub(crate) fn any_broker_address(&self) -> Option<SocketAddr> {
        self.broker_manager.all_broker_addresses().first().copied()
    }

    /// Force refresh cluster metadata
    pub(crate) async fn refresh_metadata(&self) -> Result<()> {
        let _guard = self.metadata.acquire_refresh_lock().await;

        let request = protocol::MetadataRequest {
            topics: None,
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: protocol::MetadataResponse = self.send_to_any_broker(&request).await?;
        self.broker_manager
            .refresh_from_metadata(response.brokers.clone())
            .await?;

        self.metadata.update(response).await;
        debug!("Metadata refreshed successfully");
        Ok(())
    }

    /// Fetch metadata for the given topics.
    #[allow(dead_code)]
    pub(crate) async fn fetch_metadata_for_topics(&self, topics: &[String]) -> Result<()> {
        let request_topics: Vec<protocol::MetadataRequestTopic> = topics
            .iter()
            .map(|name| protocol::MetadataRequestTopic {
                topic_id: uuid::Uuid::nil(),
                name: Some(name.clone()),
            })
            .collect();

        let request = protocol::MetadataRequest {
            topics: Some(request_topics),
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: protocol::MetadataResponse = self.send_to_any_broker(&request).await?;
        self.metadata.update(response).await;
        Ok(())
    }
}
