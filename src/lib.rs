//! Kafka Rust Client
//!
//! A pure Rust Kafka client library based on Tokio async runtime.
//! Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI/Kerberos)
//! and TLS encryption.
//!
//! # Quick Start
//!
//! ```ignore
//! use kafka_client::Client;
//!
//! // Create client — connects to cluster, discovers all brokers
//! let client = Client::builder(vec!["localhost:9092".into()])
//!     .with_plaintext()
//!     .build()
//!     .await?;
//!
//! // Producer — send messages
//! let producer = client.producer_default().await;
//! producer.send(ProducerRecord::new("my-topic", b"hello".into())).await?;
//!
//! // Consumer — read messages
//! let mut consumer = client.consumer_default();
//! consumer.subscribe(vec!["my-topic".into()]).await?;
//! let records = consumer.poll().await?;
//!
//! client.close().await?;
//! ```
//!
//! # Advanced Configuration
//!
//! ```ignore
//! // Custom producer config
//! let producer = client.producer(
//!     ProducerConfig::new().with_acks(-1).with_retries(3)
//! ).await;
//!
//! // Consumer with group coordination  
//! let mut consumer = client.consumer(
//!     ConsumerConfig::new("my-group").with_earliest()
//! );
//! ```

// Layered architecture
//
// The crate is organised into four layers. Each is public, so you can drop
// down exactly as far as a use case requires and no further:
//
// ```text
// L4  Client / Producer / Consumer / AdminClient   semantic API (start here)
// L3  connection::{ConnectionHandle, Builder}      request/response, multiplexing
// L2  wire::{KafkaCodec, KafkaFrame}               length-prefixed framing
// L1  transport::{NetworkStream, TlsConfig}        raw byte streams (TCP/TLS)
// ```
//
// Escape hatches, in increasing order of control:
//
// - [`connection::ConnectionHandle::send_raw_frame`] — reuse the multiplexing
//   reactor, but encode frames yourself.
// - [`connection::Builder::build_sequential`] — authenticated connection
//   without a reactor; strict one-request-at-a-time.
// - [`connection::Builder::build_framed`] — authenticated [`wire::KafkaFramed`];
//   you drive the socket via its request helpers
//   ([`wire::KafkaFramed::send_request`] for typed requests,
//   [`wire::KafkaFramed::send_frame`] for api-key/version + caller body, or
//   [`wire::KafkaFramed::send_raw_frame`] for pre-encoded bytes), or reach the
//   raw `tokio_util::codec::Framed` through [`wire::KafkaFramed::into_inner`]
//   for 1:1 frame relay. Intended for proxies.
pub mod admin;
mod cluster;
pub mod connection;
mod consumer;
mod error;
mod producer;
mod sasl;
pub mod transport;
pub mod wire;

// Re-exported so downstream crates can name the `Framed` types reachable via
// [`wire::KafkaFramed::into_inner`] without declaring their own `tokio-util`
// dependency (and risking a version mismatch).
pub use tokio_util;

// Public re-exports
pub use error::{KafkaError, KafkaErrorCode, Result};
pub use kafka_client_protocol as protocol;
pub use krb5_gss::gss::GssContext;
pub use krb5_gss::{KerberosCredentials, KerberosError};
pub use sasl::{SaslCredentials, SaslMechanismType};
pub use transport::{SecurityProtocol, TlsConfig};

// Producer types
pub use producer::{
    Header, PartitionRouter, PartitionRouting, Producer, ProducerConfig, ProducerRecord,
    RecordMetadata,
};

// Consumer types
pub use consumer::{
    AutoOffsetReset, Consumer, ConsumerConfig, ConsumerRecord, ConsumerStream, GroupHandle,
    OffsetHandle, PartitionAssignmentStrategy,
};

// Metadata types (read-only queries)
pub use cluster::MetadataCache;

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::sync::Arc;
use std::time::Duration;

use crate::cluster::ClusterClient;

// ===========================================================================
// Client — unified entry point
// ===========================================================================

/// Unified Kafka client.
///
/// Manages the lifecycle of the connection to a Kafka cluster internally.
/// Provides factory methods for creating [`Producer`] and [`Consumer`] instances.
///
/// # Examples
///
/// ```ignore
/// use kafka_client::Client;
///
/// let client = Client::builder(vec!["localhost:9092".into()])
///     .with_plaintext()
///     .build()
///     .await?;
///
/// let producer = client.producer_default().await;
/// let consumer = client.consumer_default();
/// ```
pub struct Client {
    cluster: Arc<ClusterClient>,
}

impl Client {
    /// Create a builder for constructing the client.
    ///
    /// Accepts hostnames or IP addresses (e.g. `"localhost:9092"`).
    /// Hostnames are resolved during `build()`.
    pub fn builder(bootstrap_servers: Vec<String>) -> ClientBuilder {
        ClientBuilder::new(bootstrap_servers)
    }

    // ------------------------------------------------------------------
    // Producer factories
    // ------------------------------------------------------------------

    /// Create a [`Producer`] with default configuration.
    ///
    /// Equivalent to `client.producer(ProducerConfig::default()).await`.
    pub async fn producer_default(&self) -> Producer {
        Producer::new(self.cluster.clone(), ProducerConfig::default()).await
    }

    /// Create a [`Producer`] with custom configuration.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let producer = client.producer(
    ///     ProducerConfig::new().with_acks(-1).with_retries(5)
    /// ).await?;
    /// ```
    pub async fn producer(&self, config: ProducerConfig) -> Producer {
        Producer::new(self.cluster.clone(), config).await
    }

    // ------------------------------------------------------------------
    // Consumer factories
    // ------------------------------------------------------------------

    /// Create a [`Consumer`] with default configuration.
    ///
    /// Creates a direct-mode consumer (no consumer group). All partitions
    /// of the subscribed topics are fetched directly from the cluster.
    /// Use [`Consumer`](Consumer) with `ConsumerConfig::new("my-group")`
    /// for group-coordinated consumption.
    pub fn consumer_default(&self) -> Consumer {
        Consumer::new(self.cluster.clone(), ConsumerConfig::default())
    }

    /// Create a [`Consumer`] with custom configuration.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Simple consumer (no consumer group)
    /// let consumer = client.consumer(ConsumerConfig::default());
    ///
    /// // Group consumer
    /// let consumer = client.consumer(
    ///     ConsumerConfig::new("my-group").with_earliest()
    /// );
    /// ```
    pub fn consumer(&self, config: ConsumerConfig) -> Consumer {
        Consumer::new(self.cluster.clone(), config)
    }

    // ------------------------------------------------------------------
    // Admin client
    // ------------------------------------------------------------------

    /// Create an [`AdminClient`](admin::AdminClient) for cluster management.
    ///
    /// Used for creating/deleting topics, listing groups, describing
    /// the cluster, and other administrative operations.
    pub fn admin(&self) -> admin::AdminClient {
        admin::AdminClient::new(self.cluster.clone())
    }

    // ------------------------------------------------------------------
    // Metadata (read-only)
    // ------------------------------------------------------------------

    /// Get a reference to the metadata cache.
    ///
    /// Useful for discovering topics, partitions, and broker addresses
    /// without sending RPC requests.
    pub fn metadata(&self) -> &MetadataCache {
        self.cluster.metadata()
    }

    // ------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------

    /// Force a metadata refresh from the cluster.
    ///
    /// Useful when you need up-to-date partition leadership information
    /// before admin operations.
    pub async fn refresh_metadata(&self) -> Result<()> {
        self.cluster.refresh_metadata().await
    }

    /// Send a request to any available broker (advanced usage).
    ///
    /// Useful for admin operations like creating/deleting topics,
    /// or custom protocol requests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use kafka_client::protocol::{CreateTopicsRequest, CreateTopicsResponse};
    ///
    /// let response: CreateTopicsResponse = client.send_to_any_broker(&request).await?;
    /// ```
    pub async fn send_to_any_broker<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: kafka_client_protocol::Request,
        Resp: kafka_client_protocol::Response,
    {
        self.cluster.send_to_any_broker(request).await
    }

    /// Close the client, releasing all broker connections.
    pub async fn close(&self) -> Result<()> {
        self.cluster.close().await
    }
}

// ===========================================================================
// ClientConfig — declarative configuration
// ===========================================================================

/// Declarative configuration for creating a [`Client`].
///
/// Alternative to the [`ClientBuilder`] — useful when config comes from
/// a file, environment, or serialized source.
///
/// # Example
/// ```ignore
/// use kafka_client::{Client, ClientConfig};
/// let client = Client::connect(ClientConfig {
///     bootstrap_servers: vec!["localhost:9092".into()],
///     client_id: "my-app".into(),
///     ..Default::default()
/// }).await?;
/// ```
pub struct ClientConfig {
    /// Bootstrap server addresses (host:port strings).
    pub bootstrap_servers: Vec<String>,
    /// Security protocol. Defaults to `Plaintext` via `ClientBuilder`.
    pub security_protocol: crate::transport::SecurityProtocol,
    /// Client ID sent to Kafka brokers.
    pub client_id: String,
    /// SASL credentials (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
    pub sasl: Option<crate::sasl::SaslCredentials>,
    /// Kerberos credentials (principal + keytab).
    pub kerberos: Option<krb5_gss::KerberosCredentials>,
    /// KDC hostname (Kerberos only).
    pub kdc_host: Option<String>,
    /// KDC port (default 88).
    pub kdc_port: u16,
    /// Broker hostname for the Kerberos service principal.
    pub broker_hostname: Option<String>,
    /// Metadata cache TTL (default 5 minutes).
    pub metadata_ttl: Duration,
    /// Maximum time to wait for a single broker request to respond.
    /// Must be longer than the consumer's `max_wait` (fetch timeout).
    /// Default: 60 seconds.
    pub request_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: Vec::new(),
            security_protocol: crate::transport::SecurityProtocol::Plaintext,
            client_id: NAME.to_string(),
            sasl: None,
            kerberos: None,
            kdc_host: None,
            kdc_port: 88,
            broker_hostname: None,
            metadata_ttl: Duration::from_secs(300),
            request_timeout: Duration::from_secs(60),
        }
    }
}

impl ClientConfig {
    /// Apply SASL credentials and set security protocol to `SaslPlaintext`.
    pub fn with_sasl(
        mut self,
        mechanism: SaslMechanismType,
        username: String,
        password: String,
    ) -> Self {
        self.sasl = Some(SaslCredentials::new(mechanism, username, password));
        self.security_protocol = crate::transport::SecurityProtocol::SaslPlaintext;
        self
    }
}

// ===========================================================================
// ClientBuilder — chainable builder (wraps ClientConfig)
// ===========================================================================

/// Builder for constructing a [`Client`].
///
/// Supports plaintext, TLS, SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512),
/// and Kerberos (SASL/GSSAPI) authentication.
///
/// # Example
/// ```ignore
/// let client = Client::builder(vec!["localhost:9092".into()])
///     .with_kerberos(creds)
///     .with_kdc("kdc.example.com", 88)
///     .build()
///     .await?;
/// ```
pub struct ClientBuilder {
    config: ClientConfig,
}

impl ClientBuilder {
    /// Create a new builder with the given bootstrap servers.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self {
            config: ClientConfig {
                bootstrap_servers,
                security_protocol: crate::transport::SecurityProtocol::Plaintext,
                client_id: NAME.to_string(),
                sasl: None,
                kerberos: None,
                metadata_ttl: Duration::from_secs(300),
                request_timeout: Duration::from_secs(60),
                kdc_host: None,
                kdc_port: 88,
                broker_hostname: None,
            },
        }
    }

    // --- Security protocol ---

    /// Use plaintext (no encryption, no authentication).
    pub fn with_plaintext(mut self) -> Self {
        self.config.security_protocol = crate::transport::SecurityProtocol::Plaintext;
        self
    }

    /// Use TLS encryption with the given domain.
    pub fn with_tls(mut self, domain: impl Into<String>) -> Self {
        self.config.security_protocol =
            crate::transport::SecurityProtocol::Ssl(crate::transport::TlsConfig {
                domain: domain.into(),
                ..Default::default()
            });
        self
    }

    /// Use TLS with full custom configuration.
    pub fn with_tls_config(mut self, tls_config: crate::transport::TlsConfig) -> Self {
        self.config.security_protocol = crate::transport::SecurityProtocol::Ssl(tls_config);
        self
    }

    // --- SASL ---

    /// Configure SASL authentication with a custom mechanism.
    ///
    /// Shortcut for `.with_sasl_credentials(...)` that also sets `SaslPlaintext`.
    pub fn with_sasl(
        mut self,
        mechanism: SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.sasl = Some(SaslCredentials::new(mechanism, username, password));
        self.config.security_protocol = crate::transport::SecurityProtocol::SaslPlaintext;
        self
    }

    /// Configure SASL + TLS authentication.
    pub fn with_sasl_tls(
        mut self,
        tls_config: crate::transport::TlsConfig,
        mechanism: SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.sasl = Some(SaslCredentials::new(mechanism, username, password));
        self.config.security_protocol = crate::transport::SecurityProtocol::SaslSsl(tls_config);
        self
    }

    // Convenience SASL shortcuts

    /// SASL PLAIN without TLS.
    pub fn with_sasl_plaintext(
        self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.with_sasl(SaslMechanismType::Plain, username, password)
    }

    /// SASL PLAIN with TLS (domain-based config).
    pub fn with_sasl_ssl(
        self,
        domain: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let tls_config = crate::transport::TlsConfig {
            domain: domain.into(),
            ..Default::default()
        };
        self.with_sasl_tls(tls_config, SaslMechanismType::Plain, username, password)
    }

    /// Set SASL credentials without modifying the security protocol.
    ///
    /// Use this when you need to set SASL + TLS separately:
    /// ```ignore
    /// Client::builder(servers)
    ///     .with_tls(domain)
    ///     .with_sasl_credentials(mech, user, pass)
    ///     .build()
    /// ```
    pub fn with_sasl_credentials(
        mut self,
        mechanism: SaslMechanismType,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.sasl = Some(SaslCredentials::new(mechanism, username, password));
        self
    }

    // --- Kerberos ---

    /// Set Kerberos credentials without modifying the security protocol.
    ///
    /// Use together with [`with_tls`](Self::with_tls) for TLS-secured Kerberos:
    /// ```ignore
    /// Client::builder(servers)
    ///     .with_tls("broker.example.com")
    ///     .with_kerberos(creds)
    ///     .with_kdc("kdc.example.com", 88)
    ///     .build()
    /// ```
    /// Or use [`with_kerberos_tls`](Self::with_kerberos_tls) for a single call.
    pub fn with_kerberos(mut self, credentials: krb5_gss::KerberosCredentials) -> Self {
        self.config.kerberos = Some(credentials);
        self
    }

    /// Configure Kerberos + TLS in one call.
    pub fn with_kerberos_tls(
        mut self,
        tls_config: crate::transport::TlsConfig,
        credentials: krb5_gss::KerberosCredentials,
    ) -> Self {
        self.config.kerberos = Some(credentials);
        self.config.security_protocol = crate::transport::SecurityProtocol::SaslSsl(tls_config);
        self
    }

    /// Set the KDC address (host:port). Only effective when Kerberos is enabled.
    pub fn with_kdc(mut self, host: impl Into<String>, port: u16) -> Self {
        self.config.kdc_host = Some(host.into());
        self.config.kdc_port = port;
        self
    }

    /// Set the broker hostname used in the Kerberos service principal.
    pub fn with_broker_hostname(mut self, host: impl Into<String>) -> Self {
        self.config.broker_hostname = Some(host.into());
        self
    }

    // --- Other settings ---

    /// Set a custom client ID (sent to Kafka brokers).
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Override the metadata cache TTL. Default is 5 minutes.
    pub fn with_metadata_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_ttl = ttl;
        self
    }

    /// Override the per-request timeout. Default is 60 seconds.
    ///
    /// This bounds how long a single broker request (produce, fetch,
    /// metadata, admin, ...) may wait for a response. It must be larger than
    /// the consumer's `max_wait`, since a fetch may legitimately block on the
    /// broker for that long.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    // --- Build ---

    /// Connect to the cluster and build the [`Client`].
    pub async fn build(self) -> Result<Client> {
        Client::connect(self.config).await
    }
}

impl Client {
    /// Connect to a Kafka cluster from a [`ClientConfig`].
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        let mut resolved = Vec::with_capacity(config.bootstrap_servers.len());
        for server in &config.bootstrap_servers {
            match tokio::net::lookup_host(server).await {
                Ok(mut addrs) => {
                    if let Some(addr) = addrs.next() {
                        resolved.push(addr);
                    } else {
                        return Err(KafkaError::Io(format!(
                            "Failed to resolve bootstrap server: {}",
                            server
                        )));
                    }
                }
                Err(e) => {
                    return Err(KafkaError::Io(format!(
                        "Failed to resolve bootstrap server '{}': {}",
                        server, e
                    )));
                }
            }
        }

        let cluster_config = crate::cluster::ClusterConfig {
            bootstrap_servers: resolved,
            security_protocol: config.security_protocol,
            client_id: config.client_id,
            metadata_ttl: config.metadata_ttl,
            sasl: config.sasl,
            kerberos: config.kerberos,
            kdc_host: config.kdc_host,
            kdc_port: config.kdc_port,
            broker_hostname: config.broker_hostname,
            request_timeout: config.request_timeout,
        };

        let cluster = ClusterClient::connect(cluster_config).await?;
        Ok(Client {
            cluster: Arc::new(cluster),
        })
    }
}

/// Convenience builder function — equivalent to `Client::builder(...)`.
pub fn builder(bootstrap_servers: Vec<String>) -> ClientBuilder {
    ClientBuilder::new(bootstrap_servers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_config_defaults() {
        let config = ClientConfig::default();
        assert!(config.bootstrap_servers.is_empty());
        assert_eq!(
            config.security_protocol,
            crate::transport::SecurityProtocol::Plaintext
        );
        assert_eq!(config.client_id, NAME);
        assert!(config.sasl.is_none());
        assert!(config.kerberos.is_none());
        assert_eq!(config.metadata_ttl, Duration::from_secs(300));
        assert_eq!(config.request_timeout, Duration::from_secs(60));
        assert_eq!(config.kdc_port, 88);
    }

    #[test]
    fn client_config_with_sasl_sets_protocol() {
        let config = ClientConfig::default().with_sasl(
            SaslMechanismType::Plain,
            "user".to_string(),
            "pass".to_string(),
        );
        assert!(config.sasl.is_some());
        assert_eq!(
            config.security_protocol,
            crate::transport::SecurityProtocol::SaslPlaintext
        );
    }

    #[test]
    fn builder_defaults_match_config() {
        let builder = ClientBuilder::new(vec!["localhost:9092".into()]);
        assert_eq!(builder.config.request_timeout, Duration::from_secs(60));
        assert!(
            builder
                .config
                .bootstrap_servers
                .contains(&"localhost:9092".to_string())
        );
    }
}
