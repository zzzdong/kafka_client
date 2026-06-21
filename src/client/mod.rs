pub mod broker_manager;
pub mod high_level;
pub mod low_level;
pub mod metadata;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::high_level::{Consumer, ConsumerConfig, Producer, ProducerConfig};
use crate::client::low_level::{ClientConfig, KafkaClient as LowLevelClient};
use crate::error::Result;
use crate::sasl::{SaslCredentials, SaslMechanismType};
use crate::transport::{SecurityProtocol, TlsConfigData};

/// Kafka 客户端构建器
pub struct KafkaClientBuilder {
    bootstrap_servers: Vec<SocketAddr>,
    security_protocol: SecurityProtocol,
    client_id: String,
    sasl_credentials: Option<SaslCredentials>,
    metadata_ttl: std::time::Duration,
}

impl KafkaClientBuilder {
    pub fn new(bootstrap_servers: Vec<SocketAddr>) -> Self {
        Self {
            bootstrap_servers,
            security_protocol: SecurityProtocol::Plaintext,
            client_id: "rust-kafka-client".to_string(),
            sasl_credentials: None,
            metadata_ttl: std::time::Duration::from_secs(300),
        }
    }

    pub fn with_plaintext(mut self) -> Self {
        self.security_protocol = SecurityProtocol::Plaintext;
        self
    }

    pub fn with_tls(mut self, domain: impl Into<String>) -> Self {
        self.security_protocol = SecurityProtocol::Ssl(TlsConfigData {
            domain: domain.into(),
            ..Default::default()
        });
        self
    }

    pub fn with_tls_config(mut self, tls_config: TlsConfigData) -> Self {
        self.security_protocol = SecurityProtocol::Ssl(tls_config);
        self
    }

    pub fn with_sasl_plaintext(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.security_protocol = SecurityProtocol::SaslPlaintext;
        self.sasl_credentials = Some(SaslCredentials {
            mechanism: SaslMechanismType::Plain,
            username: username.into(),
            password: password.into(),
            authzid: None,
        });
        self
    }

    pub fn with_sasl_ssl(
        mut self,
        domain: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.security_protocol = SecurityProtocol::SaslSsl(TlsConfigData {
            domain: domain.into(),
            ..Default::default()
        });
        self.sasl_credentials = Some(SaslCredentials {
            mechanism: SaslMechanismType::Plain,
            username: username.into(),
            password: password.into(),
            authzid: None,
        });
        self
    }

    pub fn with_sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.sasl_credentials = Some(SaslCredentials {
            mechanism: SaslMechanismType::ScramSha256,
            username: username.into(),
            password: password.into(),
            authzid: None,
        });
        self
    }

    pub fn with_sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.sasl_credentials = Some(SaslCredentials {
            mechanism: SaslMechanismType::ScramSha512,
            username: username.into(),
            password: password.into(),
            authzid: None,
        });
        self
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    pub fn with_metadata_ttl(mut self, ttl: std::time::Duration) -> Self {
        self.metadata_ttl = ttl;
        self
    }

    pub async fn build_low_level(self) -> Result<LowLevelClient> {
        let config = ClientConfig {
            bootstrap_servers: self.bootstrap_servers,
            security_protocol: self.security_protocol,
            client_id: self.client_id,
            metadata_ttl: self.metadata_ttl,
        };
        LowLevelClient::connect(config).await
    }

    pub async fn build_producer(self, producer_config: ProducerConfig) -> Result<Producer> {
        let client = Arc::new(Mutex::new(self.build_low_level().await?));
        Ok(Producer::new(client, producer_config).await)
    }

    pub async fn build_consumer(self, consumer_config: ConsumerConfig) -> Result<Consumer> {
        let client = Arc::new(Mutex::new(self.build_low_level().await?));
        Ok(Consumer::new(client, consumer_config).await)
    }
}

/// 便捷的构建函数
pub fn builder(bootstrap_servers: Vec<SocketAddr>) -> KafkaClientBuilder {
    KafkaClientBuilder::new(bootstrap_servers)
}
