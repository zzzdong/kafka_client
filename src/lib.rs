//! Kafka Rust Client
//!
//! A pure Rust Kafka client library based on Tokio async runtime.
//! Supports SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)
//! and uses a layered architecture with both low-level protocol API
//! and high-level producer/consumer API.

pub mod error;
pub mod transport;
pub mod protocol;
pub mod sasl;
pub mod connection;
pub mod client;

// Re-export commonly used types
pub use error::{KafkaError, SaslError, ProtocolError, Result};
pub use transport::{SecurityProtocol, TlsConfigData};
pub use sasl::{SaslCredentials, SaslMechanismType};
pub use connection::{KafkaConnection, NegotiatedVersions};

// Re-export client types
pub use client::{
    KafkaClientBuilder,
    builder,
};

pub use client::low_level::{
    KafkaClient as LowLevelClient,
    ClientConfig,
};

pub use client::metadata::MetadataCache;

pub use client::high_level::{
    // Producer
    Producer,
    ProducerConfig,
    ProducerRecord,
    RecordMetadata,
    Header,
    // Consumer
    Consumer,
    ConsumerConfig,
    AutoOffsetReset,
    // Partition routing
    PartitionRouter,
    PartitionRouting,
};

// Re-export protocol types
pub use protocol::{
    VersionedKafkaEncode,
    VersionedKafkaDecode,
};

pub use protocol::api::{
    // Metadata
    MetadataRequest,
    MetadataResponse,
    Broker,
    Topic,
    Partition,
    // Produce
    ProduceRequest,
    ProduceResponse,
    // Fetch
    FetchRequest,
    FetchResponse,
    ConsumerRecord,
    // ApiVersions
    ApiVersionsRequest,
    ApiVersionsResponse,
    ApiVersion,
    // SASL
    SaslHandshakeRequest,
    SaslHandshakeResponse,
    SaslAuthenticateRequest,
    SaslAuthenticateResponse,
};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");
