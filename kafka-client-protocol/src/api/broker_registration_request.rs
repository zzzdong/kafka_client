//! Auto-generated from Kafka protocol
//! Message: BrokerRegistrationRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Listener {
    /// The name of the endpoint.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The hostname.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The port.
    #[kafka(versions = "0+")]
    pub port: u16,
    /// The security protocol.
    #[kafka(versions = "0+")]
    pub security_protocol: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Feature {
    /// The feature name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The minimum supported feature level.
    #[kafka(versions = "0+")]
    pub min_supported_version: i16,
    /// The maximum supported feature level.
    #[kafka(versions = "0+")]
    pub max_supported_version: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 62,
    msg_type = "request",
    valid_versions = "0-4",
    flexible_versions = "0+"
)]
pub struct BrokerRegistrationRequest {
    /// The broker ID.
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    /// The cluster id of the broker process.
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    /// The incarnation id of the broker process.
    #[kafka(versions = "0+")]
    pub incarnation_id: Uuid,
    /// The listeners of this broker.
    #[kafka(versions = "0+")]
    pub listeners: Vec<Listener>,
    /// The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted.
    #[kafka(versions = "0+")]
    pub features: Vec<Feature>,
    /// The rack which this broker is in.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub rack: Option<String>,
    /// If the required configurations for ZK migration are present, this value is set to true.
    #[kafka(versions = "1+", default = false)]
    pub is_migrating_zk_broker: bool,
    /// Log directories configured in this broker which are available.
    #[kafka(versions = "2+")]
    pub log_dirs: Vec<Uuid>,
    /// The epoch before a clean shutdown.
    #[kafka(versions = "3+", default = -1)]
    pub previous_broker_epoch: i64,
}
