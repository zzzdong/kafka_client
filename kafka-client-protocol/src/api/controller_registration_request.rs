//! Auto-generated from Kafka protocol
//! Message: ControllerRegistrationRequest
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
    api_key = 70,
    msg_type = "request",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct ControllerRegistrationRequest {
    /// The ID of the controller to register.
    #[kafka(versions = "0+")]
    pub controller_id: i32,
    /// The controller incarnation ID, which is unique to each process run.
    #[kafka(versions = "0+")]
    pub incarnation_id: Uuid,
    /// Set if the required configurations for ZK migration are present.
    #[kafka(versions = "0+")]
    pub zk_migration_ready: bool,
    /// The listeners of this controller.
    #[kafka(versions = "0+")]
    pub listeners: Vec<Listener>,
    /// The features on this controller.
    #[kafka(versions = "0+")]
    pub features: Vec<Feature>,
}
