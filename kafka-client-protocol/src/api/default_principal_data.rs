//! Auto-generated from Kafka protocol
//! Message: DefaultPrincipalData
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0", flexible_versions = "0+")]
pub struct DefaultPrincipalData {
    /// The principal type.
    #[kafka(versions = "0+")]
    pub r#type: String,
    /// The principal name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// Whether the principal was authenticated by a delegation token on the forwarding broker.
    #[kafka(versions = "0+")]
    pub token_authenticated: bool,
}

