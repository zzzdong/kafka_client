//! Auto-generated from Kafka protocol
//! Message: HeartbeatRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 12,
    msg_type = "request",
    valid_versions = "0-4",
    flexible_versions = "4+"
)]
pub struct HeartbeatRequest {
    /// The group id.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The generation of the group.
    #[kafka(versions = "0+")]
    pub generation_id: i32,
    /// The member ID.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "3+", nullable_versions = "3+", default = None)]
    pub group_instance_id: Option<String>,
}
