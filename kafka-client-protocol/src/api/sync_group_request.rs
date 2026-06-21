//! Auto-generated from Kafka protocol
//! Message: SyncGroupRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct SyncGroupRequestAssignment {
    /// The ID of the member to assign.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The member assignment.
    #[kafka(versions = "0+")]
    pub assignment: Bytes,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 14,
    msg_type = "request",
    valid_versions = "0-5",
    flexible_versions = "4+"
)]
pub struct SyncGroupRequest {
    /// The unique group identifier.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The generation of the group.
    #[kafka(versions = "0+")]
    pub generation_id: i32,
    /// The member ID assigned by the group.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "3+", nullable_versions = "3+", default = None)]
    pub group_instance_id: Option<String>,
    /// The group protocol type.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub protocol_type: Option<String>,
    /// The group protocol name.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub protocol_name: Option<String>,
    /// Each assignment.
    #[kafka(versions = "0+")]
    pub assignments: Vec<SyncGroupRequestAssignment>,
}
