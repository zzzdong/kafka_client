//! Auto-generated from Kafka protocol
//! Message: JoinGroupResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct JoinGroupResponseMember {
    /// The group member ID.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub group_instance_id: Option<String>,
    /// The group member metadata.
    #[kafka(versions = "0+")]
    pub metadata: Bytes,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 11, msg_type = "response", valid_versions = "0-9", flexible_versions = "6+")]
pub struct JoinGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "2+", nullable_versions = "2+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The generation ID of the group.
    #[kafka(versions = "0+", default = -1)]
    pub generation_id: i32,
    /// The group protocol name.
    #[kafka(versions = "7+", nullable_versions = "7+", default = None)]
    pub protocol_type: Option<String>,
    /// The group protocol selected by the coordinator.
    #[kafka(versions = "0+", nullable_versions = "7+")]
    pub protocol_name: Option<String>,
    /// The leader of the group.
    #[kafka(versions = "0+")]
    pub leader: String,
    /// True if the leader must skip running the assignment.
    #[kafka(versions = "9+", default = false)]
    pub skip_assignment: bool,
    /// The member ID assigned by the group coordinator.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The group members.
    #[kafka(versions = "0+")]
    pub members: Vec<JoinGroupResponseMember>,
}

