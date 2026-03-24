//! Auto-generated from Kafka protocol
//! Message: LeaveGroupRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct MemberIdentity {
    /// The member ID to remove from the group.
    #[kafka(versions = "3+")]
    pub member_id: String,
    /// The group instance ID to remove from the group.
    #[kafka(versions = "3+", nullable_versions = "3+", default = None)]
    pub group_instance_id: Option<String>,
    /// The reason why the member left the group.
    #[kafka(versions = "5+", nullable_versions = "5+", default = None)]
    pub reason: Option<String>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 13, msg_type = "request", valid_versions = "0-5", flexible_versions = "4+")]
pub struct LeaveGroupRequest {
    /// The ID of the group to leave.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The member ID to remove from the group.
    #[kafka(versions = "0-2")]
    pub member_id: String,
    /// List of leaving member identities.
    #[kafka(versions = "3+")]
    pub members: Vec<MemberIdentity>,
}

