//! Auto-generated from Kafka protocol
//! Message: LeaveGroupResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct MemberResponse {
    /// The member ID to remove from the group.
    #[kafka(versions = "3+")]
    pub member_id: String,
    /// The group instance ID to remove from the group.
    #[kafka(versions = "3+", nullable_versions = "3+")]
    pub group_instance_id: Option<String>,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "3+")]
    pub error_code: i16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 13,
    msg_type = "response",
    valid_versions = "0-5",
    flexible_versions = "4+"
)]
pub struct LeaveGroupResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// List of leaving member responses.
    #[kafka(versions = "3+")]
    pub members: Vec<MemberResponse>,
}
