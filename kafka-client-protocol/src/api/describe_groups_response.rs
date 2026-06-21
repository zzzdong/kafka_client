//! Auto-generated from Kafka protocol
//! Message: DescribeGroupsResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribedGroupMember {
    /// The member id.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The unique identifier of the consumer instance provided by end user.
    #[kafka(versions = "4+", nullable_versions = "4+", default = None)]
    pub group_instance_id: Option<String>,
    /// The client ID used in the member's latest join group request.
    #[kafka(versions = "0+")]
    pub client_id: String,
    /// The client host.
    #[kafka(versions = "0+")]
    pub client_host: String,
    /// The metadata corresponding to the current group protocol in use.
    #[kafka(versions = "0+")]
    pub member_metadata: Bytes,
    /// The current assignment provided by the group leader.
    #[kafka(versions = "0+")]
    pub member_assignment: Bytes,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The describe error message, or null if there was no error.
    #[kafka(versions = "6+", nullable_versions = "6+", default = None)]
    pub error_message: Option<String>,
    /// The group ID string.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The group state string, or the empty string.
    #[kafka(versions = "0+")]
    pub group_state: String,
    /// The group protocol type, or the empty string.
    #[kafka(versions = "0+")]
    pub protocol_type: String,
    /// The group protocol data, or the empty string.
    #[kafka(versions = "0+")]
    pub protocol_data: String,
    /// The group members.
    #[kafka(versions = "0+")]
    pub members: Vec<DescribedGroupMember>,
    /// 32-bit bitfield to represent authorized operations for this group.
    #[kafka(versions = "3+", default = -2147483648)]
    pub authorized_operations: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 15,
    msg_type = "response",
    valid_versions = "0-6",
    flexible_versions = "5+"
)]
pub struct DescribeGroupsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub throttle_time_ms: i32,
    /// Each described group.
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribedGroup>,
}
