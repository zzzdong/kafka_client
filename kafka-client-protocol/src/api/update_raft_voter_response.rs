//! Auto-generated from Kafka protocol
//! Message: UpdateRaftVoterResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CurrentLeader {
    /// The replica id of the current leader or -1 if the leader is unknown.
    #[kafka(versions = "0+", default = -1)]
    pub leader_id: i32,
    /// The latest known leader epoch.
    #[kafka(versions = "0+", default = -1)]
    pub leader_epoch: i32,
    /// The node's hostname.
    #[kafka(versions = "0+")]
    pub host: String,
    /// The node's port.
    #[kafka(versions = "0+")]
    pub port: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 82,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct UpdateRaftVoterResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// Details of the current Raft cluster leader.
    #[kafka(versions = "0+", tag = 0, tagged_versions = "0+")]
    pub current_leader: CurrentLeader,
}
