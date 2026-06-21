//! Auto-generated from Kafka protocol
//! Message: ElectLeadersResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionResult {
    /// The partition id.
    #[kafka(versions = "0+")]
    pub partition_id: i32,
    /// The result error, or zero if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The result message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub error_message: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ReplicaElectionResult {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub topic: String,
    /// The results for each partition.
    #[kafka(versions = "0+")]
    pub partition_result: Vec<PartitionResult>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 43,
    msg_type = "response",
    valid_versions = "0-2",
    flexible_versions = "2+"
)]
pub struct ElectLeadersResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// The top level response error code.
    #[kafka(versions = "1+")]
    pub error_code: i16,
    /// The election results, or an empty array if the requester did not have permission and the request asks for all partitions.
    #[kafka(versions = "0+")]
    pub replica_election_results: Vec<ReplicaElectionResult>,
}
