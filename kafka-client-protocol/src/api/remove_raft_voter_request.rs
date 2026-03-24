//! Auto-generated from Kafka protocol
//! Message: RemoveRaftVoterRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 81, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct RemoveRaftVoterRequest {
    /// The cluster id of the request.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub cluster_id: Option<String>,
    /// The replica id of the voter getting removed from the topic partition.
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    /// The directory id of the voter getting removed from the topic partition.
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
}

