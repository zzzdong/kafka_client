//! Auto-generated from Kafka protocol
//! Message: DeleteRecordsResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteRecordsPartitionResult {
    /// The partition index.
    #[kafka(versions = "0+", map_key)]
    pub partition_index: i32,
    /// The partition low water mark.
    #[kafka(versions = "0+")]
    pub low_watermark: i64,
    /// The deletion error code, or 0 if the deletion succeeded.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DeleteRecordsTopicResult {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// Each partition that we wanted to delete records from.
    #[kafka(versions = "0+")]
    pub partitions: Vec<DeleteRecordsPartitionResult>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 21, msg_type = "response", valid_versions = "0-2", flexible_versions = "2+")]
pub struct DeleteRecordsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// Each topic that we wanted to delete records from.
    #[kafka(versions = "0+")]
    pub topics: Vec<DeleteRecordsTopicResult>,
}

