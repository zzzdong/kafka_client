//! Auto-generated from Kafka protocol
//! Message: DescribeProducersResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct ProducerState {
    /// The producer id.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The producer epoch.
    #[kafka(versions = "0+")]
    pub producer_epoch: i32,
    /// The last sequence number sent by the producer.
    #[kafka(versions = "0+", default = -1)]
    pub last_sequence: i32,
    /// The last timestamp sent by the producer.
    #[kafka(versions = "0+", default = -1)]
    pub last_timestamp: i64,
    /// The current epoch of the producer group.
    #[kafka(versions = "0+")]
    pub coordinator_epoch: i32,
    /// The current transaction start offset of the producer.
    #[kafka(versions = "0+", default = -1)]
    pub current_txn_start_offset: i64,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionResponse {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The partition error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The partition error message, which may be null if no additional details are available.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The active producers for the partition.
    #[kafka(versions = "0+")]
    pub active_producers: Vec<ProducerState>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicResponse {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// Each partition in the response.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionResponse>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 61, msg_type = "response", valid_versions = "0", flexible_versions = "0+")]
pub struct DescribeProducersResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// Each topic in the response.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicResponse>,
}

