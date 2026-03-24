//! Auto-generated from Kafka protocol
//! Message: WriteTxnMarkersResponse
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct WritableTxnMarkerPartitionResult {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    /// The error code, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct WritableTxnMarkerTopicResult {
    /// The topic name.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The results by partition.
    #[kafka(versions = "0+")]
    pub partitions: Vec<WritableTxnMarkerPartitionResult>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct WritableTxnMarkerResult {
    /// The current producer ID in use by the transactional ID.
    #[kafka(versions = "0+")]
    pub producer_id: i64,
    /// The results by topic.
    #[kafka(versions = "0+")]
    pub topics: Vec<WritableTxnMarkerTopicResult>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 27, msg_type = "response", valid_versions = "1-2", flexible_versions = "1+")]
pub struct WriteTxnMarkersResponse {
    /// The results for writing makers.
    #[kafka(versions = "0+")]
    pub markers: Vec<WritableTxnMarkerResult>,
}

