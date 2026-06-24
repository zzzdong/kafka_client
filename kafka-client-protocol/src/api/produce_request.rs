//! Auto-generated from Kafka protocol
//! Message: ProduceRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionProduceData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub index: i32,
    /// The record data to be produced.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub records: Option<RecordBatch>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicProduceData {
    /// The topic name.
    #[kafka(versions = "0-12", map_key)]
    pub name: String,
    /// The unique topic ID
    #[kafka(versions = "13+", map_key)]
    pub topic_id: Uuid,
    /// Each partition to produce to.
    #[kafka(versions = "0+")]
    pub partition_data: Vec<PartitionProduceData>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 0,
    msg_type = "request",
    valid_versions = "3-13",
    flexible_versions = "9+"
)]
pub struct ProduceRequest {
    /// The transactional ID, or null if the producer is not transactional.
    #[kafka(versions = "3+", nullable_versions = "3+", default = None)]
    pub transactional_id: Option<String>,
    /// The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.
    #[kafka(versions = "0+")]
    pub acks: i16,
    /// The timeout to await a response in milliseconds.
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    /// Each topic to produce to.
    #[kafka(versions = "0+")]
    pub topic_data: Vec<TopicProduceData>,
}
