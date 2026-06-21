//! Auto-generated from Kafka protocol
//! Message: ConsumerProtocolAssignment
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub topic: String,
    /// The list of partitions assigned to this consumer.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0-3", flexible_versions = "none")]
pub struct ConsumerProtocolAssignment {
    /// The list of topics and partitions assigned to this consumer.
    #[kafka(versions = "0+")]
    pub assigned_partitions: Vec<TopicPartition>,
    /// User data.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub user_data: Option<Bytes>,
}
