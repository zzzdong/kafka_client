//! Auto-generated from Kafka protocol
//! Message: ConsumerProtocolSubscription
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicPartition {
    /// The topic name.
    #[kafka(versions = "1+", map_key)]
    pub topic: String,
    /// The partition ids.
    #[kafka(versions = "1+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(msg_type = "data", valid_versions = "0-3", flexible_versions = "none")]
pub struct ConsumerProtocolSubscription {
    /// The topics that the member wants to consume.
    #[kafka(versions = "0+")]
    pub topics: Vec<String>,
    /// User data that will be passed back to the consumer.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub user_data: Option<Bytes>,
    /// The partitions that the member owns.
    #[kafka(versions = "1+", nullable_versions = "1+")]
    pub owned_partitions: Option<Vec<TopicPartition>>,
    /// The generation id of the member.
    #[kafka(versions = "2+", nullable_versions = "2+", default = -1)]
    pub generation_id: i32,
    /// The rack id of the member.
    #[kafka(versions = "3+", nullable_versions = "3+", default = None)]
    pub rack_id: Option<String>,
}

