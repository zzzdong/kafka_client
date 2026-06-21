//! Auto-generated from Kafka protocol
//! Message: ElectLeadersRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicPartitions {
    /// The name of a topic.
    #[kafka(versions = "0+", map_key)]
    pub topic: String,
    /// The partitions of this topic whose leader should be elected.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 43,
    msg_type = "request",
    valid_versions = "0-2",
    flexible_versions = "2+"
)]
pub struct ElectLeadersRequest {
    /// Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica.
    #[kafka(versions = "1+")]
    pub election_type: i8,
    /// The topic partitions to elect leaders.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub topic_partitions: Option<Vec<TopicPartitions>>,
    /// The time in ms to wait for the election to complete.
    #[kafka(versions = "0+", default = 60000)]
    pub timeout_ms: i32,
}
