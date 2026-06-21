//! Auto-generated from Kafka protocol
//! Message: OffsetFetchRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchRequestTopic {
    /// The topic name.
    #[kafka(versions = "0-7")]
    pub name: String,
    /// The partition indexes we would like to fetch offsets for.
    #[kafka(versions = "0-7")]
    pub partition_indexes: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchRequestTopics {
    /// The topic name.
    #[kafka(versions = "8-9", nullable_versions = "8-9")]
    pub name: Option<String>,
    /// The topic ID.
    #[kafka(versions = "10+", nullable_versions = "10+")]
    pub topic_id: Option<Uuid>,
    /// The partition indexes we would like to fetch offsets for.
    #[kafka(versions = "8+")]
    pub partition_indexes: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct OffsetFetchRequestGroup {
    /// The group ID.
    #[kafka(versions = "8+")]
    pub group_id: String,
    /// The member id.
    #[kafka(versions = "9+", nullable_versions = "9+", default = None)]
    pub member_id: Option<String>,
    /// The member epoch if using the new consumer protocol (KIP-848).
    #[kafka(versions = "9+", nullable_versions = "9+", default = -1)]
    pub member_epoch: i32,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    #[kafka(versions = "8+", nullable_versions = "8+")]
    pub topics: Option<Vec<OffsetFetchRequestTopics>>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 9,
    msg_type = "request",
    valid_versions = "1-10",
    flexible_versions = "6+"
)]
pub struct OffsetFetchRequest {
    /// The group to fetch offsets for.
    #[kafka(versions = "0-7")]
    pub group_id: String,
    /// Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.
    #[kafka(versions = "0-7", nullable_versions = "2-7")]
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// Each group we would like to fetch offsets for.
    #[kafka(versions = "8+")]
    pub groups: Vec<OffsetFetchRequestGroup>,
    /// Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.
    #[kafka(versions = "7+", default = false)]
    pub require_stable: bool,
}
