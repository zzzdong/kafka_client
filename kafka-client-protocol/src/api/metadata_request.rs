//! Auto-generated from Kafka protocol
//! Message: MetadataRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct MetadataRequestTopic {
    /// The topic id.
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    /// The topic name.
    #[kafka(versions = "0+", nullable_versions = "10+")]
    pub name: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 3,
    msg_type = "request",
    valid_versions = "0-13",
    flexible_versions = "9+"
)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for.
    #[kafka(versions = "0+", nullable_versions = "1+")]
    pub topics: Option<Vec<MetadataRequestTopic>>,
    /// If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.
    #[kafka(versions = "4+", default = true)]
    pub allow_auto_topic_creation: bool,
    /// Whether to include cluster authorized operations.
    #[kafka(versions = "8-10")]
    pub include_cluster_authorized_operations: bool,
    /// Whether to include topic authorized operations.
    #[kafka(versions = "8+")]
    pub include_topic_authorized_operations: bool,
}
