//! Auto-generated from Kafka protocol
//! Message: CreateTopicsRequest
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatableReplicaAssignment {
    /// The partition index.
    #[kafka(versions = "0+", map_key)]
    pub partition_index: i32,
    /// The brokers to place the partition on.
    #[kafka(versions = "0+")]
    pub broker_ids: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatableTopicConfig {
    /// The configuration name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The configuration value.
    #[kafka(versions = "0+", nullable_versions = "0+")]
    pub value: Option<String>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct CreatableTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.
    #[kafka(versions = "0+")]
    pub num_partitions: i32,
    /// The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.
    #[kafka(versions = "0+")]
    pub replication_factor: i16,
    /// The manual partition assignment, or the empty array if we are using automatic assignment.
    #[kafka(versions = "0+")]
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// The custom topic configurations to set.
    #[kafka(versions = "0+")]
    pub configs: Vec<CreatableTopicConfig>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 19,
    msg_type = "request",
    valid_versions = "2-7",
    flexible_versions = "5+"
)]
pub struct CreateTopicsRequest {
    /// The topics to create.
    #[kafka(versions = "0+")]
    pub topics: Vec<CreatableTopic>,
    /// How long to wait in milliseconds before timing out the request.
    #[kafka(versions = "0+", default = 60000)]
    pub timeout_ms: i32,
    /// If true, check that the topics can be created as specified, but don't create anything.
    #[kafka(versions = "1+", default = false)]
    pub validate_only: bool,
}
