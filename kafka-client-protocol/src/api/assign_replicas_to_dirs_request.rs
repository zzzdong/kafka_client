//! Auto-generated from Kafka protocol
//! Message: AssignReplicasToDirsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct PartitionData {
    /// The partition index.
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicData {
    /// The ID of the assigned topic.
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    /// The partitions assigned to the directory.
    #[kafka(versions = "0+")]
    pub partitions: Vec<PartitionData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DirectoryData {
    /// The ID of the directory.
    #[kafka(versions = "0+")]
    pub id: Uuid,
    /// The topics assigned to the directory.
    #[kafka(versions = "0+")]
    pub topics: Vec<TopicData>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 73, msg_type = "request", valid_versions = "0", flexible_versions = "0+")]
pub struct AssignReplicasToDirsRequest {
    /// The ID of the requesting broker.
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    /// The epoch of the requesting broker.
    #[kafka(versions = "0+", default = -1)]
    pub broker_epoch: i64,
    /// The directories to which replicas should be assigned.
    #[kafka(versions = "0+")]
    pub directories: Vec<DirectoryData>,
}

