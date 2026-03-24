//! Auto-generated from Kafka protocol
//! Message: AlterReplicaLogDirsRequest
//! DO NOT EDIT

use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use bytes::Bytes;
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterReplicaLogDirTopic {
    /// The topic name.
    #[kafka(versions = "0+", map_key)]
    pub name: String,
    /// The partition indexes.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct AlterReplicaLogDir {
    /// The absolute directory path.
    #[kafka(versions = "0+", map_key)]
    pub path: String,
    /// The topics to add to the directory.
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterReplicaLogDirTopic>,
}


#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(api_key = 34, msg_type = "request", valid_versions = "1-2", flexible_versions = "2+")]
pub struct AlterReplicaLogDirsRequest {
    /// The alterations to make for each directory.
    #[kafka(versions = "0+")]
    pub dirs: Vec<AlterReplicaLogDir>,
}

