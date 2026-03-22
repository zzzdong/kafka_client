//! AlterReplicaLogDirs API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 34

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// AlterReplicaLogDirsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 34, valid_versions = "1-2", flexible_versions = "2+")]
pub struct AlterReplicaLogDirsRequest {
    #[kafka(versions = "0+")]
    pub dirs: Vec<AlterReplicaLogDirsRequestAlterReplicaLogDir>,
}


/// AlterReplicaLogDirsRequestAlterReplicaLogDir
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterReplicaLogDirsRequestAlterReplicaLogDir {
    #[kafka(versions = "0+")]
    pub path: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterReplicaLogDirsRequestAlterReplicaLogDirTopic>,
}

/// AlterReplicaLogDirsRequestAlterReplicaLogDirTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterReplicaLogDirsRequestAlterReplicaLogDirTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// AlterReplicaLogDirsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 34, valid_versions = "1-2", flexible_versions = "2+")]
pub struct AlterReplicaLogDirsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub results: Vec<AlterReplicaLogDirsResponseAlterReplicaLogDirTopicResult>,
}


/// AlterReplicaLogDirsResponseAlterReplicaLogDirTopicResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterReplicaLogDirsResponseAlterReplicaLogDirTopicResult {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterReplicaLogDirsResponseAlterReplicaLogDirPartitionResult>,
}

/// AlterReplicaLogDirsResponseAlterReplicaLogDirPartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterReplicaLogDirsResponseAlterReplicaLogDirPartitionResult {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
