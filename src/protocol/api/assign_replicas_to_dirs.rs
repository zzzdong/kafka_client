//! AssignReplicasToDirs API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 73

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// AssignReplicasToDirsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 73, valid_versions = "0", flexible_versions = "0+")]
pub struct AssignReplicasToDirsRequest {
    #[kafka(versions = "0+")]
    pub broker_id: i32,
    #[kafka(versions = "0+")]
    pub broker_epoch: i64,
    #[kafka(versions = "0+")]
    pub directories: Vec<AssignReplicasToDirsRequestDirectoryData>,
}


/// AssignReplicasToDirsRequestDirectoryData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AssignReplicasToDirsRequestDirectoryData {
    #[kafka(versions = "0+")]
    pub id: Uuid,
    #[kafka(versions = "0+")]
    pub topics: Vec<AssignReplicasToDirsRequestTopicData>,
}

/// AssignReplicasToDirsRequestTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AssignReplicasToDirsRequestTopicData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AssignReplicasToDirsRequestPartitionData>,
}

/// AssignReplicasToDirsRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AssignReplicasToDirsRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}
/// AssignReplicasToDirsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 73, valid_versions = "0", flexible_versions = "0+")]
pub struct AssignReplicasToDirsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub directories: Vec<AssignReplicasToDirsResponseDirectoryData>,
}


/// AssignReplicasToDirsResponseDirectoryData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AssignReplicasToDirsResponseDirectoryData {
    #[kafka(versions = "0+")]
    pub id: Uuid,
    #[kafka(versions = "0+")]
    pub topics: Vec<AssignReplicasToDirsResponseTopicData>,
}

/// AssignReplicasToDirsResponseTopicData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AssignReplicasToDirsResponseTopicData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AssignReplicasToDirsResponsePartitionData>,
}

/// AssignReplicasToDirsResponsePartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AssignReplicasToDirsResponsePartitionData {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
