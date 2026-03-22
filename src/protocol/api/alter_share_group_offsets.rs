//! AlterShareGroupOffsets API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 91

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// AlterShareGroupOffsetsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 91, valid_versions = "0", flexible_versions = "0+")]
pub struct AlterShareGroupOffsetsRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<AlterShareGroupOffsetsRequestAlterShareGroupOffsetsRequestTopic>,
}


/// AlterShareGroupOffsetsRequestAlterShareGroupOffsetsRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterShareGroupOffsetsRequestAlterShareGroupOffsetsRequestTopic {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterShareGroupOffsetsRequestAlterShareGroupOffsetsRequestPartition>,
}

/// AlterShareGroupOffsetsRequestAlterShareGroupOffsetsRequestPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterShareGroupOffsetsRequestAlterShareGroupOffsetsRequestPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub start_offset: i64,
}
/// AlterShareGroupOffsetsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 91, valid_versions = "0", flexible_versions = "0+")]
pub struct AlterShareGroupOffsetsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub responses: Vec<AlterShareGroupOffsetsResponseAlterShareGroupOffsetsResponseTopic>,
}


/// AlterShareGroupOffsetsResponseAlterShareGroupOffsetsResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterShareGroupOffsetsResponseAlterShareGroupOffsetsResponseTopic {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<AlterShareGroupOffsetsResponseAlterShareGroupOffsetsResponsePartition>,
}

/// AlterShareGroupOffsetsResponseAlterShareGroupOffsetsResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AlterShareGroupOffsetsResponseAlterShareGroupOffsetsResponsePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
