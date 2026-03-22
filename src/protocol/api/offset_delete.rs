//! OffsetDelete API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 47

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// OffsetDeleteRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 47, valid_versions = "0", flexible_versions = "none")]
pub struct OffsetDeleteRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetDeleteRequestOffsetDeleteRequestTopic>,
}


/// OffsetDeleteRequestOffsetDeleteRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetDeleteRequestOffsetDeleteRequestTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetDeleteRequestOffsetDeleteRequestPartition>,
}

/// OffsetDeleteRequestOffsetDeleteRequestPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetDeleteRequestOffsetDeleteRequestPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
}
/// OffsetDeleteResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 47, valid_versions = "0", flexible_versions = "none")]
pub struct OffsetDeleteResponse {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub topics: Vec<OffsetDeleteResponseOffsetDeleteResponseTopic>,
}


/// OffsetDeleteResponseOffsetDeleteResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetDeleteResponseOffsetDeleteResponseTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<OffsetDeleteResponseOffsetDeleteResponsePartition>,
}

/// OffsetDeleteResponseOffsetDeleteResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetDeleteResponseOffsetDeleteResponsePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
}
