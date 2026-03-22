//! DescribeShareGroupOffsets API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 90

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// DescribeShareGroupOffsetsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 90, valid_versions = "0-1", flexible_versions = "0+")]
pub struct DescribeShareGroupOffsetsRequest {
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup>,
}


/// DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestGroup {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic>,
}

/// DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeShareGroupOffsetsRequestDescribeShareGroupOffsetsRequestTopic {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// DescribeShareGroupOffsetsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 90, valid_versions = "0-1", flexible_versions = "0+")]
pub struct DescribeShareGroupOffsetsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup>,
}


/// DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseGroup {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic>,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}

/// DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponseTopic {
    #[kafka(versions = "0+")]
    pub topic_name: String,
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition>,
}

/// DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeShareGroupOffsetsResponseDescribeShareGroupOffsetsResponsePartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "1+")]
    pub lag: i64,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}
