//! DescribeLogDirs API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 35

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// DescribeLogDirsRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 35, valid_versions = "1-4", flexible_versions = "2+")]
pub struct DescribeLogDirsRequest {
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeLogDirsRequestDescribableLogDirTopic>,
}


/// DescribeLogDirsRequestDescribableLogDirTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeLogDirsRequestDescribableLogDirTopic {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// DescribeLogDirsResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 35, valid_versions = "1-4", flexible_versions = "2+")]
pub struct DescribeLogDirsResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "3+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub results: Vec<DescribeLogDirsResponseDescribeLogDirsResult>,
}


/// DescribeLogDirsResponseDescribeLogDirsResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeLogDirsResponseDescribeLogDirsResult {
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub log_dir: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<DescribeLogDirsResponseDescribeLogDirsTopic>,
    #[kafka(versions = "4+")]
    pub total_bytes: i64,
    #[kafka(versions = "4+")]
    pub usable_bytes: i64,
}

/// DescribeLogDirsResponseDescribeLogDirsTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeLogDirsResponseDescribeLogDirsTopic {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<DescribeLogDirsResponseDescribeLogDirsPartition>,
}

/// DescribeLogDirsResponseDescribeLogDirsPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct DescribeLogDirsResponseDescribeLogDirsPartition {
    #[kafka(versions = "0+")]
    pub partition_index: i32,
    #[kafka(versions = "0+")]
    pub partition_size: i64,
    #[kafka(versions = "0+")]
    pub offset_lag: i64,
    #[kafka(versions = "0+")]
    pub is_future_key: bool,
}
