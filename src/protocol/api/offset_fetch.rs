//! OffsetFetch API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 9

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// OffsetFetchRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 9, valid_versions = "1-10", flexible_versions = "6+")]
pub struct OffsetFetchRequest {
    #[kafka(versions = "0-7")]
    pub group_id: String,
    #[kafka(versions = "0-7")]
    pub topics: Vec<OffsetFetchRequestOffsetFetchRequestTopic>,
    #[kafka(versions = "8+")]
    pub groups: Vec<OffsetFetchRequestOffsetFetchRequestGroup>,
    #[kafka(versions = "7+")]
    pub require_stable: bool,
}


/// OffsetFetchRequestOffsetFetchRequestTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchRequestOffsetFetchRequestTopic {
    #[kafka(versions = "0-7")]
    pub name: String,
    #[kafka(versions = "0-7")]
    pub partition_indexes: Vec<i32>,
}

/// OffsetFetchRequestOffsetFetchRequestGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchRequestOffsetFetchRequestGroup {
    #[kafka(versions = "8+")]
    pub group_id: String,
    #[kafka(versions = "9+")]
    pub member_id: String,
    #[kafka(versions = "9+")]
    pub member_epoch: i32,
    #[kafka(versions = "8+")]
    pub topics: Vec<OffsetFetchRequestOffsetFetchRequestTopics>,
}

/// OffsetFetchRequestOffsetFetchRequestTopics
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchRequestOffsetFetchRequestTopics {
    #[kafka(versions = "8-9")]
    pub name: String,
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    #[kafka(versions = "8+")]
    pub partition_indexes: Vec<i32>,
}
/// OffsetFetchResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 9, valid_versions = "1-10", flexible_versions = "6+")]
pub struct OffsetFetchResponse {
    #[kafka(versions = "3+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0-7")]
    pub topics: Vec<OffsetFetchResponseOffsetFetchResponseTopic>,
    #[kafka(versions = "2-7")]
    pub error_code: i16,
    #[kafka(versions = "8+")]
    pub groups: Vec<OffsetFetchResponseOffsetFetchResponseGroup>,
}


/// OffsetFetchResponseOffsetFetchResponseTopic
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchResponseOffsetFetchResponseTopic {
    #[kafka(versions = "0-7")]
    pub name: String,
    #[kafka(versions = "0-7")]
    pub partitions: Vec<OffsetFetchResponseOffsetFetchResponsePartition>,
}

/// OffsetFetchResponseOffsetFetchResponsePartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchResponseOffsetFetchResponsePartition {
    #[kafka(versions = "0-7")]
    pub partition_index: i32,
    #[kafka(versions = "0-7")]
    pub committed_offset: i64,
    #[kafka(versions = "5-7")]
    pub committed_leader_epoch: i32,
    #[kafka(versions = "0-7")]
    pub metadata: String,
    #[kafka(versions = "0-7")]
    pub error_code: i16,
}

/// OffsetFetchResponseOffsetFetchResponseGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchResponseOffsetFetchResponseGroup {
    #[kafka(versions = "8+")]
    pub group_id: String,
    #[kafka(versions = "8+")]
    pub topics: Vec<OffsetFetchResponseOffsetFetchResponseTopics>,
    #[kafka(versions = "8+")]
    pub error_code: i16,
}

/// OffsetFetchResponseOffsetFetchResponseTopics
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchResponseOffsetFetchResponseTopics {
    #[kafka(versions = "8-9")]
    pub name: String,
    #[kafka(versions = "10+")]
    pub topic_id: Uuid,
    #[kafka(versions = "8+")]
    pub partitions: Vec<OffsetFetchResponseOffsetFetchResponsePartitions>,
}

/// OffsetFetchResponseOffsetFetchResponsePartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct OffsetFetchResponseOffsetFetchResponsePartitions {
    #[kafka(versions = "8+")]
    pub partition_index: i32,
    #[kafka(versions = "8+")]
    pub committed_offset: i64,
    #[kafka(versions = "8+")]
    pub committed_leader_epoch: i32,
    #[kafka(versions = "8+")]
    pub metadata: String,
    #[kafka(versions = "8+")]
    pub error_code: i16,
}
