//! ReadShareGroupStateSummary API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 87

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ReadShareGroupStateSummaryRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 87, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ReadShareGroupStateSummaryRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub topics: Vec<ReadShareGroupStateSummaryRequestReadStateSummaryData>,
}


/// ReadShareGroupStateSummaryRequestReadStateSummaryData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateSummaryRequestReadStateSummaryData {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ReadShareGroupStateSummaryRequestPartitionData>,
}

/// ReadShareGroupStateSummaryRequestPartitionData
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateSummaryRequestPartitionData {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
}
/// ReadShareGroupStateSummaryResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 87, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ReadShareGroupStateSummaryResponse {
    #[kafka(versions = "0+")]
    pub results: Vec<ReadShareGroupStateSummaryResponseReadStateSummaryResult>,
}


/// ReadShareGroupStateSummaryResponseReadStateSummaryResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateSummaryResponseReadStateSummaryResult {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<ReadShareGroupStateSummaryResponsePartitionResult>,
}

/// ReadShareGroupStateSummaryResponsePartitionResult
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ReadShareGroupStateSummaryResponsePartitionResult {
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub state_epoch: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub start_offset: i64,
    #[kafka(versions = "1+")]
    pub delivery_complete_count: i32,
}
