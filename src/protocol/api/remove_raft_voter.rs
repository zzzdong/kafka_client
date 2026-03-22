//! RemoveRaftVoter API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 81

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// RemoveRaftVoterRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 81, valid_versions = "0", flexible_versions = "0+")]
pub struct RemoveRaftVoterRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
}

/// RemoveRaftVoterResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 81, valid_versions = "0", flexible_versions = "0+")]
pub struct RemoveRaftVoterResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}

