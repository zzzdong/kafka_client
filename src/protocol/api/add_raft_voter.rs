//! AddRaftVoter API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 80

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// AddRaftVoterRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 80, valid_versions = "0-1", flexible_versions = "0+")]
pub struct AddRaftVoterRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
    #[kafka(versions = "0+")]
    pub listeners: Vec<AddRaftVoterRequestListener>,
    #[kafka(versions = "1+")]
    pub ack_when_committed: bool,
}


/// AddRaftVoterRequestListener
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct AddRaftVoterRequestListener {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i16,
}
/// AddRaftVoterResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 80, valid_versions = "0-1", flexible_versions = "0+")]
pub struct AddRaftVoterResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
}

