//! UpdateRaftVoter API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 82

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// UpdateRaftVoterRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 82, valid_versions = "0", flexible_versions = "0+")]
pub struct UpdateRaftVoterRequest {
    #[kafka(versions = "0+")]
    pub cluster_id: String,
    #[kafka(versions = "0+")]
    pub current_leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub voter_id: i32,
    #[kafka(versions = "0+")]
    pub voter_directory_id: Uuid,
    #[kafka(versions = "0+")]
    pub listeners: Vec<UpdateRaftVoterRequestListener>,
    #[kafka(versions = "0+")]
    pub kraft_version_feature: UpdateRaftVoterRequestKRaftVersionFeature,
}


/// UpdateRaftVoterRequestListener
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct UpdateRaftVoterRequestListener {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i16,
}

/// UpdateRaftVoterRequestKRaftVersionFeature
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct UpdateRaftVoterRequestKRaftVersionFeature {
    #[kafka(versions = "0+")]
    pub min_supported_version: i16,
    #[kafka(versions = "0+")]
    pub max_supported_version: i16,
}
/// UpdateRaftVoterResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 82, valid_versions = "0", flexible_versions = "0+")]
pub struct UpdateRaftVoterResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub current_leader: UpdateRaftVoterResponseCurrentLeader,
}


/// UpdateRaftVoterResponseCurrentLeader
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct UpdateRaftVoterResponseCurrentLeader {
    #[kafka(versions = "0+")]
    pub leader_id: i32,
    #[kafka(versions = "0+")]
    pub leader_epoch: i32,
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: i32,
}
