//! ShareGroupHeartbeat API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 76

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// ShareGroupHeartbeatRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 76, valid_versions = "1", flexible_versions = "0+")]
pub struct ShareGroupHeartbeatRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    #[kafka(versions = "0+")]
    pub rack_id: String,
    #[kafka(versions = "0+")]
    pub subscribed_topic_names: Vec<String>,
}

/// ShareGroupHeartbeatResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 76, valid_versions = "1", flexible_versions = "0+")]
pub struct ShareGroupHeartbeatResponse {
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    #[kafka(versions = "0+")]
    pub error_code: i16,
    #[kafka(versions = "0+")]
    pub error_message: String,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    #[kafka(versions = "0+")]
    pub heartbeat_interval_ms: i32,
    #[kafka(versions = "0+")]
    pub assignment: ShareGroupHeartbeatResponseAssignment,
}


/// ShareGroupHeartbeatResponseAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareGroupHeartbeatResponseAssignment {
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<TopicPartitions>,
}

/// ShareGroupHeartbeatResponseTopicPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ShareGroupHeartbeatResponseTopicPartitions {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
