//! ConsumerGroupHeartbeat API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 68

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
    Uuid,
};

/// ConsumerGroupHeartbeatRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 68, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ConsumerGroupHeartbeatRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    #[kafka(versions = "0+")]
    pub instance_id: String,
    #[kafka(versions = "0+")]
    pub rack_id: String,
    #[kafka(versions = "0+")]
    pub rebalance_timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub subscribed_topic_names: Vec<String>,
    #[kafka(versions = "1+")]
    pub subscribed_topic_regex: String,
    #[kafka(versions = "0+")]
    pub server_assignor: String,
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<ConsumerGroupHeartbeatRequestTopicPartitions>,
}


/// ConsumerGroupHeartbeatRequestTopicPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupHeartbeatRequestTopicPartitions {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// ConsumerGroupHeartbeatResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 68, valid_versions = "0-1", flexible_versions = "0+")]
pub struct ConsumerGroupHeartbeatResponse {
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
    pub assignment: ConsumerGroupHeartbeatResponseAssignment,
}


/// ConsumerGroupHeartbeatResponseAssignment
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupHeartbeatResponseAssignment {
    #[kafka(versions = "0+")]
    pub topic_partitions: Vec<TopicPartitions>,
}

/// ConsumerGroupHeartbeatResponseTopicPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct ConsumerGroupHeartbeatResponseTopicPartitions {
    #[kafka(versions = "0+")]
    pub topic_id: Uuid,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
