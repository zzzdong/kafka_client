//! StreamsGroupHeartbeat API
//!
//! 使用 derive 宏自动生成
//!
//! API Key: 88

use bytes::{Bytes, BytesMut};
use crate::protocol::{
    Message, RequestMessage, ResponseMessage, ProtocolResult,
    KafkaMessage, KafkaRequest, KafkaResponse,
    RequestHeader,
};

/// StreamsGroupHeartbeatRequest
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaRequest)]
#[kafka(api_key = 88, valid_versions = "0", flexible_versions = "0+")]
pub struct StreamsGroupHeartbeatRequest {
    #[kafka(versions = "0+")]
    pub group_id: String,
    #[kafka(versions = "0+")]
    pub member_id: String,
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    #[kafka(versions = "0+")]
    pub endpoint_information_epoch: i32,
    #[kafka(versions = "0+")]
    pub instance_id: String,
    #[kafka(versions = "0+")]
    pub rack_id: String,
    #[kafka(versions = "0+")]
    pub rebalance_timeout_ms: i32,
    #[kafka(versions = "0+")]
    pub topology: StreamsGroupHeartbeatRequestTopology,
    #[kafka(versions = "0+")]
    pub active_tasks: Vec<TaskIds>,
    #[kafka(versions = "0+")]
    pub standby_tasks: Vec<TaskIds>,
    #[kafka(versions = "0+")]
    pub warmup_tasks: Vec<TaskIds>,
    #[kafka(versions = "0+")]
    pub process_id: String,
    #[kafka(versions = "0+")]
    pub user_endpoint: Endpoint,
    #[kafka(versions = "0+")]
    pub client_tags: Vec<KeyValue>,
    #[kafka(versions = "0+")]
    pub task_offsets: Vec<TaskOffset>,
    #[kafka(versions = "0+")]
    pub task_end_offsets: Vec<TaskOffset>,
    #[kafka(versions = "0+")]
    pub shutdown_application: bool,
}


/// StreamsGroupHeartbeatRequestTopology
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestTopology {
    #[kafka(versions = "0+")]
    pub epoch: i32,
    #[kafka(versions = "0+")]
    pub subtopologies: Vec<StreamsGroupHeartbeatRequestSubtopology>,
}

/// StreamsGroupHeartbeatRequestSubtopology
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestSubtopology {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub source_topics: Vec<String>,
    #[kafka(versions = "0+")]
    pub source_topic_regex: Vec<String>,
    #[kafka(versions = "0+")]
    pub state_changelog_topics: Vec<TopicInfo>,
    #[kafka(versions = "0+")]
    pub repartition_sink_topics: Vec<String>,
    #[kafka(versions = "0+")]
    pub repartition_source_topics: Vec<TopicInfo>,
    #[kafka(versions = "0+")]
    pub copartition_groups: Vec<StreamsGroupHeartbeatRequestCopartitionGroup>,
}

/// StreamsGroupHeartbeatRequestCopartitionGroup
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestCopartitionGroup {
    #[kafka(versions = "0+")]
    pub source_topics: Vec<i16>,
    #[kafka(versions = "0+")]
    pub source_topic_regex: Vec<i16>,
    #[kafka(versions = "0+")]
    pub repartition_source_topics: Vec<i16>,
}

/// StreamsGroupHeartbeatRequestKeyValue
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestKeyValue {
    #[kafka(versions = "0+")]
    pub key: String,
    #[kafka(versions = "0+")]
    pub value: String,
}

/// StreamsGroupHeartbeatRequestTopicInfo
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestTopicInfo {
    #[kafka(versions = "0+")]
    pub name: String,
    #[kafka(versions = "0+")]
    pub partitions: i32,
    #[kafka(versions = "0+")]
    pub replication_factor: i16,
    #[kafka(versions = "0+")]
    pub topic_configs: Vec<StreamsGroupHeartbeatRequestKeyValue>,
}

/// StreamsGroupHeartbeatRequestEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestEndpoint {
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: StreamsGroupHeartbeatRequestuint16,
}

/// StreamsGroupHeartbeatRequestTaskOffset
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestTaskOffset {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub partition: i32,
    #[kafka(versions = "0+")]
    pub offset: i64,
}

/// StreamsGroupHeartbeatRequestTaskIds
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatRequestTaskIds {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}
/// StreamsGroupHeartbeatResponse
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage, KafkaResponse)]
#[kafka(api_key = 88, valid_versions = "0", flexible_versions = "0+")]
pub struct StreamsGroupHeartbeatResponse {
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
    pub acceptable_recovery_lag: i32,
    #[kafka(versions = "0+")]
    pub task_offset_interval_ms: i32,
    #[kafka(versions = "0+")]
    pub status: Vec<Status>,
    #[kafka(versions = "0+")]
    pub active_tasks: Vec<TaskIds>,
    #[kafka(versions = "0+")]
    pub standby_tasks: Vec<TaskIds>,
    #[kafka(versions = "0+")]
    pub warmup_tasks: Vec<TaskIds>,
    #[kafka(versions = "0+")]
    pub endpoint_information_epoch: i32,
    #[kafka(versions = "0+")]
    pub partitions_by_user_endpoint: Vec<StreamsGroupHeartbeatResponseEndpointToPartitions>,
}


/// StreamsGroupHeartbeatResponseEndpointToPartitions
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatResponseEndpointToPartitions {
    #[kafka(versions = "0+")]
    pub user_endpoint: Endpoint,
    #[kafka(versions = "0+")]
    pub active_partitions: Vec<TopicPartition>,
    #[kafka(versions = "0+")]
    pub standby_partitions: Vec<TopicPartition>,
}

/// StreamsGroupHeartbeatResponseStatus
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatResponseStatus {
    #[kafka(versions = "0+")]
    pub status_code: i8,
    #[kafka(versions = "0+")]
    pub status_detail: String,
}

/// StreamsGroupHeartbeatResponseTopicPartition
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatResponseTopicPartition {
    #[kafka(versions = "0+")]
    pub topic: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

/// StreamsGroupHeartbeatResponseTaskIds
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatResponseTaskIds {
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

/// StreamsGroupHeartbeatResponseEndpoint
#[derive(Debug, Clone, Default, PartialEq, KafkaMessage)]
pub struct StreamsGroupHeartbeatResponseEndpoint {
    #[kafka(versions = "0+")]
    pub host: String,
    #[kafka(versions = "0+")]
    pub port: StreamsGroupHeartbeatResponseuint16,
}
