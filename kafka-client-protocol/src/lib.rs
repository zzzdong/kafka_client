//! Kafka 协议定义
//!
//! 包含 Kafka 协议的具体定义，由 codegen 生成，随 Kafka 版本更新

pub mod api;
pub mod version;

// 重新导出生成的 API 结构体
pub use api::*;

// 导出常用消息的内联子类型，避免外部模块引用私有子模块
pub use api::consumer_protocol_assignment::{ConsumerProtocolAssignment, TopicPartition};
pub use api::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
pub use api::fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData};
pub use api::find_coordinator_request::FindCoordinatorRequest;
pub use api::find_coordinator_response::FindCoordinatorResponse;
pub use api::heartbeat_request::HeartbeatRequest;
pub use api::heartbeat_response::HeartbeatResponse;
pub use api::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol};
pub use api::join_group_response::JoinGroupResponse;
pub use api::leave_group_request::{LeaveGroupRequest, MemberIdentity};
pub use api::leave_group_response::LeaveGroupResponse;
pub use api::list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic};
pub use api::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsResponse, ListOffsetsTopicResponse,
};
pub use api::metadata_request::{MetadataRequest, MetadataRequestTopic};
pub use api::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
pub use api::offset_commit_request::{
    OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
pub use api::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
pub use api::offset_fetch_request::{
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
pub use api::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
    OffsetFetchResponsePartitions, OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
pub use api::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData};
pub use api::produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse};
pub use api::sync_group_request::{SyncGroupRequest, SyncGroupRequestAssignment};
pub use api::sync_group_response::SyncGroupResponse;

// 重新导出 version
pub use version::{VersionRange, versions};

// 重新导出 core 中的核心类型（方便用户使用）
pub use kafka_client_protocol_core::{
    KafkaMessage, Message, ProtocolError, Record, RecordBatch, Request, Response,
};
