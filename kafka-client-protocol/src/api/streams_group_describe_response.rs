//! Auto-generated from Kafka protocol
//! Message: StreamsGroupDescribeResponse
//! DO NOT EDIT

use bytes::Bytes;
use kafka_client_protocol_core::{KafkaMessage, RecordBatch};
use uuid::Uuid;

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Endpoint {
    /// host of the endpoint
    #[kafka(versions = "0+")]
    pub host: String,
    /// port of the endpoint
    #[kafka(versions = "0+")]
    pub port: u16,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TaskOffset {
    /// The subtopology identifier.
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    /// The partition.
    #[kafka(versions = "0+")]
    pub partition: i32,
    /// The offset.
    #[kafka(versions = "0+")]
    pub offset: i64,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Assignment {
    /// Active tasks for this client.
    #[kafka(versions = "0+")]
    pub active_tasks: Vec<TaskIds>,
    /// Standby tasks for this client.
    #[kafka(versions = "0+")]
    pub standby_tasks: Vec<TaskIds>,
    /// Warm-up tasks for this client.
    #[kafka(versions = "0+")]
    pub warmup_tasks: Vec<TaskIds>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TaskIds {
    /// The subtopology identifier.
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    /// The partitions of the input topics processed by this member.
    #[kafka(versions = "0+")]
    pub partitions: Vec<i32>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct KeyValue {
    /// key of the config
    #[kafka(versions = "0+")]
    pub key: String,
    /// value of the config
    #[kafka(versions = "0+")]
    pub value: String,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct TopicInfo {
    /// The name of the topic.
    #[kafka(versions = "0+")]
    pub name: String,
    /// The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
    #[kafka(versions = "0+")]
    pub partitions: i32,
    /// The replication factor of the topic. Can be 0 if the default replication factor should be used.
    #[kafka(versions = "0+")]
    pub replication_factor: i16,
    /// Topic-level configurations as key-value pairs.
    #[kafka(versions = "0+")]
    pub topic_configs: Vec<KeyValue>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Subtopology {
    /// String to uniquely identify the subtopology.
    #[kafka(versions = "0+")]
    pub subtopology_id: String,
    /// The topics the subtopology reads from.
    #[kafka(versions = "0+")]
    pub source_topics: Vec<String>,
    /// The repartition topics the subtopology writes to.
    #[kafka(versions = "0+")]
    pub repartition_sink_topics: Vec<String>,
    /// The set of state changelog topics associated with this subtopology. Created automatically.
    #[kafka(versions = "0+")]
    pub state_changelog_topics: Vec<TopicInfo>,
    /// The set of source topics that are internally created repartition topics. Created automatically.
    #[kafka(versions = "0+")]
    pub repartition_source_topics: Vec<TopicInfo>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Topology {
    /// The epoch of the currently initialized topology for this group.
    #[kafka(versions = "0+")]
    pub epoch: i32,
    /// The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub subtopologies: Option<Vec<Subtopology>>,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct Member {
    /// The member ID.
    #[kafka(versions = "0+")]
    pub member_id: String,
    /// The member epoch.
    #[kafka(versions = "0+")]
    pub member_epoch: i32,
    /// The member instance ID for static membership.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub instance_id: Option<String>,
    /// The rack ID.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub rack_id: Option<String>,
    /// The client ID.
    #[kafka(versions = "0+")]
    pub client_id: String,
    /// The client host.
    #[kafka(versions = "0+")]
    pub client_host: String,
    /// The epoch of the topology on the client.
    #[kafka(versions = "0+")]
    pub topology_epoch: i32,
    /// Identity of the streams instance that may have multiple clients.
    #[kafka(versions = "0+")]
    pub process_id: String,
    /// User-defined endpoint for Interactive Queries. Null if not defined for this client.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub user_endpoint: Option<Endpoint>,
    /// Used for rack-aware assignment algorithm.
    #[kafka(versions = "0+")]
    pub client_tags: Vec<KeyValue>,
    /// Cumulative changelog offsets for tasks.
    #[kafka(versions = "0+")]
    pub task_offsets: Vec<TaskOffset>,
    /// Cumulative changelog end offsets for tasks.
    #[kafka(versions = "0+")]
    pub task_end_offsets: Vec<TaskOffset>,
    /// The current assignment.
    #[kafka(versions = "0+")]
    pub assignment: Assignment,
    /// The target assignment.
    #[kafka(versions = "0+")]
    pub target_assignment: Assignment,
    /// True for classic members that have not been upgraded yet.
    #[kafka(versions = "0+")]
    pub is_classic: bool,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    #[kafka(versions = "0+")]
    pub error_code: i16,
    /// The top-level error message, or null if there was no error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub error_message: Option<String>,
    /// The group ID string.
    #[kafka(versions = "0+")]
    pub group_id: String,
    /// The group state string, or the empty string.
    #[kafka(versions = "0+")]
    pub group_state: String,
    /// The group epoch.
    #[kafka(versions = "0+")]
    pub group_epoch: i32,
    /// The assignment epoch.
    #[kafka(versions = "0+")]
    pub assignment_epoch: i32,
    /// The topology metadata currently initialized for the streams application. Can be null in case of a describe error.
    #[kafka(versions = "0+", nullable_versions = "0+", default = None)]
    pub topology: Option<Topology>,
    /// The members.
    #[kafka(versions = "0+")]
    pub members: Vec<Member>,
    /// 32-bit bitfield to represent authorized operations for this group.
    #[kafka(versions = "0+", default = -2147483648)]
    pub authorized_operations: i32,
}

#[derive(KafkaMessage, Debug, Clone, Default, PartialEq)]
#[kafka(
    api_key = 89,
    msg_type = "response",
    valid_versions = "0",
    flexible_versions = "0+"
)]
pub struct StreamsGroupDescribeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    #[kafka(versions = "0+")]
    pub throttle_time_ms: i32,
    /// Each described group.
    #[kafka(versions = "0+")]
    pub groups: Vec<DescribedGroup>,
}
