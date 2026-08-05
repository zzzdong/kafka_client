//! Admin client — cluster management and inspection.
//!
//! Provides a high-level API for administrative operations against a
//! Kafka cluster. Created via [`Client::admin()`](crate::Client::admin).
//!
//! # Example
//!
//! ```ignore
//! use kafka_client::{Client, admin::NewTopic};
//!
//! let client = Client::builder(vec!["localhost:9092".to_string()])
//!     .build().await?;
//! let admin = client.admin();
//!
//! // Create a topic
//! admin.create_topic(&NewTopic::new("orders", 3, 3)).await?;
//!
//! // List all topics
//! let topics = admin.list_topics().await?;
//! for t in &topics { println!("{}", t.name); }
//!
//! // Describe the cluster
//! let cluster = admin.describe_cluster().await?;
//! println!("{} brokers, controller: {:?}", cluster.brokers.len(), cluster.controller_id);
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::net;

use crate::cluster::ClusterClient;
use crate::error::{KafkaError, KafkaErrorCode, Result};

use crate::protocol::alter_configs_request::{
    AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
};
use crate::protocol::alter_configs_response::AlterConfigsResponse;
use crate::protocol::create_acls_request::{AclCreation, CreateAclsRequest};
use crate::protocol::create_acls_response::CreateAclsResponse;
use crate::protocol::delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest};
use crate::protocol::delete_acls_response::DeleteAclsResponse;
use crate::protocol::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use crate::protocol::delete_records_response::DeleteRecordsResponse;
use crate::protocol::describe_acls_request::DescribeAclsRequest;
use crate::protocol::describe_acls_response::DescribeAclsResponse;
use crate::protocol::describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource};
use crate::protocol::describe_configs_response::DescribeConfigsResponse;
use crate::protocol::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteGroupsRequest, DeleteGroupsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, ListGroupsRequest, ListGroupsResponse,
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsResponse, ListOffsetsTopic,
    MetadataRequest, MetadataResponse, OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchResponse,
    create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig},
    delete_topics_request::DeleteTopicState,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};
use kafka_client_protocol::{Request, Response};

// ===========================================================================
// Admin DTOs (lightweight, user-facing types)
// ===========================================================================

/// Specification for creating a new topic.
#[derive(Debug, Clone)]
pub struct NewTopic {
    /// Topic name (required).
    pub name: String,
    /// Number of partitions.
    pub num_partitions: i32,
    /// Replication factor.
    pub replication_factor: i16,
    /// Optional per-partition replica assignments.
    /// When specified, `num_partitions` and `replication_factor` are ignored.
    pub replica_assignments: Option<Vec<Vec<i32>>>,
    /// Optional topic-level configs (e.g. `("retention.ms", "86400000")`).
    pub configs: Vec<(String, String)>,
}

impl NewTopic {
    /// Create a new topic with the given name, partition count, and
    /// replication factor.
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            replica_assignments: None,
            configs: Vec::new(),
        }
    }

    /// Set a topic-level configuration.
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((key.into(), value.into()));
        self
    }

    /// Use custom partition replica assignments instead of a uniform
    /// replication factor.
    pub fn with_replica_assignments(mut self, assignments: Vec<Vec<i32>>) -> Self {
        self.replica_assignments = Some(assignments);
        self
    }
}

/// Result of a topic create/delete operation.
#[derive(Debug, Clone)]
pub struct AdminTopicResult {
    /// Topic name.
    pub name: String,
    /// Error code (0 = success).
    pub error_code: KafkaErrorCode,
    /// Error message, if any.
    pub error_message: Option<String>,
}

impl AdminTopicResult {
    /// Returns `true` if the operation succeeded for this topic.
    pub fn is_success(&self) -> bool {
        self.error_code.is_ok()
    }

    /// Returns `true` if the topic already existed.
    pub fn already_exists(&self) -> bool {
        self.error_code == KafkaErrorCode::TOPIC_ALREADY_EXISTS
    }
}

/// Summary of a topic (from `list_topics`).
#[derive(Debug, Clone)]
pub struct AdminTopic {
    /// Topic name.
    pub name: String,
    /// Whether this is an internal topic (e.g. `__consumer_offsets`).
    pub internal: bool,
    /// Number of partitions.
    pub partitions: usize,
}

/// Detailed per-partition info (from `describe_topics`).
#[derive(Debug, Clone)]
pub struct AdminPartitionInfo {
    /// Partition index.
    pub partition: i32,
    /// Leader broker ID.
    pub leader_id: i32,
    /// Replica broker IDs.
    pub replicas: Vec<i32>,
    /// In-sync replica broker IDs.
    pub isr: Vec<i32>,
}

/// Detailed topic description (from `describe_topics`).
#[derive(Debug, Clone)]
pub struct AdminTopicDescription {
    /// Topic name.
    pub name: String,
    /// Whether this is an internal topic.
    pub internal: bool,
    /// Per-partition details.
    pub partitions: Vec<AdminPartitionInfo>,
}

/// A broker in the cluster.
#[derive(Debug, Clone)]
pub struct AdminBroker {
    /// Broker ID.
    pub id: i32,
    /// Hostname.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Socket address.
    pub addr: Option<SocketAddr>,
}

/// Cluster summary.
#[derive(Debug, Clone)]
pub struct AdminClusterInfo {
    /// Cluster ID (if available).
    pub cluster_id: Option<String>,
    /// Current controller broker ID.
    pub controller_id: Option<i32>,
    /// All brokers in the cluster.
    pub brokers: Vec<AdminBroker>,
}

/// Consumer group listing entry.
#[derive(Debug, Clone)]
pub struct AdminGroup {
    /// Group ID.
    pub group_id: String,
    /// Protocol type (e.g. "consumer").
    pub protocol_type: String,
    /// Group state (e.g. "Stable", "Empty"), when reported by the broker.
    pub state: String,
}

/// Consumer group member.
#[derive(Debug, Clone)]
pub struct AdminGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
}

/// A committed offset for a single topic-partition of a consumer group.
///
/// Returned by [`AdminClient::fetch_group_offsets`]. `log_end_offset` and
/// `lag` are resolved from the partition's high-watermark; they are `-1`
/// when the high-watermark could not be determined (e.g. the topic was
/// deleted after the offset was committed).
#[derive(Debug, Clone)]
pub struct GroupOffset {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Last committed offset for the partition.
    pub committed_offset: i64,
    /// High-watermark (log-end offset) of the partition.
    pub log_end_offset: i64,
    /// Lag = `log_end_offset - committed_offset` (clamped to `>= 0`).
    pub lag: i64,
    /// Partition metadata string committed alongside the offset.
    pub metadata: String,
}

/// Consumer group detailed description.
#[derive(Debug, Clone)]
pub struct AdminGroupDescription {
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g. "Stable", "PreparingRebalance").
    pub state: String,
    /// Protocol type (e.g. "consumer").
    pub protocol_type: String,
    /// Members of the group and their assignments.
    pub members: Vec<AdminGroupMember>,
}

/// Specification for committing a partition offset.
#[derive(Debug, Clone)]
pub struct OffsetCommitSpec {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Offset to commit (-1 = latest, -2 = earliest, or a specific offset).
    pub offset: i64,
    /// Optional metadata string.
    pub metadata: Option<String>,
}

// ===========================================================================
// ACL types (Kafka AclBinding / AclBindingFilter)
// ===========================================================================

/// Kafka ACL resource types (numeric values match the wire protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclResourceType {
    Unknown,
    Any,
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
    User,
}

impl AclResourceType {
    pub fn as_i8(self) -> i8 {
        match self {
            AclResourceType::Unknown => 0,
            AclResourceType::Any => 1,
            AclResourceType::Topic => 2,
            AclResourceType::Group => 3,
            AclResourceType::Cluster => 4,
            AclResourceType::TransactionalId => 5,
            AclResourceType::DelegationToken => 6,
            AclResourceType::User => 7,
        }
    }

    pub fn from_i8(v: i8) -> Self {
        match v {
            1 => AclResourceType::Any,
            2 => AclResourceType::Topic,
            3 => AclResourceType::Group,
            4 => AclResourceType::Cluster,
            5 => AclResourceType::TransactionalId,
            6 => AclResourceType::DelegationToken,
            7 => AclResourceType::User,
            _ => AclResourceType::Unknown,
        }
    }
}

/// Kafka ACL operations (numeric values match the wire protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclOperation {
    Unknown,
    Any,
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

impl AclOperation {
    pub fn as_i8(self) -> i8 {
        match self {
            AclOperation::Unknown => 0,
            AclOperation::Any => 1,
            AclOperation::All => 2,
            AclOperation::Read => 3,
            AclOperation::Write => 4,
            AclOperation::Create => 5,
            AclOperation::Delete => 6,
            AclOperation::Alter => 7,
            AclOperation::Describe => 8,
            AclOperation::ClusterAction => 9,
            AclOperation::DescribeConfigs => 10,
            AclOperation::AlterConfigs => 11,
            AclOperation::IdempotentWrite => 12,
        }
    }

    pub fn from_i8(v: i8) -> Self {
        match v {
            1 => AclOperation::Any,
            2 => AclOperation::All,
            3 => AclOperation::Read,
            4 => AclOperation::Write,
            5 => AclOperation::Create,
            6 => AclOperation::Delete,
            7 => AclOperation::Alter,
            8 => AclOperation::Describe,
            9 => AclOperation::ClusterAction,
            10 => AclOperation::DescribeConfigs,
            11 => AclOperation::AlterConfigs,
            12 => AclOperation::IdempotentWrite,
            _ => AclOperation::Unknown,
        }
    }
}

/// Kafka ACL permission types (numeric values match the wire protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPermissionType {
    Unknown,
    Any,
    Deny,
    Allow,
}

impl AclPermissionType {
    pub fn as_i8(self) -> i8 {
        match self {
            AclPermissionType::Unknown => 0,
            AclPermissionType::Any => 1,
            AclPermissionType::Deny => 2,
            AclPermissionType::Allow => 3,
        }
    }

    pub fn from_i8(v: i8) -> Self {
        match v {
            1 => AclPermissionType::Any,
            2 => AclPermissionType::Deny,
            3 => AclPermissionType::Allow,
            _ => AclPermissionType::Unknown,
        }
    }
}

/// Kafka ACL resource pattern types (numeric values match the wire protocol).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclPatternType {
    Unknown,
    Any,
    Match,
    Literal,
    Prefixed,
}

impl AclPatternType {
    pub fn as_i8(self) -> i8 {
        match self {
            AclPatternType::Unknown => 0,
            AclPatternType::Any => 1,
            AclPatternType::Match => 2,
            AclPatternType::Literal => 3,
            AclPatternType::Prefixed => 4,
        }
    }

    pub fn from_i8(v: i8) -> Self {
        match v {
            1 => AclPatternType::Any,
            2 => AclPatternType::Match,
            3 => AclPatternType::Literal,
            4 => AclPatternType::Prefixed,
            _ => AclPatternType::Unknown,
        }
    }
}

/// A concrete ACL binding: who (principal) may do what (operation) on a
/// resource from which host.
#[derive(Debug, Clone)]
pub struct AclBinding {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission_type: AclPermissionType,
}

impl AclBinding {
    /// Create a literal-pattern ACL binding.
    pub fn new(
        resource_type: AclResourceType,
        resource_name: impl Into<String>,
        principal: impl Into<String>,
        host: impl Into<String>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type: AclPatternType::Literal,
            principal: principal.into(),
            host: host.into(),
            operation,
            permission_type,
        }
    }

    pub fn with_pattern_type(mut self, pattern_type: AclPatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }
}

/// A filter for describing/deleting ACLs. `None` fields match anything.
#[derive(Debug, Clone, Default)]
pub struct AclBindingFilter {
    pub resource_type: Option<AclResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<AclPatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission_type: Option<AclPermissionType>,
}

/// Result of creating an ACL.
#[derive(Debug, Clone)]
pub struct AclCreationResult {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
}

/// Result of deleting ACLs matching a filter.
#[derive(Debug, Clone)]
pub struct AclDeleteResult {
    pub error_code: KafkaErrorCode,
    pub error_message: Option<String>,
    pub matching_acls: Vec<AclBinding>,
}

/// A broker/topic configuration entry.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
}

/// Per-partition result of `delete_records`.
#[derive(Debug, Clone)]
pub struct DeleteRecordsResult {
    pub partition: i32,
    pub low_watermark: i64,
    pub error_code: KafkaErrorCode,
}

// ===========================================================================
// AdminClient
// ===========================================================================

/// Kafka admin client — cluster management and inspection.
///
/// Created via [`Client::admin()`](crate::Client::admin).
pub struct AdminClient {
    cluster: Arc<ClusterClient>,
}

impl AdminClient {
    pub(crate) fn new(cluster: Arc<ClusterClient>) -> Self {
        Self { cluster }
    }

    // ------------------------------------------------------------------
    // Topic management
    // ------------------------------------------------------------------

    /// Create one or more topics.
    ///
    /// Topics that already exist are tolerated (error code 36).
    ///
    /// # Example
    ///
    /// ```ignore
    /// admin
    ///     .create_topics(&[
    ///         NewTopic::new("orders", 3, 3)
    ///             .with_config("retention.ms", "86400000"),
    ///         NewTopic::new("payments", 6, 3),
    ///     ])
    ///     .await?;
    /// ```
    pub async fn create_topics(&self, topics: &[NewTopic]) -> Result<Vec<AdminTopicResult>> {
        let creatable: Vec<CreatableTopic> = topics
            .iter()
            .map(|t| {
                let assignments: Vec<CreatableReplicaAssignment> = t
                    .replica_assignments
                    .as_ref()
                    .map(|a| {
                        a.iter()
                            .enumerate()
                            .map(|(idx, ids)| CreatableReplicaAssignment {
                                partition_index: idx as i32,
                                broker_ids: ids.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                let configs: Vec<CreatableTopicConfig> = t
                    .configs
                    .iter()
                    .map(|(k, v)| CreatableTopicConfig {
                        name: k.clone(),
                        value: Some(v.clone()),
                    })
                    .collect();

                CreatableTopic {
                    name: t.name.clone(),
                    num_partitions: t.num_partitions,
                    replication_factor: t.replication_factor,
                    assignments,
                    configs,
                }
            })
            .collect();

        let request = CreateTopicsRequest {
            topics: creatable,
            timeout_ms: 30_000,
            validate_only: false,
        };

        // CreateTopics must be sent to the controller; a non-controller
        // broker replies NOT_CONTROLLER (41), and the controller may hand
        // over mid-operation (topic created but response 41). Retry with a
        // metadata refresh so the next attempt hits the current controller.
        const MAX_ATTEMPTS: u32 = 5;
        let mut last_results = Vec::new();
        for attempt in 0..MAX_ATTEMPTS {
            let response: CreateTopicsResponse = self.send_to_controller(&request).await?;
            last_results = response
                .topics
                .into_iter()
                .map(|t| AdminTopicResult {
                    name: t.name,
                    error_code: KafkaErrorCode::from_i16(t.error_code),
                    error_message: t.error_message,
                })
                .collect();
            let not_controller = last_results
                .iter()
                .any(|r| r.error_code == KafkaErrorCode::NOT_CONTROLLER);
            if not_controller && attempt + 1 < MAX_ATTEMPTS {
                let _ = self.cluster.refresh_metadata().await;
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                continue;
            }
            break;
        }
        Ok(last_results)
    }

    /// Create a single topic. Convenience wrapper around [`create_topics`].
    pub async fn create_topic(&self, topic: &NewTopic) -> Result<AdminTopicResult> {
        let mut results = self.create_topics(std::slice::from_ref(topic)).await?;
        results
            .pop()
            .ok_or_else(|| KafkaError::InvalidConfiguration("no result returned".into()))
    }

    /// Delete one or more topics.
    pub async fn delete_topics(
        &self,
        topic_names: &[impl AsRef<str>],
    ) -> Result<Vec<AdminTopicResult>> {
        let topics: Vec<DeleteTopicState> = topic_names
            .iter()
            .map(|n| DeleteTopicState {
                name: Some(n.as_ref().to_string()),
                topic_id: uuid::Uuid::nil(),
            })
            .collect();

        let topic_names_vec: Vec<String> =
            topic_names.iter().map(|n| n.as_ref().to_string()).collect();

        let request = DeleteTopicsRequest {
            topics: topics.clone(),
            topic_names: topic_names_vec,
            timeout_ms: 30_000,
        };

        // DeleteTopics is also a controller operation (see create_topics).
        const MAX_ATTEMPTS: u32 = 5;
        let mut last_results = Vec::new();
        for attempt in 0..MAX_ATTEMPTS {
            let response: DeleteTopicsResponse = self.send_to_controller(&request).await?;
            last_results = response
                .responses
                .into_iter()
                .map(|r| AdminTopicResult {
                    name: r.name.unwrap_or_default(),
                    error_code: KafkaErrorCode::from_i16(r.error_code),
                    error_message: r.error_message,
                })
                .collect();
            let not_controller = last_results
                .iter()
                .any(|r| r.error_code == KafkaErrorCode::NOT_CONTROLLER);
            if not_controller && attempt + 1 < MAX_ATTEMPTS {
                let _ = self.cluster.refresh_metadata().await;
                tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
                continue;
            }
            break;
        }
        Ok(last_results)
    }

    /// Delete a single topic. Convenience wrapper around [`delete_topics`].
    pub async fn delete_topic(&self, name: &str) -> Result<AdminTopicResult> {
        let mut results = self.delete_topics(&[name]).await?;
        results
            .pop()
            .ok_or_else(|| KafkaError::InvalidConfiguration("no result returned".into()))
    }

    /// List all topics in the cluster.
    ///
    /// Returns basic metadata: name, internal flag, and partition count.
    /// The internal metadata cache is refreshed first.
    pub async fn list_topics(&self) -> Result<Vec<AdminTopic>> {
        self.cluster.refresh_metadata().await?;
        let topics = self.cluster.metadata().get_all_topics().await;
        Ok(topics
            .into_iter()
            .filter(|t| !t.is_internal) // internal topics are noise for most users
            .map(|t| AdminTopic {
                name: t.name.unwrap_or_default(),
                internal: t.is_internal,
                partitions: t.partitions.len(),
            })
            .collect())
    }

    /// Describe specific topics with full partition-level detail.
    pub async fn describe_topics(
        &self,
        topic_names: &[impl AsRef<str>],
    ) -> Result<Vec<AdminTopicDescription>> {
        let name_list: Vec<String> = topic_names.iter().map(|s| s.as_ref().to_string()).collect();

        let request_topics: Vec<crate::protocol::MetadataRequestTopic> = name_list
            .iter()
            .map(|name| crate::protocol::MetadataRequestTopic {
                topic_id: uuid::Uuid::nil(),
                name: Some(name.clone()),
            })
            .collect();

        let request = MetadataRequest {
            topics: Some(request_topics),
            allow_auto_topic_creation: false,
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false,
        };

        let response: MetadataResponse = self.cluster.send_to_any_broker(&request).await?;

        let descriptions = response
            .topics
            .into_iter()
            .map(|t| {
                let partitions = t
                    .partitions
                    .iter()
                    .map(|p| AdminPartitionInfo {
                        partition: p.partition_index,
                        leader_id: p.leader_id,
                        replicas: p.replica_nodes.clone(),
                        isr: p.isr_nodes.clone(),
                    })
                    .collect();

                AdminTopicDescription {
                    name: t.name.unwrap_or_default(),
                    internal: t.is_internal,
                    partitions,
                }
            })
            .collect();

        Ok(descriptions)
    }

    // ------------------------------------------------------------------
    // Cluster inspection
    // ------------------------------------------------------------------

    /// Describe the cluster: cluster ID, controller, and all brokers.
    ///
    /// Refreshes the metadata cache to ensure fresh results.
    pub async fn describe_cluster(&self) -> Result<AdminClusterInfo> {
        self.cluster.refresh_metadata().await?;
        let metadata = self.cluster.metadata();

        let brokers: Vec<AdminBroker> = metadata
            .get_all_brokers()
            .await
            .into_iter()
            .map(|b| {
                let host = b.host;
                let port = b.port;
                let addr_str = format!("{}:{}", host, port);
                AdminBroker {
                    id: b.node_id,
                    host: host.clone(),
                    port,
                    addr: addr_str.parse().ok(),
                }
            })
            .collect();

        Ok(AdminClusterInfo {
            cluster_id: metadata.get_cluster_id().await,
            controller_id: metadata.get_controller_id().await,
            brokers,
        })
    }

    // ------------------------------------------------------------------
    // Consumer group inspection
    // ------------------------------------------------------------------

    /// List all consumer groups in the cluster.
    pub async fn list_groups(&self) -> Result<Vec<AdminGroup>> {
        let request = ListGroupsRequest {
            states_filter: vec![],
            types_filter: vec![],
        };

        // ListGroups only returns the groups *coordinated by the broker that
        // answers the request*, so we must query every broker and merge the
        // results to get a complete cluster-wide view.
        self.cluster.refresh_metadata().await?;

        let mut groups: Vec<AdminGroup> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut errors: Vec<String> = Vec::new();

        let addrs: Vec<SocketAddr> = self.cluster.all_broker_addresses();
        // Query every broker in parallel; ListGroups only returns groups
        // coordinated by the broker that answers.
        let futures: Vec<_> = addrs
            .iter()
            .map(|addr| {
                let addr = *addr;
                let value = request.clone();
                async move {
                    (
                        addr,
                        self.cluster
                            .send_to_broker::<ListGroupsRequest, ListGroupsResponse>(addr, &value)
                            .await,
                    )
                }
            })
            .collect();

        for (addr, outcome) in join_all(futures).await {
            match outcome {
                Ok(response) => {
                    if response.error_code != 0 {
                        tracing::warn!(
                            "ListGroups on broker {} failed: {}",
                            addr,
                            KafkaErrorCode::from_i16(response.error_code)
                        );
                        errors.push(format!("{}: {}", addr, response.error_code));
                        continue;
                    }
                    for g in response.groups {
                        if seen.insert(g.group_id.clone()) {
                            groups.push(AdminGroup {
                                group_id: g.group_id,
                                protocol_type: g.protocol_type,
                                state: g.group_state,
                            });
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("ListGroups on broker {} failed: {}", addr, e);
                    errors.push(format!("{}: {}", addr, e));
                }
            }
        }

        if groups.is_empty() && !errors.is_empty() {
            return Err(KafkaError::Io(errors.join("; ")));
        }
        if !errors.is_empty() {
            // Partial failure: some brokers did not answer. The caller still
            // gets the groups we did collect, but the result is incomplete.
            tracing::warn!(
                "list_groups: {} of {} brokers failed, result may be incomplete: {}",
                errors.len(),
                addrs.len(),
                errors.join("; ")
            );
        }

        groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(groups)
    }

    /// Describe specific consumer groups.
    ///
    /// Returns detailed information including members and their state.
    pub async fn describe_groups(
        &self,
        group_ids: &[impl AsRef<str>],
    ) -> Result<Vec<AdminGroupDescription>> {
        let ids: Vec<String> = group_ids.iter().map(|s| s.as_ref().to_string()).collect();

        // DescribeGroups only returns member/state info for groups this
        // broker coordinates. Group the requested ids by their coordinator
        // (found via FindCoordinator) and send one request per coordinator.
        let coord_futures: Vec<_> = ids
            .iter()
            .map(|id| {
                let id = id.clone();
                async move { (id.clone(), self.find_group_coordinator(&id).await) }
            })
            .collect();
        let coord_results = join_all(coord_futures).await;

        let mut by_coordinator: std::collections::HashMap<SocketAddr, Vec<String>> =
            std::collections::HashMap::new();
        for (id, res) in coord_results {
            let coord = res?;
            by_coordinator.entry(coord).or_default().push(id);
        }

        let describe_futures: Vec<_> = by_coordinator
            .into_iter()
            .map(|(coord, coord_groups)| async move {
                let request = DescribeGroupsRequest {
                    groups: coord_groups,
                    include_authorized_operations: false,
                };

                let response: DescribeGroupsResponse =
                    self.cluster.send_to_broker(coord, &request).await?;
                Ok::<_, KafkaError>(response)
            })
            .collect();
        let describe_results = join_all(describe_futures).await;

        let mut descriptions = Vec::new();
        for result in describe_results {
            let response = result?;
            for g in response.groups {
                if g.error_code != 0 {
                    return Err(KafkaError::GroupError {
                        group_id: g.group_id.clone(),
                        error: KafkaErrorCode::from_i16(g.error_code),
                    });
                }

                let members = g
                    .members
                    .into_iter()
                    .map(|m| AdminGroupMember {
                        member_id: m.member_id,
                        client_id: m.client_id,
                        client_host: m.client_host,
                    })
                    .collect();

                descriptions.push(AdminGroupDescription {
                    group_id: g.group_id,
                    state: g.group_state,
                    protocol_type: g.protocol_type,
                    members,
                });
            }
        }

        descriptions.sort_by(|a, b| a.group_id.cmp(&b.group_id));
        Ok(descriptions)
    }

    /// Delete a consumer group.
    ///
    /// # Example
    ///
    /// ```ignore
    /// admin.delete_group("my-consumer-group").await?;
    /// ```
    pub async fn delete_group(&self, group_id: &str) -> Result<()> {
        let request = DeleteGroupsRequest {
            groups_names: vec![group_id.to_string()],
        };
        let coord = self.find_group_coordinator(group_id).await?;
        let response: DeleteGroupsResponse = self.cluster.send_to_broker(coord, &request).await?;

        for r in response.results {
            if r.error_code != 0 {
                return Err(KafkaError::GroupError {
                    group_id: r.group_id,
                    error: KafkaErrorCode::from_i16(r.error_code),
                });
            }
        }
        Ok(())
    }

    /// Fetch the committed offsets of a consumer group across all topics.
    ///
    /// Returns one [`GroupOffset`] per topic-partition the group has committed
    /// an offset for. The high-watermark (log-end offset) and therefore the
    /// `lag` are resolved for each partition; they are set to `-1` when the
    /// partition's log-end offset cannot be determined.
    ///
    /// The request is routed to the group coordinator (found via
    /// `FindCoordinator`), which is where committed offsets are stored.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let offsets = admin.fetch_group_offsets("my-consumer-group").await?;
    /// for o in &offsets {
    ///     println!("{}:{} offset={} lag={}", o.topic, o.partition, o.committed_offset, o.lag);
    /// }
    /// ```
    pub async fn fetch_group_offsets(&self, group_id: &str) -> Result<Vec<GroupOffset>> {
        let coord = self.find_group_coordinator(group_id).await?;

        let request = OffsetFetchRequest {
            group_id: group_id.to_string(),
            topics: None,
            groups: vec![OffsetFetchRequestGroup {
                group_id: group_id.to_string(),
                member_id: None,
                member_epoch: -1,
                topics: None,
            }],
            require_stable: false,
        };

        let response: OffsetFetchResponse = self.cluster.send_to_broker(coord, &request).await?;

        // Helper: collect (topic, partition_index, committed_offset, metadata) from any
        // response layout so we only write the log-end-offset resolution once.
        struct RawPartition {
            topic: String,
            partition_index: i32,
            committed_offset: i64,
            metadata: Option<String>,
        }

        let mut raw = Vec::<RawPartition>::new();

        if !response.groups.is_empty() {
            // Protocol version 8+ — response layout uses per-group wrappers.
            for grp in response.groups {
                if grp.group_id != group_id {
                    continue;
                }
                if grp.error_code != 0 {
                    return Err(KafkaError::GroupError {
                        group_id: group_id.to_string(),
                        error: KafkaErrorCode::from_i16(grp.error_code),
                    });
                }
                for t in grp.topics {
                    // Protocol v10+ reports topics by id (name is empty);
                    // resolve the name back through the metadata cache.
                    let name = if !t.name.is_empty() {
                        t.name.clone()
                    } else if !t.topic_id.is_nil() {
                        self.cluster
                            .metadata()
                            .get_topic_name_by_id(t.topic_id)
                            .await
                            .unwrap_or_else(|| format!("unknown-{}", t.topic_id))
                    } else {
                        continue;
                    };
                    for p in t.partitions {
                        if p.error_code != 0 {
                            continue;
                        }
                        raw.push(RawPartition {
                            topic: name.clone(),
                            partition_index: p.partition_index,
                            committed_offset: p.committed_offset,
                            metadata: p.metadata.clone(),
                        });
                    }
                }
            }
        } else {
            // Protocol version 0-7 — response layout uses flat topic list.
            if response.error_code != 0 {
                return Err(KafkaError::GroupError {
                    group_id: group_id.to_string(),
                    error: KafkaErrorCode::from_i16(response.error_code),
                });
            }
            for t in &response.topics {
                let name = &t.name;
                if name.is_empty() {
                    continue;
                }
                for p in &t.partitions {
                    if p.error_code != 0 {
                        continue;
                    }
                    raw.push(RawPartition {
                        topic: name.clone(),
                        partition_index: p.partition_index,
                        committed_offset: p.committed_offset,
                        metadata: p.metadata.clone(),
                    });
                }
            }
        }

        // Resolve log-end-offset and build the final GroupOffset for every partition.
        let mut offsets = Vec::with_capacity(raw.len());
        for rp in raw {
            let log_end = self
                .fetch_log_end_offset(&rp.topic, rp.partition_index)
                .await
                .unwrap_or(-1);
            let lag = if log_end >= 0 && rp.committed_offset >= 0 {
                (log_end - rp.committed_offset).max(0)
            } else {
                -1
            };
            offsets.push(GroupOffset {
                topic: rp.topic,
                partition: rp.partition_index,
                committed_offset: rp.committed_offset,
                log_end_offset: log_end,
                lag,
                metadata: rp.metadata.unwrap_or_default(),
            });
        }

        Ok(offsets)
    }

    /// Resolve the socket address of a consumer group's coordinator.
    async fn find_group_coordinator(&self, group_id: &str) -> Result<SocketAddr> {
        let request = FindCoordinatorRequest {
            key: group_id.to_string(),
            key_type: 0,
            coordinator_keys: vec![group_id.to_string()],
        };

        const MAX_ATTEMPTS: u32 = 10;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let response: FindCoordinatorResponse =
                self.cluster.send_to_any_broker(&request).await?;

            // error_code 15 = GROUP_COORDINATOR_NOT_AVAILABLE (retryable),
            // e.g. while __consumer_offsets is being created/reassigned.
            let retryable = response.error_code == 15
                || response
                    .coordinators
                    .first()
                    .map(|c| c.error_code == 15)
                    .unwrap_or(false);

            if retryable && attempt < MAX_ATTEMPTS {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            if response.error_code != 0 {
                return Err(KafkaError::NoCoordinator);
            }
            let (host, port) = if !response.host.is_empty() {
                (response.host.clone(), response.port)
            } else if let Some(coord) = response.coordinators.first() {
                if coord.error_code != 0 {
                    return Err(KafkaError::NoCoordinator);
                }
                (coord.host.clone(), coord.port)
            } else {
                return Err(KafkaError::NoCoordinator);
            };
            return net::lookup_host(format!("{}:{}", host, port))
                .await
                .map_err(|_| KafkaError::NoCoordinator)?
                .next()
                .ok_or(KafkaError::NoCoordinator);
        }
    }

    /// Resolve the current controller's address from the metadata cache.
    async fn controller_addr(&self) -> Option<SocketAddr> {
        let controller_id = self.cluster.metadata().get_controller_id().await?;
        self.cluster
            .metadata()
            .get_broker_address(controller_id)
            .await
    }

    /// Send a request to the current controller, falling back to any broker
    /// when the controller is unknown or the controller connection fails.
    /// Callers retry when the response reports `NOT_CONTROLLER`.
    async fn send_to_controller<Req, Resp>(&self, request: &Req) -> Result<Resp>
    where
        Req: Request,
        Resp: Response,
    {
        if let Some(addr) = self.controller_addr().await
            && let Ok(resp) = self.cluster.send_to_broker(addr, request).await
        {
            return Ok(resp);
        }
        let _ = self.cluster.refresh_metadata().await;
        self.cluster.send_to_any_broker(request).await
    }

    /// Resolve the high-watermark (log-end offset) of a single partition via a
    /// `ListOffsets` request (timestamp `-1`, meaning latest). Returns `-1`
    /// when the leader cannot be determined or the request fails.
    async fn fetch_log_end_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let leader_addr = self
            .cluster
            .metadata()
            .get_partition_leader(topic, partition)
            .await
            .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopic {
                name: topic.to_string(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: partition,
                    current_leader_epoch: -1,
                    timestamp: -1,
                }],
            }],
            timeout_ms: 5000,
        };
        let response: ListOffsetsResponse = self
            .cluster
            .send_to_broker::<ListOffsetsRequest, ListOffsetsResponse>(leader_addr, &request)
            .await?;
        for t in &response.topics {
            if t.name == topic
                && let Some(p) = t.partitions.iter().find(|p| p.partition_index == partition)
            {
                if p.error_code != 0 {
                    break;
                }
                return Ok(p.offset);
            }
        }
        Err(KafkaError::PartitionNotFound(topic.to_string(), partition))
    }

    /// Commit offsets for a consumer group.
    ///
    /// This is a low-level administrative operation; for normal consumers,
    /// use [`Consumer::offsets()`](crate::Consumer::offsets) instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// admin.commit_offsets("my-group", &[
    ///     OffsetCommitSpec { topic: "orders".into(), partition: 0, offset: 42, metadata: None },
    /// ]).await?;
    /// ```
    pub async fn commit_offsets(&self, group_id: &str, offsets: &[OffsetCommitSpec]) -> Result<()> {
        // Group offsets by topic
        let mut topics: std::collections::HashMap<String, Vec<OffsetCommitRequestPartition>> =
            std::collections::HashMap::new();

        for spec in offsets {
            topics
                .entry(spec.topic.clone())
                .or_default()
                .push(OffsetCommitRequestPartition {
                    partition_index: spec.partition,
                    committed_offset: spec.offset,
                    committed_leader_epoch: -1,
                    committed_metadata: spec.metadata.clone(),
                });
        }

        // OffsetCommit must be sent to the group's coordinator; a non-
        // coordinator broker replies with NOT_COORDINATOR for every partition.
        // v10+ addresses topics by id, so resolve ids from the metadata cache
        // and retry with a refresh if the broker reports UNKNOWN_TOPIC_ID
        // (e.g. the topic was created just now and metadata lagged).
        const MAX_ATTEMPTS: u32 = 3;
        for attempt in 0..MAX_ATTEMPTS {
            let mut request_topics = Vec::with_capacity(topics.len());
            for (name, partitions) in &topics {
                let mut topic_id = self
                    .cluster
                    .metadata()
                    .get_topic(name)
                    .await
                    .map(|t| t.topic_id)
                    .unwrap_or_else(uuid::Uuid::nil);
                if topic_id.is_nil() {
                    let _ = self.cluster.refresh_metadata().await;
                    topic_id = self
                        .cluster
                        .metadata()
                        .get_topic(name)
                        .await
                        .map(|t| t.topic_id)
                        .unwrap_or_else(uuid::Uuid::nil);
                }
                request_topics.push(OffsetCommitRequestTopic {
                    name: name.clone(),
                    topic_id,
                    partitions: partitions.clone(),
                });
            }

            let request = OffsetCommitRequest {
                group_id: group_id.to_string(),
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                group_instance_id: None,
                retention_time_ms: -1,
                topics: request_topics,
            };

            let coord = self.find_group_coordinator(group_id).await?;
            let response: OffsetCommitResponse =
                self.cluster.send_to_broker(coord, &request).await?;

            let unknown_topic = response.topics.iter().flat_map(|t| &t.partitions).any(|p| {
                KafkaErrorCode::from_i16(p.error_code) == KafkaErrorCode::UNKNOWN_TOPIC_ID
            });
            if unknown_topic && attempt + 1 < MAX_ATTEMPTS {
                let _ = self.cluster.refresh_metadata().await;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }

            for t in &response.topics {
                for p in &t.partitions {
                    if p.error_code != 0 {
                        return Err(KafkaError::OffsetCommitError(KafkaErrorCode::from_i16(
                            p.error_code,
                        )));
                    }
                }
            }
            return Ok(());
        }
        Err(KafkaError::OffsetCommitError(
            KafkaErrorCode::UNKNOWN_TOPIC_ID,
        ))
    }

    /// Refresh the internal metadata cache (force refresh).
    pub async fn refresh_metadata(&self) -> Result<()> {
        self.cluster.refresh_metadata().await
    }

    // ------------------------------------------------------------------
    // Broker configuration
    // ------------------------------------------------------------------

    /// Query a broker configuration value (e.g. `"max.message.bytes"`).
    ///
    /// Uses the Kafka `DescribeConfigs` API (resource type `BROKER=4`).
    /// Returns `None` if the config key is unknown, the broker doesn't
    /// support this API, or the value is not a valid integer.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let max_bytes = admin.get_broker_config("max.message.bytes").await?;
    /// if let Some(bytes) = max_bytes {
    ///     println!("Broker max message size: {} bytes", bytes);
    /// }
    /// ```
    pub async fn get_broker_config(&self, key: &str) -> Option<usize> {
        self.cluster.query_broker_config(key).await
    }

    // ------------------------------------------------------------------
    // ACL management
    // ------------------------------------------------------------------

    /// Create one or more ACL bindings.
    pub async fn create_acls(&self, acls: &[AclBinding]) -> Result<Vec<AclCreationResult>> {
        let request = CreateAclsRequest {
            creations: acls
                .iter()
                .map(|a| AclCreation {
                    resource_type: a.resource_type.as_i8(),
                    resource_name: a.resource_name.clone(),
                    resource_pattern_type: a.pattern_type.as_i8(),
                    principal: a.principal.clone(),
                    host: a.host.clone(),
                    operation: a.operation.as_i8(),
                    permission_type: a.permission_type.as_i8(),
                })
                .collect(),
        };
        let response: CreateAclsResponse = self.cluster.send_to_any_broker(&request).await?;
        Ok(response
            .results
            .into_iter()
            .map(|r| AclCreationResult {
                error_code: KafkaErrorCode::from_i16(r.error_code),
                error_message: r.error_message,
            })
            .collect())
    }

    /// Describe ACLs matching a filter (an empty filter matches everything).
    pub async fn describe_acls(&self, filter: &AclBindingFilter) -> Result<Vec<AclBinding>> {
        let request = DescribeAclsRequest {
            resource_type_filter: filter
                .resource_type
                .map(|t| t.as_i8())
                .unwrap_or(AclResourceType::Any.as_i8()),
            resource_name_filter: filter.resource_name.clone(),
            pattern_type_filter: filter
                .pattern_type
                .map(|p| p.as_i8())
                .unwrap_or(AclPatternType::Any.as_i8()),
            principal_filter: filter.principal.clone(),
            host_filter: filter.host.clone(),
            operation: filter
                .operation
                .map(|o| o.as_i8())
                .unwrap_or(AclOperation::Any.as_i8()),
            permission_type: filter
                .permission_type
                .map(|p| p.as_i8())
                .unwrap_or(AclPermissionType::Any.as_i8()),
        };
        let response: DescribeAclsResponse = self.cluster.send_to_any_broker(&request).await?;
        if response.error_code != 0 {
            return Err(KafkaError::AdminError {
                operation: "describe_acls".into(),
                code: KafkaErrorCode::from_i16(response.error_code),
                message: response.error_message.unwrap_or_default(),
            });
        }

        let mut bindings = Vec::new();
        for res in response.resources {
            let resource_type = AclResourceType::from_i8(res.resource_type);
            let resource_name = res.resource_name.clone();
            let pattern_type = AclPatternType::from_i8(res.pattern_type);
            for acl in res.acls {
                bindings.push(AclBinding {
                    resource_type,
                    resource_name: resource_name.clone(),
                    pattern_type,
                    principal: acl.principal,
                    host: acl.host,
                    operation: AclOperation::from_i8(acl.operation),
                    permission_type: AclPermissionType::from_i8(acl.permission_type),
                });
            }
        }
        Ok(bindings)
    }

    /// Delete ACLs matching the given filters.
    pub async fn delete_acls(&self, filters: &[AclBindingFilter]) -> Result<Vec<AclDeleteResult>> {
        let request = DeleteAclsRequest {
            filters: filters
                .iter()
                .map(|f| DeleteAclsFilter {
                    resource_type_filter: f
                        .resource_type
                        .map(|t| t.as_i8())
                        .unwrap_or(AclResourceType::Any.as_i8()),
                    resource_name_filter: f.resource_name.clone(),
                    pattern_type_filter: f
                        .pattern_type
                        .map(|p| p.as_i8())
                        .unwrap_or(AclPatternType::Any.as_i8()),
                    principal_filter: f.principal.clone(),
                    host_filter: f.host.clone(),
                    operation: f
                        .operation
                        .map(|o| o.as_i8())
                        .unwrap_or(AclOperation::Any.as_i8()),
                    permission_type: f
                        .permission_type
                        .map(|p| p.as_i8())
                        .unwrap_or(AclPermissionType::Any.as_i8()),
                })
                .collect(),
        };
        let response: DeleteAclsResponse = self.cluster.send_to_any_broker(&request).await?;
        Ok(response
            .filter_results
            .into_iter()
            .map(|r| AclDeleteResult {
                error_code: KafkaErrorCode::from_i16(r.error_code),
                error_message: r.error_message,
                matching_acls: r
                    .matching_acls
                    .into_iter()
                    .map(|m| AclBinding {
                        resource_type: AclResourceType::from_i8(m.resource_type),
                        resource_name: m.resource_name,
                        pattern_type: AclPatternType::from_i8(m.pattern_type),
                        principal: m.principal,
                        host: m.host,
                        operation: AclOperation::from_i8(m.operation),
                        permission_type: AclPermissionType::from_i8(m.permission_type),
                    })
                    .collect(),
            })
            .collect())
    }

    // ------------------------------------------------------------------
    // Configuration management
    // ------------------------------------------------------------------

    /// Alter the configuration of a broker/topic resource.
    ///
    /// `resource_type` is one of `2` (topic), `4` (broker), `3` (group), etc.
    pub async fn alter_configs(
        &self,
        resource_type: i8,
        resource_name: &str,
        configs: &[(String, String)],
    ) -> Result<()> {
        let request = AlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type,
                resource_name: resource_name.to_string(),
                configs: configs
                    .iter()
                    .map(|(name, value)| AlterableConfig {
                        name: name.clone(),
                        value: Some(value.clone()),
                    })
                    .collect(),
            }],
            validate_only: false,
        };
        let response: AlterConfigsResponse = self.cluster.send_to_any_broker(&request).await?;
        for r in response.responses {
            if r.error_code != 0 {
                return Err(KafkaError::AdminError {
                    operation: format!("alter_configs({resource_name})"),
                    code: KafkaErrorCode::from_i16(r.error_code),
                    message: r.error_message.unwrap_or_default(),
                });
            }
        }
        Ok(())
    }

    /// Convenience: alter a topic's configuration (e.g. `retention.ms`).
    pub async fn alter_topic_configs(
        &self,
        topic: &str,
        configs: &[(String, String)],
    ) -> Result<()> {
        self.alter_configs(2, topic, configs).await
    }

    /// Describe the configuration entries of a broker/topic resource.
    pub async fn describe_configs(
        &self,
        resource_type: i8,
        resource_name: &str,
    ) -> Result<Vec<ConfigEntry>> {
        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type,
                resource_name: resource_name.to_string(),
                configuration_keys: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        };
        let response: DescribeConfigsResponse = self.cluster.send_to_any_broker(&request).await?;
        let mut entries = Vec::new();
        for r in response.results {
            if r.error_code != 0 {
                return Err(KafkaError::AdminError {
                    operation: format!("describe_configs({resource_name})"),
                    code: KafkaErrorCode::from_i16(r.error_code),
                    message: r.error_message.unwrap_or_default(),
                });
            }
            for c in r.configs {
                entries.push(ConfigEntry {
                    name: c.name,
                    value: c.value,
                });
            }
        }
        Ok(entries)
    }

    // ------------------------------------------------------------------
    // Records & offsets management
    // ------------------------------------------------------------------

    /// Delete records before the given offsets (sent to each partition
    /// leader). Returns the resulting low watermark per partition.
    pub async fn delete_records(
        &self,
        topic: &str,
        partitions: &[(i32, i64)],
    ) -> Result<Vec<DeleteRecordsResult>> {
        let mut by_leader: HashMap<SocketAddr, Vec<(i32, i64)>> = HashMap::new();
        for (partition, offset) in partitions {
            let leader = self
                .cluster
                .metadata()
                .get_partition_leader(topic, *partition)
                .await
                .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), *partition))?;
            by_leader
                .entry(leader)
                .or_default()
                .push((*partition, *offset));
        }

        let mut results = Vec::new();
        for (leader, parts) in by_leader {
            let request = DeleteRecordsRequest {
                topics: vec![DeleteRecordsTopic {
                    name: topic.to_string(),
                    partitions: parts
                        .iter()
                        .map(|(partition_index, offset)| DeleteRecordsPartition {
                            partition_index: *partition_index,
                            offset: *offset,
                        })
                        .collect(),
                }],
                timeout_ms: 30_000,
            };
            let response: DeleteRecordsResponse =
                self.cluster.send_to_broker(leader, &request).await?;
            for t in response.topics {
                for p in t.partitions {
                    results.push(DeleteRecordsResult {
                        partition: p.partition_index,
                        low_watermark: p.low_watermark,
                        error_code: KafkaErrorCode::from_i16(p.error_code),
                    });
                }
            }
        }
        results.sort_by_key(|r| r.partition);
        Ok(results)
    }

    /// Reset a consumer group's committed offsets (simple commit with
    /// generation `-1`). Pass offsets resolved via `fetch_log_end_offset` /
    /// list-offsets for "earliest"/"latest" semantics.
    pub async fn reset_group_offsets(
        &self,
        group_id: &str,
        offsets: &[(String, i32, i64)],
    ) -> Result<()> {
        let specs: Vec<OffsetCommitSpec> = offsets
            .iter()
            .map(|(topic, partition, offset)| OffsetCommitSpec {
                topic: topic.clone(),
                partition: *partition,
                offset: *offset,
                metadata: None,
            })
            .collect();
        self.commit_offsets(group_id, &specs).await
    }
}
