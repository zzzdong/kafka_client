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
//! let client = Client::builder(vec!["localhost:9092".parse().unwrap()])
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

use std::net::SocketAddr;
use std::sync::Arc;

use crate::cluster::ClusterClient;
use crate::error::{KafkaError, Result};
use crate::protocol::{
    CreateTopicsRequest, CreateTopicsResponse, DeleteGroupsRequest, DeleteGroupsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribeGroupsRequest, DescribeGroupsResponse,
    ListGroupsRequest, ListGroupsResponse, MetadataRequest, MetadataResponse, OffsetCommitRequest,
    OffsetCommitResponse,
    create_topics_request::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig},
    delete_topics_request::DeleteTopicState,
    offset_commit_request::{OffsetCommitRequestPartition, OffsetCommitRequestTopic},
};

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
    pub error_code: i16,
    /// Error message, if any.
    pub error_message: Option<String>,
}

impl AdminTopicResult {
    /// Returns `true` if the operation succeeded for this topic.
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Returns `true` if the topic already existed (code 36 = TOPIC_ALREADY_EXISTS).
    pub fn already_exists(&self) -> bool {
        self.error_code == 36
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
                            .map(|ids| CreatableReplicaAssignment {
                                partition_index: -1,
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

        let response: CreateTopicsResponse = self.cluster.send_to_any_broker(&request).await?;
        let results = response
            .topics
            .into_iter()
            .map(|t| AdminTopicResult {
                name: t.name,
                error_code: t.error_code,
                error_message: t.error_message,
            })
            .collect();

        Ok(results)
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

        let response: DeleteTopicsResponse = self.cluster.send_to_any_broker(&request).await?;
        let results = response
            .responses
            .into_iter()
            .map(|r| AdminTopicResult {
                name: r.name.unwrap_or_default(),
                error_code: r.error_code,
                error_message: r.error_message,
            })
            .collect();

        Ok(results)
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
            cluster_id: None, // MetadataCache doesn't expose this yet
            controller_id: None,
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

        let response: ListGroupsResponse = self.cluster.send_to_any_broker(&request).await?;
        let groups = response
            .groups
            .into_iter()
            .map(|g| AdminGroup {
                group_id: g.group_id,
                protocol_type: g.protocol_type,
            })
            .collect();

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

        let request = DescribeGroupsRequest {
            groups: ids.clone(),
            include_authorized_operations: false,
        };

        let response: DescribeGroupsResponse = self.cluster.send_to_any_broker(&request).await?;

        let descriptions = response
            .groups
            .into_iter()
            .map(|g| {
                let members = g
                    .members
                    .into_iter()
                    .map(|m| AdminGroupMember {
                        member_id: m.member_id,
                        client_id: m.client_id,
                        client_host: m.client_host,
                    })
                    .collect();

                AdminGroupDescription {
                    group_id: g.group_id,
                    state: g.group_state,
                    protocol_type: g.protocol_type,
                    members,
                }
            })
            .collect();

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
        let _response: DeleteGroupsResponse = self.cluster.send_to_any_broker(&request).await?;
        Ok(())
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

        let request = OffsetCommitRequest {
            group_id: group_id.to_string(),
            generation_id_or_member_epoch: -1,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: topics
                .into_iter()
                .map(|(name, partitions)| OffsetCommitRequestTopic {
                    name,
                    topic_id: uuid::Uuid::nil(),
                    partitions,
                })
                .collect(),
        };

        let _response: OffsetCommitResponse = self.cluster.send_to_any_broker(&request).await?;
        Ok(())
    }

    /// Refresh the internal metadata cache (force refresh).
    pub async fn refresh_metadata(&self) -> Result<()> {
        self.cluster.refresh_metadata().await
    }
}
