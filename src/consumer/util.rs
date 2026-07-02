use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use crate::cluster::ClusterClient;
use crate::consumer::config::PartitionAssignmentStrategy;
use crate::error::{KafkaError, Result};
use crate::protocol::{
    ConsumerProtocolAssignment, FindCoordinatorRequest, FindCoordinatorResponse, JoinGroupResponse,
    ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic, OffsetFetchRequest,
    OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics, TopicPartition,
};
use kafka_client_protocol::KafkaErrorCode;

pub(crate) async fn find_coordinator_raw(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
) -> Result<SocketAddr> {
    let request = FindCoordinatorRequest {
        key: group_id.to_string(),
        key_type: 0,
        coordinator_keys: vec![group_id.to_string()],
    };
    let response: FindCoordinatorResponse = cluster.send_to_any_broker(&request).await?;
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
    format!("{}:{}", host, port)
        .to_socket_addrs()
        .map_err(|_| KafkaError::NoCoordinator)?
        .next()
        .ok_or(KafkaError::NoCoordinator)
}

pub(crate) fn build_protocol_metadata(topics: &[String]) -> Bytes {
    let mut buf = BytesMut::new();
    use bytes::BufMut;
    buf.put_i16(2);
    buf.put_i32(topics.len() as i32);
    for t in topics {
        buf.put_i16(t.len() as i16);
        buf.put_slice(t.as_bytes());
    }
    buf.put_i32(-1);
    buf.put_i32(0);
    buf.put_i32(-1);
    buf.freeze()
}

pub(crate) async fn compute_all_assignments(
    topics: &[String],
    join_response: &JoinGroupResponse,
    cluster: &Arc<ClusterClient>,
    strategy: PartitionAssignmentStrategy,
) -> Result<HashMap<String, ConsumerProtocolAssignment>> {
    let all_members: Vec<&str> = join_response
        .members
        .iter()
        .map(|m| m.member_id.as_str())
        .collect();
    if all_members.is_empty() {
        return Ok(HashMap::new());
    }

    let mut member_assignments: HashMap<String, Vec<TopicPartition>> = HashMap::new();
    for mid in &all_members {
        member_assignments.insert(mid.to_string(), Vec::new());
    }

    for topic in topics {
        let partitions = cluster
            .metadata()
            .get_partitions(topic)
            .await
            .unwrap_or_default();
        match strategy {
            PartitionAssignmentStrategy::Range => {
                let n = all_members.len();
                let per = partitions.len() / n;
                let rem = partitions.len() % n;
                let mut idx = 0;
                for (i, mid) in all_members.iter().enumerate() {
                    let count = per + if i < rem { 1 } else { 0 };
                    if count > 0 {
                        member_assignments
                            .get_mut(*mid)
                            .unwrap()
                            .push(TopicPartition {
                                topic: topic.to_string(),
                                partitions: partitions[idx..idx + count].to_vec(),
                            });
                        idx += count;
                    }
                }
            }
            PartitionAssignmentStrategy::RoundRobin => {
                for (i, &p) in partitions.iter().enumerate() {
                    member_assignments
                        .get_mut(all_members[i % all_members.len()])
                        .unwrap()
                        .push(TopicPartition {
                            topic: topic.to_string(),
                            partitions: vec![p],
                        });
                }
            }
            PartitionAssignmentStrategy::CooperativeSticky => {
                // Simplified CooperativeSticky: use sorted round-robin.
                // A complete implementation would track previous assignments
                // (per member) and minimize partition movement across
                // rebalances.  TODO: implement true sticky assignment.
                let mut sorted_members = all_members.to_vec();
                sorted_members.sort();
                for (i, &p) in partitions.iter().enumerate() {
                    member_assignments
                        .get_mut(sorted_members[i % sorted_members.len()])
                        .unwrap()
                        .push(TopicPartition {
                            topic: topic.to_string(),
                            partitions: vec![p],
                        });
                }
            }
        }
    }

    Ok(member_assignments
        .into_iter()
        .map(|(mid, tps)| {
            (
                mid,
                ConsumerProtocolAssignment {
                    assigned_partitions: tps,
                    user_data: None,
                },
            )
        })
        .collect())
}

pub(crate) async fn fetch_committed_offsets_raw(
    cluster: &Arc<ClusterClient>,
    group_id: &str,
    coord: SocketAddr,
    assignment: &HashMap<String, Vec<i32>>,
) -> Result<HashMap<String, HashMap<i32, i64>>> {
    let mut topic_ids = Vec::with_capacity(assignment.len());
    for topic in assignment.keys() {
        let tid = cluster
            .metadata()
            .get_topic(topic)
            .await
            .map(|t| t.topic_id)
            .unwrap_or_else(uuid::Uuid::nil);
        topic_ids.push(tid);
    }

    let topics: Vec<OffsetFetchRequestTopics> = assignment
        .iter()
        .zip(topic_ids)
        .map(|((topic, partitions), tid)| OffsetFetchRequestTopics {
            name: topic.clone(),
            topic_id: tid,
            partition_indexes: partitions.clone(),
        })
        .collect();
    let legacy: Vec<OffsetFetchRequestTopic> = assignment
        .iter()
        .map(|(topic, partitions)| OffsetFetchRequestTopic {
            name: topic.clone(),
            partition_indexes: partitions.clone(),
        })
        .collect();

    let request = OffsetFetchRequest {
        group_id: String::new(),
        topics: if legacy.is_empty() {
            None
        } else {
            Some(legacy)
        },
        groups: vec![OffsetFetchRequestGroup {
            group_id: group_id.to_string(),
            member_id: None,
            member_epoch: -1,
            topics: Some(topics),
        }],
        require_stable: false,
    };
    let response: crate::protocol::OffsetFetchResponse =
        cluster.send_to_broker(coord, &request).await?;

    let mut result: HashMap<String, HashMap<i32, i64>> = HashMap::new();
    for grp in response.groups {
        if grp.group_id != group_id {
            continue;
        }
        for t in grp.topics {
            let name = if !t.name.is_empty() {
                t.name.clone()
            } else if !t.topic_id.is_nil() {
                cluster
                    .metadata()
                    .get_topic_name_by_id(t.topic_id)
                    .await
                    .unwrap_or_else(|| format!("unknown-{}", t.topic_id))
            } else {
                continue;
            };
            let entry = result.entry(name).or_default();
            for p in t.partitions {
                if p.error_code == 0 && p.committed_offset >= 0 {
                    entry.insert(p.partition_index, p.committed_offset);
                }
            }
        }
    }
    Ok(result)
}

pub(crate) fn map_heartbeat_error(
    error_code: i16,
    generation_id: i32,
    member_id: &str,
) -> Result<()> {
    match KafkaErrorCode::from_i16(error_code) {
        KafkaErrorCode::NONE => Ok(()),
        KafkaErrorCode::REBALANCE_IN_PROGRESS => Err(KafkaError::RebalanceRequired),
        KafkaErrorCode::ILLEGAL_GENERATION => Err(KafkaError::IllegalGeneration(generation_id)),
        KafkaErrorCode::UNKNOWN_MEMBER_ID => {
            Err(KafkaError::UnknownMemberId(member_id.to_string()))
        }
        code => Err(KafkaError::Protocol(format!("Heartbeat failed: {}", code))),
    }
}

pub(crate) async fn list_offset_for(
    cluster: &Arc<ClusterClient>,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Result<i64> {
    let leader_addr = cluster
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
                timestamp,
            }],
        }],
        timeout_ms: -1,
    };
    let response = cluster
        .send_to_broker::<ListOffsetsRequest, crate::protocol::ListOffsetsResponse>(
            leader_addr,
            &request,
        )
        .await?;
    for tr in response.topics {
        if tr.name == topic {
            for pr in tr.partitions {
                if pr.partition_index == partition {
                    if pr.error_code != 0 {
                        break;
                    }
                    return Ok(pr.offset);
                }
            }
        }
    }
    Err(KafkaError::OffsetNotFound(topic.to_string(), partition))
}
