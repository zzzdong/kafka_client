use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use tracing::warn;

use crate::cluster::ClusterClient;
use crate::consumer::config::{AutoOffsetReset, PartitionAssignmentStrategy};
use crate::consumer::types::{ConsumerRecord, FetchParams, Header};
use crate::error::{KafkaError, Result};
use crate::protocol::{
    ConsumerProtocolAssignment, FetchPartition, FetchRequest, FetchTopic, FindCoordinatorRequest,
    FindCoordinatorResponse, JoinGroupResponse, ListOffsetsPartition, ListOffsetsRequest,
    ListOffsetsTopic, OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
    OffsetFetchRequestTopics, TopicPartition,
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
            PartitionAssignmentStrategy::Range | PartitionAssignmentStrategy::CooperativeSticky => {
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

pub(crate) async fn fetch_partition_simple(
    cluster: &Arc<ClusterClient>,
    topic: &str,
    partition: i32,
    offset: i64,
    params: &FetchParams,
    auto_offset_reset: AutoOffsetReset,
) -> Result<Vec<ConsumerRecord>> {
    if cluster.metadata().is_expired().await {
        cluster.refresh_metadata().await?;
    }
    let leader_addr = cluster
        .metadata()
        .get_partition_leader(topic, partition)
        .await
        .ok_or_else(|| KafkaError::PartitionNotFound(topic.to_string(), partition))?;
    let topic_id = cluster
        .metadata()
        .get_topic(topic)
        .await
        .map(|t| t.topic_id)
        .unwrap_or_else(uuid::Uuid::nil);

    let build_req = |fetch_offset: i64| FetchRequest {
        cluster_id: None,
        replica_id: -1,
        replica_state: Default::default(),
        max_wait_ms: params.timeout_ms,
        min_bytes: params.min_bytes,
        max_bytes: params.max_bytes,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            topic_id,
            partitions: vec![FetchPartition {
                partition,
                current_leader_epoch: -1,
                fetch_offset,
                last_fetched_epoch: -1,
                log_start_offset: -1,
                partition_max_bytes: params.partition_max_bytes,
                replica_directory_id: uuid::Uuid::nil(),
                high_watermark: 0,
            }],
        }],
        forgotten_topics_data: vec![],
        rack_id: String::new(),
    };

    let response: crate::protocol::FetchResponse = cluster
        .send_to_broker(leader_addr, &build_req(offset))
        .await?;

    if has_offset_out_of_range(&response, topic, partition) {
        warn!(
            "OFFSET_OUT_OF_RANGE for {}/{} (offset={}), resetting",
            topic, partition, offset
        );
        let ts = match auto_offset_reset {
            AutoOffsetReset::Latest => -1i64,
            AutoOffsetReset::Earliest => -2i64,
            AutoOffsetReset::None => return Err(KafkaError::NoOffsetStored),
        };
        let new_offset = list_offset_for(cluster, topic, partition, ts)
            .await
            .unwrap_or(0);
        let retry_response: crate::protocol::FetchResponse = cluster
            .send_to_broker(leader_addr, &build_req(new_offset))
            .await?;
        return parse_fetch_response(retry_response, topic, topic_id, partition);
    }

    parse_fetch_response(response, topic, topic_id, partition)
}

fn has_offset_out_of_range(
    response: &crate::protocol::FetchResponse,
    topic_name: &str,
    partition_index: i32,
) -> bool {
    for tr in &response.responses {
        if tr.topic != topic_name && tr.topic_id.is_nil() {
            continue;
        }
        for p in &tr.partitions {
            if p.partition_index == partition_index
                && p.error_code == KafkaErrorCode::OFFSET_OUT_OF_RANGE.code()
            {
                return true;
            }
        }
    }
    false
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

fn parse_fetch_response(
    response: crate::protocol::FetchResponse,
    topic_name: &str,
    topic_id: uuid::Uuid,
    partition_index: i32,
) -> Result<Vec<ConsumerRecord>> {
    let mut records = Vec::new();
    for tr in response.responses {
        if tr.topic != topic_name && tr.topic_id != topic_id {
            continue;
        }
        for pr in tr.partitions {
            if pr.partition_index != partition_index {
                continue;
            }
            if pr.error_code != 0 {
                if pr.error_code == KafkaErrorCode::REBALANCE_IN_PROGRESS.code() {
                    warn!("REBALANCE_IN_PROGRESS for partition {}", pr.partition_index);
                    continue;
                }
                warn!(
                    "Fetch error for partition {}: {}",
                    pr.partition_index,
                    KafkaErrorCode::from_i16(pr.error_code)
                );
                continue;
            }
            let Some(batch) = pr.records else {
                continue;
            };
            let base_offset = batch.base_offset;
            let first_ts = batch.first_timestamp;
            for (idx, rec) in batch.records.into_iter().enumerate() {
                records.push(ConsumerRecord {
                    topic: topic_name.to_string(),
                    partition: partition_index,
                    offset: base_offset + idx as i64,
                    timestamp: first_ts + rec.timestamp_delta,
                    key: rec.key,
                    value: rec.value.unwrap_or_default(),
                    headers: rec
                        .headers
                        .into_iter()
                        .map(|h| Header {
                            key: h.key,
                            value: h.value.unwrap_or_default(),
                        })
                        .collect(),
                });
            }
        }
    }
    Ok(records)
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
