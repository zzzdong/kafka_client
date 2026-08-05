use bytes::{Bytes, BytesMut};
use bytes::Buf;
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
use kafka_client_protocol::Message;

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

/// Build the consumer protocol subscription metadata (version 2).
///
/// The member's previous assignment is embedded in `user_data` so the group
/// leader can run sticky balancing across rebalances.
pub(crate) fn build_protocol_metadata(
    topics: &[String],
    previous_assignment: &HashMap<String, Vec<i32>>,
) -> Bytes {
    let mut buf = BytesMut::new();
    use bytes::BufMut;
    buf.put_i16(2);
    buf.put_i32(topics.len() as i32);
    for t in topics {
        buf.put_i16(t.len() as i16);
        buf.put_slice(t.as_bytes());
    }
    if previous_assignment.is_empty() {
        buf.put_i32(-1);
    } else {
        let assignment = ConsumerProtocolAssignment {
            assigned_partitions: previous_assignment
                .iter()
                .map(|(topic, partitions)| TopicPartition {
                    topic: topic.clone(),
                    partitions: partitions.clone(),
                })
                .collect(),
            user_data: None,
        };
        let mut abuf = BytesMut::new();
        let _ = assignment.encode(&mut abuf, 0);
        buf.put_i32(abuf.len() as i32);
        buf.put_slice(&abuf);
    }
    buf.put_i32(0); // owned partitions
    buf.put_i32(-1); // generation id
    buf.freeze()
}

/// Extract a member's previous assignment from its subscription metadata.
/// Returns an empty map on any malformed/absent payload.
fn parse_previous_assignment(metadata: &[u8]) -> HashMap<String, Vec<i32>> {
    fn read_i16(buf: &mut Bytes) -> Option<i16> {
        if buf.len() < 2 {
            None
        } else {
            Some(buf.get_i16())
        }
    }
    fn read_i32(buf: &mut Bytes) -> Option<i32> {
        if buf.len() < 4 {
            None
        } else {
            Some(buf.get_i32())
        }
    }

    let mut buf = Bytes::from(metadata.to_vec());
    if buf.len() < 2 {
        return HashMap::new();
    }
    buf.advance(2); // subscription version

    // topics: int32 count, then int16-length strings
    let topic_count = match read_i32(&mut buf) {
        Some(v) if v >= 0 => v as usize,
        _ => return HashMap::new(),
    };
    for _ in 0..topic_count {
        let len = match read_i16(&mut buf) {
            Some(v) if v >= 0 => v as usize,
            _ => return HashMap::new(),
        };
        if buf.len() < len {
            return HashMap::new();
        }
        buf.advance(len);
    }

    // user_data: int32 length or -1
    let ud_len = match read_i32(&mut buf) {
        Some(v) => v,
        None => return HashMap::new(),
    };
    if ud_len < 0 || buf.len() < ud_len as usize {
        return HashMap::new();
    }
    let mut ud = buf.split_to(ud_len as usize);
    let Ok(assignment) = ConsumerProtocolAssignment::decode(&mut ud, 0) else {
        return HashMap::new();
    };

    let mut result = HashMap::new();
    for tp in assignment.assigned_partitions {
        result.insert(tp.topic, tp.partitions);
    }
    result
}

/// Greedy sticky assignment: keep each member's previous partitions, assign
/// unowned partitions to the least-loaded members, then move partitions from
/// overloaded to underloaded members while preserving ownership where
/// possible (the partition moved is the one the overloaded member acquired
/// most recently).
fn sticky_assign(
    members: &[String],
    all_partitions: &[(String, i32)],
    previous: &HashMap<String, Vec<(String, i32)>>,
) -> HashMap<String, Vec<(String, i32)>> {
    let mut assignment: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for m in members {
        assignment.insert(m.clone(), Vec::new());
    }

    // Seed with the previous assignment (only members still in the group).
    let mut owned_by: HashMap<(String, i32), String> = HashMap::new();
    for (member, parts) in previous {
        if !assignment.contains_key(member) {
            continue;
        }
        for p in parts {
            if all_partitions.contains(p) && !owned_by.contains_key(p) {
                assignment.get_mut(member).unwrap().push(p.clone());
                owned_by.insert(p.clone(), member.clone());
            }
        }
    }

    // Unowned partitions go to the least-loaded members.
    let unowned: Vec<(String, i32)> = all_partitions
        .iter()
        .filter(|p| !owned_by.contains_key(*p))
        .cloned()
        .collect();
    for p in unowned {
        let min_member = members
            .iter()
            .min_by_key(|m| assignment[*m].len())
            .cloned()
            .unwrap();
        assignment.get_mut(&min_member).unwrap().push(p.clone());
        owned_by.insert(p, min_member);
    }

    // Balance until the difference between the most- and least-loaded members
    // is at most one partition.
    loop {
        let (max_m, max_count) = members
            .iter()
            .map(|m| (m, assignment[m].len()))
            .max_by_key(|(_, count)| *count)
            .unwrap();
        let (min_m, min_count) = members
            .iter()
            .map(|m| (m, assignment[m].len()))
            .min_by_key(|(_, count)| *count)
            .unwrap();
        if max_count <= min_count + 1 {
            break;
        }
        let to_move = assignment[max_m]
            .last()
            .cloned()
            .or_else(|| assignment[max_m].first().cloned());
        let Some(p) = to_move else {
            break;
        };
        assignment.get_mut(max_m).unwrap().retain(|x| *x != p);
        assignment.get_mut(min_m).unwrap().push(p);
    }

    assignment
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

    // Previous assignment per member (carried in subscription user_data),
    // used by the sticky strategies to minimize partition movement.
    let mut previous: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for m in &join_response.members {
        let parsed = parse_previous_assignment(&m.metadata);
        let parts: Vec<(String, i32)> = parsed
            .into_iter()
            .flat_map(|(topic, partitions)| {
                partitions
                    .into_iter()
                    .map(move |partition| (topic.clone(), partition))
            })
            .collect();
        previous.insert(m.member_id.clone(), parts);
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
            // Sticky strategies are computed across all topics after this
            // loop (they need the members' previous assignments).
            PartitionAssignmentStrategy::Sticky | PartitionAssignmentStrategy::CooperativeSticky => {}
        }
    }

    if strategy == PartitionAssignmentStrategy::Sticky
        || strategy == PartitionAssignmentStrategy::CooperativeSticky
    {
        let mut all_partitions: Vec<(String, i32)> = Vec::new();
        for topic in topics {
            let partitions = cluster
                .metadata()
                .get_partitions(topic)
                .await
                .unwrap_or_default();
            for p in partitions {
                all_partitions.push((topic.clone(), p));
            }
        }
        let members: Vec<String> = all_members.iter().map(|s| s.to_string()).collect();
        let assigned = sticky_assign(&members, &all_partitions, &previous);
        member_assignments = assigned
            .into_iter()
            .map(|(member, parts)| {
                let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
                for (topic, partition) in parts {
                    by_topic.entry(topic).or_default().push(partition);
                }
                (
                    member,
                    by_topic
                        .into_iter()
                        .map(|(topic, partitions)| TopicPartition { topic, partitions })
                        .collect(),
                )
            })
            .collect();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(ps: &[(i32, &str)]) -> Vec<(String, i32)> {
        ps.iter()
            .map(|(p, t)| (t.to_string(), *p))
            .collect()
    }

    #[test]
    fn sticky_assigns_unowned_partitions_evenly() {
        let members = vec!["a".into(), "b".into()];
        let all = parts(&[(0, "t"), (1, "t"), (2, "t"), (3, "t"), (4, "t")]);
        let previous = HashMap::new();
        let assignment = sticky_assign(&members, &all, &previous);

        let total: usize = assignment.values().map(|v| v.len()).sum();
        assert_eq!(total, 5);
        let mut count_vals: Vec<usize> = assignment.values().map(|v| v.len()).collect();
        count_vals.sort();
        assert_eq!(count_vals, vec![2, 3], "balanced within one partition");
    }

    #[test]
    fn sticky_keeps_previous_assignment_when_balanced() {
        let members = vec!["a".into(), "b".into()];
        let all = parts(&[(0, "t"), (1, "t"), (2, "t"), (3, "t"), (4, "t")]);
        let mut previous = HashMap::new();
        // Already balanced: a owns 0,1; b owns 2,3,4.
        previous.insert("a".into(), parts(&[(0, "t"), (1, "t")]));
        previous.insert("b".into(), parts(&[(2, "t"), (3, "t"), (4, "t")]));

        let assignment = sticky_assign(&members, &all, &previous);
        assert_eq!(assignment["a"].len(), 2, "balanced assignment must not move partitions");
        assert_eq!(assignment["b"].len(), 3);
        for (_, p) in &assignment["a"] {
            assert!(p == &0 || p == &1, "a should keep its previous partitions");
        }
    }

    #[test]
    fn sticky_moves_only_surplus_partitions() {
        let members = vec!["a".into(), "b".into()];
        let all = parts(&[(0, "t"), (1, "t"), (2, "t")]);
        let mut previous = HashMap::new();
        // a owns all three; after balancing b must get exactly one and a keeps two.
        previous.insert("a".into(), parts(&[(0, "t"), (1, "t"), (2, "t")]));

        let assignment = sticky_assign(&members, &all, &previous);
        assert_eq!(assignment["a"].len(), 2, "a keeps two partitions");
        assert_eq!(assignment["b"].len(), 1, "b gets one partition");
    }

    #[test]
    fn previous_assignment_drops_departed_members_and_missing_partitions() {
        let members = vec!["a".into()];
        let all = parts(&[(0, "t"), (1, "t")]);
        let mut previous = HashMap::new();
        previous.insert(
            "a".into(),
            parts(&[(0, "t"), (99, "gone-topic")]),
        );
        previous.insert("departed".into(), parts(&[(1, "t")]));

        let assignment = sticky_assign(&members, &all, &previous);
        assert_eq!(assignment["a"].len(), 2, "gone-topic partition dropped, unowned t:1 assigned");
        assert!(
            assignment["a"].iter().all(|(t, _)| t == "t"),
            "only partitions of subscribed topics survive"
        );
    }

    #[test]
    fn subscription_metadata_roundtrip_carries_previous_assignment() {
        let mut previous = HashMap::new();
        previous.insert("t".to_string(), vec![0, 2]);
        let metadata = build_protocol_metadata(&["t".to_string()], &previous);
        let parsed = parse_previous_assignment(&metadata);
        assert_eq!(parsed.get("t"), Some(&vec![0, 2]));
    }

    #[test]
    fn subscription_metadata_without_assignment_parses_empty() {
        let metadata = build_protocol_metadata(&["t".to_string()], &HashMap::new());
        assert!(parse_previous_assignment(&metadata).is_empty());
    }

    #[test]
    fn malformed_subscription_metadata_parses_empty() {
        assert!(parse_previous_assignment(&[]).is_empty());
        assert!(parse_previous_assignment(&[0x00]).is_empty());
        assert!(parse_previous_assignment(&[0x00, 0x02, 0xff]).is_empty());
    }
}
