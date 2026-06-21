//! API version mapping
//! DO NOT EDIT

/// 客户端支持的 API 版本范围（静态数组）
pub const CLIENT_SUPPORTED_VERSIONS: &[(i16, i16, i16)] = &[
    (62, 0, 4), // BrokerRegistrationRequest
    (50, 0, 0), // DescribeUserScramCredentialsResponse
    (51, 0, 0), // AlterUserScramCredentialsRequest
    (39, 1, 2), // RenewDelegationTokenRequest
    (1, 4, 18), // FetchRequest
    (20, 1, 6), // DeleteTopicsRequest
    (90, 0, 1), // DescribeShareGroupOffsetsResponse
    (0, 3, 13), // ProduceRequest
    (30, 1, 3), // CreateAclsResponse
    (16, 0, 5), // ListGroupsRequest
    (29, 1, 3), // DescribeAclsRequest
    (31, 1, 3), // DeleteAclsResponse
    (9, 1, 10), // OffsetFetchRequest
    (71, 0, 0), // GetTelemetrySubscriptionsRequest
    (63, 0, 1), // BrokerHeartbeatResponse
    (50, 0, 0), // DescribeUserScramCredentialsRequest
    (35, 1, 4), // DescribeLogDirsResponse
    (29, 1, 3), // DescribeAclsResponse
    (74, 0, 1), // ListConfigResourcesResponse
    (32, 1, 4), // DescribeConfigsRequest
    (6, 0, 0),  // UpdateMetadataRequest
    (27, 1, 2), // WriteTxnMarkersRequest
    (69, 0, 1), // ConsumerGroupDescribeResponse
    (53, 0, 1), // BeginQuorumEpochResponse
    (81, 0, 0), // RemoveRaftVoterResponse
    (60, 0, 2), // DescribeClusterRequest
    (70, 0, 0), // ControllerRegistrationRequest
    (1, 4, 18), // FetchResponse
    (19, 2, 7), // CreateTopicsRequest
    (92, 0, 0), // DeleteShareGroupOffsetsRequest
    (21, 0, 2), // DeleteRecordsRequest
    (14, 0, 5), // SyncGroupResponse
    (78, 1, 2), // ShareFetchRequest
    (40, 1, 2), // ExpireDelegationTokenRequest
    (72, 0, 0), // PushTelemetryResponse
    (13, 0, 5), // LeaveGroupResponse
    (36, 0, 2), // SaslAuthenticateResponse
    (90, 0, 1), // DescribeShareGroupOffsetsRequest
    (26, 0, 5), // EndTxnRequest
    (17, 0, 1), // SaslHandshakeRequest
    (52, 0, 2), // VoteRequest
    (3, 0, 13), // MetadataResponse
    (11, 0, 9), // JoinGroupRequest
    (49, 0, 1), // AlterClientQuotasResponse
    (79, 1, 2), // ShareAcknowledgeResponse
    (64, 0, 0), // UnregisterBrokerRequest
    (31, 1, 3), // DeleteAclsRequest
    (17, 0, 1), // SaslHandshakeResponse
    (42, 0, 2), // DeleteGroupsResponse
    (37, 0, 3), // CreatePartitionsResponse
    (44, 0, 1), // IncrementalAlterConfigsRequest
    (8, 2, 10), // OffsetCommitRequest
    (10, 0, 6), // FindCoordinatorRequest
    (87, 0, 1), // ReadShareGroupStateSummaryResponse
    (24, 0, 5), // AddPartitionsToTxnResponse
    (13, 0, 5), // LeaveGroupRequest
    (83, 0, 0), // InitializeShareGroupStateRequest
    (89, 0, 0), // StreamsGroupDescribeResponse
    (59, 0, 1), // FetchSnapshotRequest
    (64, 0, 0), // UnregisterBrokerResponse
    (24, 0, 5), // AddPartitionsToTxnRequest
    (38, 1, 3), // CreateDelegationTokenResponse
    (10, 0, 6), // FindCoordinatorResponse
    (54, 0, 1), // EndQuorumEpochResponse
    (61, 0, 0), // DescribeProducersRequest
    (89, 0, 0), // StreamsGroupDescribeRequest
    (67, 0, 0), // AllocateProducerIdsResponse
    (34, 1, 2), // AlterReplicaLogDirsRequest
    (86, 0, 0), // DeleteShareGroupStateRequest
    (71, 0, 0), // GetTelemetrySubscriptionsResponse
    (36, 0, 2), // SaslAuthenticateRequest
    (80, 0, 1), // AddRaftVoterRequest
    (82, 0, 0), // UpdateRaftVoterResponse
    (42, 0, 2), // DeleteGroupsRequest
    (6, 0, 0),  // UpdateMetadataResponse
    (91, 0, 0), // AlterShareGroupOffsetsRequest
    (46, 0, 0), // ListPartitionReassignmentsRequest
    (12, 0, 4), // HeartbeatRequest
    (63, 0, 1), // BrokerHeartbeatRequest
    (2, 1, 11), // ListOffsetsResponse
    (12, 0, 4), // HeartbeatResponse
    (22, 0, 6), // InitProducerIdResponse
    (41, 1, 3), // DescribeDelegationTokenResponse
    (4, 0, 0),  // LeaderAndIsrRequest
    (46, 0, 0), // ListPartitionReassignmentsResponse
    (79, 1, 2), // ShareAcknowledgeRequest
    (75, 0, 0), // DescribeTopicPartitionsResponse
    (81, 0, 0), // RemoveRaftVoterRequest
    (62, 0, 4), // BrokerRegistrationResponse
    (5, 0, 0),  // StopReplicaResponse
    (16, 0, 5), // ListGroupsResponse
    (32, 1, 4), // DescribeConfigsResponse
    (0, 3, 13), // ProduceResponse
    (8, 2, 10), // OffsetCommitResponse
    (55, 0, 2), // DescribeQuorumRequest
    (76, 1, 1), // ShareGroupHeartbeatRequest
    (48, 0, 1), // DescribeClientQuotasResponse
    (70, 0, 0), // ControllerRegistrationResponse
    (5, 0, 0),  // StopReplicaRequest
    (23, 2, 4), // OffsetForLeaderEpochRequest
    (18, 0, 4), // ApiVersionsRequest
    (84, 0, 0), // ReadShareGroupStateResponse
    (57, 0, 2), // UpdateFeaturesRequest
    (19, 2, 7), // CreateTopicsResponse
    (3, 0, 13), // MetadataRequest
    (22, 0, 6), // InitProducerIdRequest
    (26, 0, 5), // EndTxnResponse
    (74, 0, 1), // ListConfigResourcesRequest
    (4, 0, 0),  // LeaderAndIsrResponse
    (30, 1, 3), // CreateAclsRequest
    (75, 0, 0), // DescribeTopicPartitionsRequest
    (28, 0, 5), // TxnOffsetCommitResponse
    (85, 0, 1), // WriteShareGroupStateRequest
    (14, 0, 5), // SyncGroupRequest
    (92, 0, 0), // DeleteShareGroupOffsetsResponse
    (72, 0, 0), // PushTelemetryRequest
    (33, 0, 2), // AlterConfigsRequest
    (58, 0, 0), // EnvelopeRequest
    (53, 0, 1), // BeginQuorumEpochRequest
    (80, 0, 1), // AddRaftVoterResponse
    (25, 0, 4), // AddOffsetsToTxnRequest
    (52, 0, 2), // VoteResponse
    (68, 0, 1), // ConsumerGroupHeartbeatResponse
    (60, 0, 2), // DescribeClusterResponse
    (66, 0, 2), // ListTransactionsRequest
    (86, 0, 0), // DeleteShareGroupStateResponse
    (73, 0, 0), // AssignReplicasToDirsRequest
    (59, 0, 1), // FetchSnapshotResponse
    (67, 0, 0), // AllocateProducerIdsRequest
    (85, 0, 1), // WriteShareGroupStateResponse
    (47, 0, 0), // OffsetDeleteResponse
    (88, 0, 0), // StreamsGroupHeartbeatResponse
    (45, 0, 1), // AlterPartitionReassignmentsRequest
    (57, 0, 2), // UpdateFeaturesResponse
    (76, 1, 1), // ShareGroupHeartbeatResponse
    (38, 1, 3), // CreateDelegationTokenRequest
    (39, 1, 2), // RenewDelegationTokenResponse
    (73, 0, 0), // AssignReplicasToDirsResponse
    (82, 0, 0), // UpdateRaftVoterRequest
    (49, 0, 1), // AlterClientQuotasRequest
    (88, 0, 0), // StreamsGroupHeartbeatRequest
    (2, 1, 11), // ListOffsetsRequest
    (11, 0, 9), // JoinGroupResponse
    (7, 0, 0),  // ControlledShutdownResponse
    (18, 0, 4), // ApiVersionsResponse
    (35, 1, 4), // DescribeLogDirsRequest
    (83, 0, 0), // InitializeShareGroupStateResponse
    (51, 0, 0), // AlterUserScramCredentialsResponse
    (37, 0, 3), // CreatePartitionsRequest
    (55, 0, 2), // DescribeQuorumResponse
    (78, 1, 2), // ShareFetchResponse
    (9, 1, 10), // OffsetFetchResponse
    (66, 0, 2), // ListTransactionsResponse
    (56, 2, 3), // AlterPartitionRequest
    (43, 0, 2), // ElectLeadersResponse
    (47, 0, 0), // OffsetDeleteRequest
    (84, 0, 0), // ReadShareGroupStateRequest
    (34, 1, 2), // AlterReplicaLogDirsResponse
    (21, 0, 2), // DeleteRecordsResponse
    (58, 0, 0), // EnvelopeResponse
    (87, 0, 1), // ReadShareGroupStateSummaryRequest
    (65, 0, 0), // DescribeTransactionsRequest
    (15, 0, 6), // DescribeGroupsResponse
    (33, 0, 2), // AlterConfigsResponse
    (65, 0, 0), // DescribeTransactionsResponse
    (43, 0, 2), // ElectLeadersRequest
    (44, 0, 1), // IncrementalAlterConfigsResponse
    (91, 0, 0), // AlterShareGroupOffsetsResponse
    (15, 0, 6), // DescribeGroupsRequest
    (7, 0, 0),  // ControlledShutdownRequest
    (61, 0, 0), // DescribeProducersResponse
    (68, 0, 1), // ConsumerGroupHeartbeatRequest
    (54, 0, 1), // EndQuorumEpochRequest
    (69, 0, 1), // ConsumerGroupDescribeRequest
    (20, 1, 6), // DeleteTopicsResponse
    (45, 0, 1), // AlterPartitionReassignmentsResponse
    (77, 1, 1), // ShareGroupDescribeRequest
    (28, 0, 5), // TxnOffsetCommitRequest
    (40, 1, 2), // ExpireDelegationTokenResponse
    (25, 0, 4), // AddOffsetsToTxnResponse
    (77, 1, 1), // ShareGroupDescribeResponse
    (41, 1, 3), // DescribeDelegationTokenRequest
    (27, 1, 2), // WriteTxnMarkersResponse
    (23, 2, 4), // OffsetForLeaderEpochResponse
    (56, 2, 3), // AlterPartitionResponse
    (48, 0, 1), // DescribeClientQuotasRequest
];

/// 获取指定 API 的支持版本范围
pub fn get_version_range(api_key: i16) -> Option<(i16, i16)> {
    CLIENT_SUPPORTED_VERSIONS
        .iter()
        .find(|&&(key, _, _)| key == api_key)
        .map(|&(_, min, max)| (min, max))
}

/// 检查是否支持指定版本
pub fn supports_version(api_key: i16, version: i16) -> bool {
    CLIENT_SUPPORTED_VERSIONS
        .iter()
        .find(|&&(key, _, _)| key == api_key)
        .map_or(false, |&(_, min, max)| version >= min && version <= max)
}

/// 返回指定 API 开始启用 flexible 编码/解码的版本号。
///
/// 这些值来自 `kafka-client-protocol/src/api/*_request/response.rs` 中各消息定义的
/// `flexible_versions` 属性。当前测试 broker 对 flexible 响应的支持不稳定，因此
/// 客户端在协商版本时会降到 `flex - 1` 的非 flexible 版本。
pub fn get_flexible_version(api_key: i16) -> Option<i16> {
    match api_key {
        0 => Some(9),  // Produce
        1 => Some(12), // Fetch
        2 => Some(6),  // ListOffsets
        3 => Some(9),  // Metadata
        8 => Some(8),  // OffsetCommit
        9 => Some(6),  // OffsetFetch
        10 => Some(3), // FindCoordinator
        11 => Some(6), // JoinGroup
        12 => Some(4), // Heartbeat
        13 => Some(4), // LeaveGroup
        14 => Some(4), // SyncGroup
        15 => Some(5), // DescribeGroups
        16 => Some(3), // ListGroups
        18 => Some(3), // ApiVersions
        19 => Some(5), // CreateTopics
        20 => Some(4), // DeleteTopics
        22 => Some(2), // InitProducerId
        25 => Some(3), // AddOffsetsToTxn
        26 => Some(3), // EndTxn
        28 => Some(3), // TxnOffsetCommit
        32 => Some(4), // DescribeConfigs
        33 => Some(2), // AlterConfigs
        36 => Some(2), // SaslAuthenticate
        37 => Some(3), // CreatePartitions
        42 => Some(2), // DeleteGroups
        44 => Some(1), // IncrementalAlterConfigs
        56 => Some(3), // AlterPartition
        61 => Some(0), // DescribeProducers (flexible from inception)
        _ => None,
    }
}
