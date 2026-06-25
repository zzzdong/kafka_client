//! API version mapping
//! DO NOT EDIT

/// 客户端支持的 API 版本范围（静态数组）
pub const CLIENT_SUPPORTED_VERSIONS: &[(i16, i16, i16)] = &[
    (0, 3, 13), // ProduceRequest
    (0, 3, 13), // ProduceResponse
    (1, 4, 18), // FetchRequest
    (1, 4, 18), // FetchResponse
    (2, 1, 11), // ListOffsetsResponse
    (2, 1, 11), // ListOffsetsRequest
    (3, 0, 13), // MetadataResponse
    (3, 0, 13), // MetadataRequest
    (4, 0, 0),  // LeaderAndIsrRequest
    (4, 0, 0),  // LeaderAndIsrResponse
    (5, 0, 0),  // StopReplicaResponse
    (5, 0, 0),  // StopReplicaRequest
    (6, 0, 0),  // UpdateMetadataRequest
    (6, 0, 0),  // UpdateMetadataResponse
    (7, 0, 0),  // ControlledShutdownResponse
    (7, 0, 0),  // ControlledShutdownRequest
    (8, 2, 10), // OffsetCommitRequest
    (8, 2, 10), // OffsetCommitResponse
    (9, 1, 10), // OffsetFetchRequest
    (9, 1, 10), // OffsetFetchResponse
    (10, 0, 6), // FindCoordinatorRequest
    (10, 0, 6), // FindCoordinatorResponse
    (11, 0, 9), // JoinGroupRequest
    (11, 0, 9), // JoinGroupResponse
    (12, 0, 4), // HeartbeatRequest
    (12, 0, 4), // HeartbeatResponse
    (13, 0, 5), // LeaveGroupResponse
    (13, 0, 5), // LeaveGroupRequest
    (14, 0, 5), // SyncGroupResponse
    (14, 0, 5), // SyncGroupRequest
    (15, 0, 6), // DescribeGroupsResponse
    (15, 0, 6), // DescribeGroupsRequest
    (16, 0, 5), // ListGroupsRequest
    (16, 0, 5), // ListGroupsResponse
    (17, 0, 1), // SaslHandshakeRequest
    (17, 0, 1), // SaslHandshakeResponse
    (18, 0, 4), // ApiVersionsRequest
    (18, 0, 4), // ApiVersionsResponse
    (19, 2, 7), // CreateTopicsRequest
    (19, 2, 7), // CreateTopicsResponse
    (20, 1, 6), // DeleteTopicsRequest
    (20, 1, 6), // DeleteTopicsResponse
    (21, 0, 2), // DeleteRecordsRequest
    (21, 0, 2), // DeleteRecordsResponse
    (22, 0, 6), // InitProducerIdResponse
    (22, 0, 6), // InitProducerIdRequest
    (23, 2, 4), // OffsetForLeaderEpochRequest
    (23, 2, 4), // OffsetForLeaderEpochResponse
    (24, 0, 5), // AddPartitionsToTxnResponse
    (24, 0, 5), // AddPartitionsToTxnRequest
    (25, 0, 4), // AddOffsetsToTxnRequest
    (25, 0, 4), // AddOffsetsToTxnResponse
    (26, 0, 5), // EndTxnRequest
    (26, 0, 5), // EndTxnResponse
    (27, 1, 2), // WriteTxnMarkersRequest
    (27, 1, 2), // WriteTxnMarkersResponse
    (28, 0, 5), // TxnOffsetCommitResponse
    (28, 0, 5), // TxnOffsetCommitRequest
    (29, 1, 3), // DescribeAclsRequest
    (29, 1, 3), // DescribeAclsResponse
    (30, 1, 3), // CreateAclsResponse
    (30, 1, 3), // CreateAclsRequest
    (31, 1, 3), // DeleteAclsResponse
    (31, 1, 3), // DeleteAclsRequest
    (32, 1, 4), // DescribeConfigsRequest
    (32, 1, 4), // DescribeConfigsResponse
    (33, 0, 2), // AlterConfigsRequest
    (33, 0, 2), // AlterConfigsResponse
    (34, 1, 2), // AlterReplicaLogDirsRequest
    (34, 1, 2), // AlterReplicaLogDirsResponse
    (35, 1, 5), // DescribeLogDirsResponse
    (35, 1, 5), // DescribeLogDirsRequest
    (36, 0, 2), // SaslAuthenticateResponse
    (36, 0, 2), // SaslAuthenticateRequest
    (37, 0, 3), // CreatePartitionsResponse
    (37, 0, 3), // CreatePartitionsRequest
    (38, 1, 3), // CreateDelegationTokenResponse
    (38, 1, 3), // CreateDelegationTokenRequest
    (39, 1, 2), // RenewDelegationTokenRequest
    (39, 1, 2), // RenewDelegationTokenResponse
    (40, 1, 2), // ExpireDelegationTokenRequest
    (40, 1, 2), // ExpireDelegationTokenResponse
    (41, 1, 3), // DescribeDelegationTokenResponse
    (41, 1, 3), // DescribeDelegationTokenRequest
    (42, 0, 2), // DeleteGroupsResponse
    (42, 0, 2), // DeleteGroupsRequest
    (43, 0, 2), // ElectLeadersResponse
    (43, 0, 2), // ElectLeadersRequest
    (44, 0, 1), // IncrementalAlterConfigsRequest
    (44, 0, 1), // IncrementalAlterConfigsResponse
    (45, 0, 1), // AlterPartitionReassignmentsRequest
    (45, 0, 1), // AlterPartitionReassignmentsResponse
    (46, 0, 0), // ListPartitionReassignmentsRequest
    (46, 0, 0), // ListPartitionReassignmentsResponse
    (47, 0, 0), // OffsetDeleteResponse
    (47, 0, 0), // OffsetDeleteRequest
    (48, 0, 1), // DescribeClientQuotasResponse
    (48, 0, 1), // DescribeClientQuotasRequest
    (49, 0, 1), // AlterClientQuotasResponse
    (49, 0, 1), // AlterClientQuotasRequest
    (50, 0, 0), // DescribeUserScramCredentialsResponse
    (50, 0, 0), // DescribeUserScramCredentialsRequest
    (51, 0, 0), // AlterUserScramCredentialsRequest
    (51, 0, 0), // AlterUserScramCredentialsResponse
    (52, 0, 2), // VoteRequest
    (52, 0, 2), // VoteResponse
    (53, 0, 1), // BeginQuorumEpochResponse
    (53, 0, 1), // BeginQuorumEpochRequest
    (54, 0, 1), // EndQuorumEpochResponse
    (54, 0, 1), // EndQuorumEpochRequest
    (55, 0, 2), // DescribeQuorumRequest
    (55, 0, 2), // DescribeQuorumResponse
    (56, 2, 3), // AlterPartitionRequest
    (56, 2, 3), // AlterPartitionResponse
    (57, 0, 2), // UpdateFeaturesRequest
    (57, 0, 2), // UpdateFeaturesResponse
    (58, 0, 0), // EnvelopeRequest
    (58, 0, 0), // EnvelopeResponse
    (59, 0, 1), // FetchSnapshotRequest
    (59, 0, 1), // FetchSnapshotResponse
    (60, 0, 2), // DescribeClusterRequest
    (60, 0, 2), // DescribeClusterResponse
    (61, 0, 0), // DescribeProducersRequest
    (61, 0, 0), // DescribeProducersResponse
    (62, 0, 4), // BrokerRegistrationRequest
    (62, 0, 4), // BrokerRegistrationResponse
    (63, 0, 2), // BrokerHeartbeatResponse
    (63, 0, 2), // BrokerHeartbeatRequest
    (64, 0, 0), // UnregisterBrokerRequest
    (64, 0, 0), // UnregisterBrokerResponse
    (65, 0, 0), // DescribeTransactionsRequest
    (65, 0, 0), // DescribeTransactionsResponse
    (66, 0, 2), // ListTransactionsRequest
    (66, 0, 2), // ListTransactionsResponse
    (67, 0, 0), // AllocateProducerIdsResponse
    (67, 0, 0), // AllocateProducerIdsRequest
    (68, 0, 1), // ConsumerGroupHeartbeatResponse
    (68, 0, 1), // ConsumerGroupHeartbeatRequest
    (69, 0, 1), // ConsumerGroupDescribeResponse
    (69, 0, 1), // ConsumerGroupDescribeRequest
    (70, 0, 0), // ControllerRegistrationRequest
    (70, 0, 0), // ControllerRegistrationResponse
    (71, 0, 0), // GetTelemetrySubscriptionsRequest
    (71, 0, 0), // GetTelemetrySubscriptionsResponse
    (72, 0, 0), // PushTelemetryResponse
    (72, 0, 0), // PushTelemetryRequest
    (73, 0, 0), // AssignReplicasToDirsRequest
    (73, 0, 0), // AssignReplicasToDirsResponse
    (74, 0, 1), // ListConfigResourcesResponse
    (74, 0, 1), // ListConfigResourcesRequest
    (75, 0, 0), // DescribeTopicPartitionsResponse
    (75, 0, 0), // DescribeTopicPartitionsRequest
    (76, 1, 1), // ShareGroupHeartbeatRequest
    (76, 1, 1), // ShareGroupHeartbeatResponse
    (77, 1, 1), // ShareGroupDescribeRequest
    (77, 1, 1), // ShareGroupDescribeResponse
    (78, 1, 2), // ShareFetchRequest
    (78, 1, 2), // ShareFetchResponse
    (79, 1, 2), // ShareAcknowledgeResponse
    (79, 1, 2), // ShareAcknowledgeRequest
    (80, 0, 1), // AddRaftVoterRequest
    (80, 0, 1), // AddRaftVoterResponse
    (81, 0, 0), // RemoveRaftVoterResponse
    (81, 0, 0), // RemoveRaftVoterRequest
    (82, 0, 0), // UpdateRaftVoterResponse
    (82, 0, 0), // UpdateRaftVoterRequest
    (83, 0, 0), // InitializeShareGroupStateRequest
    (83, 0, 0), // InitializeShareGroupStateResponse
    (84, 0, 0), // ReadShareGroupStateResponse
    (84, 0, 0), // ReadShareGroupStateRequest
    (85, 0, 1), // WriteShareGroupStateRequest
    (85, 0, 1), // WriteShareGroupStateResponse
    (86, 0, 0), // DeleteShareGroupStateRequest
    (86, 0, 0), // DeleteShareGroupStateResponse
    (87, 0, 1), // ReadShareGroupStateSummaryResponse
    (87, 0, 1), // ReadShareGroupStateSummaryRequest
    (88, 0, 0), // StreamsGroupHeartbeatResponse
    (88, 0, 0), // StreamsGroupHeartbeatRequest
    (89, 0, 0), // StreamsGroupDescribeResponse
    (89, 0, 0), // StreamsGroupDescribeRequest
    (90, 0, 1), // DescribeShareGroupOffsetsResponse
    (90, 0, 1), // DescribeShareGroupOffsetsRequest
    (91, 0, 0), // AlterShareGroupOffsetsRequest
    (91, 0, 0), // AlterShareGroupOffsetsResponse
    (92, 0, 0), // DeleteShareGroupOffsetsRequest
    (92, 0, 0), // DeleteShareGroupOffsetsResponse
];

/// 获取指定 API 的支持版本范围
pub fn get_version_range(api_key: i16) -> Option<(i16, i16)> {
    CLIENT_SUPPORTED_VERSIONS
        .binary_search_by_key(&api_key, |&(key, _, _)| key)
        .ok()
        .map(|idx| {
            let (_, min, max) = CLIENT_SUPPORTED_VERSIONS[idx];
            (min, max)
        })
}

/// 检查是否支持指定版本
pub fn supports_version(api_key: i16, version: i16) -> bool {
    CLIENT_SUPPORTED_VERSIONS
        .binary_search_by_key(&api_key, |&(key, _, _)| key)
        .ok()
        .map(|idx| {
            let (_, min, max) = CLIENT_SUPPORTED_VERSIONS[idx];
            version >= min && version <= max
        })
        .unwrap_or(false)
}
