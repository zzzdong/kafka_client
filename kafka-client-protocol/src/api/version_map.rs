//! API version mapping
//! DO NOT EDIT

/// 客户端支持的 API 版本范围（静态数组）
pub const CLIENT_SUPPORTED_VERSIONS: &[(i16, i16, i16)] = &[
    (25, 0, 4), // AddOffsetsToTxnRequest
    (25, 0, 4), // AddOffsetsToTxnResponse
    (24, 0, 5), // AddPartitionsToTxnRequest
    (24, 0, 5), // AddPartitionsToTxnResponse
    (80, 0, 1), // AddRaftVoterRequest
    (80, 0, 1), // AddRaftVoterResponse
    (67, 0, 0), // AllocateProducerIdsRequest
    (67, 0, 0), // AllocateProducerIdsResponse
    (49, 0, 1), // AlterClientQuotasRequest
    (49, 0, 1), // AlterClientQuotasResponse
    (33, 0, 2), // AlterConfigsRequest
    (33, 0, 2), // AlterConfigsResponse
    (45, 0, 1), // AlterPartitionReassignmentsRequest
    (45, 0, 1), // AlterPartitionReassignmentsResponse
    (56, 2, 3), // AlterPartitionRequest
    (56, 2, 3), // AlterPartitionResponse
    (34, 1, 2), // AlterReplicaLogDirsRequest
    (34, 1, 2), // AlterReplicaLogDirsResponse
    (91, 0, 0), // AlterShareGroupOffsetsRequest
    (91, 0, 0), // AlterShareGroupOffsetsResponse
    (51, 0, 0), // AlterUserScramCredentialsRequest
    (51, 0, 0), // AlterUserScramCredentialsResponse
    (18, 0, 4), // ApiVersionsRequest
    (18, 0, 4), // ApiVersionsResponse
    (73, 0, 0), // AssignReplicasToDirsRequest
    (73, 0, 0), // AssignReplicasToDirsResponse
    (53, 0, 1), // BeginQuorumEpochRequest
    (53, 0, 1), // BeginQuorumEpochResponse
    (63, 0, 2), // BrokerHeartbeatRequest
    (63, 0, 2), // BrokerHeartbeatResponse
    (62, 0, 4), // BrokerRegistrationRequest
    (62, 0, 4), // BrokerRegistrationResponse
    (69, 0, 1), // ConsumerGroupDescribeRequest
    (69, 0, 1), // ConsumerGroupDescribeResponse
    (68, 0, 1), // ConsumerGroupHeartbeatRequest
    (68, 0, 1), // ConsumerGroupHeartbeatResponse
    (7, 0, 0),  // ControlledShutdownRequest
    (7, 0, 0),  // ControlledShutdownResponse
    (70, 0, 0), // ControllerRegistrationRequest
    (70, 0, 0), // ControllerRegistrationResponse
    (30, 1, 3), // CreateAclsRequest
    (30, 1, 3), // CreateAclsResponse
    (38, 1, 3), // CreateDelegationTokenRequest
    (38, 1, 3), // CreateDelegationTokenResponse
    (37, 0, 3), // CreatePartitionsRequest
    (37, 0, 3), // CreatePartitionsResponse
    (19, 2, 7), // CreateTopicsRequest
    (19, 2, 7), // CreateTopicsResponse
    (31, 1, 3), // DeleteAclsRequest
    (31, 1, 3), // DeleteAclsResponse
    (42, 0, 2), // DeleteGroupsRequest
    (42, 0, 2), // DeleteGroupsResponse
    (21, 0, 2), // DeleteRecordsRequest
    (21, 0, 2), // DeleteRecordsResponse
    (92, 0, 0), // DeleteShareGroupOffsetsRequest
    (92, 0, 0), // DeleteShareGroupOffsetsResponse
    (86, 0, 0), // DeleteShareGroupStateRequest
    (86, 0, 0), // DeleteShareGroupStateResponse
    (20, 1, 6), // DeleteTopicsRequest
    (20, 1, 6), // DeleteTopicsResponse
    (29, 1, 3), // DescribeAclsRequest
    (29, 1, 3), // DescribeAclsResponse
    (48, 0, 1), // DescribeClientQuotasRequest
    (48, 0, 1), // DescribeClientQuotasResponse
    (60, 0, 2), // DescribeClusterRequest
    (60, 0, 2), // DescribeClusterResponse
    (32, 1, 4), // DescribeConfigsRequest
    (32, 1, 4), // DescribeConfigsResponse
    (41, 1, 3), // DescribeDelegationTokenRequest
    (41, 1, 3), // DescribeDelegationTokenResponse
    (15, 0, 6), // DescribeGroupsRequest
    (15, 0, 6), // DescribeGroupsResponse
    (35, 1, 5), // DescribeLogDirsRequest
    (35, 1, 5), // DescribeLogDirsResponse
    (61, 0, 0), // DescribeProducersRequest
    (61, 0, 0), // DescribeProducersResponse
    (55, 0, 2), // DescribeQuorumRequest
    (55, 0, 2), // DescribeQuorumResponse
    (90, 0, 1), // DescribeShareGroupOffsetsRequest
    (90, 0, 1), // DescribeShareGroupOffsetsResponse
    (75, 0, 0), // DescribeTopicPartitionsRequest
    (75, 0, 0), // DescribeTopicPartitionsResponse
    (65, 0, 0), // DescribeTransactionsRequest
    (65, 0, 0), // DescribeTransactionsResponse
    (43, 0, 2), // ElectLeadersRequest
    (50, 0, 0), // DescribeUserScramCredentialsRequest
    (50, 0, 0), // DescribeUserScramCredentialsResponse
    (43, 0, 2), // ElectLeadersResponse
    (54, 0, 1), // EndQuorumEpochRequest
    (54, 0, 1), // EndQuorumEpochResponse
    (26, 0, 5), // EndTxnRequest
    (26, 0, 5), // EndTxnResponse
    (58, 0, 0), // EnvelopeRequest
    (58, 0, 0), // EnvelopeResponse
    (40, 1, 2), // ExpireDelegationTokenRequest
    (40, 1, 2), // ExpireDelegationTokenResponse
    (1, 4, 18), // FetchRequest
    (1, 4, 18), // FetchResponse
    (59, 0, 1), // FetchSnapshotRequest
    (59, 0, 1), // FetchSnapshotResponse
    (10, 0, 6), // FindCoordinatorRequest
    (10, 0, 6), // FindCoordinatorResponse
    (71, 0, 0), // GetTelemetrySubscriptionsRequest
    (71, 0, 0), // GetTelemetrySubscriptionsResponse
    (12, 0, 4), // HeartbeatRequest
    (12, 0, 4), // HeartbeatResponse
    (44, 0, 1), // IncrementalAlterConfigsRequest
    (44, 0, 1), // IncrementalAlterConfigsResponse
    (83, 0, 0), // InitializeShareGroupStateRequest
    (83, 0, 0), // InitializeShareGroupStateResponse
    (22, 0, 6), // InitProducerIdRequest
    (22, 0, 6), // InitProducerIdResponse
    (11, 0, 9), // JoinGroupRequest
    (11, 0, 9), // JoinGroupResponse
    (4, 0, 0),  // LeaderAndIsrRequest
    (4, 0, 0),  // LeaderAndIsrResponse
    (13, 0, 5), // LeaveGroupRequest
    (13, 0, 5), // LeaveGroupResponse
    (74, 0, 1), // ListConfigResourcesRequest
    (74, 0, 1), // ListConfigResourcesResponse
    (16, 0, 5), // ListGroupsRequest
    (16, 0, 5), // ListGroupsResponse
    (2, 1, 11), // ListOffsetsRequest
    (2, 1, 11), // ListOffsetsResponse
    (46, 0, 0), // ListPartitionReassignmentsRequest
    (46, 0, 0), // ListPartitionReassignmentsResponse
    (66, 0, 2), // ListTransactionsRequest
    (66, 0, 2), // ListTransactionsResponse
    (3, 0, 13), // MetadataRequest
    (3, 0, 13), // MetadataResponse
    (8, 2, 10), // OffsetCommitRequest
    (8, 2, 10), // OffsetCommitResponse
    (47, 0, 0), // OffsetDeleteRequest
    (47, 0, 0), // OffsetDeleteResponse
    (9, 1, 10), // OffsetFetchRequest
    (9, 1, 10), // OffsetFetchResponse
    (23, 2, 4), // OffsetForLeaderEpochRequest
    (23, 2, 4), // OffsetForLeaderEpochResponse
    (0, 3, 13), // ProduceRequest
    (0, 3, 13), // ProduceResponse
    (72, 0, 0), // PushTelemetryRequest
    (72, 0, 0), // PushTelemetryResponse
    (84, 0, 0), // ReadShareGroupStateRequest
    (84, 0, 0), // ReadShareGroupStateResponse
    (87, 0, 1), // ReadShareGroupStateSummaryRequest
    (87, 0, 1), // ReadShareGroupStateSummaryResponse
    (81, 0, 0), // RemoveRaftVoterRequest
    (81, 0, 0), // RemoveRaftVoterResponse
    (39, 1, 2), // RenewDelegationTokenRequest
    (39, 1, 2), // RenewDelegationTokenResponse
    (36, 0, 2), // SaslAuthenticateRequest
    (36, 0, 2), // SaslAuthenticateResponse
    (17, 0, 1), // SaslHandshakeRequest
    (17, 0, 1), // SaslHandshakeResponse
    (79, 1, 2), // ShareAcknowledgeRequest
    (79, 1, 2), // ShareAcknowledgeResponse
    (78, 1, 2), // ShareFetchRequest
    (78, 1, 2), // ShareFetchResponse
    (77, 1, 1), // ShareGroupDescribeRequest
    (77, 1, 1), // ShareGroupDescribeResponse
    (76, 1, 1), // ShareGroupHeartbeatRequest
    (76, 1, 1), // ShareGroupHeartbeatResponse
    (5, 0, 0),  // StopReplicaRequest
    (5, 0, 0),  // StopReplicaResponse
    (89, 0, 0), // StreamsGroupDescribeRequest
    (89, 0, 0), // StreamsGroupDescribeResponse
    (88, 0, 0), // StreamsGroupHeartbeatRequest
    (88, 0, 0), // StreamsGroupHeartbeatResponse
    (14, 0, 5), // SyncGroupRequest
    (14, 0, 5), // SyncGroupResponse
    (28, 0, 5), // TxnOffsetCommitRequest
    (52, 0, 2), // VoteRequest
    (28, 0, 5), // TxnOffsetCommitResponse
    (64, 0, 0), // UnregisterBrokerRequest
    (64, 0, 0), // UnregisterBrokerResponse
    (57, 0, 2), // UpdateFeaturesRequest
    (57, 0, 2), // UpdateFeaturesResponse
    (6, 0, 0),  // UpdateMetadataRequest
    (6, 0, 0),  // UpdateMetadataResponse
    (82, 0, 0), // UpdateRaftVoterRequest
    (82, 0, 0), // UpdateRaftVoterResponse
    (52, 0, 2), // VoteResponse
    (85, 0, 1), // WriteShareGroupStateRequest
    (85, 0, 1), // WriteShareGroupStateResponse
    (27, 1, 2), // WriteTxnMarkersRequest
    (27, 1, 2), // WriteTxnMarkersResponse
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
