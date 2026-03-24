// src/downloader.rs
//! Kafka 协议定义下载器

use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

const KAFKA_GITHUB_RAW: &str = "https://raw.githubusercontent.com/apache/kafka";

/// 下载错误类型
#[derive(Debug, Error)]
pub enum DownloadError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Invalid version: {0}")]
    InvalidVersion(String),

    #[error("No files downloaded")]
    NoFilesDownloaded,
}

/// API 定义（包含请求和响应文件名）
#[derive(Debug, Clone)]
pub struct ApiDefinition {
    /// API 基础名称（如 "Produce"）
    pub base_name: &'static str,
    /// 请求文件名（如 "ProduceRequest.json"）
    pub request_file: &'static str,
    /// 响应文件名（如 "ProduceResponse.json"）
    pub response_file: Option<&'static str>,
    /// API Key
    pub api_key: Option<u16>,
}

impl ApiDefinition {
    /// 创建标准 API（有 Request 和 Response）
    pub const fn standard(
        base_name: &'static str,
        request_file: &'static str,
        response_file: &'static str,
        api_key: Option<u16>,
    ) -> Self {
        Self {
            base_name,
            request_file,
            response_file: Some(response_file),
            api_key,
        }
    }

    /// 创建单文件定义（只有一个 .json 文件）
    pub const fn single_file(name: &'static str) -> Self {
        Self {
            base_name: name,
            request_file: name,
            response_file: None,
            api_key: None,
        }
    }

    /// 获取所有需要下载的文件名
    pub fn files_to_download(&self) -> Vec<&'static str> {
        let mut files = vec![self.request_file];
        if let Some(resp) = self.response_file {
            files.push(resp);
        }
        files
    }
}

/// 完整的 Kafka API 列表（基于 Kafka 4.2 实际文件）
pub const KNOWN_APIS: &[ApiDefinition] = &[
    // ============ 标准 API（有 Request + Response）============
    ApiDefinition::standard(
        "AddOffsetsToTxn",
        "AddOffsetsToTxnRequest.json",
        "AddOffsetsToTxnResponse.json",
        Some(25),
    ),
    ApiDefinition::standard(
        "AddPartitionsToTxn",
        "AddPartitionsToTxnRequest.json",
        "AddPartitionsToTxnResponse.json",
        Some(24),
    ),
    ApiDefinition::standard(
        "AddRaftVoter",
        "AddRaftVoterRequest.json",
        "AddRaftVoterResponse.json",
        Some(80),
    ),
    ApiDefinition::standard(
        "AllocateProducerIds",
        "AllocateProducerIdsRequest.json",
        "AllocateProducerIdsResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "AlterClientQuotas",
        "AlterClientQuotasRequest.json",
        "AlterClientQuotasResponse.json",
        Some(49),
    ),
    ApiDefinition::standard(
        "AlterConfigs",
        "AlterConfigsRequest.json",
        "AlterConfigsResponse.json",
        Some(33),
    ),
    ApiDefinition::standard(
        "AlterPartition",
        "AlterPartitionRequest.json",
        "AlterPartitionResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "AlterPartitionReassignments",
        "AlterPartitionReassignmentsRequest.json",
        "AlterPartitionReassignmentsResponse.json",
        Some(45),
    ),
    ApiDefinition::standard(
        "AlterReplicaLogDirs",
        "AlterReplicaLogDirsRequest.json",
        "AlterReplicaLogDirsResponse.json",
        Some(34),
    ),
    ApiDefinition::standard(
        "AlterShareGroupOffsets",
        "AlterShareGroupOffsetsRequest.json",
        "AlterShareGroupOffsetsResponse.json",
        Some(91),
    ),
    ApiDefinition::standard(
        "AlterUserScramCredentials",
        "AlterUserScramCredentialsRequest.json",
        "AlterUserScramCredentialsResponse.json",
        Some(51),
    ),
    ApiDefinition::standard(
        "ApiVersions",
        "ApiVersionsRequest.json",
        "ApiVersionsResponse.json",
        Some(18),
    ),
    ApiDefinition::standard(
        "AssignReplicasToDirs",
        "AssignReplicasToDirsRequest.json",
        "AssignReplicasToDirsResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "BeginQuorumEpoch",
        "BeginQuorumEpochRequest.json",
        "BeginQuorumEpochResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "BrokerHeartbeat",
        "BrokerHeartbeatRequest.json",
        "BrokerHeartbeatResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "BrokerRegistration",
        "BrokerRegistrationRequest.json",
        "BrokerRegistrationResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "ConsumerGroupDescribe",
        "ConsumerGroupDescribeRequest.json",
        "ConsumerGroupDescribeResponse.json",
        Some(69),
    ),
    ApiDefinition::standard(
        "ConsumerGroupHeartbeat",
        "ConsumerGroupHeartbeatRequest.json",
        "ConsumerGroupHeartbeatResponse.json",
        Some(68),
    ),
    ApiDefinition::standard(
        "ControlledShutdown",
        "ControlledShutdownRequest.json",
        "ControlledShutdownResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "ControllerRegistration",
        "ControllerRegistrationRequest.json",
        "ControllerRegistrationResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "CreateAcls",
        "CreateAclsRequest.json",
        "CreateAclsResponse.json",
        Some(30),
    ),
    ApiDefinition::standard(
        "CreateDelegationToken",
        "CreateDelegationTokenRequest.json",
        "CreateDelegationTokenResponse.json",
        Some(38),
    ),
    ApiDefinition::standard(
        "CreatePartitions",
        "CreatePartitionsRequest.json",
        "CreatePartitionsResponse.json",
        Some(37),
    ),
    ApiDefinition::standard(
        "CreateTopics",
        "CreateTopicsRequest.json",
        "CreateTopicsResponse.json",
        Some(19),
    ),
    ApiDefinition::standard(
        "DeleteAcls",
        "DeleteAclsRequest.json",
        "DeleteAclsResponse.json",
        Some(31),
    ),
    ApiDefinition::standard(
        "DeleteGroups",
        "DeleteGroupsRequest.json",
        "DeleteGroupsResponse.json",
        Some(42),
    ),
    ApiDefinition::standard(
        "DeleteRecords",
        "DeleteRecordsRequest.json",
        "DeleteRecordsResponse.json",
        Some(21),
    ),
    ApiDefinition::standard(
        "DeleteShareGroupOffsets",
        "DeleteShareGroupOffsetsRequest.json",
        "DeleteShareGroupOffsetsResponse.json",
        Some(92),
    ),
    ApiDefinition::standard(
        "DeleteShareGroupState",
        "DeleteShareGroupStateRequest.json",
        "DeleteShareGroupStateResponse.json",
        Some(86),
    ),
    ApiDefinition::standard(
        "DeleteTopics",
        "DeleteTopicsRequest.json",
        "DeleteTopicsResponse.json",
        Some(20),
    ),
    ApiDefinition::standard(
        "DescribeAcls",
        "DescribeAclsRequest.json",
        "DescribeAclsResponse.json",
        Some(29),
    ),
    ApiDefinition::standard(
        "DescribeClientQuotas",
        "DescribeClientQuotasRequest.json",
        "DescribeClientQuotasResponse.json",
        Some(48),
    ),
    ApiDefinition::standard(
        "DescribeCluster",
        "DescribeClusterRequest.json",
        "DescribeClusterResponse.json",
        Some(60),
    ),
    ApiDefinition::standard(
        "DescribeConfigs",
        "DescribeConfigsRequest.json",
        "DescribeConfigsResponse.json",
        Some(32),
    ),
    ApiDefinition::standard(
        "DescribeDelegationToken",
        "DescribeDelegationTokenRequest.json",
        "DescribeDelegationTokenResponse.json",
        Some(41),
    ),
    ApiDefinition::standard(
        "DescribeGroups",
        "DescribeGroupsRequest.json",
        "DescribeGroupsResponse.json",
        Some(15),
    ),
    ApiDefinition::standard(
        "DescribeLogDirs",
        "DescribeLogDirsRequest.json",
        "DescribeLogDirsResponse.json",
        Some(35),
    ),
    ApiDefinition::standard(
        "DescribeProducers",
        "DescribeProducersRequest.json",
        "DescribeProducersResponse.json",
        Some(61),
    ),
    ApiDefinition::standard(
        "DescribeQuorum",
        "DescribeQuorumRequest.json",
        "DescribeQuorumResponse.json",
        Some(55),
    ),
    ApiDefinition::standard(
        "DescribeShareGroupOffsets",
        "DescribeShareGroupOffsetsRequest.json",
        "DescribeShareGroupOffsetsResponse.json",
        Some(90),
    ),
    ApiDefinition::standard(
        "DescribeTopicPartitions",
        "DescribeTopicPartitionsRequest.json",
        "DescribeTopicPartitionsResponse.json",
        Some(75),
    ),
    ApiDefinition::standard(
        "DescribeTransactions",
        "DescribeTransactionsRequest.json",
        "DescribeTransactionsResponse.json",
        Some(65),
    ),
    ApiDefinition::standard(
        "DescribeUserScramCredentials",
        "DescribeUserScramCredentialsRequest.json",
        "DescribeUserScramCredentialsResponse.json",
        Some(50),
    ),
    ApiDefinition::standard(
        "ElectLeaders",
        "ElectLeadersRequest.json",
        "ElectLeadersResponse.json",
        Some(43),
    ),
    ApiDefinition::standard(
        "EndQuorumEpoch",
        "EndQuorumEpochRequest.json",
        "EndQuorumEpochResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "EndTxn",
        "EndTxnRequest.json",
        "EndTxnResponse.json",
        Some(26),
    ),
    ApiDefinition::standard(
        "Envelope",
        "EnvelopeRequest.json",
        "EnvelopeResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "ExpireDelegationToken",
        "ExpireDelegationTokenRequest.json",
        "ExpireDelegationTokenResponse.json",
        Some(40),
    ),
    ApiDefinition::standard("Fetch", "FetchRequest.json", "FetchResponse.json", Some(1)),
    ApiDefinition::standard(
        "FetchSnapshot",
        "FetchSnapshotRequest.json",
        "FetchSnapshotResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "FindCoordinator",
        "FindCoordinatorRequest.json",
        "FindCoordinatorResponse.json",
        Some(10),
    ),
    ApiDefinition::standard(
        "GetTelemetrySubscriptions",
        "GetTelemetrySubscriptionsRequest.json",
        "GetTelemetrySubscriptionsResponse.json",
        Some(71),
    ),
    ApiDefinition::standard(
        "Heartbeat",
        "HeartbeatRequest.json",
        "HeartbeatResponse.json",
        Some(12),
    ),
    ApiDefinition::standard(
        "IncrementalAlterConfigs",
        "IncrementalAlterConfigsRequest.json",
        "IncrementalAlterConfigsResponse.json",
        Some(44),
    ),
    ApiDefinition::standard(
        "InitProducerId",
        "InitProducerIdRequest.json",
        "InitProducerIdResponse.json",
        Some(22),
    ),
    ApiDefinition::standard(
        "InitializeShareGroupState",
        "InitializeShareGroupStateRequest.json",
        "InitializeShareGroupStateResponse.json",
        Some(83),
    ),
    ApiDefinition::standard(
        "JoinGroup",
        "JoinGroupRequest.json",
        "JoinGroupResponse.json",
        Some(11),
    ),
    ApiDefinition::standard(
        "LeaderAndIsr",
        "LeaderAndIsrRequest.json",
        "LeaderAndIsrResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "LeaveGroup",
        "LeaveGroupRequest.json",
        "LeaveGroupResponse.json",
        Some(13),
    ),
    ApiDefinition::standard(
        "ListConfigResources",
        "ListConfigResourcesRequest.json",
        "ListConfigResourcesResponse.json",
        Some(74),
    ),
    ApiDefinition::standard(
        "ListGroups",
        "ListGroupsRequest.json",
        "ListGroupsResponse.json",
        Some(16),
    ),
    ApiDefinition::standard(
        "ListOffsets",
        "ListOffsetsRequest.json",
        "ListOffsetsResponse.json",
        Some(2),
    ),
    ApiDefinition::standard(
        "ListPartitionReassignments",
        "ListPartitionReassignmentsRequest.json",
        "ListPartitionReassignmentsResponse.json",
        Some(46),
    ),
    ApiDefinition::standard(
        "ListTransactions",
        "ListTransactionsRequest.json",
        "ListTransactionsResponse.json",
        Some(66),
    ),
    ApiDefinition::standard(
        "Metadata",
        "MetadataRequest.json",
        "MetadataResponse.json",
        Some(3),
    ),
    ApiDefinition::standard(
        "OffsetCommit",
        "OffsetCommitRequest.json",
        "OffsetCommitResponse.json",
        Some(8),
    ),
    ApiDefinition::standard(
        "OffsetDelete",
        "OffsetDeleteRequest.json",
        "OffsetDeleteResponse.json",
        Some(47),
    ),
    ApiDefinition::standard(
        "OffsetFetch",
        "OffsetFetchRequest.json",
        "OffsetFetchResponse.json",
        Some(9),
    ),
    ApiDefinition::standard(
        "OffsetForLeaderEpoch",
        "OffsetForLeaderEpochRequest.json",
        "OffsetForLeaderEpochResponse.json",
        Some(23),
    ),
    ApiDefinition::standard(
        "Produce",
        "ProduceRequest.json",
        "ProduceResponse.json",
        Some(0),
    ),
    ApiDefinition::standard(
        "PushTelemetry",
        "PushTelemetryRequest.json",
        "PushTelemetryResponse.json",
        Some(72),
    ),
    ApiDefinition::standard(
        "ReadShareGroupState",
        "ReadShareGroupStateRequest.json",
        "ReadShareGroupStateResponse.json",
        Some(84),
    ),
    ApiDefinition::standard(
        "ReadShareGroupStateSummary",
        "ReadShareGroupStateSummaryRequest.json",
        "ReadShareGroupStateSummaryResponse.json",
        Some(87),
    ),
    ApiDefinition::standard(
        "RemoveRaftVoter",
        "RemoveRaftVoterRequest.json",
        "RemoveRaftVoterResponse.json",
        Some(81),
    ),
    ApiDefinition::standard(
        "RenewDelegationToken",
        "RenewDelegationTokenRequest.json",
        "RenewDelegationTokenResponse.json",
        Some(39),
    ),
    ApiDefinition::standard(
        "SaslAuthenticate",
        "SaslAuthenticateRequest.json",
        "SaslAuthenticateResponse.json",
        Some(36),
    ),
    ApiDefinition::standard(
        "SaslHandshake",
        "SaslHandshakeRequest.json",
        "SaslHandshakeResponse.json",
        Some(17),
    ),
    ApiDefinition::standard(
        "ShareAcknowledge",
        "ShareAcknowledgeRequest.json",
        "ShareAcknowledgeResponse.json",
        Some(79),
    ),
    ApiDefinition::standard(
        "ShareFetch",
        "ShareFetchRequest.json",
        "ShareFetchResponse.json",
        Some(78),
    ),
    ApiDefinition::standard(
        "ShareGroupDescribe",
        "ShareGroupDescribeRequest.json",
        "ShareGroupDescribeResponse.json",
        Some(77),
    ),
    ApiDefinition::standard(
        "ShareGroupHeartbeat",
        "ShareGroupHeartbeatRequest.json",
        "ShareGroupHeartbeatResponse.json",
        Some(76),
    ),
    ApiDefinition::standard(
        "StopReplica",
        "StopReplicaRequest.json",
        "StopReplicaResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "StreamsGroupDescribe",
        "StreamsGroupDescribeRequest.json",
        "StreamsGroupDescribeResponse.json",
        Some(89),
    ),
    ApiDefinition::standard(
        "StreamsGroupHeartbeat",
        "StreamsGroupHeartbeatRequest.json",
        "StreamsGroupHeartbeatResponse.json",
        Some(88),
    ),
    ApiDefinition::standard(
        "SyncGroup",
        "SyncGroupRequest.json",
        "SyncGroupResponse.json",
        Some(14),
    ),
    ApiDefinition::standard(
        "TxnOffsetCommit",
        "TxnOffsetCommitRequest.json",
        "TxnOffsetCommitResponse.json",
        Some(28),
    ),
    ApiDefinition::standard(
        "UnregisterBroker",
        "UnregisterBrokerRequest.json",
        "UnregisterBrokerResponse.json",
        Some(64),
    ),
    ApiDefinition::standard(
        "UpdateFeatures",
        "UpdateFeaturesRequest.json",
        "UpdateFeaturesResponse.json",
        Some(57),
    ),
    ApiDefinition::standard(
        "UpdateMetadata",
        "UpdateMetadataRequest.json",
        "UpdateMetadataResponse.json",
        None,
    ),
    ApiDefinition::standard(
        "UpdateRaftVoter",
        "UpdateRaftVoterRequest.json",
        "UpdateRaftVoterResponse.json",
        None,
    ),
    ApiDefinition::standard("Vote", "VoteRequest.json", "VoteResponse.json", None),
    ApiDefinition::standard(
        "WriteShareGroupState",
        "WriteShareGroupStateRequest.json",
        "WriteShareGroupStateResponse.json",
        Some(85),
    ),
    ApiDefinition::standard(
        "WriteTxnMarkers",
        "WriteTxnMarkersRequest.json",
        "WriteTxnMarkersResponse.json",
        Some(27),
    ),
    // ============ 单文件记录（只有单个 .json 文件）============
    ApiDefinition::single_file("ConsumerProtocolAssignment.json"),
    ApiDefinition::single_file("ConsumerProtocolSubscription.json"),
    ApiDefinition::single_file("DefaultPrincipalData.json"),
    ApiDefinition::single_file("EndTxnMarker.json"),
    ApiDefinition::single_file("KRaftVersionRecord.json"),
    ApiDefinition::single_file("LeaderChangeMessage.json"),
    ApiDefinition::single_file("SnapshotFooterRecord.json"),
    ApiDefinition::single_file("SnapshotHeaderRecord.json"),
    ApiDefinition::single_file("VotersRecord.json"),
    // ============ 请求/响应头 ============
    ApiDefinition::single_file("RequestHeader.json"),
    ApiDefinition::single_file("ResponseHeader.json"),
];

/// 下载配置
#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub version: String,
    pub output_dir: PathBuf,
    pub force: bool,
    pub apis: Option<Vec<String>>, // 如果指定，只下载这些 API 的基础名称
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            version: "4.2.0".to_string(),
            output_dir: PathBuf::from("protocol-definitions"),
            force: false,
            apis: None,
        }
    }
}

/// 下载 Kafka 协议定义
pub fn download_protocol_definitions(config: &DownloadConfig) -> Result<PathBuf, DownloadError> {
    let proto_dir = &config.output_dir;

    if proto_dir.exists() && !config.force {
        return Err(DownloadError::InvalidVersion(format!(
            "Directory already exists: {:?}. Use --force to overwrite.",
            proto_dir
        )));
    }

    fs::create_dir_all(&proto_dir)?;

    // 确定要下载的 API
    let apis_to_download: Vec<&ApiDefinition> = if let Some(ref api_names) = config.apis {
        KNOWN_APIS
            .iter()
            .filter(|api| api_names.iter().any(|name| name == api.base_name))
            .collect()
    } else {
        KNOWN_APIS.iter().collect()
    };

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let mut downloaded = 0;
    let mut failed = Vec::new();

    for api in apis_to_download {
        for file in api.files_to_download() {
            let url = format!(
                "{}/{}/clients/src/main/resources/common/message/{}",
                KAFKA_GITHUB_RAW, config.version, file
            );
            let output_path = proto_dir.join(file);

            eprintln!("Downloading {}...", file);

            match client.get(&url).send() {
                Ok(response) => {
                    if response.status().is_success() {
                        let content = response.text()?;
                        fs::write(&output_path, content)?;
                        eprintln!("  ✓ Saved to {:?}", output_path);
                        downloaded += 1;
                    } else {
                        eprintln!("  ✗ Failed: HTTP {}, file: {}", response.status(), file);
                        failed.push(file.to_string());
                    }
                }
                Err(e) => {
                    eprintln!("  ✗ Error: {}", e);
                    failed.push(file.to_string());
                }
            }
        }
    }

    eprintln!(
        "\nDownload complete: {} files downloaded, {} failed",
        downloaded,
        failed.len()
    );

    if downloaded == 0 {
        return Err(DownloadError::NoFilesDownloaded);
    }

    Ok(proto_dir.to_path_buf())
}

/// 从本地 Kafka 源码目录复制协议定义
pub fn copy_from_local_source(
    source_dir: &Path,
    output_dir: &Path,
    version: Option<&str>,
) -> Result<PathBuf, DownloadError> {
    let version_str = version.unwrap_or("local");
    let proto_dir = output_dir.join(version_str);

    fs::create_dir_all(&proto_dir)?;

    let message_dir = source_dir
        .join("clients")
        .join("src")
        .join("main")
        .join("resources")
        .join("common")
        .join("message");

    if !message_dir.exists() {
        return Err(DownloadError::InvalidVersion(format!(
            "Message directory not found: {:?}",
            message_dir
        )));
    }

    let mut copied = 0;

    for entry in fs::read_dir(&message_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let file_name = path.file_name().unwrap();
            let dest = proto_dir.join(file_name);
            fs::copy(&path, &dest)?;
            eprintln!("Copied {:?} -> {:?}", path.file_name().unwrap(), dest);
            copied += 1;
        }
    }

    eprintln!("\nCopied {} files from {:?}", copied, message_dir);

    Ok(proto_dir)
}

/// 获取可用的 Kafka 版本列表（从 GitHub API）
pub fn fetch_available_versions() -> Result<Vec<String>, DownloadError> {
    let client = reqwest::blocking::Client::new();
    let url = "https://api.github.com/repos/apache/kafka/tags";

    let response: serde_json::Value = client
        .get(url)
        .header("User-Agent", "kafka-protocol-codegen")
        .send()?
        .json()?;

    let versions = response
        .as_array()
        .ok_or_else(|| DownloadError::InvalidVersion("Invalid response from GitHub".to_string()))?
        .iter()
        .filter_map(|tag| tag.get("name").and_then(|n| n.as_str()))
        .filter(|name| name.starts_with("kafka-"))
        .map(|name| name.trim_start_matches("kafka-").to_string())
        .collect();

    Ok(versions)
}
