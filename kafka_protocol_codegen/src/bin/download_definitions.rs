//! 下载 Kafka 协议定义
//!
//! 从 Apache Kafka 官方 GitHub 仓库下载协议定义 JSON 文件
//!
//! 使用方法:
//!   cargo run --bin download_definitions -- [options]
//!
//! 选项:
//!   -v, --version <VERSION>  Kafka 版本 (默认: 4.2.0)
//!   -o, --output <PATH>      输出目录 (默认: protocol_definitions)
//!   -l, --list <PATH>        包含文件列表的文本文件 (默认: 从 messages.txt 解析)

use std::path::PathBuf;

const KAFKA_GITHUB_RAW: &str = "https://raw.githubusercontent.com/apache/kafka";

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut version = "4.2.0".to_string();
    let mut output_dir = PathBuf::from("protocol_definitions");
    let mut list_file: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-v" | "--version" => {
                if i + 1 < args.len() {
                    version = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: --version requires a value");
                    std::process::exit(1);
                }
            }
            "-o" | "--output" => {
                if i + 1 < args.len() {
                    output_dir = PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: --output requires a value");
                    std::process::exit(1);
                }
            }
            "-l" | "--list" => {
                if i + 1 < args.len() {
                    list_file = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else {
                    eprintln!("Error: --list requires a value");
                    std::process::exit(1);
                }
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage();
                std::process::exit(1);
            }
        }
    }

    println!("Downloading Kafka protocol definitions...");
    println!("Version: {}", version);
    println!("Output: {}", output_dir.display());
    println!();

    // 获取文件列表
    let files = match list_file {
        Some(path) => parse_file_list(&path),
        None => {
            // 尝试从 messages.txt 解析
            let default_list = PathBuf::from("docs/messages.txt");
            if default_list.exists() {
                parse_file_list(&default_list)
            } else {
                // 使用内置的常用 API 列表
                get_default_api_list()
            }
        }
    };

    if let Err(e) = download_protocol_definitions(&version, &output_dir, &files) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    println!("\nDone!");
}

fn print_usage() {
    println!("Usage: download_definitions [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -v, --version <VERSION>  Kafka version (default: 4.2.0)");
    println!("  -o, --output <PATH>      Output directory (default: protocol_definitions)");
    println!("  -l, --list <PATH>        File containing list of message files");
    println!("  -h, --help               Print this help message");
}

fn parse_file_list(path: &PathBuf) -> Vec<String> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.ends_with(".json") {
                Some(line.to_string())
            } else if line.contains(".json") {
                // 提取文件名部分
                line.split_whitespace()
                    .find(|s| s.ends_with(".json"))
                    .map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn get_default_api_list() -> Vec<String> {
    let apis = vec![
        "AddOffsetsToTxn", "AddPartitionsToTxn", "AllocateProducerIds",
        "AlterClientQuotas", "AlterConfigs", "AlterPartitionReassignments",
        "AlterReplicaLogDirs", "AlterUserScramCredentials", "ApiVersions",
        "BeginQuorumEpoch", "BrokerHeartbeat", "BrokerRegistration",
        "ConsumerGroupDescribe", "ConsumerGroupHeartbeat", "ControlledShutdown",
        "ControllerRegistration", "CreateAcls", "CreateDelegationToken",
        "CreatePartitions", "CreateTopics", "DeleteAcls", "DeleteGroups",
        "DeleteRecords", "DeleteTopics", "DescribeAcls", "DescribeClientQuotas",
        "DescribeCluster", "DescribeConfigs", "DescribeDelegationToken",
        "DescribeGroups", "DescribeLogDirs", "DescribeProducers", "DescribeQuorum",
        "DescribeTopicPartitions", "DescribeTransactions", "DescribeUserScramCredentials",
        "ElectLeaders", "EndQuorumEpoch", "EndTxn", "ExpireDelegationToken",
        "Fetch", "FetchSnapshot", "FindCoordinator", "GetTelemetrySubscriptions",
        "Heartbeat", "IncrementalAlterConfigs", "InitProducerId", "JoinGroup",
        "LeaveGroup", "ListClientMetricsResources", "ListGroups", "ListOffsets",
        "ListPartitionReassignments", "ListTransactions", "Metadata", "OffsetCommit",
        "OffsetDelete", "OffsetFetch", "OffsetForLeaderEpoch", "Produce",
        "PushTelemetry", "RenewDelegationToken", "SaslAuthenticate", "SaslHandshake",
        "SyncGroup", "TxnOffsetCommit", "UpdateFeatures", "UpdateMetadata",
        "Vote", "WriteTxnMarkers",
    ];

    let mut files = Vec::new();
    for api in apis {
        files.push(format!("{}Request.json", api));
        files.push(format!("{}Response.json", api));
    }
    files
}

fn download_protocol_definitions(
    version: &str,
    output_dir: &PathBuf,
    files: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(output_dir)?;

    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let mut success_count = 0;
    let mut fail_count = 0;

    for file_name in files {
        let url = format!(
            "{}/{}/clients/src/main/resources/common/message/{}",
            KAFKA_GITHUB_RAW, version, file_name
        );
        let output_path = output_dir.join(file_name);

        print!("Downloading {}... ", file_name);

        match download_file(&client, &url, &output_path) {
            Ok(true) => {
                println!("OK");
                success_count += 1;
            }
            Ok(false) => {
                println!("Not found");
                fail_count += 1;
            }
            Err(e) => {
                println!("Error: {}", e);
                fail_count += 1;
            }
        }
    }

    println!();
    println!("Summary: {} succeeded, {} failed", success_count, fail_count);

    Ok(())
}

fn download_file(
    client: &reqwest::blocking::Client,
    url: &str,
    output_path: &PathBuf,
) -> Result<bool, Box<dyn std::error::Error>> {
    match client.get(url).send() {
        Ok(response) => {
            if response.status().is_success() {
                let content = response.text()?;
                std::fs::write(output_path, content)?;
                Ok(true)
            } else if response.status().as_u16() == 404 {
                Ok(false)
            } else {
                Err(format!("HTTP {}", response.status()).into())
            }
        }
        Err(e) => Err(e.into()),
    }
}
