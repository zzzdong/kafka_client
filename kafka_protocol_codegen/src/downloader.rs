//! Kafka 协议定义下载器
//!
//! 从 Kafka 官方仓库下载协议定义 JSON 文件

use crate::error::{Error, Result};
use std::fs;
use std::path::Path;

const KAFKA_GITHUB_RAW: &str = "https://raw.githubusercontent.com/apache/kafka";

/// 下载 Kafka 协议定义
pub fn download_protocol_definitions(version: &str, out_dir: &Path) -> Result<std::path::PathBuf> {
    let proto_dir = out_dir.join("kafka_protocol").join(version);
    fs::create_dir_all(&proto_dir)?;

    // 主要的 API 定义文件
    let apis = vec![
        "ApiVersions",
        "Metadata",
        "Produce",
        "Fetch",
        "ListOffsets",
        "DescribeConfigs",
        "SaslHandshake",
        "SaslAuthenticate",
        "CreateTopics",
        "DeleteTopics",
        "OffsetCommit",
        "OffsetFetch",
        "FindCoordinator",
        "JoinGroup",
        "Heartbeat",
        "LeaveGroup",
        "SyncGroup",
        "DescribeGroups",
        "ListGroups",
    ];

    let client = reqwest::blocking::Client::new();

    for api in &apis {
        let url = format!(
            "{}/{}/clients/src/main/resources/common/message/{}.json",
            KAFKA_GITHUB_RAW, version, api
        );
        let output_path = proto_dir.join(format!("{}.json", api));

        println!("Downloading {}...", url);

        match client.get(&url).send() {
            Ok(response) => {
                if response.status().is_success() {
                    let content = response.text()?;
                    fs::write(&output_path, content)?;
                    println!("  Saved to {:?}", output_path);
                } else {
                    eprintln!("  Failed to download {}: {}", api, response.status());
                }
            }
            Err(e) => {
                eprintln!("  Error downloading {}: {}", api, e);
            }
        }
    }

    Ok(proto_dir)
}

/// 从本地 Kafka 源码目录复制协议定义
pub fn copy_from_local_source(source_dir: &Path, out_dir: &Path) -> Result<std::path::PathBuf> {
    let proto_dir = out_dir.join("kafka_protocol");
    fs::create_dir_all(&proto_dir)?;

    let message_dir = source_dir
        .join("clients")
        .join("src")
        .join("main")
        .join("resources")
        .join("common")
        .join("message");

    if !message_dir.exists() {
        return Err(Error::Config(format!(
            "Message directory not found: {:?}",
            message_dir
        )));
    }

    for entry in fs::read_dir(&message_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            let file_name = path.file_name().unwrap();
            let dest = proto_dir.join(file_name);
            fs::copy(&path, &dest)?;
            println!("Copied {:?} to {:?}", path, dest);
        }
    }

    Ok(proto_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    #[ignore = "Requires network access"]
    fn test_download() {
        let temp_dir = TempDir::new().unwrap();
        let result = download_protocol_definitions("3.6.0", temp_dir.path());
        assert!(result.is_ok());
    }
}
