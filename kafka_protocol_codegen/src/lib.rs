//! Kafka Protocol Code Generator
//!
//! 类似于 prost-build，用于从 Kafka 协议定义生成 Rust 代码

pub mod parser;
pub mod generator;
pub mod downloader;
pub mod error;

use std::path::{Path, PathBuf};

pub use error::{Error, Result};

/// 代码生成器配置
#[derive(Debug, Clone)]
pub struct Config {
    /// 输出目录
    out_dir: PathBuf,
    /// 协议定义目录
    proto_dir: Option<PathBuf>,
    /// 是否下载最新协议定义
    download: bool,
    /// Kafka 版本
    kafka_version: String,
}

impl Config {
    pub fn new() -> Self {
        Self {
            out_dir: PathBuf::from(std::env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string())),
            proto_dir: None,
            download: false,
            kafka_version: "3.6.0".to_string(),
        }
    }

    pub fn out_dir<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.out_dir = path.as_ref().to_path_buf();
        self
    }

    pub fn proto_dir<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.proto_dir = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn download(mut self, download: bool) -> Self {
        self.download = download;
        self
    }

    pub fn kafka_version<S: Into<String>>(mut self, version: S) -> Self {
        self.kafka_version = version.into();
        self
    }

    /// 编译协议定义
    pub fn compile(self) -> Result<()> {
        // 1. 下载或定位协议定义
        let proto_dir = if self.download {
            downloader::download_protocol_definitions(&self.kafka_version, &self.out_dir)?
        } else {
            self.proto_dir.ok_or_else(|| Error::Config("proto_dir or download must be specified".to_string()))?
        };

        // 2. 解析所有协议定义
        let mut all_messages = Vec::new();
        for entry in std::fs::read_dir(&proto_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let content = std::fs::read_to_string(&path)?;
                let message: parser::MessageDefinition = serde_json::from_str(&content)
                    .map_err(|e| Error::Parse(format!("Failed to parse {:?}: {}", path, e)))?;
                all_messages.push(message);
            }
        }

        // 3. 生成代码
        let generator = generator::CodeGenerator::new(&self.out_dir);
        generator.generate(&all_messages)?;

        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// 便捷的编译函数
pub fn compile_protos() -> Result<()> {
    Config::new().compile()
}
