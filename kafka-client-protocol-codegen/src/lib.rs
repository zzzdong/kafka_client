// src/lib.rs
//! Kafka 协议代码生成库

mod types;
mod parser;
mod generator;
mod downloader;
mod utils;

pub use types::*;
pub use parser::parse_directory;
pub use generator::generate_api_module;
pub use downloader::{
    download_protocol_definitions, copy_from_local_source,
    DownloadConfig, DownloadError, KNOWN_APIS, fetch_available_versions,
};
pub use utils::*;