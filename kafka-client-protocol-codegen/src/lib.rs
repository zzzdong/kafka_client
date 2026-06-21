// src/lib.rs
//! Kafka 协议代码生成库

mod downloader;
mod generator;
mod parser;
mod types;
mod utils;

pub use downloader::{
    DownloadConfig, DownloadError, KNOWN_APIS, copy_from_local_source,
    download_protocol_definitions, fetch_available_versions,
};
pub use generator::generate_api_module;
pub use parser::parse_directory;
pub use types::*;
pub use utils::*;
