// src/bin/download_definitions.rs
use std::path::PathBuf;
use clap::Parser;
use kafka_client_protocol_codegen::{
    download_protocol_definitions, copy_from_local_source,
    DownloadConfig, fetch_available_versions,
};

#[derive(Parser, Debug)]
#[command(name = "kafka-download")]
#[command(about = "Download Kafka protocol JSON definitions")]
struct Args {
    #[arg(short, long, default_value = "4.2")]
    version: String,
    
    #[arg(short, long, default_value = "protocol-definitions")]
    output: PathBuf,
    
    #[arg(short, long)]
    force: bool,
    
    #[arg(long)]
    local_source: Option<PathBuf>,
    
    #[arg(short, long)]
    apis: Option<String>,
    
    #[arg(long)]
    list_versions: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    
    if args.list_versions {
        println!("Fetching available Kafka versions...");
        match fetch_available_versions() {
            Ok(versions) => {
                println!("Available versions:");
                for v in versions.iter().take(20) {
                    println!("  {}", v);
                }
                if versions.len() > 20 {
                    println!("  ... and {} more", versions.len() - 20);
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch versions: {}", e);
            }
        }
        return Ok(());
    }
    
    if let Some(source) = args.local_source {
        println!("Copying from local source: {:?}", source);
        let output = copy_from_local_source(&source, &args.output, Some(&args.version))?;
        println!("Copied to: {:?}", output);
        return Ok(());
    }
    
    let apis = args.apis.map(|s| {
        s.split(',').map(|s| s.trim().to_string()).collect()
    });
    
    let config = DownloadConfig {
        version: args.version,
        output_dir: args.output,
        force: args.force,
        apis,
    };
    
    println!("Downloading Kafka {} protocol definitions...", config.version);
    let output_dir = download_protocol_definitions(&config)?;
    println!("Downloaded to: {:?}", output_dir);
    
    Ok(())
}