// src/bin/generate_api.rs
use clap::Parser;
use kafka_client_protocol_codegen::{generate_api_module, parse_directory};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "kafka-gen")]
#[command(about = "Generate Rust code from Kafka protocol JSON definitions")]
struct Args {
    #[arg(short, long, default_value = "protocol-definitions")]
    input: PathBuf,

    #[arg(short, long, default_value = "kafka-client-protocol/src/api")]
    output: PathBuf,

    #[arg(short, long)]
    force: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if !args.input.exists() {
        eprintln!("Error: Input directory does not exist: {:?}", args.input);
        std::process::exit(1);
    }

    println!("Parsing JSON definitions from: {:?}", args.input);
    let (messages, structs) = parse_directory(&args.input)?;

    println!(
        "Found {} messages, {} structs",
        messages.len(),
        structs.len()
    );

    if args.output.exists() && !args.force {
        eprintln!("Error: Output directory exists: {:?}", args.output);
        eprintln!("Use --force to overwrite");
        std::process::exit(1);
    }

    println!("Generating code to: {:?}", args.output);
    generate_api_module(&messages, &structs, &args.output)?;

    println!("Done!");
    Ok(())
}
