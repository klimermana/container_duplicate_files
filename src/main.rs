use std::io::{self, BufReader, Write};
use std::path::Path;

use anyhow::{anyhow, Result};
use chrono::Local;
use clap::Parser;
use docker_duplicate_files::analyzer::Analyzer;
use docker_duplicate_files::cli::Args;
use env_logger::Builder;
use log::info;

fn main() -> Result<()> {
    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] {}: {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Info)
        .init();

    let args = Args::parse();
    let analyzer = if let Some(image_path) = args.image {
        info!("Running on image: {}", image_path);
        Analyzer::load_from_path(image_path, args.min_size, args.no_compression)?
    } else {
        info!("Running on image from stdin");
        let stdin = io::stdin();
        let reader = BufReader::new(stdin.lock());
        Analyzer::load(reader, args.min_size, args.no_compression)?
    };

    let output_path = if let Some(output) = args.output {
        output
    } else {
        return Err(anyhow!(
            "Streaming output not yet supported. Please provide a path with -o"
        ));
    };

    info!("Finding duplicates...");
    let duplicates = analyzer.find_duplicates()?;
    let _ = analyzer.print_possible_savings(&duplicates);
    let _ = analyzer.create_deduplicated_image(duplicates, Path::new(&output_path))?;
    Ok(())
}
