use std::fs::File;
use std::io::{self, BufReader, Write};

use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use docker_duplicate_files::analyzer::Analyzer;
use docker_duplicate_files::cli::Args;
use env_logger::Builder;
use log::info;

fn main() -> Result<()> {
    let args = Args::parse();

    let mut builder = Builder::new();

    builder.format(|buf, record| {
        writeln!(
            buf,
            "[{}] {}: {}",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.level(),
            record.args()
        )
    });

    if args.stdout {
        builder.filter_level(log::LevelFilter::Warn);
    } else {
        builder.filter_level(log::LevelFilter::Info);
    }
    builder.init();

    let analyzer = if let Some(image_path) = args.image {
        info!("Running on image: {}", image_path);
        Analyzer::load_from_path(image_path, args.min_size, args.no_compression)?
    } else {
        info!("Running on image from stdin");
        let stdin = io::stdin();
        let reader = BufReader::new(stdin.lock());
        Analyzer::load(reader, args.min_size, args.no_compression)?
    };

    info!("Finding duplicates...");
    let duplicates = analyzer.find_duplicates()?;
    let _ = analyzer.print_possible_savings(&duplicates);

    if let Some(output_path_str) = args.output {
        info!("Writing deduplicated image to {}", output_path_str);
        let output_file = File::create(&output_path_str)
            .with_context(|| format!("Failed to create output file: {}", output_path_str))?;
        analyzer.create_deduplicated_image(duplicates, output_file)?;
    } else {
        info!("Writing deduplicated image to stdout");
        let stdout = io::stdout();
        let writer = stdout.lock();
        analyzer.create_deduplicated_image(duplicates, writer)?;
    }
    Ok(())
}
