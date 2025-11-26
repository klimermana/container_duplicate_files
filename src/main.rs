use std::io::Write;
use std::path::Path;

use anyhow::Result;
use chrono::Local;
use clap::Parser;
use docker_duplicate_files::Analyzer;
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
    info!("Running on image: {}", args.image);

    let analyzer = Analyzer::load(args.image, args.min_size)?;
    info!("Finding duplicates...");
    let duplicates = analyzer.find_duplicates()?;
    let _ = analyzer.print_possible_savings(&duplicates);
    let _ = analyzer.create_deduplicated_image(
        duplicates,
        Path::new(&args.output),
        args.no_compression,
    )?;
    Ok(())
}
