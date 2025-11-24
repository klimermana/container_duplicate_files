use anyhow::Result;
use clap::Parser;
use std::path::Path;

use docker_duplicate_files::{Analyzer, cli::Args};

fn main() -> Result<()> {
    let args = Args::parse();
    println!("Running on image: {}", args.image);

    let analyzer = Analyzer::load(args.image, args.min_size)?;
    let duplicates = analyzer.find_duplicates()?;
    let _ = analyzer.print_possible_savings(&duplicates);
    let _ = analyzer.create_deduplicated_image(duplicates, Path::new(&args.output))?;
    Ok(())
}
