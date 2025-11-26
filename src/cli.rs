use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Docker image to examine
    #[arg(short, long)]
    pub image: String,

    #[arg(short, long)]
    pub output: String,

    /// minimum size of an object to track
    #[arg(short, long, default_value_t = 1_000_000)]
    pub min_size: u64,

    /// Disable layer compression
    #[arg(long)]
    pub no_compression: bool,
}
