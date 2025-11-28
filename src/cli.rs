use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Docker image to examine. If not specified, stdin will be used
    #[arg(short, long)]
    pub image: Option<String>,

    /// Output file. If not specified, stdout will be used
    #[arg(short, long)]
    pub output: Option<String>,

    /// minimum size of an object to track
    #[arg(short, long, default_value_t = 1_000_000)]
    pub min_size: u64,

    /// Disable layer compression
    #[arg(long)]
    pub no_compression: bool,
}
