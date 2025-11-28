use anyhow::{Result, anyhow};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Docker image to examine. If not specified, stdin will be used
    #[arg(short, long)]
    pub image: Option<String>,

    /// Output file path. Cannot be used with --stdout.
    #[arg(short, long)]
    pub output: Option<String>,

    /// Write to stdout. Cannot be used with -o.
    #[arg(long, action = clap::ArgAction::SetTrue)]
    pub stdout: bool,

    /// minimum size of an object to track
    #[arg(short, long, default_value_t = 1_000_000)]
    pub min_size: u64,

    /// Disable layer compression
    #[arg(long)]
    pub no_compression: bool,
}

impl Args {
    pub fn validate(&self) -> Result<()> {
        if self.output.is_none() && !self.stdout {
            return Err(anyhow!("Either --output or --stdout must be specified"));
        }
        Ok(())
    }
}
