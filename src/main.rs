use anyhow::anyhow;
use bollard::Docker;
use clap::Parser;
use flate2::read::GzDecoder;
use humansize::{BINARY, format_size};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::path::PathBuf;
use std::{fs, io::BufReader, io::Read};
use tar::Archive;
use tempfile::TempDir;
use tempfile::tempdir;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Manifest {
    pub config: String,
    pub repo_tags: Vec<String>,
    pub layers: Vec<String>,
}

pub type ManifestFile = Vec<Manifest>;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub layer_index: usize,
}

#[derive(Debug, Clone)]
pub struct Layer {
    pub path: PathBuf,
    pub layer_index: usize,
}

pub struct Analyzer {
    pub tmp_dir: TempDir,
    pub layers: Vec<Layer>,
    pub min_size: u64,
}

fn connect_docker() -> Result<Docker, bollard::errors::Error> {
    if let Ok(docker) = Docker::connect_with_socket_defaults() {
        return Ok(docker);
    }

    // macOS-specific paths for Docker Desktop
    let macos_paths = [
        "unix:///Users/$USER/.docker/run/docker.sock",
        "unix:///var/run/docker.sock",
        "unix:///Users/$USER/.colima/default/docker.sock",
    ];

    // Try macOS paths
    for path in macos_paths {
        let expanded_path = if path.contains("$USER") {
            let user = env::var("USER").unwrap_or_else(|_| "".to_string());
            path.replace("$USER", &user)
        } else {
            path.to_string()
        };

        if let Ok(docker) =
            Docker::connect_with_socket(&expanded_path, 120, bollard::API_DEFAULT_VERSION)
        {
            return Ok(docker);
        }
    }

    Err(bollard::errors::Error::SocketNotFoundError(
        "Could not find Docker socket".to_string(),
    ))
}

impl Analyzer {
    pub fn load(image: String, min_size: u64) -> Result<Self, Box<dyn std::error::Error>> {
        //Ugly way to determine input arg, do this better
        if image.contains(".tar") {
            Ok(Analyzer::load_from_tar(image, min_size)?)
        } else if image.contains(":") {
            Ok(Analyzer::load_from_docker(image, min_size)?)
        } else {
            Err(anyhow!("Unexpected image string {}", image).into())
        }
    }

    pub fn load_from_tar(image: String, min_size: u64) -> Result<Self, Box<dyn std::error::Error>> {
        let tmp_dir = tempdir()?;
        let image = File::open(image)?;
        let tar_file = BufReader::new(image);
        let mut archive = Archive::new(tar_file);
        let extracted_dir = tmp_dir.path();
        archive.unpack(extracted_dir)?;

        let manifest_file = extracted_dir.join("manifest.json");
        let content = fs::read_to_string(&manifest_file)?;
        // Manifest files are a list with single element
        let manifests: ManifestFile = serde_json::from_str(&content)?;
        let manifest = manifests
            .into_iter()
            .next()
            .ok_or("No manifest.json found")?;
        let layers = manifest
            .layers
            .iter()
            .enumerate()
            .map(|(idx, l)| Layer {
                path: extracted_dir.join(l),
                layer_index: idx,
            })
            .collect();

        println!("{:#?}", manifest);
        Ok(Self {
            tmp_dir,
            layers,
            min_size,
        })
    }

    fn load_from_docker(image: String, min_size: u64) -> Result<Self, Box<dyn std::error::Error>> {
        // Can't seem to get the layer blobs from the docker daemon without first saving the image.
        // Might need to export to an OCI image first?
        // Ignoring for now
        Err(anyhow!("Not implemented analyzing layers on docker image, export first").into())
    }

    pub fn scan_files(&self) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
        self.layers
            .iter()
            .try_fold(Vec::new(), |mut acc, layer| match self.scan_layer(layer) {
                Ok(files) => {
                    acc.extend(files);
                    Ok(acc)
                }
                Err(e) => Err(anyhow!("Error scanning layer: {:?} {}", layer, e).into()),
            })
    }

    fn scan_layer(&self, layer: &Layer) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
        let file = File::open(layer.path.as_path())?;
        if let Ok(files) = self.try_scan_gzipped(file, layer.layer_index) {
            return Ok(files);
        }

        let file = File::open(layer.path.as_path())?;
        self.scan_tar_archive(file, layer.layer_index)
    }

    fn try_scan_gzipped<R: Read>(
        &self,
        reader: R,
        layer_index: usize,
    ) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
        let decoder = GzDecoder::new(reader);
        self.scan_tar_archive(decoder, layer_index)
    }

    fn scan_tar_archive<R: Read>(
        &self,
        reader: R,
        layer_index: usize,
    ) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
        let mut archive = Archive::new(reader);
        let mut files = Vec::new();
        for entry in archive.entries()? {
            let mut entry = entry?;

            if !entry.header().entry_type().is_file() {
                continue;
            }

            let size = entry.header().size()?;

            if size < self.min_size {
                continue;
            }

            let path = entry.path()?.to_string_lossy().to_string();

            if path.contains("/.wh.") || path.ends_with(".wh..wh..opq") {
                // ignore removed files for now
                continue;
            }

            let mut hasher = Sha256::new();
            let mut buffer = Vec::new();
            entry.read_to_end(&mut buffer)?;
            hasher.update(&buffer);
            let hash = format!("{:x}", hasher.finalize());

            files.push(FileInfo {
                path,
                size,
                hash,
                layer_index,
            });
        }
        Ok(files)
    }

    pub fn find_duplicates(&self) -> Result<Vec<FileInfo>, Box<dyn std::error::Error>> {
        let files = self.scan_files()?;
        //println!("{:#?}", files);

        //HashMap for Sha -> FileInfo
        let mut seen_files = HashMap::new();
        // Vec of duplicates
        let mut duplicates = Vec::new();
        for file_info in files {
            if seen_files.contains_key(&file_info.hash) {
                duplicates.push(file_info);
            } else {
                seen_files.insert(file_info.hash.clone(), file_info);
            }
        }
        Ok(duplicates)
    }

    fn print_possible_savings(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut duplicates = self.find_duplicates()?;
        duplicates.sort_by_key(|d| Reverse(d.size));
        println!("=============================");
        println!("Total duplicate files: {}", duplicates.len());
        println!(
            "Total duplicate size: {}",
            format_size(duplicates.iter().map(|f| f.size).sum::<u64>(), BINARY)
        );
        println!("=============================");
        println!("Duplicate files:");
        for dup in duplicates {
            println!("\t{}, size: {}", dup.path, format_size(dup.size, BINARY))
        }
        println!("=============================");
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Docker image to examine
    #[arg(short, long)]
    image: String,

    /// minimum size of an object to track
    #[arg(short, long, default_value_t = 1_000_000)]
    min_size: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Running on image: {}", args.image);

    let analyzer = Analyzer::load(args.image, args.min_size)?;

    let _ = analyzer.print_possible_savings();
    Ok(())
}
