use anyhow::{Context, Result, anyhow};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use humansize::{BINARY, format_size};
use itertools::Itertools;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::HashMap;
use std::fs::File;
use std::io::copy;
use std::path::{Path, PathBuf};
use std::{fs, io::BufReader, io::Read};
use tar::{Archive, Builder};
use tempfile::TempDir;
use tempfile::tempdir;
use walkdir::WalkDir;

use crate::schemas::*;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
    pub hash: String,
    pub layer_index: usize,
}

#[derive(Debug, Clone)]
pub struct DuplicateInfo {
    pub original: FileInfo,
    pub duplicates: Vec<FileInfo>,
    pub total_savings: u64,
}

#[derive(Debug)]
pub enum LinkType {
    Sym,
    Hard,
}

#[derive(Debug)]
pub struct DeDupTransaction {
    layer: usize,
    original_path: String,
    target_path: String,
    link_type: LinkType,
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
    original_manifest: Manifest,
}

fn is_gzipped(file_path: &Path) -> Result<bool> {
    let mut file = File::open(file_path)?;
    let mut magic_bytes = [0u8; 2];
    file.read_exact(&mut magic_bytes)?;
    Ok(magic_bytes[0] == 0x1f && magic_bytes[1] == 0x8b)
}

impl Analyzer {
    pub fn load(image: String, min_size: u64) -> Result<Self> {
        //Ugly way to determine input arg, do this better
        if image.contains(".tar") {
            Ok(Analyzer::load_from_tar(image, min_size)?)
        } else {
            Err(anyhow!(
                "Unexpected image string {}, must be exported tar file",
                image
            )
            .into())
        }
    }

    pub fn load_from_tar(image: String, min_size: u64) -> Result<Self> {
        let tmp_dir = tempdir()?;
        let image = File::open(image)?;
        let tar_file = BufReader::new(image);
        let mut archive = Archive::new(tar_file);
        let extracted_dir = tmp_dir.path();
        archive.unpack(extracted_dir)?;

        let manifest_file = extracted_dir.join("manifest.json");
        let manifest = Manifest::from_file(&manifest_file)?;
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
            original_manifest: manifest,
        })
    }

    pub fn scan_files(&self) -> Result<Vec<FileInfo>> {
        Ok(self
            .layers
            .par_iter()
            .map(|layer| {
                self.scan_layer(layer)
                    .map_err(|e| anyhow!("Error scanning layer: {:?} {}", layer, e))
            })
            .collect::<Result<Vec<Vec<FileInfo>>, _>>()?
            .into_iter()
            .flatten()
            .collect())
    }

    fn scan_layer(&self, layer: &Layer) -> Result<Vec<FileInfo>> {
        //println!(
        //    "Scanning layer {}/{}...",
        //    layer.layer_index + 1,
        //    self.layers.len()
        //);
        if is_gzipped(&layer.path)? {
            let file = File::open(layer.path.as_path())?;
            let decoder = GzDecoder::new(file);
            self.scan_tar_archive(decoder, layer.layer_index)
        } else {
            let file = File::open(layer.path.as_path())?;
            self.scan_tar_archive(file, layer.layer_index)
        }
    }

    fn scan_tar_archive<R: Read>(&self, reader: R, layer_index: usize) -> Result<Vec<FileInfo>> {
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
            copy(&mut entry, &mut hasher)?;
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

    pub fn find_duplicates(&self) -> Result<Vec<DuplicateInfo>> {
        let files = self.scan_files()?;
        let mut files_by_hash: HashMap<String, Vec<FileInfo>> = HashMap::new();
        for file in files {
            files_by_hash
                .entry(file.hash.clone())
                .or_default()
                .push(file);
        }
        Ok(files_by_hash
            .into_iter()
            .filter(|(_, files)| files.len() > 1)
            .map(|(_, mut files)| {
                files.sort_by_key(|f| f.layer_index);
                let target = files.remove(0);
                let savings = target.size * files.len() as u64;
                DuplicateInfo {
                    original: target,
                    duplicates: files,
                    total_savings: savings,
                }
            })
            .sorted_by_key(|d| Reverse(d.total_savings))
            .collect())
    }

    pub fn print_possible_savings(&self, duplicates: &Vec<DuplicateInfo>) -> Result<()> {
        println!("=============================");
        println!("Total duplicate files: {}", duplicates.len());
        println!(
            "Total duplicate size: {}",
            format_size(
                duplicates.iter().map(|f| f.total_savings).sum::<u64>(),
                BINARY
            )
        );
        println!("=============================");
        println!("Duplicate files:");
        for dup_info in duplicates.iter() {
            println!(
                "\tOriginal: {}, layer: {} size: {}",
                dup_info.original.path,
                dup_info.original.layer_index,
                format_size(dup_info.original.size, BINARY)
            );
            for dup in dup_info.duplicates.iter() {
                println!("\tDuplicate: {}, layer: {}", dup.path, dup.layer_index);
            }
        }
        println!("=============================");
        Ok(())
    }

    pub fn generate_modification_plan(
        &self,
        duplicates: Vec<DuplicateInfo>,
    ) -> Result<HashMap<usize, Vec<DeDupTransaction>>> {
        Ok(duplicates
            .iter()
            .flat_map(|d| d.duplicates.iter().map(move |f| (d, f)))
            .map(|(d, f)| {
                (
                    f.layer_index,
                    DeDupTransaction {
                        layer: f.layer_index,
                        original_path: d.original.path.clone(),
                        target_path: f.path.clone(),
                        link_type: if d.original.layer_index == f.layer_index {
                            LinkType::Hard
                        } else {
                            LinkType::Sym
                        },
                    },
                )
            })
            .into_group_map())
    }

    fn extract_layer(&self, layer_tar: &Path, dest: &Path) -> Result<()> {
        let file = File::open(layer_tar)?;
        if is_gzipped(&layer_tar)? {
            let decoder = GzDecoder::new(file);
            let mut archive = Archive::new(decoder);
            archive.unpack(dest)?;
        } else {
            let mut archive = Archive::new(file);
            archive.unpack(dest)?;
        }
        Ok(())
    }

    fn process_layer(
        &self,
        layer: &Layer,
        modifications: &Vec<DeDupTransaction>,
        output_dir: &Path,
    ) -> Result<Layer> {
        let extract_dir = tempdir()?;
        self.extract_layer(&layer.path, extract_dir.path())?;
        for modif in modifications {
            let file_path = extract_dir.path().join(&modif.target_path);
            let _ = fs::remove_file(&file_path);
            let source_path = PathBuf::from(&modif.original_path);
            match modif.link_type {
                LinkType::Sym => {
                    std::os::unix::fs::symlink(&source_path, &file_path).context(format!(
                        "Failed to create symlink {} -> {}",
                        file_path.display(),
                        source_path.display()
                    ))?;
                }
                LinkType::Hard => {
                    // TODO
                    //fs::hard_link(&source_path, &file_path).context(format!(
                    //    "Failed to create hardlink {} -> {}",
                    //    file_path.display(),
                    //    source_path.display()
                    //))?;
                    std::os::unix::fs::symlink(&source_path, &file_path).context(format!(
                        "Failed to create symlink {} -> {}",
                        file_path.display(),
                        source_path.display()
                    ))?;
                }
            }
        }
        let new_layer_filename = format!("layer-{}.tar.gz", layer.layer_index);
        let new_layer_path = output_dir.join(&new_layer_filename);
        let tar_file = File::create(&new_layer_path)?;

        let encoder = GzEncoder::new(tar_file, Compression::default());
        let mut builder = Builder::new(encoder);

        builder.follow_symlinks(false);
        builder
            .append_dir_all("", extract_dir.path())
            .context("Failed to pack layer")?;

        builder.finish().context("Failed to finalize tar")?;

        Ok(Layer {
            path: new_layer_path,
            layer_index: layer.layer_index,
        })
    }

    fn update_manifest(&self, new_image_dir: &Path, new_layers: &Vec<Layer>) -> Result<()> {
        let blobs_dir = new_image_dir.join("blobs/sha256");
        fs::create_dir_all(&blobs_dir)?;

        let mut new_refs = Vec::new();
        for layer in new_layers {
            let mut file = File::open(&layer.path)?;
            let mut hasher = Sha256::new();
            std::io::copy(&mut file, &mut hasher)?;
            let digest = format!("{:x}", hasher.finalize());
            let blob_path = blobs_dir.join(&digest);
            fs::copy(&layer.path, &blob_path)?;

            let relative_path = format!("blobs/sha256/{}", digest);
            new_refs.push(relative_path);
        }
        let mut new_manifest = self.original_manifest.clone();
        new_manifest.layers = new_refs;
        new_manifest.repo_tags = vec!["test:smaller".to_string()];
        let new_manifest_path = new_image_dir.join("manifest.json");
        let _ = new_manifest.write_to_file(&new_manifest_path);
        Ok(())
    }

    pub fn create_deduplicated_image(
        &self,
        duplicates: Vec<DuplicateInfo>,
        output_path: &Path,
    ) -> Result<()> {
        let work_dir = tempdir()?;
        let work_path = work_dir.path();
        let new_layer_dir = work_path.join("new_layers");
        let staging_dir = work_path.join("staging");
        fs::create_dir(&new_layer_dir)?;
        let plan = self.generate_modification_plan(duplicates)?;

        let new_layers: Result<Vec<_>> = self
            .layers
            .par_iter()
            .map(|layer| match plan.get(&layer.layer_index) {
                Some(mods) => self.process_layer(layer, mods, &new_layer_dir),
                None => Ok(layer.clone()),
            })
            .collect();

        let new_layers = new_layers?;

        self.update_manifest(&staging_dir, &new_layers)?;

        let config_src = self.tmp_dir.path().join(&self.original_manifest.config);
        let config_dst = staging_dir.join(&self.original_manifest.config);

        fs::copy(config_src, config_dst)?;

        let output_file = File::create(output_path).context(format!(
            "Failed to create output file: {}",
            output_path.display()
        ))?;

        let mut builder = Builder::new(output_file);

        // Add all files from the new image directory
        builder
            .append_dir_all(".", staging_dir)
            .context("Failed to pack final image")?;

        builder.finish().context("Failed to finalize output tar")?;

        Ok(())
    }
}
