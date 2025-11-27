use std::cmp::Reverse;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::{fs, io};

use anyhow::{Context, Result, anyhow};
use env_logger::builder;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use humansize::{BINARY, format_size};
use itertools::Itertools;
use log::info;
use rapidhash::v3::{RapidSecrets, rapidhash_v3_file_seeded};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use sha2::{Digest, Sha256};
use tar::{Archive, Builder};
use tempfile::{TempDir, tempdir};

use crate::schemas::*;
use crate::tee_writer::TeeWriter;

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
    pub hash: String,
}

impl Layer {
    pub fn open_reader(&self) -> Result<Box<dyn Read>> {
        let file = File::open(&self.path)?;
        if is_gzipped(&self.path)? {
            Ok(Box::new(GzDecoder::new(file)))
        } else {
            Ok(Box::new(file))
        }
    }
}

pub struct Analyzer {
    pub tmp_dir: TempDir,
    pub layers: Vec<Layer>,
    pub min_size: u64,
    no_compression: bool,
    original_manifest: Manifest,
    original_config: DockerConfig,
}

const GZIP_MAGIC_BYTES: [u8; 2] = [0x1f, 0x8b];

fn is_gzipped(file_path: &Path) -> Result<bool> {
    let mut file = File::open(file_path)?;
    let mut magic_bytes = [0u8; 2];
    file.read_exact(&mut magic_bytes)?;
    Ok(magic_bytes == GZIP_MAGIC_BYTES)
}

impl Analyzer {
    pub fn load(image: String, min_size: u64, no_compression: bool) -> Result<Self> {
        if image.ends_with(".tar") || image.ends_with(".tar.gz") || image.ends_with(".tar.xz") {
            Ok(Analyzer::load_from_tar(image, min_size, no_compression)?)
        } else {
            Err(anyhow!(
                "Unexpected image string {}, must be an exported tar file",
                image
            )
            .into())
        }
    }

    pub fn load_from_tar(image: String, min_size: u64, no_compression: bool) -> Result<Self> {
        let tmp_dir = tempdir()?;
        let image = File::open(image)?;
        let tar_file = BufReader::new(image);
        let mut archive = Archive::new(tar_file);
        let extracted_dir = tmp_dir.path();
        archive.unpack(extracted_dir)?;

        let manifest_file = extracted_dir.join("manifest.json");
        let manifest = Manifest::from_file(&manifest_file)?;

        let config_path = extracted_dir.join(&manifest.config);
        let config = DockerConfig::from_file(&config_path)?;

        let layers = manifest
            .layers
            .iter()
            .enumerate()
            .map(|(idx, l)| {
                let layer_path = extracted_dir.join(l);
                let hash = config.rootfs.diff_ids.get(idx).cloned().unwrap_or_default();
                Layer {
                    path: layer_path,
                    layer_index: idx,
                    hash,
                }
            })
            .collect();

        info!("{:#?}", manifest);
        Ok(Self {
            tmp_dir,
            layers,
            min_size,
            no_compression,
            original_manifest: manifest,
            original_config: config,
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
        // Grouping by size first then only hashing the files with same size was slower
        //  due to having to re-decompress the layers for a second pass
        let mut archive = Archive::new(layer.open_reader()?);
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
            //let mut hasher = blake3::Hasher::new();
            //copy(&mut entry, &mut hasher)?;
            //let hash = hasher.finalize().to_string();
            // rapidhash ~ 11% faster
            let hash = rapidhash_v3_file_seeded(&mut entry, &RapidSecrets::seed(0))?;
            files.push(FileInfo {
                path,
                size,
                hash: hash.to_string(),
                layer_index: layer.layer_index,
            });
        }

        Ok(files)
    }

    pub fn find_duplicates(&self) -> Result<Vec<DuplicateInfo>> {
        let files = self.scan_files()?;
        info!("Done scanning files...");
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
        info!("=============================");
        info!("Total duplicate files: {}", duplicates.len());
        info!(
            "Total duplicate size: {}",
            format_size(
                duplicates.iter().map(|f| f.total_savings).sum::<u64>(),
                BINARY
            )
        );
        info!("=============================");
        info!("Duplicate files:");
        for dup_info in duplicates.iter() {
            info!(
                "\tOriginal: {}, layer: {} size: {}",
                dup_info.original.path,
                dup_info.original.layer_index,
                format_size(dup_info.original.size, BINARY)
            );
            for dup in dup_info.duplicates.iter() {
                info!("\tDuplicate: {}, layer: {}", dup.path, dup.layer_index);
            }
        }
        info!("=============================");
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

    fn build_layer_tar<W: Write>(
        &self,
        layer: &Layer,
        modifications: &Vec<DeDupTransaction>,
        writer: W,
    ) -> Result<(W, Sha256)> {
        let hasher = Sha256::new();
        let tee = TeeWriter::new(writer, hasher);
        let mut builder = Builder::new(tee);

        builder.follow_symlinks(false);

        let mods_by_target: HashMap<PathBuf, &DeDupTransaction> = modifications
            .iter()
            .map(|m| (PathBuf::from(m.target_path.clone()), m))
            .collect();

        let mut archive = Archive::new(layer.open_reader()?);

        for entry_result in archive.entries()? {
            let mut entry = entry_result?;
            let path = entry.path()?.into_owned();

            if mods_by_target.contains_key(&path) {
                info!("Replacing {} with a link", path.display());
                continue;
            }

            let mut header = entry.header().clone();
            builder.append_data(&mut header, &path, &mut entry)?;
        }

        for modif in modifications {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o777);
            header.set_uid(0);
            header.set_gid(0);
            header.set_mtime(0);
            header.set_entry_type(tar::EntryType::Symlink);
            let link_name = PathBuf::from(&modif.original_path);
            match modif.link_type {
                LinkType::Sym => {
                    builder
                        .append_link(&mut header, &modif.target_path, &link_name)
                        .with_context(|| {
                            format!(
                                "Failed to add symlink {} -> {}",
                                &modif.target_path, &modif.original_path
                            )
                        })?;
                }
                LinkType::Hard => {
                    builder
                        .append_link(&mut header, &modif.target_path, &link_name)
                        .with_context(|| {
                            format!(
                                "Failed to add hardlink as symlink {} -> {}",
                                &modif.target_path, &modif.original_path
                            )
                        })?;
                }
            }
        }

        let tee = builder
            .into_inner()
            .context("Failed to finalize tar file")?;
        Ok(tee.into_inner())
    }

    fn process_layer(
        &self,
        layer: &Layer,
        modifications: &Vec<DeDupTransaction>,
        output_dir: &Path,
    ) -> Result<Layer> {
        let new_layer_filename = if self.no_compression {
            format!("layer-{}.tar", layer.layer_index)
        } else {
            format!("layer-{}.tar.gz", layer.layer_index)
        };
        let new_layer_path = output_dir.join(&new_layer_filename);
        let tar_file = File::create(&new_layer_path)?;

        let uncompressed_hash = if self.no_compression {
            let (mut tar_file, hasher) = self.build_layer_tar(layer, modifications, tar_file)?;
            tar_file.flush()?;
            format!("sha256:{:x}", hasher.finalize())
        } else {
            let gz_encoder = GzEncoder::new(tar_file, Compression::default());
            let (gz_encoder, hasher) = self.build_layer_tar(layer, modifications, gz_encoder)?;
            let hash = format!("sha256:{:x}", hasher.finalize());
            gz_encoder.finish().context("Failed to finish gzip")?;
            hash
        };

        Ok(Layer {
            path: new_layer_path,
            layer_index: layer.layer_index,
            hash: uncompressed_hash,
        })
    }

    fn update_manifest(&self, new_image_dir: &Path, new_layers: &Vec<Layer>) -> Result<()> {
        let blobs_dir = new_image_dir.join("blobs/sha256");
        fs::create_dir_all(&blobs_dir)?;

        let mut new_refs = Vec::new();
        for layer in new_layers {
            if self.no_compression {
                let relative_path = format!("blobs/sha256/{}", layer.hash);
                new_refs.push(relative_path);
            } else {
                let mut file = File::open(&layer.path)?;
                let mut hasher = Sha256::new();
                std::io::copy(&mut file, &mut hasher)?;
                let digest = format!("{:x}", hasher.finalize());
                let blob_path = blobs_dir.join(&digest);
                fs::copy(&layer.path, &blob_path)?;

                let relative_path = format!("blobs/sha256/{}", digest);
                new_refs.push(relative_path);
            }
        }
        let mut new_manifest = self.original_manifest.clone();
        new_manifest.layers = new_refs;
        new_manifest.repo_tags = vec!["test:smaller".to_string()];
        let new_manifest_path = new_image_dir.join("manifest.json");
        let _ = new_manifest.write_to_file(&new_manifest_path);
        Ok(())
    }

    fn update_config(&self, new_image_dir: &Path, new_layers: &Vec<Layer>) -> Result<()> {
        let mut new_config = self.original_config.clone();
        new_config.rootfs.diff_ids = new_layers.iter().map(|l| l.hash.clone()).collect();

        let config_path = new_image_dir.join(&self.original_manifest.config);
        let config_json = new_config.to_json()?;
        if let Some(parent_dir) = config_path.parent() {
            fs::create_dir_all(parent_dir)?;
        } else {
            return Err(anyhow!(
                "Unable to get the parent directory for new config file"
            ));
        }
        fs::write(config_path, config_json)?;
        info!("Finish writing config");

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
        info!("Creating modification plan...");
        let plan = self.generate_modification_plan(duplicates)?;

        info!("Processing layers...");
        let new_layers: Result<Vec<_>> = self
            .layers
            .par_iter()
            .map(|layer| match plan.get(&layer.layer_index) {
                Some(mods) => self.process_layer(layer, mods, &new_layer_dir),
                None => Ok(layer.clone()),
            })
            .collect();

        let new_layers = new_layers?;

        info!("Updating configs...");
        self.update_config(&staging_dir, &new_layers)?;
        self.update_manifest(&staging_dir, &new_layers)?;

        let output_file = File::create(output_path).context(format!(
            "Failed to create output file: {}",
            output_path.display()
        ))?;

        info!("Packing new image...");
        let mut builder = Builder::new(output_file);

        // Add all files from the new image directory
        builder
            .append_dir_all(".", staging_dir)
            .context("Failed to pack final image")?;

        builder.finish().context("Failed to finalize output tar")?;

        Ok(())
    }
}
