use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct Manifest {
    pub config: String,
    pub repo_tags: Vec<String>,
    pub layers: Vec<String>,
}

pub type ManifestFile = Vec<Manifest>;

impl Manifest {
    pub fn from_str(contents: &str) -> Result<Self> {
        let manifests: ManifestFile = serde_json::from_str(contents)?;
        let manifest = manifests
            .into_iter()
            .next()
            .ok_or(anyhow!("No manifest.json found"))?;
        Ok(manifest)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_str(&contents)
    }

    pub fn write_to_file(&self, path: &Path) -> Result<()> {
        let manifests = vec![self];
        let manifest_json = serde_json::to_string_pretty(&manifests)?;
        fs::write(path, manifest_json)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerConfig {
    pub architecture: String,
    pub config: ContainerConfig,
    pub created: String,
    pub history: Vec<HistoryEntry>,
    pub os: String,
    pub rootfs: RootFs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerConfig {
    #[serde(rename = "Env")]
    pub env: Option<Vec<String>>,

    #[serde(rename = "Cmd")]
    pub cmd: Option<Vec<String>>,

    #[serde(rename = "WorkingDir")]
    pub working_dir: Option<String>,

    #[serde(rename = "Labels")]
    pub labels: Option<HashMap<String, String>>,

    #[serde(rename = "ArgsEscaped")]
    pub args_escaped: Option<bool>,

    #[serde(rename = "Entrypoint")]
    pub entrypoint: Option<Vec<String>>,

    #[serde(rename = "User")]
    pub user: Option<String>,

    #[serde(rename = "ExposedPorts")]
    pub exposed_ports: Option<HashMap<String, serde_json::Value>>,

    #[serde(rename = "Volumes")]
    pub volumes: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEntry {
    pub created: String,
    pub created_by: String,

    #[serde(default)]
    pub comment: String,

    #[serde(default)]
    pub empty_layer: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootFs {
    #[serde(rename = "type")]
    pub fs_type: String,

    // sha256 of the uncompressed layer
    pub diff_ids: Vec<String>,
}

impl DockerConfig {
    pub fn from_str(contents: &str) -> Result<Self> {
        let config: DockerConfig = serde_json::from_str(contents)?;
        Ok(config)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_str(&contents)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_docker_config() {
        let json = r#"{
            "architecture": "arm64",
            "config": {
                "Env": ["PATH=/usr/local/bin"],
                "Cmd": ["/bin/sh"],
                "WorkingDir": "/app",
                "Labels": {
                    "maintainer": "test@example.com"
                },
                "ArgsEscaped": true
            },
            "created": "2025-11-13T17:58:06.296708481-05:00",
            "history": [
                {
                    "created": "2025-10-08T11:10:40Z",
                    "created_by": "CMD [\"/bin/sh\"]",
                    "comment": "buildkit.dockerfile.v0",
                    "empty_layer": true
                }
            ],
            "os": "linux",
            "rootfs": {
                "type": "layers",
                "diff_ids": [
                    "sha256:8ff721756ec0097ba331876f1502858f8849716bdf720516fafa96c72a8d7dac"
                ]
            }
        }"#;

        let config: DockerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.architecture, "arm64");
        assert_eq!(config.os, "linux");
        assert_eq!(config.rootfs.fs_type, "layers");
    }
}
