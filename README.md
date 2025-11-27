# Docker Duplicate File Finder

`docker_duplicate_files` is a command-line tool for analyzing Docker images to find duplicate files across layers. It can then generate a new, space-optimized image where duplicate files are replaced with hardlinks or symbolic links, which can significantly reduce the overall image size.

## Description

This tool inspects Docker images that have been saved as tarballs (using `docker save`). It identifies files with identical content that appear in multiple locations, either within the same layer or across different layers.

Based on this analysis, it can create a new image where:
- Duplicates within the same layer are replaced with **hardlinks**.
- Duplicates in different layers are replaced with **symbolic links** to the canonical file in the earliest layer.

This process reduces storage redundancy without changing the logical file structure of the image, making your container images smaller and more efficient.

## Usage

### 1. Save a Docker Image

First, save the Docker image you want to analyze as a `.tar` file:

```sh
docker save your-image:latest > your-image.tar
```

### 2. Run the Analyzer

Next, run the `docker_duplicate_files` tool, providing the input image tarball and specifying an output path for the new, deduplicated image.

```sh
cargo run --release -- --image your-image.tar --output your-image-deduped.tar
```

By default, the tool only considers files with a size of 1MB or greater. You can adjust this with the `--min-size` flag (in bytes). For example, to process files larger than 100KB:

```sh
cargo run --release -- --image your-image.tar --output your-image-deduped.tar --min-size 100000
```

### 3. Load the New Image

Finally, load the optimized image back into Docker:

```sh
docker load < your-image-deduped.tar
```

### Command-Line Arguments

- `--image <path>`: (Required) Path to the input Docker image tarball.
- `--output <path>`: (Required) Path where the new, deduplicated image tarball will be saved.
- `--min-size <bytes>`: The minimum size of a file to be considered for deduplication. Defaults to `1000000` (1MB).
- `--no-compression`: Flag to disable compressing of output layers.

## Building from Source

To build the project from source, you need to have Rust and Cargo installed.

1.  Clone the repository:
    ```sh
    git clone https://github.com/aklimerman/docker_duplicate_files.git
    cd docker_duplicate_files
    ```

2.  Build the release executable:
    ```sh
    cargo build --release
    ```

The compiled binary will be available at `target/release/docker_duplicate_files`.
