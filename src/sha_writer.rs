use ring::digest::{Context, SHA256};
use std::io::{self, Write};

/// A wrapper around ring's SHA256 Context that implements the Write trait.
/// This allows you to use it as a drop-in replacement for other hashers.
pub struct Sha256Writer {
    context: Context,
}

impl Sha256Writer {
    /// Creates a new SHA256 writer
    pub fn new() -> Self {
        Self {
            context: Context::new(&SHA256),
        }
    }

    pub fn finalize(self) -> Vec<u8> {
        let digest = self.context.finish();
        digest.as_ref().to_vec()
    }

    pub fn finalize_hex(self) -> String {
        let digest = self.context.finish();
        Self::bytes_to_hex(digest.as_ref())
    }
    fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

impl Default for Sha256Writer {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for Sha256Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.context.update(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // No-op for hash functions
        Ok(())
    }
}
