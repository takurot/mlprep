use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize)]
pub struct Metrics {
    #[serde(skip)]
    start_time: Instant,
    pub rows_read: usize,
    pub rows_written: usize,
    pub step_durations_ms: HashMap<String, u64>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            rows_read: 0,
            rows_written: 0,
            step_durations_ms: HashMap::new(),
        }
    }

    pub fn record_step(&mut self, step_name: &str, duration: Duration) {
        self.step_durations_ms
            .insert(step_name.to_string(), duration.as_millis() as u64);
    }

    pub fn total_duration(&self) -> Duration {
        self.start_time.elapsed()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Serialize)]
pub struct Lineage {
    pub run_id: String,
    pub timestamp: DateTime<Utc>,
    pub inputs: Vec<InputFileStats>,
    // We could add output path here too
    pub outputs: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct InputFileStats {
    pub path: String,
    pub hash: String, // SHA256 hex
    pub size_bytes: u64,
}

pub fn compute_file_hash<P: AsRef<Path>>(path: P) -> io::Result<String> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0; 8192]; // 8KB buffer

    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}
