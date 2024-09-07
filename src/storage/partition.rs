use core::fmt;
use std::{
    fs,
    io::Error,
    path::Path,
    sync::{Arc, Mutex, RwLock},
};

use chrono::{DateTime, Utc};

use crate::core::message::{Message, RawData};

use super::segment::Segment;

/// Partition is an immutable log of messages.
pub struct Partition {
    /// ID of the partition that is stored in f.e. topics.
    number: usize,
    /// base_path is the path, where this partition stores data.
    path: String,
    /// segment_size is the max amount of messages that could be stored in one segment.
    segment_size: usize,
    /// next_offset is an offset that will be given to the next created message in this log.
    next_offset: usize,

    segments: Arc<RwLock<Vec<Arc<Mutex<Segment>>>>>,
}

impl Partition {
    pub fn new(path: String, number: usize, segment_size: usize) -> Result<Self, Error> {
        let dir_path = format!("{}/{:08}", &path, number);
        match Path::new(&dir_path).exists() {
            true => Self::load(format!("{}/", dir_path), number, segment_size),
            false => Self::init(dir_path, number, segment_size),
        }
    }

    pub fn write(
        &mut self,
        _timestamp: DateTime<Utc>,
        _key: Option<RawData>,
        _value: RawData,
    ) -> Result<(), Error> {
        todo!()
    }

    pub fn read(&self, _offset: usize) -> Result<Message, Error> {
        todo!()
    }

    fn init(path: String, number: usize, segment_size: usize) -> Result<Self, Error> {
        fs::create_dir_all(&path)?;

        let segment = Segment::new(path.clone(), 0, Self::segment_size(0, segment_size))?;

        Ok(Self {
            number,
            path,
            segment_size,
            next_offset: 0,
            segments: Arc::new(RwLock::new(vec![Arc::new(Mutex::new(segment))])),
        })
    }

    fn load(path: String, number: usize, segment_size: usize) -> Result<Self, Error> {
        let paths: Vec<String> = fs::read_dir(&path)?
            .filter_map(|entry| entry.ok()) // Filter out errors
            .filter(|entry| entry.path().is_file()) // Ensure it's a file
            .filter(|entry| entry.path().extension() == Some("log".as_ref())) // Check for ".log" extension
            .filter_map(|entry| entry.path().canonicalize().ok()) // Convert to absolute path
            .filter_map(|path| path.to_str().map(|s| s.to_string())) // Convert to String
            .collect();

        let mut segments = Vec::with_capacity(paths.len());
        for i in 0..paths.len() {
            let s = Segment::new(path.clone(), i as i32, Self::segment_size(i, segment_size))?;
            segments.push(s);
        }

        let next_offset = segments.last().unwrap().size().unwrap_or_default();

        let segments = segments
            .into_iter()
            .map(|s| Arc::new(Mutex::new(s)))
            .collect();

        Ok(Self {
            number,
            path,
            segment_size,
            next_offset,
            segments: Arc::new(RwLock::new(segments)),
        })
    }

    fn segment_size(n: usize, ss: usize) -> (usize, usize) {
        (n * ss, (n + 1) * ss)
    }
}

impl fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Partition #{}, P: `{}`, SS: {:?}, NO: {}]",
            self.number, &self.path, self.segment_size, self.next_offset,
        )
    }
}
