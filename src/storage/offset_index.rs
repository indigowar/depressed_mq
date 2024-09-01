use std::{
    fs::{File, OpenOptions},
    io,
};

use serde::{Deserialize, Serialize};

// OffsetIndex used to manage XXXXXX.index file for XXXXXX partition.
// It stores `logical offset` to `physical byte offset`.
pub struct OffsetIndex {
    file: File,
}

#[derive(Serialize, Deserialize)]
struct Index {
    logical: usize,
    physical: usize,
}

impl OffsetIndex {
    pub fn new(path: String) -> Result<Self, io::Error> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(file) => Ok(Self { file }),
            Err(e) => Err(e),
        }
    }

    pub fn read(&self, _logical: usize) -> Result<usize, io::Error> {
        todo!()
    }

    pub fn write(&mut self, _logical: usize, _physical: usize) -> Result<(), io::Error> {
        todo!()
    }
}
