use std::{io, path::PathBuf};

// OffsetIndex used to manage XXXXXX.index file for XXXXXX partition.
// It stores `logical offset` to `physical byte offset`.
pub struct OffsetIndex {}

impl OffsetIndex {
    pub fn new(_path: PathBuf) -> Self {
        todo!()
    }

    pub fn read(&self, _logical_offset: usize) -> Result<usize, io::Error> {
        todo!()
    }

    pub fn write(
        &mut self,
        _logical_offset: usize,
        _physical_offset: usize,
    ) -> Result<(), io::Error> {
        todo!()
    }
}
