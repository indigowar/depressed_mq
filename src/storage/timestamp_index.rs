use std::io;

use chrono::{DateTime, Utc};

// TimestampIndex used to manage XXXXXXXXx.timestamp file for XXXXXXXXx partition.
// It stores `timestamp` to `logical offset`.
pub struct TimestampIndex {}

impl TimestampIndex {
    pub fn new(_path: String) -> Result<Self, io::Error> {
        todo!()
    }

    pub fn read(&self, _timestamp: DateTime<Utc>) -> Result<usize, io::Error> {
        todo!()
    }

    pub fn write(
        &mut self,
        _timestamp: DateTime<Utc>,
        _logical_offset: usize,
    ) -> Result<(), io::Error> {
        todo!()
    }
}
