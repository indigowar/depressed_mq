use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub struct TimestampIndex {
    file: RefCell<File>,
    path: String,
}

impl TimestampIndex {
    pub fn new(path: String) -> Result<Self, Error> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.clone())
        {
            Ok(file) => Ok(Self {
                file: RefCell::new(file),
                path,
            }),
            Err(e) => Err(e),
        }
    }

    pub fn read(&self, _timestamp: DateTime<Utc>) -> Result<usize, Error> {
        let timestamp = _timestamp.timestamp_nanos_opt().unwrap();

        let mut buffer = vec![0u8; Index::size()];

        self.file.borrow_mut().seek(SeekFrom::Start(0))?;
        loop {
            let index = self.read_index(Some(&mut buffer))?;
            if index.timestamp == timestamp {
                return Ok(index.offset);
            }
        }
    }

    pub fn write(&self, timestamp: DateTime<Utc>, offset: usize) -> Result<(), Error> {
        let data = Index::serialize(Index::new(timestamp, offset))?;

        let mut file = self.file.borrow_mut();

        file.seek(SeekFrom::End(0))?;
        file.write_all(&data)
    }

    fn read_index(&self, buffer: Option<&mut [u8]>) -> Result<Index, Error> {
        let mut owned_buffer;
        let buffer = match buffer {
            Some(buf) => buf,
            None => {
                owned_buffer = vec![0u8; Index::size()];
                owned_buffer.as_mut_slice()
            }
        };

        let bytes_read = self.file.borrow_mut().read(buffer)?;
        if bytes_read != Index::size() {
            return Err(Error::new(
                ErrorKind::NotFound,
                "index not found in the file",
            ));
        }

        Index::deserialize(buffer)
    }
}

#[derive(Serialize, Deserialize, Default)]
struct Index {
    offset: usize,
    timestamp: i64,
}

impl Index {
    pub fn new(ts: DateTime<Utc>, offset: usize) -> Index {
        Index {
            offset,
            timestamp: ts.timestamp_nanos_opt().unwrap(),
        }
    }

    pub fn size() -> usize {
        let index = Index::default();

        bincode::serialize(&index).unwrap().len()
    }

    pub fn deserialize(buffer: &[u8]) -> Result<Self, Error> {
        bincode::deserialize(buffer).map_err(Self::error)
    }

    pub fn serialize(index: Index) -> Result<Vec<u8>, Error> {
        bincode::serialize(&index).map_err(Self::error)
    }

    fn error(e: bincode::Error) -> Error {
        Error::new(ErrorKind::InvalidData, e)
    }
}
