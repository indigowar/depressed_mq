use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
};

use chrono::{DateTime, TimeZone, Utc};
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

    pub fn write(&self, timestamp: DateTime<Utc>, offset: usize) -> Result<(), Error> {
        let data = Index::serialize(Index {
            offset,
            timestamp: timestamp.timestamp_nanos_opt().unwrap(),
        })?;

        let mut file = self.file.borrow_mut();

        file.seek(SeekFrom::End(0))?;
        file.write_all(&data)
    }

    pub fn read(&self, timestamp: DateTime<Utc>) -> Result<usize, Error> {
        let timestamp = timestamp.timestamp_nanos_opt().unwrap();
        let mut buffer = vec![0u8; Index::size()];

        self.file.borrow_mut().seek(SeekFrom::Start(0))?;
        loop {
            let index = self.read_index(Some(&mut buffer))?;
            if index.timestamp == timestamp {
                return Ok(index.offset);
            }
        }
    }

    pub fn latest_timestamp(&self) -> Result<DateTime<Utc>, Error> {
        let offset = -(Index::size() as i64);

        self.file.borrow_mut().seek(SeekFrom::End(offset))?;
        let index = self.read_index(None)?;

        Ok(Utc.timestamp_nanos(index.timestamp))
    }

    pub fn size(&self) -> Result<usize, Error> {
        let file_size = fs::metadata(self.path.clone())?.size() as usize;
        Ok(file_size / Index::size())
    }

    fn read_index(&self, buffer: Option<&mut [u8]>) -> Result<Index, Error> {
        let mut owned_buffer;
        let buffer = match buffer {
            Some(b) => b,
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
    pub fn size() -> usize {
        let object = Self::default();
        bincode::serialize(&object).unwrap().len()
    }

    pub fn deserialize(buffer: &[u8]) -> Result<Self, Error> {
        bincode::deserialize(buffer).map_err(Self::error)
    }

    pub fn serialize(object: Self) -> Result<Vec<u8>, Error> {
        bincode::serialize(&object).map_err(Self::error)
    }

    fn error(e: bincode::Error) -> Error {
        Error::new(ErrorKind::InvalidData, e)
    }
}
