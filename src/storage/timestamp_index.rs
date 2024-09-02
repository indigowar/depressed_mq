use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// TimestampIndex used to manage XXXXXXXXx.timestamp file for XXXXXXXXx partition.
// It stores `timestamp` to `logical offset`.
pub struct TimestampIndex {
    file: File,
}

#[derive(Serialize, Deserialize)]
struct Index {
    offset: usize,
    timestamp: i64,
}

impl Index {
    pub fn size_serialized() -> usize {
        let index = Index {
            offset: 0,
            timestamp: 0,
        };
        serde_cbor::to_vec(&index).unwrap().len()
    }
}

impl TimestampIndex {
    pub fn new(path: String) -> Result<Self, Error> {
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

    pub fn read(&mut self, timestamp: DateTime<Utc>) -> Result<usize, Error> {
        let timestamp = timestamp.timestamp_nanos_opt().unwrap();
        let index_size = Index::size_serialized();

        let mut buffer = Vec::with_capacity(index_size);

        self.file.seek(SeekFrom::Start(0))?;

        loop {
            let bytes_read = self.file.read(&mut buffer)?;
            if bytes_read != index_size {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "index not found in the file",
                ));
            }

            let index: Index =
                serde_cbor::from_slice(&buffer).map_err(Self::serialization_error)?;

            if index.timestamp == timestamp {
                return Ok(index.offset);
            }
        }
    }

    pub fn write(&mut self, timestamp: DateTime<Utc>, offset: usize) -> Result<(), Error> {
        let index = Index {
            offset,
            timestamp: timestamp.timestamp_nanos_opt().unwrap(),
        };

        let encoded = serde_cbor::to_vec(&index).map_err(Self::serialization_error)?;

        self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&encoded)
    }

    fn serialization_error(e: serde_cbor::Error) -> Error {
        Error::new(ErrorKind::InvalidData, e)
    }
}
