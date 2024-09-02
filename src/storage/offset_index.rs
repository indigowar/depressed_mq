use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
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

impl Index {
    pub fn size_serialized() -> usize {
        let index = Index {
            logical: 0,
            physical: 0,
        };
        serde_cbor::to_vec(&index).unwrap().len()
    }
}

impl OffsetIndex {
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

    pub fn read(&mut self, logical: usize) -> Result<usize, Error> {
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

            if index.logical == logical {
                return Ok(index.physical);
            }
        }
    }

    pub fn write(&mut self, logical: usize, physical: usize) -> Result<(), Error> {
        let index = Index { logical, physical };
        let encoded = serde_cbor::to_vec(&index).map_err(Self::serialization_error)?;

        self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&encoded)
    }

    fn serialization_error(e: serde_cbor::Error) -> Error {
        Error::new(ErrorKind::InvalidData, e)
    }
}
