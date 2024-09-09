use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
};

use serde::{Deserialize, Serialize};

pub struct OffsetIndex {
    file: RefCell<File>,
    path: String,
}

impl OffsetIndex {
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

    pub fn write(&mut self, logical: usize, physical: usize) -> Result<(), Error> {
        let data = Index::serialize(Index { logical, physical })?;

        let mut file = self.file.borrow_mut();

        file.seek(SeekFrom::End(0))?;
        file.write_all(&data)
    }

    pub fn read(&self, logical: usize) -> Result<usize, Error> {
        let mut buffer = vec![0; Index::size()];

        self.file.borrow_mut().seek(SeekFrom::Start(0))?;
        loop {
            let index = self.read_index(Some(&mut buffer))?;
            if index.logical == logical {
                return Ok(index.physical);
            }
        }
    }

    pub fn size(&self) -> Result<usize, Error> {
        let file_size = fs::metadata(self.path.clone())?.size() as usize;
        Ok(file_size / Index::size())
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
    logical: usize,
    physical: usize,
}

impl Index {
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
