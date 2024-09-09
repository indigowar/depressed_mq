use core::fmt;
use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::message::Message;

use super::{offset_index::OffsetIndex, timestamp_index::TimestampIndex};

pub struct Segment {
    base_path: String,
    number: i32,
    log_path: String,
    range: (usize, usize),

    log: RefCell<File>,
    offset_index: OffsetIndex,
    time_index: TimestampIndex,
}

impl Segment {
    pub fn new(path: String, number: i32, range: (usize, usize)) -> Result<Self, Error> {
        let log_path = format!("{}/{:08}.log", path, number);
        let offset_index_path = format!("{}/{:08}.index", path, number);
        let time_index_path = format!("{}/{:08}.timeindex", path, number);

        let log_file = Self::open_log_file(log_path.clone())?;
        let offset_index = OffsetIndex::new(offset_index_path)?;
        let time_index = TimestampIndex::new(time_index_path)?;

        Ok(Self {
            base_path: path,
            number,
            log_path,
            range,
            log: RefCell::new(log_file),
            offset_index,
            time_index,
        })
    }

    pub fn write(&mut self, message: Message) -> Result<(), Error> {
        self.offset_range_guard(message.offset)?;

        let logical_offset = message.offset;
        let physical_offset = fs::metadata(self.log_path.clone()).unwrap().size() as usize;
        let timestamp = message.timestamp;

        let data = Self::serialize_message(message)?;
        let header = Header::serialize(Header { size: data.len() })?;

        let mut file = self.log.borrow_mut();

        file.seek(SeekFrom::End(0))?;
        file.write_all(&header)?;
        file.write_all(&data)?;
        file.flush()?;

        self.offset_index.write(logical_offset, physical_offset)?;
        self.time_index.write(timestamp, logical_offset)
    }

    pub fn read(&self, offset: usize) -> Result<Message, Error> {
        self.offset_range_guard(offset)?;

        let physical_offset = self.offset_index.read(offset)?;
        self.log
            .borrow_mut()
            .seek(SeekFrom::Start(physical_offset as u64))?;

        let header = self.read_header(None)?;
        let mut buffer = vec![0u8; header.size];
        self.log.borrow_mut().read(&mut buffer)?;

        Self::deserialize_message(&buffer)
    }

    pub fn read_by_timestamp(&self, timestamp: DateTime<Utc>) -> Result<Message, Error> {
        let offset = self.time_index.read(timestamp)?;
        self.read(offset)
    }

    pub fn size(&self) -> Result<usize, Error> {
        let offset_size = self.offset_index.size()?;
        let timestamp_size = self.time_index.size()?;

        if offset_size != timestamp_size {
            return Err(Error::new(
                ErrorKind::Other,
                "offset index size is different than timestamp index size",
            ));
        }

        Ok(offset_size)
    }

    pub fn belongs_to_segment(&self, offset: usize) -> bool {
        offset >= self.range.0 && offset < self.range.1
    }

    fn offset_range_guard(&self, offset: usize) -> Result<(), Error> {
        if offset >= self.range.0 && offset < self.range.1 {
            return Ok(());
        }

        Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "given offset({}) does not belong to this segment{:?}",
                offset, self.range
            ),
        ))
    }

    fn read_header(&self, buffer: Option<&mut [u8]>) -> Result<Header, Error> {
        let mut owned_buffer;
        let buffer = match buffer {
            Some(b) => b,
            None => {
                owned_buffer = vec![0u8; Header::size()];
                owned_buffer.as_mut_slice()
            }
        };

        self.log.borrow_mut().read_exact(buffer)?;

        Header::deserialize(buffer)
    }

    fn open_log_file(path: String) -> Result<File, Error> {
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
    }

    fn deserialize_message(buffer: &[u8]) -> Result<Message, Error> {
        bincode::deserialize(&buffer).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("failed to deserialize a Message: {}", e),
            )
        })
    }

    fn serialize_message(msg: Message) -> Result<Vec<u8>, Error> {
        bincode::serialize(&msg).map_err(|e| {
            Error::new(
                ErrorKind::InvalidData,
                format!("failed to serialize a Message: {}", e),
            )
        })
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[Segment #{} from `{}` with range {:?}]",
            self.number, self.base_path, self.range
        )
    }
}

// Header is written before an actual message and stores the size of that message,
// this done to simplify the read.
#[derive(Deserialize, Serialize, Default)]
struct Header {
    size: usize,
}

impl Header {
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
