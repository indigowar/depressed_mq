use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, Write},
    os::unix::fs::MetadataExt,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::core::message::Message;

use super::{offset_index::OffsetIndex, timestamp_index::TimestampIndex};

pub struct Segment {
    log: File,
    log_path: String,
    offset_index: OffsetIndex,
    time_index: TimestampIndex,

    range: (usize, usize),
}

impl Segment {
    pub fn new(path: String, number: i32, range: (usize, usize)) -> Result<Segment, Error> {
        let log_path = format!("{}/{:08}.log", path, number);
        let offset_index_path = format!("{}/{:08}.index", path, number);
        let time_index_path = format!("{}/{:08}.timeindex", path, number);

        let log = Self::open_log_file(&log_path)?;
        let offset_index = OffsetIndex::new(offset_index_path)?;
        let time_index = TimestampIndex::new(time_index_path)?;

        Ok(Segment {
            log,
            log_path,
            offset_index,
            time_index,
            range,
        })
    }

    pub fn write(&mut self, message: Message) -> Result<(), Error> {
        if self.range.0 < message.offset || self.range.1 >= message.offset {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "offset is out of segment's range",
            ));
        }

        let value = serde_cbor::to_vec(&message).map_err(Self::serialization_error)?;
        let header =
            serde_cbor::to_vec(&Header { size: value.len() }).map_err(Self::serialization_error)?;

        let physical_offset = std::fs::metadata(&self.log_path)?.size() as usize;
        let timestamp = message.timestamp;

        self.log.seek(std::io::SeekFrom::End(0))?;

        self.log.write(&header)?;
        self.log.write(&value)?;
        self.log.flush()?;

        self.offset_index.write(message.offset, physical_offset)?;
        self.time_index.write(timestamp, message.offset)
    }

    pub fn read(&mut self, offset: usize) -> Result<Message, Error> {
        if self.range.0 < offset || self.range.1 >= offset {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "offset is out of segment's range",
            ));
        }

        let physical = self.offset_index.read(offset)? as u64;
        self.log.seek(std::io::SeekFrom::Start(physical))?;

        let header = self.read_header()?;
        let mut buf: Vec<u8> = Vec::with_capacity(header.size);

        self.log.read_exact(buf.as_mut_slice())?;

        match serde_cbor::from_slice::<Message>(buf.as_slice()) {
            Ok(m) => Ok(m),
            Err(e) => Err(Self::serialization_error(e)),
        }
    }

    pub fn read_by_timestamp(&mut self, timestamp: DateTime<Utc>) -> Result<Message, Error> {
        let offset = self.time_index.read(timestamp)?;
        self.read(offset)
    }

    fn read_header(&mut self) -> Result<Header, Error> {
        let mut buffer = Vec::with_capacity(Header::size_serialized());
        self.log.read_exact(&mut buffer)?;
        match serde_cbor::from_slice::<Header>(&buffer) {
            Ok(h) => Ok(h),
            Err(e) => Err(Self::serialization_error(e)),
        }
    }

    fn open_log_file(path: &String) -> Result<File, Error> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
    }

    fn serialization_error(e: serde_cbor::Error) -> Error {
        Error::new(ErrorKind::InvalidData, e)
    }
}

// Header is written before an actual message and stores the size of that message,
// this done to simplify the read.
#[derive(Deserialize, Serialize)]
struct Header {
    size: usize,
}

impl Header {
    pub fn size_serialized() -> usize {
        let header = Header { size: 0 };
        serde_cbor::to_vec(&header).unwrap().len()
    }
}
