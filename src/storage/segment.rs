use std::{
    fs::{File, OpenOptions},
    io::{Error, ErrorKind, Read, Seek, Write},
};

use serde::{Deserialize, Serialize};

use crate::core::message::Message;

use super::{offset_index::OffsetIndex, timestamp_index::TimestampIndex};

pub struct Segment {
    log: File,
    offset_index: OffsetIndex,
    time_index: TimestampIndex,

    range: (usize, usize),
}

impl Segment {
    pub fn new(path: String, number: i32, range: (usize, usize)) -> Result<Segment, Error> {
        let log_path = format!("{}/{:08}.log", path, number);
        let offset_index_path = format!("{}/{:08}.index", path, number);
        let time_index_path = format!("{}/{:08}.timeindex", path, number);

        let log = Self::open_log_file(log_path)?;
        let offset_index = OffsetIndex::new(offset_index_path)?;
        let time_index = TimestampIndex::new(time_index_path)?;

        Ok(Segment {
            log,
            offset_index,
            time_index,
            range,
        })
    }

    pub fn write(&mut self, message: Message) -> Result<(), Error> {
        let value = serde_cbor::to_vec(&message).unwrap();
        let header = serde_cbor::to_vec(&Header { size: value.len() }).unwrap();

        self.log.seek(std::io::SeekFrom::End(0))?;

        self.log.write(&header)?;
        self.log.write(&value)?;

        self.log.flush()
    }

    pub fn read(&mut self, offset: usize) -> Result<Message, Error> {
        let physical = self.offset_index.read(offset)? as u64;

        self.log.seek(std::io::SeekFrom::Start(physical))?;
        let header = self.read_header()?;

        let mut buf: Vec<u8> = Vec::with_capacity(header.size);
        self.log.read_exact(buf.as_mut_slice())?;

        match serde_cbor::from_slice::<Message>(buf.as_slice()) {
            Ok(m) => Ok(m),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e.to_string())),
        }
    }

    fn read_header(&mut self) -> Result<Header, Error> {
        const HEADER_SIZE: usize = std::mem::size_of::<Header>();
        let mut buffer = [0; HEADER_SIZE];

        self.log.read_exact(&mut buffer)?;

        match serde_cbor::from_slice::<Header>(&buffer) {
            Ok(h) => Ok(h),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e.to_string())),
        }
    }

    fn open_log_file(path: String) -> Result<File, Error> {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
    }
}

// Header is written before an actual message and stores the size of that message,
// this done to simplify the read.
#[derive(Deserialize, Serialize)]
struct Header {
    size: usize,
}
