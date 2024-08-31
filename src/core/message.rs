use core::fmt;

use chrono::{DateTime, Utc};

pub type RawData = Vec<u8>;

#[derive(Debug, Clone)]
pub struct Message {
    pub offset: usize,
    pub size: usize,
    pub timestamp: DateTime<Utc>,

    pub key: Option<RawData>,
    pub value: RawData,
    // TODO: maybe add:
    // - attributes(HashMap<String, String>)
    // - headers(Vec<String>
    //
    // They are used in kafka, but idk how to use them here
}

impl Message {
    pub fn new(
        offset: usize,
        timestamp: DateTime<Utc>,
        key: Option<RawData>,
        value: RawData,
    ) -> Self {
        Self {
            offset,
            size: value.len(),
            timestamp,
            key,
            value,
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Offset {}]", self.offset)?;
        write!(f, "[Size {}]", self.size)?;
        write!(f, "[Timestamp {}]", self.timestamp)?;
        match &self.key {
            Some(v) => write!(f, "[Key {:?}", v),
            None => write!(f, "[Key NONE]"),
        }?;
        write!(f, "[Data {:?}]", self.value)
    }
}
