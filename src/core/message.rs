use core::fmt;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer, Serialize};

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

#[derive(Serialize, Deserialize)]
struct SerializedMessage {
    pub offset: usize,
    pub size: usize,
    pub timestamp: i64,

    pub key: Option<RawData>,
    pub value: RawData,
}

impl From<Message> for SerializedMessage {
    fn from(value: Message) -> Self {
        Self {
            offset: value.offset,
            size: value.size,
            timestamp: value.timestamp.timestamp_nanos_opt().unwrap(),
            key: value.key,
            value: value.value,
        }
    }
}

impl Into<Message> for SerializedMessage {
    fn into(self) -> Message {
        Message {
            offset: self.offset,
            size: self.size,
            timestamp: Utc.timestamp_nanos(self.timestamp),
            key: self.key,
            value: self.value,
        }
    }
}

impl Serialize for Message {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SerializedMessage::from(self.clone()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized: SerializedMessage = Deserialize::deserialize(deserializer)?;
        Ok(serialized.into())
    }
}
