use chrono::{DateTime, Utc};

use crate::message::{Message, RawData};

const SEGMENT_CAPACITY: usize = 1000;

pub struct Partition {
    segments: Vec<InMemorySegment>,
    size: usize,
}

impl Partition {
    pub fn new() -> Self {
        Self {
            segments: vec![InMemorySegment::new(0)],
            size: 0,
        }
    }

    pub fn write(
        &mut self,

        timestamp: DateTime<Utc>,
        key: Option<RawData>,
        value: RawData,
    ) -> Result<(), String> {
        match self.segments.last_mut() {
            Some(s) => {
                if s.len() == SEGMENT_CAPACITY {
                    self.segments.push(InMemorySegment::new(0));
                    return self.write(timestamp, key, value);
                }

                let latest = self.size;
                self.size += 1;
                match s.write(Message::new(latest, timestamp, key, value)) {
                    Some(_) => Ok(()),
                    None => Err("failed to write".into()),
                }
            }
            None => {
                self.segments.push(InMemorySegment::new(0));
                self.write(timestamp, key, value)
            }
        }
    }

    pub fn read(&self, offset: usize) -> Result<Message, String> {
        for segment in &self.segments {
            if offset >= segment.start_offset() && offset < segment.end_offset() {
                if let Some(message) = segment.read(offset) {
                    return Ok(message);
                } else {
                    return Err(format!("Message not found at offset {}", offset));
                }
            }
        }
        Err(format!("Offset {} out of bounds", offset))
    }
}

struct InMemorySegment {
    start: usize, // starting offset
    end: usize,   // ending offset

    data: Vec<Message>,
}

impl InMemorySegment {
    pub fn new(start: usize) -> Self {
        Self {
            start,
            end: start + SEGMENT_CAPACITY,
            data: Vec::with_capacity(SEGMENT_CAPACITY),
        }
    }

    pub fn start_offset(&self) -> usize {
        self.start
    }

    pub fn end_offset(&self) -> usize {
        self.end
    }

    pub fn read(&self, offset: usize) -> Option<Message> {
        if offset < self.start {
            return None;
        }

        let index = offset - self.start;
        self.data.get(index).cloned()
    }

    pub fn write(&mut self, msg: Message) -> Option<usize> {
        if msg.offset < self.start || msg.offset > self.end {
            return None;
        }

        self.data.push(msg.clone());

        Some(msg.offset)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}
