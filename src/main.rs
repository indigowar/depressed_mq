use core::fmt;
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};

pub type Attribute = String;

pub type RawData = Vec<u8>;

pub type Headers = HashMap<String, String>;

#[derive(Debug, Default, Clone)]
pub struct Message {
    pub offset: usize,
    pub size: usize,
    pub attrs: Vec<Attribute>,
    pub headers: Headers,
    pub timestamp: DateTime<Utc>,

    pub key: Option<RawData>,
    pub value: RawData,
}

pub struct MessageCreationArguments {
    pub attrs: Vec<Attribute>,
    pub headers: Headers,
    pub timestamp: DateTime<Utc>,

    pub key: Option<RawData>,
    pub value: RawData,
}

impl Message {
    pub fn new(offset: usize, args: MessageCreationArguments) -> Self {
        Self {
            offset,
            size: args.value.len(),
            attrs: args.attrs,
            headers: args.headers,
            timestamp: args.timestamp,
            key: args.key,
            value: args.value,
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Offset {}]", self.offset)?;
        write!(f, "[Size {}]", self.size)?;
        write!(f, "[Timestamp {}]", self.timestamp)
    }
}

const SEGMENT_CAPACITY: usize = 1000;

pub struct MemorySegment {
    start: usize, // starting offset
    end: usize,   // ending offset

    pub data: Vec<Message>,
}

impl MemorySegment {
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

pub struct Partition {
    segments: Vec<MemorySegment>,
    size: usize,
}

impl Partition {
    pub fn new() -> Self {
        Self {
            segments: vec![MemorySegment::new(0)],
            size: 0,
        }
    }

    pub fn write(&mut self, args: MessageCreationArguments) -> Result<(), String> {
        match self.segments.last_mut() {
            Some(s) => {
                if s.len() == SEGMENT_CAPACITY {
                    self.segments.push(MemorySegment::new(0));
                    return self.write(args);
                }

                let latest = self.size;
                self.size += 1;
                match s.write(Message::new(latest, args)) {
                    Some(_) => Ok(()),
                    None => Err("failed to write".into()),
                }
            }
            None => {
                self.segments.push(MemorySegment::new(0));
                self.write(args)
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

fn generate_random_vec() -> Vec<u8> {
    let size = {
        let start = SystemTime::now();
        let duration = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        duration.as_secs() as usize % 101
    };
    let mut vec = vec![0u8; size];
    fill_random_bytes(&mut vec);
    vec
}

fn fill_random_bytes(buf: &mut [u8]) {
    let mut file = File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    file.read_exact(buf).expect("Failed to read random bytes");
}

fn main() {
    println!("Hello, world!");

    let mut p = Partition::new();

    for _ in 0..10 {
        p.write(MessageCreationArguments {
            attrs: vec![],
            headers: HashMap::new(),
            timestamp: Utc::now(),
            key: None,
            value: generate_random_vec(),
        })
        .unwrap();
    }

    for i in 0..10 {
        match p.read(i) {
            Ok(msg) => println!("{}", msg),
            Err(e) => println!("{}", e),
        }
    }
}
