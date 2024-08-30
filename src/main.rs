use core::fmt;
use std::{
    collections::{HashMap, VecDeque},
    fs::File,
    io::Read,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{DateTime, Utc};

pub type Attribute = String;

pub type RawData = Vec<u8>;

pub type Headers = HashMap<String, String>;

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

pub struct Partition {
    queue: VecDeque<Message>,
}

impl Partition {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    pub fn publish(&mut self, args: MessageCreationArguments) {
        let offset = match self.queue.back() {
            Some(m) => m.offset + 1,
            None => 0,
        };
        self.queue.push_back(Message::new(offset, args))
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
        p.publish(MessageCreationArguments {
            attrs: vec![],
            headers: HashMap::new(),
            timestamp: Utc::now(),
            key: None,
            value: generate_random_vec(),
        })
    }

    for msg in p.queue {
        println!("{}", msg);
    }
}
