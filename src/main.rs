mod core;
mod partition;
mod storage;

use std::{
    fs::File,
    io::Read,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::Utc;

use partition::Partition;

fn main() {
    println!("Hello, world!");

    let mut p = Partition::new();

    for _ in 0..10 {
        p.write(Utc::now(), None, generate_random_vec()).unwrap();
    }

    for i in 0..10 {
        match p.read(i) {
            Ok(msg) => println!("{}", msg),
            Err(e) => println!("{}", e),
        }
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

    let mut file = File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    file.read_exact(&mut vec)
        .expect("Failed to read random bytes");
    vec
}
