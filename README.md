# depressed_mq

It was created for the sake of creation...

Built in rust for the sake of rust.

# Task

- [x] Write a basic structure of the message queue;
- [x] Write a simple FS driver to store things on the drive;
    - [x] Make partition save the messages to a log named `XXXXXXXX.log`;
    - [x] Make partition create an index for logical offset(from the message) to physical offset(offset in file in bytes) in file `XXXXXXX.index`;
    - [x] Make partition create an index for timestamp to logical offset in file `XXXXXXX.timeindex`;
- [ ] Write a simple topic system
- [ ] Write a basic TCP server that handles producing/consuming;
- [ ] Write a distribution mechanism that will send over the messages to other brokers
don't even know how(??)
