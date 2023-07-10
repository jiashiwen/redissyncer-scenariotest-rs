use rmp_serde::Serializer;

use serde::{Deserialize, Serialize};

use std::{
    fs::{File, OpenOptions},
    io::{LineWriter, Read, Write},
};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Human {
    age: u32,
    name: String,
}

fn main() {
    let _ = std::fs::remove_file("/tmp/test.bin");
    let mut buf = Vec::new();

    println!("{:?}", "\n".as_bytes()[0].eq(&10));

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open("/tmp/test.bin")
        .unwrap();

    let mut line_writer = LineWriter::new(&file);

    for i in 0..5 {
        let val = Human {
            age: i,
            name: "John".into(),
        };

        //序列化 message pack
        val.serialize(&mut Serializer::new(&mut buf)).unwrap();

        //添加分隔符
        buf.push(10);
        &line_writer.write_all(&buf);
        // &mut file.write_all(&buf);

        buf.clear();
    }
    let mut read_file = File::open("/tmp/test.bin").unwrap();
    // let mut reader = BufReader::new(read_file);

    let mut v: Vec<u8> = vec![];
    const BUFFER_LEN: usize = 32;
    // let mut buffer = [0u8; BUFFER_LEN];

    loop {
        let mut buffer = [0u8; BUFFER_LEN];
        let read_count = read_file.read(&mut buffer).unwrap();

        for i in 0..read_count {
            if buffer[i].eq(&10) {
                println!("{:?}", v);
                let h = rmp_serde::from_slice::<Human>(&v).unwrap();
                println!("{:?}", h);
                v.clear();
            } else {
                v.push(buffer[i]);
            }
        }

        if read_count != BUFFER_LEN {
            break;
        }
    }
}
