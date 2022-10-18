use std::{
    fs::{self, File, OpenOptions},
    io::{LineWriter, Write},
    thread, time,
};

// 验证多线程，可控制时间cancel
// 验证rayon 线程池
fn main() -> Result<(), rayon::ThreadPoolBuildError> {
    fs::remove_dir_all("/tmp/aba").unwrap();

    let pool_out = rayon::ThreadPoolBuilder::new().num_threads(3).build()?;

    pool_out.scope(|s| {
        for i in 0..3 {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open("/tmp/write_one.bin")
                .unwrap();
            let num = i;
            s.spawn(move |_| {
                let str = "thread".to_string() + &num.to_string();
                let mut lw = LineWriter::new(&file);
                for _ in 0..500 {
                    write_to_file(&mut lw, &str.as_bytes().to_vec());
                }
            });
        }
    });

    Ok(())
}

fn task(n: usize) {
    let mut count = 0;
    for i in 0..n {
        count += i;
    }
    thread::sleep(time::Duration::from_secs(n as u64));
    println!("{}", count);
    // count
}

fn write_to_file(f: &mut LineWriter<&File>, buf: &Vec<u8>) {
    f.write_all(buf);
}
