use std::{thread, time};

// 验证多线程，可控制时间cancel
// 验证rayon 线程池
fn main() -> Result<(), rayon::ThreadPoolBuildError> {
    let pool = rayon::ThreadPoolBuilder::new().num_threads(2).build()?;
    pool.scope(|s| {
        for i in 0..9 {
            s.spawn(move |_| task(i));
        }
    });


    Ok(())

// thread::sleep(time::Duration::from_secs(20));

// let n = pool.install(|| fib(20));
// println!("{}", n);
}

fn task(n: usize) {
    let mut count = 0;
    for i in 0..n {
        count += i;
    }
    thread::sleep(time::Duration::from_secs(2));
    println!("{}", count);
    // count
}

fn fib(n: usize) -> usize {
    if n == 0 || n == 1 {
        return n;
    }
    let (a, b) = rayon::join(|| fib(n - 1), || fib(n - 2)); // runs inside of `pool`
    return a + b;
}