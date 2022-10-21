use chrono::{DateTime, Local};
use crossbeam::channel::{after, at, select, tick, unbounded, Receiver};

use std::time::{Duration, Instant};
use std::{thread, time};

// 验证多线程，可控制时间cancel
// 验证rayon 线程池
fn main() -> Result<(), rayon::ThreadPoolBuildError> {
    let (sender, r) = unbounded::<isize>();

    let pool = rayon::ThreadPoolBuilder::new().num_threads(2).build()?;
    let i = 15;
    pool.scope(|_| {
        println!("second scope");
    });
    pool.scope(|s| {
        for _ in 0..2 {
            s.spawn(move |_| ticker_task());
        }
        s.spawn(move |_| {
            println!("print {}", i);
        });
        s.spawn(move |_| task(i));
        s.spawn(move |_| deadline_task(Duration::from_secs(10)));
        s.spawn(move |_| signal_task(r));
        s.spawn(move |_| {
            thread::sleep(Duration::from_secs(4));
            let _ = sender.send(10);
        });
        // s.spawn(move |s| delay_task(Duration::from_secs(10), s));
        s.spawn(move |_| delay_task(Duration::from_secs(10)));
        s.spawn(move |_| println!("end"));
    });
    pool.scope(|s| {
        println!("second scope");
    });
    // for i in 0..20 {
    //     pool.scope(move |s| {
    //         let mut rng = rand::thread_rng();
    //         let secs = rng.gen_range(0..5);
    //         thread::sleep(Duration::from_secs(secs));
    //         println!("{}", i);
    //     });
    // }

    Ok(())
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

// 定时任务
fn ticker_task() {
    let tick = tick(Duration::from_secs(5));
    let mut count = 0 as usize;

    loop {
        select! {
            recv(tick) -> _ => {
                println!("{}",count);
            }
            default  =>{
                count += 1;
            }
        }
    }
}

// 定时结束任务
fn deadline_task(timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        select! {
             recv(at(deadline)) -> _ => {
                println!("timed out");
                return;
            },
             default => {
                // do your task
            }
        }
    }
}

// 发送结束任务信号
fn signal_task(r: Receiver<isize>) {
    loop {
        select! {
            recv(r) -> msg => {
                println!("receive signal {:?}",msg);
                return
            }
        }
    }
}

// 延时任务
// fn delay_task(delay: Duration, s: &Scope) {
fn delay_task(delay: Duration) {
    let after = after(delay);
    loop {
        select! {
            recv(after) -> _ => {
                println!("delays passed!");
                // thread::sleep(Duration::from_secs(5));
                // s.spawn(move |s| after_delay())
                rayon::scope(move |s| s.spawn(move |_| after_delay()));
            }
        }
    }
}

fn after_delay() {
    loop {
        let fmt = "%Y-%m-%d %H:%M:%S";

        let now: DateTime<Local> = Local::now();
        now.format(fmt);
        println!("after delay, now:{:?}", now.to_string());
        thread::sleep(Duration::from_secs(5));
    }
}

fn fib(n: usize) -> usize {
    if n == 0 || n == 1 {
        return n;
    }
    let (a, b) = rayon::join(|| fib(n - 1), || fib(n - 2)); // runs inside of `pool`
    return a + b;
}
