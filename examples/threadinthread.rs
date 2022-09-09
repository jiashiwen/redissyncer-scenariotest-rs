use std::{thread, time};

// 验证多线程，可控制时间cancel
// 验证rayon 线程池
fn main() -> Result<(), rayon::ThreadPoolBuildError> {
    // let (sender, r) = unbounded::<isize>();

    let pool_out = rayon::ThreadPoolBuilder::new().num_threads(2).build()?;

    // let i = 15;
    pool_out.scope(|s| {
        for _ in 0..10 {
            s.spawn(move |_| {
                let pool_task = rayon::ThreadPoolBuilder::new()
                    .num_threads(2)
                    .build()
                    .unwrap();
                for i in 0..5 {
                    pool_task.scope(|s| {
                        s.spawn(move |_| {
                            task(i);
                        })
                    });
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
