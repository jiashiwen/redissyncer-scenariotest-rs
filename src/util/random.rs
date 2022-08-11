use rand::Rng;

//生成定长随机字符串
pub fn rand_string(len: usize) -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";
    let mut rng = rand::thread_rng();

    let str: String = (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    str
}

#[cfg(test)]
mod test {
    use std::time::Instant;
    use super::*;

    //cargo test util::random::test::test_rend_string -- --nocapture
    #[test]
    fn test_rend_string() {
        for _ in 0..10 {
            let str = rand_string(8);
            println!("{}", str);
        }
        let now = Instant::now();
        for i in 0..10000 {
            let str = rand_string(32);
            // println!("{}-{}", i, str);
        }
        println!("时间差： {}", now.elapsed().as_millis());
    }
}
