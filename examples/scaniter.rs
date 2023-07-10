use redis::FromRedisValue;

// ToDo
// 实现一个SCAN 迭代器，返回固定长度的 Vec<String>
pub struct ScanBatch<'a, T: FromRedisValue> {
    pub scan_iter: redis::Iter<'a, T>,
    pub batch: usize,
}

fn main() {}
