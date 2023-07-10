use anyhow::Result;
use std::{fs::File, io::Read};

use super::FailKeys;

pub fn compare_from_file(path: &str) -> Result<FailKeys> {
    // open file
    let mut file = File::open(path)?;
    // 反序列化文件
    let mut buf = vec![];
    let _ = file.read_to_end(&mut buf);
    let fk = rmp_serde::from_slice::<FailKeys>(&buf)?;
    let iffies = fk.compare()?;
    let new_result = FailKeys {
        source: fk.source.clone(),
        target: fk.target.clone(),
        iffy_keys: iffies,
        ttl_diff: fk.ttl_diff,
        batch: fk.batch,
        reverse: fk.reverse,
    };

    Ok(new_result)
}

#[cfg(test)]
mod test {
    use super::compare_from_file;

    //cargo test compare::compare_from_file::test::test_compare_from_file --  --nocapture
    #[test]
    fn test_compare_from_file() {
        let r = compare_from_file("./cr_1666835473_9zEz/1666835487459719000_7f0q.cr");
        println!("{:?}", r);
    }
}
