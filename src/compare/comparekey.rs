use crate::compare::compare_error::{CompareError, CompareErrorType};

use crate::util::{RedisKey, RedisKeyType};
use anyhow::{Error, Result};
use redis::{AsyncIter, FromRedisValue, RedisResult, ToRedisArgs, Value};
use redis::{ConnectionLike, Iter};
use std::collections::HashMap;
use std::str::from_utf8;

pub struct Comparer<'a> {
    pub sconn: &'a mut (dyn ConnectionLike + 'a),
    pub tconn: &'a mut (dyn ConnectionLike + 'a),
    pub ttl_diff: usize,
    pub batch: usize,
}

impl<'a> Comparer<'a> {
    pub fn new(s_conn: &'a mut dyn ConnectionLike, t_conn: &'a mut dyn ConnectionLike) -> Self {
        Self {
            sconn: s_conn,
            tconn: t_conn,
            ttl_diff: 1,
            batch: 10,
        }
    }

    pub fn compare_key(&mut self, key: RedisKey) -> Result<()> {
        return match key.key_type {
            RedisKeyType::TypeString => self.compare_string(key),
            RedisKeyType::TypeList => self.compare_list(key),
            RedisKeyType::TypeSet => self.compare_set(key),
            RedisKeyType::TypeZSet => self.compare_zset(key),
            RedisKeyType::TypeHash => self.compare_hash(key),
        };
    }
}

//ToDo 优化错误类型输出
impl<'a> Comparer<'a> {
    pub fn compare_string(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        let t_exist = key_exists(key.key.clone(), self.tconn)?;

        if !t_exist {
            return Err(Error::from(CompareError::from_str(
                "key not exists target",
                CompareErrorType::ExistsErr,
            )));
        }

        // source target 值是否相等
        let sval = self
            .sconn
            .req_command(redis::cmd("get").arg(key.key.clone()))?;
        let tval = self
            .tconn
            .req_command(redis::cmd("get").arg(key.key.clone()))?;
        if !sval.eq(&tval) {
            return Err(Error::from(CompareError::from_str(
                "key value not equal",
                CompareErrorType::StringValueNotEqual,
            )));
        }

        // ttl差值是否在规定范围内
        let s_ttl = ttl(key.key.clone(), self.sconn)?;
        let t_ttl = ttl(key.key.clone(), self.tconn)?;

        if self.ttl_diff < (s_ttl - t_ttl).abs() as usize {
            return Err(Error::from(CompareError::from_str(
                "ttl diff too large",
                CompareErrorType::TTLDiff,
            )));
        }

        Ok(())
    }

    pub fn compare_list(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        let t_exist = key_exists(key.key.clone(), self.tconn)?;
        if !t_exist {
            return Err(Error::from(CompareError::from_str(
                "key not exists target",
                CompareErrorType::ExistsErr,
            )));
        }

        //比较 list 长度是否一致
        let s_len = list_len(key.key.clone(), self.sconn)?;
        let t_len = list_len(key.key.clone(), self.tconn)?;

        if !s_len.eq(&t_len) {
            return Err(Error::from(CompareError::from_str(
                "list len diff",
                CompareErrorType::ExistsErr,
            )));
        }

        // 遍历source，核对target中相应的值是否一致
        let quotient = s_len / self.batch; // integer division, decimals are truncated
        let remainder = s_len % self.batch;

        let mut lrangeend: isize = 0;
        if quotient != 0 {
            for i in 0..quotient {
                if i == quotient - 1 {
                    lrangeend = (quotient * self.batch) as isize;
                } else {
                    lrangeend = ((self.batch - 1) + i * self.batch) as isize;
                }

                let s_elements = lrange(
                    key.key.clone(),
                    (0 + i * self.batch) as isize,
                    lrangeend,
                    self.sconn,
                )?;
                let t_elements = lrange(
                    key.key.clone(),
                    (0 + i * self.batch) as isize,
                    lrangeend,
                    self.tconn,
                )?;

                for i in 0..s_elements.len() {
                    let s_val = s_elements.get(i);
                    let t_val = t_elements.get(i);

                    if !s_val.eq(&t_val) {
                        return Err(Error::from(CompareError::from_str(
                            "list index value diff",
                            CompareErrorType::ExistsErr,
                        )));
                    }
                }
            }
        }

        if remainder != 0 {
            let mut start: isize = 0;
            if quotient == 0 {
                start = 0;
            } else {
                start = (quotient * self.batch) as isize + 1;
            }

            let s_elements = lrange(
                key.key.clone(),
                start,
                (remainder + quotient * self.batch) as isize,
                self.sconn,
            )?;
            let t_elements = lrange(
                key.key.clone(),
                start,
                (remainder + quotient * self.batch) as isize,
                self.tconn,
            )?;
            for i in 0..s_elements.len() {
                let s_val = s_elements.get(i);
                let t_val = t_elements.get(i);

                if !s_val.eq(&t_val) {
                    return Err(Error::from(CompareError::from_str(
                        "list index value diff",
                        CompareErrorType::ExistsErr,
                    )));
                }
            }
        }

        // ttl差值是否在规定范围内
        let s_ttl = ttl(key.key.clone(), self.sconn)?;
        let t_ttl = ttl(key.key.clone(), self.tconn)?;

        if self.ttl_diff < (s_ttl - t_ttl).abs() as usize {
            return Err(Error::from(CompareError::from_str(
                "ttl diff too large",
                CompareErrorType::TTLDiff,
            )));
        }
        Ok(())
    }

    pub fn compare_set(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        let t_exist = key_exists(key.key.clone(), self.tconn)?;
        if !t_exist {
            return Err(Error::from(CompareError::from_str(
                "key not exists target",
                CompareErrorType::ExistsErr,
            )));
        }

        // 比较 set 元素数量 是否一致
        let s_size = scard(key.key.clone(), self.sconn)?;
        let t_size = scard(key.key.clone(), self.tconn)?;

        if !s_size.eq(&t_size) {
            return Err(Error::from(CompareError::from_str(
                "set members diff",
                CompareErrorType::SetCardDiff,
            )));
        }

        // 遍历source，核对在target是否存在
        let mut cmd_sscan = redis::cmd("sscan");
        cmd_sscan.arg(key.key.clone()).cursor_arg(0);
        let iter: Iter<String> = cmd_sscan.iter(self.sconn)?;
        for item in iter {
            let is = sismumber(key.key.clone(), item, self.tconn)?;
            if !is {
                return Err(Error::from(CompareError::from_str(
                    "set members diff",
                    CompareErrorType::SetMemberNotIn,
                )));
            }
        }

        // ttl差值是否在规定范围内
        let s_ttl = ttl(key.key.clone(), self.sconn)?;
        let t_ttl = ttl(key.key.clone(), self.tconn)?;

        if self.ttl_diff < (s_ttl - t_ttl).abs() as usize {
            return Err(Error::from(CompareError::from_str(
                "ttl diff too large",
                CompareErrorType::TTLDiff,
            )));
        }
        Ok(())
    }

    pub fn compare_zset(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        let t_exist = key_exists(key.key.clone(), self.tconn)?;
        if !t_exist {
            return Err(Error::from(CompareError::from_str(
                "key not exists target",
                CompareErrorType::ExistsErr,
            )));
        }

        // 比较 zset 元素数量 是否一致
        let s_size = zcard(key.key.clone(), self.sconn)?;
        let t_size = zcard(key.key.clone(), self.tconn)?;

        if !s_size.eq(&t_size) {
            return Err(Error::from(CompareError::from_str(
                "Sorted set members diff",
                CompareErrorType::ZSetCardDiff,
            )));
        }

        // 遍历source，核对在target score 和 值是否一致
        let mut cmd_zscan = redis::cmd("zscan");
        cmd_zscan.arg(key.key.clone()).cursor_arg(0);
        let iter: Iter<String> = cmd_zscan.iter(self.sconn)?;
        let mut count = 0 as usize;
        let mut member = "".to_string();

        for item in iter {
            if count % 2 == 0 {
                member = item.clone();
            } else {
                let t_scroe = zscore(key.key.clone(), member.clone(), self.tconn)?;

                if !t_scroe.to_string().eq(&item) {
                    return Err(Error::from(CompareError::from_str(
                        "zset member score diff",
                        CompareErrorType::ZSetMemberScoreDiff,
                    )));
                }
            }
            count += 1;
        }

        // ttl差值是否在规定范围内
        let s_ttl = ttl(key.key.clone(), self.sconn)?;
        let t_ttl = ttl(key.key.clone(), self.tconn)?;

        if self.ttl_diff < (s_ttl - t_ttl).abs() as usize {
            return Err(Error::from(CompareError::from_str(
                "ttl diff too large",
                CompareErrorType::TTLDiff,
            )));
        }
        Ok(())
    }

    pub fn compare_hash(&mut self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        let t_exist = key_exists(key.key.clone(), self.tconn)?;
        if !t_exist {
            return Err(Error::from(CompareError::from_str(
                "key not exists target",
                CompareErrorType::ExistsErr,
            )));
        }

        // 比较 hash 元素数量 是否一致
        let s_len = hlen(key.key.clone(), self.sconn)?;
        let t_len = hlen(key.key.clone(), self.tconn)?;
        if s_len != t_len {
            return Err(Error::from(CompareError::from_str(
                "hash len diff",
                CompareErrorType::HashLenDiff,
            )));
        }

        // 遍历source，核对在target field 和 value 是否一致
        let mut cmd_hscan = redis::cmd("hscan");
        cmd_hscan.arg(key.key.clone()).cursor_arg(0);
        let iter: Iter<String> = cmd_hscan.iter(self.sconn)?;
        let mut count = 0 as usize;
        let mut field = "".to_string();
        for item in iter {
            if count % 2 == 0 {
                field = item;
            } else {
                let t_val = hget(key.key.clone(), field.clone(), self.tconn)?;
                if !item.eq(&t_val) {
                    return Err(Error::from(CompareError::from_str(
                        "hash field value diff",
                        CompareErrorType::HashFieldValueDiff,
                    )));
                }
            }

            count += 1;
        }

        // ttl差值是否在规定范围内
        let s_ttl = ttl(key.key.clone(), self.sconn)?;
        let t_ttl = ttl(key.key.clone(), self.tconn)?;

        if self.ttl_diff < (s_ttl - t_ttl).abs() as usize {
            return Err(Error::from(CompareError::from_str(
                "ttl diff too large",
                CompareErrorType::TTLDiff,
            )));
        }
        Ok(())
    }
}

// key 是否存在
pub fn key_exists<T>(key: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<bool>
where
    T: ToRedisArgs,
{
    let exists: bool = redis::cmd("exists").arg(key).query(conn)?;
    Ok(exists)
}

pub fn ttl<T>(key: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<isize>
where
    T: ToRedisArgs,
{
    let ttl: isize = redis::cmd("ttl").arg(key).query(conn)?;
    Ok(ttl)
}

// List 长度
pub fn list_len<T>(key: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<usize>
where
    T: ToRedisArgs,
{
    let l: usize = redis::cmd("llen").arg(key).query(conn)?;

    Ok(l)
}

//Lrange
pub fn lrange<T>(
    key: T,
    start: isize,
    end: isize,
    conn: &mut dyn redis::ConnectionLike,
) -> RedisResult<Vec<String>>
where
    T: ToRedisArgs,
{
    let elements: Vec<String> = redis::cmd("lrange")
        .arg(key)
        .arg(start)
        .arg(end)
        .query(conn)?;
    Ok(elements)
}

// scard 获取set 元素数量
pub fn scard<T>(key: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<usize>
where
    T: ToRedisArgs,
{
    let size: usize = redis::cmd("scard").arg(key).query(conn)?;
    Ok(size)
}

// sismumber
pub fn sismumber<T>(key: T, member: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<bool>
where
    T: ToRedisArgs,
{
    let is: bool = redis::cmd("SISMEMBER").arg(key).arg(member).query(conn)?;
    Ok(is)
}

//zcard 获取 sorted set 元素数量
pub fn zcard<T>(key: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<usize>
where
    T: ToRedisArgs,
{
    let size: usize = redis::cmd("zcard").arg(key).query(conn)?;
    Ok(size)
}

//zrank
pub fn zrank<T>(key: T, member: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<f64>
where
    T: ToRedisArgs,
{
    let size: f64 = redis::cmd("zrank").arg(key).arg(member).query(conn)?;
    Ok(size)
}

//zscore
pub fn zscore<T>(key: T, member: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<f64>
where
    T: ToRedisArgs,
{
    let size: f64 = redis::cmd("zscore").arg(key).arg(member).query(conn)?;
    Ok(size)
}

// hlen
pub fn hlen<T>(key: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<usize>
where
    T: ToRedisArgs,
{
    let size: usize = redis::cmd("hlen").arg(key).query(conn)?;
    Ok(size)
}

// hget
pub fn hget<T>(key: T, field: T, conn: &mut dyn redis::ConnectionLike) -> RedisResult<String>
where
    T: ToRedisArgs,
{
    let v: Value = redis::cmd("hget").arg(key).arg(field).query(conn)?;

    return match v {
        Value::Nil => Ok("".to_string()),
        Value::Int(val) => Ok(val.to_string()),
        Value::Data(ref val) => match from_utf8(val) {
            Ok(x) => Ok(x.to_string()),
            Err(_) => Ok("".to_string()),
        },
        Value::Bulk(ref values) => Ok("".to_string()),
        Value::Okay => Ok("ok".to_string()),
        Value::Status(ref s) => Ok(s.to_string()),
    };
}

pub fn pttl<T, C>(key: T, conn: &mut C) -> RedisResult<isize>
where
    T: ToRedisArgs,
    C: redis::ConnectionLike,
{
    let ttl: isize = redis::cmd("pttl").arg(key).query(conn)?;
    Ok(ttl)
}

// 获取key类型
pub fn key_type<T, C>(key: T, con: &mut C) -> RedisResult<RedisKeyType>
where
    T: ToRedisArgs,
    C: ConnectionLike,
{
    let key_type: RedisKeyType = redis::cmd("TYPE").arg(key).query(con)?;
    Ok(key_type)
}

//scan
pub fn scan<C, T>(con: &mut C) -> RedisResult<Iter<'_, T>>
where
    C: ConnectionLike,
    T: FromRedisValue,
{
    let mut c = redis::cmd("SCAN");
    c.cursor_arg(0);
    c.iter(con)
}

// async scan
async fn scan_async<'a, T: FromRedisValue + 'a>(
    con: &'a mut (dyn redis::aio::ConnectionLike + Send),
) -> RedisResult<AsyncIter<'a, T>> {
    let mut c = redis::cmd("SCAN");
    c.cursor_arg(0);
    c.iter_async(con).await
}

// 获取redis实例配置参数
fn get_instance_parameters<C>(con: &mut C) -> RedisResult<HashMap<String, String>>
where
    C: ConnectionLike,
{
    let mut cmd_config = redis::cmd("config");
    let r_config = con.req_command(&cmd_config.arg("get").arg("*"))?;
    let mut parameters = HashMap::new();
    if let Value::Bulk(ref values) = r_config {
        for i in (0..values.len()).step_by(2) {
            let key = values.get(i);
            let val = values.get(i + 1);

            if let Some(k) = key {
                if let Value::Data(ref s) = k {
                    match from_utf8(s) {
                        Ok(kstr) => {
                            let mut vstr = String::from("");
                            match val {
                                None => vstr = "".to_string(),
                                Some(vv) => {
                                    if let Value::Data(ref val) = vv {
                                        match from_utf8(val) {
                                            Ok(x) => vstr = x.to_string(),
                                            Err(_) => {}
                                        };
                                    }
                                }
                            };
                            parameters.insert(kstr.to_string(), vstr);
                        }
                        Err(_) => {}
                    }
                }
            }
        }
    }

    Ok(parameters)
}

#[cfg(test)]
mod test {
    use super::*;
    use redis::{ConnectionLike, ToRedisArgs};

    static S_URL: &str = "redis://:redistest0102@114.67.76.82:16377/?timeout=1s";
    static T_URL: &str = "redis://:redistest0102@114.67.120.120:16376/?timeout=1s";

    //cargo test compare::comparekey::test::test_Comparer_string --  --nocapture
    #[test]
    fn test_Comparer_string() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let cmd_set = redis::cmd("set");

        let cmd_expire = redis::cmd("expire");
        let s_r = comparer
            .sconn
            .req_command(cmd_set.clone().arg("a").arg("1123aaa"));
        let t_r = comparer
            .tconn
            .req_command(cmd_set.clone().arg("a").arg("1123aaa"));

        comparer
            .sconn
            .req_command(cmd_expire.clone().arg("a").arg(100 as isize));
        comparer
            .tconn
            .req_command(cmd_expire.clone().arg("a").arg(100 as isize));
        let key_str = RedisKey {
            key: "a".to_string(),
            key_type: RedisKeyType::TypeString,
        };

        let mut r = comparer.compare_string(key_str);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_Comparer_list --  --nocapture
    #[test]
    fn test_Comparer_list() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);
        let mut cmd_rpush = redis::cmd("rpush");

        let key_list = RedisKey {
            key: "r1".to_string(),
            key_type: RedisKeyType::TypeList,
        };

        for i in 0..20 as i32 {
            comparer
                .sconn
                .req_command(cmd_rpush.clone().arg(key_list.key.clone()).arg(i));
            comparer
                .tconn
                .req_command(cmd_rpush.clone().arg(key_list.key.clone()).arg(i));
        }

        let r = comparer.compare_list(key_list);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_Comparer_set --  --nocapture
    #[test]
    fn test_Comparer_set() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let mut cmd_sadd = redis::cmd("sadd");

        let key_set = RedisKey {
            key: "s1".to_string(),
            key_type: RedisKeyType::TypeList,
        };

        for i in 0..100 as i32 {
            comparer
                .sconn
                .req_command(cmd_sadd.clone().arg(key_set.key.clone()).arg(i));
            comparer
                .tconn
                .req_command(cmd_sadd.clone().arg(key_set.key.clone()).arg(i));
        }

        let r = comparer.compare_set(key_set);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_comparer_sorted_set --  --nocapture
    #[test]
    fn test_comparer_sorted_set() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let mut cmd_zadd = redis::cmd("zadd");

        let key_zset = RedisKey {
            key: "z1".to_string(),
            key_type: RedisKeyType::TypeZSet,
        };

        for i in 0..20 as i32 {
            comparer.sconn.req_command(
                cmd_zadd
                    .clone()
                    .arg(key_zset.key.clone())
                    .arg(i)
                    .arg(key_zset.key.clone() + &*i.to_string()),
            );
            comparer.tconn.req_command(
                cmd_zadd
                    .clone()
                    .arg(key_zset.key.clone())
                    .arg(i)
                    .arg(key_zset.key.clone() + &*i.to_string()),
            );
        }

        let r = comparer.compare_zset(key_zset);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_comparer_hash --  --nocapture
    #[test]
    fn test_comparer_hash() {
        let s_client = redis::Client::open(S_URL).unwrap();
        let t_client = redis::Client::open(T_URL).unwrap();
        let mut scon = s_client.get_connection().unwrap();
        let mut tcon = t_client.get_connection().unwrap();

        let mut comparer: Comparer = Comparer::new(&mut scon, &mut tcon);

        let cmd_hset = redis::cmd("hset");

        let key_hash = RedisKey {
            key: "h1".to_string(),
            key_type: RedisKeyType::TypeHash,
        };

        for i in 0..20 as i32 {
            comparer.sconn.req_command(
                cmd_hset
                    .clone()
                    .arg(key_hash.key.clone())
                    .arg(key_hash.key.clone() + &"__".to_string() + &*i.to_string())
                    .arg(key_hash.key.clone() + &*i.to_string()),
            );
            comparer.tconn.req_command(
                cmd_hset
                    .clone()
                    .arg(key_hash.key.clone())
                    .arg(key_hash.key.clone() + &"__".to_string() + &*i.to_string())
                    .arg(key_hash.key.clone() + &*i.to_string()),
            );
        }

        let r = comparer.compare_hash(key_hash);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_new_comparer --  --nocapture
    #[test]
    fn test_new_comparer() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut scon = client.get_connection().unwrap();
        let mut tcon = client.get_connection().unwrap();

        let comparer: Comparer = Comparer::new(&mut scon, &mut tcon);
        let cmd = redis::cmd("ping");
        let r = comparer.sconn.req_command(&cmd);
        println!("{:?}", r);
    }

    //cargo test compare::comparekey::test::test_key_exists --  --nocapture
    #[test]
    fn test_key_exists() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let exists = key_exists("hll".to_redis_args(), &mut conn);
        println!("{:?}", exists);
    }

    //cargo test compare::comparekey::test::test_ttl --  --nocapture
    #[test]
    fn test_ttl() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let ttl = ttl("hl", &mut conn);
        let pttl = pttl("hll", &mut conn);
        println!("ttl is:{:?} , pttl is: {:?}", ttl, pttl);
    }

    //cargo test compare::comparekey::test::test_key_type --  --nocapture
    #[test]
    fn test_key_type() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();

        let k_type = key_type("h1", &mut conn);
        println!("key type  is:{:?}", k_type);
    }

    //cargo test compare::comparekey::test::test_get_instance_parameters --  --nocapture
    #[test]
    fn test_get_instance_parameters() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let k_type = get_instance_parameters(&mut conn).unwrap();
        for item in k_type.iter() {
            println!("{}:{}", item.0, item.1)
        }
    }

    //cargo test compare::comparekey::test::test_select --  --nocapture
    #[test]
    fn test_select() {
        let client = redis::Client::open(S_URL).unwrap();

        let mut conn = client.get_connection().unwrap();
        let mut cmd_select = redis::cmd("select");
        conn.req_command(&cmd_select.arg(2));
        let mut cmd_set = redis::cmd("set");
        cmd_set.arg("a").arg("aa").execute(&mut conn);
    }
}
