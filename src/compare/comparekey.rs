use std::collections::HashMap;
use std::str::from_utf8;
use crate::redisdatagen::RedisKeyType;
use redis::{AsyncIter, Connection, ConnectionLike, FromRedisValue, RedisResult, ToRedisArgs, Value};
use anyhow::Result;
use redis::Iter;

pub struct RedisKey {
    pub key: String,
    pub key_type: RedisKeyType,
}

pub struct Comparer<'a> {
    pub sconn: &'a mut (dyn ConnectionLike + 'a),
    pub tconn: &'a mut (dyn ConnectionLike + 'a),
    pub ttl_diff: u32,
}

impl<'a> Comparer<'a> {
    pub fn new(s_conn: &'a mut dyn ConnectionLike, t_conn: &'a mut dyn ConnectionLike) -> Self
        where {
        Self {
            sconn: s_conn,
            tconn: t_conn,
            ttl_diff: 1,
        }
    }

    pub fn compare_string(&self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        // soure 和 target 值是否相等
        // ttl差值是否在规定范围内
        Ok(())
    }

    pub fn compare_list(&self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        // 遍历source，核对target中相应的值是否一致
        // ttl差值是否在规定范围内
        Ok(())
    }

    pub fn compare_set(&self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        // 遍历source，核对在target是否存在
        // ttl差值是否在规定范围内
        Ok(())
    }

    pub fn compare_zset(&self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        // 遍历source，核对在target score 和 值是否一致
        // ttl差值是否在规定范围内
        Ok(())
    }

    pub fn compare_hash(&self, key: RedisKey) -> Result<()> {
        // target端key是否存在
        // 遍历source，核对在target field 和 value 是否一致
        // ttl差值是否在规定范围内
        Ok(())
    }
}


// key 是否存在
pub fn key_exists<T, C>(key: T, conn: &mut C) -> RedisResult<bool>
    where
        T: ToRedisArgs,
        C: redis::ConnectionLike,
{
    let exists: bool = redis::cmd("exists").arg(key).query(conn)?;
    Ok(exists)
}

pub fn ttl<T, C>(key: T, conn: &mut C) -> RedisResult<isize>
    where
        T: ToRedisArgs,
        C: redis::ConnectionLike,
{
    let ttl: isize = redis::cmd("ttl").arg(key).query(conn)?;
    Ok(ttl)
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
async fn scan_async<'a, T: FromRedisValue + 'a>(con: &'a mut (dyn redis::aio::ConnectionLike + Send)) -> RedisResult<AsyncIter<'a, T>>

{
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
    let r_config = con
        .req_command(&cmd_config.arg("get").arg("*"))?;
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
    use redis::{Connection, ConnectionLike, ToRedisArgs};

    static redis_url: &str = "redis://:redistest0102@114.67.76.82:16377/?timeout=1s";

    //cargo test compare::comparekey::test::test_new_Comparer --  --nocapture
    #[test]
    fn test_new_Comparer() {
        let client = redis::Client::open(redis_url).unwrap();
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
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let exists = key_exists("hll".to_redis_args(), &mut conn);
        println!("{:?}", exists);
    }

    //cargo test compare::comparekey::test::test_ttl --  --nocapture
    #[test]
    fn test_ttl() {
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let ttl = ttl("hl", &mut conn);
        let pttl = pttl("hll", &mut conn);
        println!("ttl is:{:?} , pttl is: {:?}", ttl, pttl);
    }

    //cargo test compare::comparekey::test::test_key_type --  --nocapture
    #[test]
    fn test_key_type() {
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();

        let k_type = key_type("h1", &mut conn);
        println!("key type  is:{:?}", k_type);
    }

    //cargo test compare::comparekey::test::test_get_instance_parameters --  --nocapture
    #[test]
    fn test_get_instance_parameters() {
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let k_type = get_instance_parameters(&mut conn).unwrap();
        for item in k_type.iter() {
            println!("{}:{}", item.0, item.1)
        }
    }

    //cargo test compare::comparekey::test::test_select --  --nocapture
    #[test]
    fn test_select() {
        let client = redis::Client::open(redis_url).unwrap();

        let mut conn = client.get_connection().unwrap();
        let mut cmd_select = redis::cmd("select");
        conn.req_command(&cmd_select.arg(2));
        let mut cmd_set = redis::cmd("set");
        cmd_set.arg("a").arg("aa").execute(&mut conn);
    }
}
