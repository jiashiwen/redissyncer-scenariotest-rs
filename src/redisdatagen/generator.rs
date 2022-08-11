use core::fmt;
use std::fmt::{Formatter, write};
use redis::{aio, Connection, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use crate::util::rand_string;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use redis::ToRedisArgs;


#[derive(Debug, EnumIter)]
pub enum RedisKeyType {
    TypeString,
    TypeList,
    TypeSet,
    TypeZSet,
    TypeHash,
}

impl FromRedisValue for RedisKeyType {
    fn from_redis_value(v: &Value) -> RedisResult<RedisKeyType> {
        match *v {
            Value::Status(ref val) => match val.to_string().as_str() {
                "string" => Ok(RedisKeyType::TypeString),
                "list" => Ok(RedisKeyType::TypeList),
                "set" => Ok(RedisKeyType::TypeSet),
                "zset" => Ok(RedisKeyType::TypeZSet),
                "hash" => Ok(RedisKeyType::TypeHash),
                _ => Err(RedisError::from((
                    ErrorKind::TypeError,
                    "Response was of incompatible type",
                    format!("{:?} (response was {:?})", "Response type not string compatible.", val),
                )))
            },
            // _ => invalid_type_error!(v, "Response type not string compatible."),
            _ => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!("{:?} (response was {:?})", "Response type not string compatible.", v),
            )))
        }
    }
}

impl fmt::Display for RedisKeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisKeyType::TypeString => { write!(f, "string") }
            RedisKeyType::TypeList => { write!(f, "list") }
            RedisKeyType::TypeSet => { write!(f, "set") }
            RedisKeyType::TypeZSet => { write!(f, "zset") }
            RedisKeyType::TypeHash => { write!(f, "hash") }
        }
    }
}


// 按照指定类型生成指定长度的key
pub fn gen_key(key_type: RedisKeyType, len: usize) -> String {
    let mut prefix = key_type.to_string();
    let rand = rand_string(len);
    prefix.push('_');
    prefix.push_str(&*rand);
    prefix
}

// 生成指定长度的 string key
pub async fn gen_async_string<C>(keys: usize, key_len: usize, conn: &mut C) -> RedisResult<()>
    where
        C: aio::ConnectionLike, {
    let prefix = gen_key(RedisKeyType::TypeString, key_len);
    for i in 0..keys {
        let key = prefix.clone() + "_" + &*i.to_string();
        let mut cmd_set = redis::cmd("set");
        let r_set = conn
            .req_packed_command(&cmd_set.arg(key.to_redis_args()).arg(key.to_redis_args()))
            .await?;
    }
    Ok(())
}

// 根据给定key的长度生成 string 类型 kv ，keys 为 kv 的数量
pub fn gen_string<C>(keys: usize, key_len: usize, conn: &mut C) -> RedisResult<()>
    where
        C: redis::ConnectionLike,
{
    let prefix = gen_key(RedisKeyType::TypeString, key_len);
    for i in 0..keys {
        let key = prefix.clone() + "_" + &*i.to_string();
        let mut cmd_set = redis::cmd("set");
        let r_set = conn
            .req_command(&cmd_set.arg(key.to_redis_args()).arg(key.to_redis_args()))?;
    }
    Ok(())
}

// 根据给定key的长度生成 string 类型 kv ，keys 为 kv 的数量,可定义 value 长度
pub fn gen_big_string<C>(keys: usize, key_len: usize, value_len: usize, conn: &mut C) -> RedisResult<()>
    where
        C: redis::ConnectionLike, {
    let key_prefix = gen_key(RedisKeyType::TypeString, key_len);
    let val = rand_string(value_len);

    for i in 0..keys {
        let key = key_prefix.clone() + "_" + &*i.to_string();
        let mut cmd_set = redis::cmd("set");
        let r_set = conn
            .req_command(&cmd_set.arg(key.to_redis_args()).arg(val.clone().to_redis_args()))?;

        // log::info!(target:"root","{:?}",r_set);
        log::info!("{:?}",r_set);
    }

    Ok(())
}

pub fn gen_list<C>(key_len: usize, list_len: usize, conn: &mut C) -> RedisResult<()>
    where
        C: redis::ConnectionLike, {
    let key = gen_key(RedisKeyType::TypeList, key_len);

    for i in 0..list_len {
        let val = key.clone() + "_" + &*i.to_string();
        let mut cmd_rpush = redis::cmd("rpush");
        let _ = conn.req_command(&cmd_rpush.arg(key.clone().to_redis_args()).arg(val.to_redis_args()))?;
    }
    Ok(())
}

pub fn gen_set<C>(key_len: usize, set_size: usize, conn: &mut C) -> RedisResult<()>
    where
        C: redis::ConnectionLike, {
    let key = gen_key(RedisKeyType::TypeSet, key_len);
    for i in 0..set_size {
        let val = key.clone() + "_" + &*i.to_string();
        let mut cmd_sadd = redis::cmd("sadd");
        let _ = conn.req_command(&cmd_sadd
            .arg(key.clone().to_redis_args())
            .arg(val.to_redis_args()))?;
    }
    Ok(())
}

pub fn gen_zset<C>(key_len: usize, zset_size: usize, conn: &mut C) -> RedisResult<()>
    where
        C: redis::ConnectionLike, {
    let key = gen_key(RedisKeyType::TypeZSet, key_len);
    for i in 0..zset_size {
        let val = key.clone() + "_" + &*i.to_string();
        let mut cmd_zadd = redis::cmd("zadd");
        let _ = conn.req_command(&cmd_zadd
            .arg(key.clone().to_redis_args())
            .arg(&*i.to_string().to_redis_args())
            .arg(val.to_redis_args()))?;
    }
    Ok(())
}

pub fn gen_hash<C>(key_len: usize, hash_size: usize, conn: &mut C) -> RedisResult<()>
    where
        C: redis::ConnectionLike, {
    let key = gen_key(RedisKeyType::TypeHash, key_len);
    for i in 0..hash_size {
        let field = key.clone() + "_" + &*i.to_string();
        let mut cmd_hset = redis::cmd("hset");
        let _ = conn.req_command(&cmd_hset
            .arg(field.to_redis_args())
            .arg(i.to_redis_args()))?;
    }
    Ok(())
}


// ToDo
// 测试原生redis cluster 连接及操作
// 数据测试函数，按照数据类别编写函数，尽量测试到该类型相关的所有操作作为增量数据


#[cfg(test)]
mod test {
    use futures::TryFutureExt;
    use strum::IntoEnumIterator;
    use tokio::runtime::Runtime;
    use crate::init_log;
    use super::*;

    //cargo test redisdatagen::generator::test::test_gen_key --  --nocapture
    #[test]
    fn test_gen_key() {
        let len = 8 as usize;
        let mut ri = RedisKeyType::iter();
        for key_type in ri {
            let gen_k = gen_key(key_type, len);
            println!("{}", gen_k);
        }
    }

    //cargo test redisdatagen::generator::test::test_gen_string --  --nocapture
    #[test]
    fn test_gen_string() {
        let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16377/").unwrap();
        let mut conn = client.get_connection().unwrap();
        gen_string(10, 8, &mut conn);
    }

    //cargo test redisdatagen::generator::test::test_gen_big_string --  --nocapture
    #[test]
    fn test_gen_big_string() {
        init_log();
        let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16377/").unwrap();
        let mut conn = client.get_connection().unwrap();
        gen_big_string(1000, 8, 1000, &mut conn);
    }

    //cargo test redisdatagen::generator::test::test_gen_async_string --  --nocapture
    #[test]
    fn test_gen_async_string() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let client = redis::Client::open("redis://:redistest0102@114.67.76.82:16377/").unwrap();
            let mut conn = client.get_async_connection().await.unwrap();
            gen_async_string(10, 8, &mut conn).await;
        });
    }
}


