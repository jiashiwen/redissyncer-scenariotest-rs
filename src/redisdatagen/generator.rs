use std::fmt::{Display, Formatter, write};
use redis::{aio, Connection, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use crate::util::rand_string;
use enum_iterator::{all, Sequence};
use redis::ToRedisArgs;


#[derive(Debug, PartialEq, Sequence)]
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
            _ => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!("{:?} (response was {:?})", "Response type not string compatible.", v),
            )))
        }
    }
}

impl Display for RedisKeyType {
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

// #[derive(Debug, Clone)]
pub struct BigKeyGenerator<'a> {
    // Key 长度既字符数量
    pub KeyLen: usize,
    // Value 长度既字符数量
    pub ValueLen: usize,
    // 集合长度 set、zset、hash长度
    pub CollectionLength: usize,
    // redis connection
    pub RedisConn: &'a mut dyn redis::ConnectionLike,
}

impl<'a> BigKeyGenerator<'a> {
    pub fn default(conn: &'a mut dyn redis::ConnectionLike) -> Self {
        Self {
            KeyLen: 1,
            ValueLen: 1,
            CollectionLength: 1,
            RedisConn: conn,
        }
    }

    pub fn exec(&mut self) -> RedisResult<()> {
        gen_big_string(self.CollectionLength, self.KeyLen, self.ValueLen, self.RedisConn)?;
        gen_list(self.KeyLen, self.ValueLen, self.CollectionLength, self.RedisConn)?;
        gen_set(self.KeyLen, self.ValueLen, self.CollectionLength, self.RedisConn)?;
        gen_zset(self.KeyLen, self.ValueLen, self.CollectionLength, self.RedisConn)?;
        gen_hash(self.KeyLen, self.ValueLen, self.CollectionLength, self.RedisConn)?;
        Ok(())
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
        conn.req_command(&cmd_set.arg(key.to_redis_args()).arg(key.to_redis_args()))?;
    }
    Ok(())
}

// 根据给定key的长度生成 string 类型 kv ，keys 为 kv 的数量,可定义 value 长度
pub fn gen_big_string(keys: usize, key_len: usize, value_len: usize, conn: &mut dyn redis::ConnectionLike) -> RedisResult<()> {
    let key_prefix = gen_key(RedisKeyType::TypeString, key_len);

    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }

    for i in 0..keys {
        let key = key_prefix.clone() + "_" + &*i.to_string();
        let mut cmd_set = redis::cmd("set");
        let r_set = conn
            .req_command(&cmd_set.arg(key.to_redis_args())
                .arg(val.clone() + &*i.to_string()))?;

        // log::info!(target:"root","{:?}",r_set);
        log::info!("{:?}",r_set);
    }

    Ok(())
}

pub fn gen_list(key_len: usize, value_len: usize, list_len: usize, conn: &mut dyn redis::ConnectionLike) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeList, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }

    for i in 0..list_len {
        let mut cmd_rpush = redis::cmd("rpush");
        let _ = conn.req_command(&cmd_rpush.arg(key.clone().to_redis_args()).arg(val.clone() + &*i.to_string()))?;
    }
    Ok(())
}

pub fn gen_set(key_len: usize, value_len: usize, set_size: usize, conn: &mut dyn redis::ConnectionLike) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeSet, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }
    for i in 0..set_size {
        let mut cmd_sadd = redis::cmd("sadd");
        let _ = conn.req_command(&cmd_sadd
            .arg(key.clone().to_redis_args())
            .arg(val.clone() + &*i.to_string()))?;
    }
    Ok(())
}

pub fn gen_zset(key_len: usize, value_len: usize, zset_size: usize, conn: &mut dyn redis::ConnectionLike) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeZSet, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }
    for i in 0..zset_size {
        let mut cmd_zadd = redis::cmd("zadd");
        let _ = conn.req_command(&cmd_zadd
            .arg(key.clone().to_redis_args())
            .arg(&*i.to_string().to_redis_args())
            .arg(val.clone() + &*i.to_string()))?;
    }
    Ok(())
}

pub fn gen_hash(key_len: usize, value_len: usize, hash_size: usize, conn: &mut dyn redis::ConnectionLike) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeHash, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }
    for i in 0..hash_size {
        let field = key.clone() + "_" + &*i.to_string();
        let mut cmd_hset = redis::cmd("hset");
        let _ = conn.req_command(&cmd_hset
            .arg(key.clone())
            .arg(key.clone() + &*i.to_string())
            .arg(val.clone() + &*i.to_string()))?;
    }
    Ok(())
}


// ToDo
// 测试原生redis cluster 连接及操作
// 数据测试函数，按照数据类别编写函数，尽量测试到该类型相关的所有操作作为增量数据


#[cfg(test)]
mod test {
    use futures::TryFutureExt;
    use tokio::runtime::Runtime;
    use crate::init_log;
    use super::*;

    static redis_url: &str = "redis://:redistest0102@114.67.76.82:16377/?timeout=1s";

    //cargo test redisdatagen::generator::test::test_BigKeyGenerator_exec --  --nocapture
    #[test]
    fn test_BigKeyGenerator_exec() {
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_connection().unwrap();
        let mut gen = BigKeyGenerator::default(&mut conn);
        gen.KeyLen = 4;
        gen.ValueLen = 8;
        gen.CollectionLength = 10;
        let r = gen.exec();
        println!("{:?}", r);
    }

    //cargo test redisdatagen::generator::test::test_gen_key --  --nocapture
    #[test]
    fn test_gen_key() {
        let len = 8 as usize;
        // let mut ri = RedisKeyType::itor();
        let ri = all::<RedisKeyType>().collect::<Vec<_>>();
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


