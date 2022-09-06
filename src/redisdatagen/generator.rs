use crate::util::rand_string;
use anyhow::{anyhow, Result};
use enum_iterator::Sequence;
use redis::ToRedisArgs;
use redis::{aio, ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, PartialEq, Sequence, Clone)]
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
                    format!(
                        "{:?} (response was {:?})",
                        "Response type not string compatible.", val
                    ),
                ))),
            },
            _ => Err(RedisError::from((
                ErrorKind::TypeError,
                "Response was of incompatible type",
                format!(
                    "{:?} (response was {:?})",
                    "Response type not string compatible.", v
                ),
            ))),
        }
    }
}

impl Display for RedisKeyType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisKeyType::TypeString => {
                write!(f, "string")
            }
            RedisKeyType::TypeList => {
                write!(f, "list")
            }
            RedisKeyType::TypeSet => {
                write!(f, "set")
            }
            RedisKeyType::TypeZSet => {
                write!(f, "zset")
            }
            RedisKeyType::TypeHash => {
                write!(f, "hash")
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct GenerateBigKey {
    #[serde(default = "GenerateBigKey::redis_url_default")]
    pub redis_url: String,
    #[serde(default = "GenerateBigKey::redis_version_default")]
    pub redis_version: String,
    // 生成批次，按批次多次生成数据
    #[serde(default = "GenerateBigKey::generate_batch_default")]
    pub generate_batch: usize,
    #[serde(default = "GenerateBigKey::threads_default")]
    pub threads: usize,
    #[serde(default = "GenerateBigKey::key_len_default")]
    pub key_len: usize,
    #[serde(default = "GenerateBigKey::value_len_default")]
    pub value_len: usize,
    // 集合长度 set、zset、hash长度
    #[serde(default = "GenerateBigKey::collection_length_default")]
    pub collection_length: usize,
    #[serde(default = "GenerateBigKey::log_out_default")]
    pub log_out: bool,
}

impl GenerateBigKey {}

impl Default for GenerateBigKey {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            redis_version: "".to_string(),
            generate_batch: 1,
            threads: 1,
            key_len: 4,
            value_len: 4,
            collection_length: 10,
            log_out: false,
        }
    }
}

// 生成默认值
impl GenerateBigKey {
    fn redis_url_default() -> String {
        "redis://127.0.0.1:6379".to_string()
    }
    fn redis_version_default() -> String {
        "4".to_string()
    }
    fn generate_batch_default() -> usize {
        1
    }
    fn threads_default() -> usize {
        1
    }
    fn key_len_default() -> usize {
        4
    }
    fn value_len_default() -> usize {
        4
    }
    // 集合长度 set、zset、hash长度
    fn collection_length_default() -> usize {
        10
    }
    fn log_out_default() -> bool {
        false
    }

    pub fn exec(&self) -> Result<()> {
        let client = redis::Client::open(self.redis_url.clone())
            .map_err(|err| anyhow!("{}", err.to_string()))?;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(self.threads)
            .build()
            .map_err(|err| anyhow!("{}", err.to_string()))?;

        pool.scope(|s| {
            for i in 0..self.threads {
                let r_conn = client.get_connection();
                match r_conn {
                    Ok(mut conn) => {
                        s.spawn(move |_| {
                            let mut bkg = BigKeyGenerator::default(&mut conn);
                            bkg.key_len = self.key_len;
                            bkg.value_len = self.value_len;
                            bkg.collection_length = self.collection_length;

                            for _ in 0..self.generate_batch {
                                let r = bkg.exec();
                                if self.log_out {
                                    log::info!(
                                        "exec generate big key result: {:?}, thread number: {}",
                                        r,
                                        i
                                    );
                                }
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
        });

        Ok(())
    }
}

// #[derive(Debug, Clone)]
pub struct BigKeyGenerator<'a> {
    // Key 长度既字符数量
    pub key_len: usize,
    // Value 长度既字符数量
    pub value_len: usize,
    // 集合长度 set、zset、hash长度
    pub collection_length: usize,
    pub log_out: bool,
    // redis connection
    pub redis_conn: &'a mut dyn redis::ConnectionLike,
}

impl<'a> BigKeyGenerator<'a> {
    pub fn default(conn: &'a mut dyn redis::ConnectionLike) -> Self {
        Self {
            key_len: 1,
            value_len: 1,
            collection_length: 1,
            log_out: false,
            redis_conn: conn,
        }
    }

    pub fn exec(&mut self) -> RedisResult<()> {
        gen_big_string(
            self.collection_length,
            self.key_len,
            self.value_len,
            self.redis_conn,
        )?;
        gen_list(
            self.key_len,
            self.value_len,
            self.collection_length,
            self.redis_conn,
        )?;
        gen_set(
            self.key_len,
            self.value_len,
            self.collection_length,
            self.redis_conn,
        )?;
        gen_zset(
            self.key_len,
            self.value_len,
            self.collection_length,
            self.redis_conn,
        )?;
        gen_hash(
            self.key_len,
            self.value_len,
            self.collection_length,
            self.redis_conn,
        )?;
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
    C: aio::ConnectionLike,
{
    let prefix = gen_key(RedisKeyType::TypeString, key_len);
    for i in 0..keys {
        let key = prefix.clone() + "_" + &*i.to_string();
        let mut cmd_set = redis::cmd("set");
        let _ = conn
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
pub fn gen_big_string(
    keys: usize,
    key_len: usize,
    value_len: usize,
    conn: &mut dyn redis::ConnectionLike,
) -> RedisResult<()> {
    let key_prefix = gen_key(RedisKeyType::TypeString, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }

    for i in 0..keys {
        let key = key_prefix.clone() + "_" + &*i.to_string();
        let mut cmd_set = redis::cmd("set");
        let _ = conn.req_command(
            &cmd_set
                .arg(key.to_redis_args())
                .arg(val.clone() + &*i.to_string()),
        )?;
    }

    Ok(())
}

pub fn gen_list(
    key_len: usize,
    value_len: usize,
    list_len: usize,
    conn: &mut dyn redis::ConnectionLike,
) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeList, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }

    for i in 0..list_len {
        let mut cmd_rpush = redis::cmd("rpush");
        let _ = conn.req_command(
            &cmd_rpush
                .arg(key.clone().to_redis_args())
                .arg(val.clone() + &*i.to_string()),
        )?;
    }
    Ok(())
}

pub fn gen_set(
    key_len: usize,
    value_len: usize,
    set_size: usize,
    conn: &mut dyn redis::ConnectionLike,
) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeSet, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }
    for i in 0..set_size {
        let mut cmd_sadd = redis::cmd("sadd");
        let _ = conn.req_command(
            &cmd_sadd
                .arg(key.clone().to_redis_args())
                .arg(val.clone() + &*i.to_string()),
        )?;
    }
    Ok(())
}

pub fn gen_zset(
    key_len: usize,
    value_len: usize,
    zset_size: usize,
    conn: &mut dyn redis::ConnectionLike,
) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeZSet, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }
    for i in 0..zset_size {
        let mut cmd_zadd = redis::cmd("zadd");
        let _ = conn.req_command(
            &cmd_zadd
                .arg(key.clone().to_redis_args())
                .arg(&*i.to_string().to_redis_args())
                .arg(val.clone() + &*i.to_string()),
        )?;
    }
    Ok(())
}

pub fn gen_hash(
    key_len: usize,
    value_len: usize,
    hash_size: usize,
    conn: &mut dyn redis::ConnectionLike,
) -> RedisResult<()> {
    let key = gen_key(RedisKeyType::TypeHash, key_len);
    let mut val = "".to_string();
    if (value_len - 1) > 0 {
        val = rand_string(value_len - 1);
    }
    for i in 0..hash_size {
        let field = key.clone() + "_" + &*i.to_string();
        let mut cmd_hset = redis::cmd("hset");
        let _ = conn.req_command(
            &cmd_hset
                .arg(key.clone())
                .arg(key.clone() + &*i.to_string())
                .arg(val.clone() + &*i.to_string()),
        )?;
    }
    Ok(())
}

// ToDo
// 测试原生redis cluster 连接及操作
// 数据测试函数，按照数据类别编写函数，尽量测试到该类型相关的所有操作作为增量数据

#[cfg(test)]
mod test {
    use super::*;
    use crate::init_log;
    use enum_iterator::all;
    use tokio::runtime::Runtime;

    static REDIS_URL: &str = "redis://:redistest0102@114.67.76.82:16377/?timeout=1s";

    //cargo test redisdatagen::generator::test::test_big_key_generator_exec --  --nocapture
    #[test]
    fn test_big_key_generator_exec() {
        let client = redis::Client::open(REDIS_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let mut gen = BigKeyGenerator::default(&mut conn);
        gen.key_len = 4;
        gen.value_len = 8;
        gen.collection_length = 10;
        let r = gen.exec();
        println!("{:?}", r);
    }

    //cargo test redisdatagen::generator::test::test_gen_key --  --nocapture
    #[test]
    fn test_gen_key() {
        let len = 8 as usize;
        let ri = all::<RedisKeyType>().collect::<Vec<_>>();
        for key_type in ri {
            let gen_k = gen_key(key_type, len);
            println!("{}", gen_k);
        }
    }

    //cargo test redisdatagen::generator::test::test_gen_string --  --nocapture
    #[test]
    fn test_gen_string() {
        println!("{:?}", GenerateBigKey::default());
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
