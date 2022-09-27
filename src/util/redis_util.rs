use anyhow::{anyhow, Result};

use redis::{ConnectionLike, Iter, RedisError};
use redis::{FromRedisValue, RedisResult, ToRedisArgs, Value};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

use crate::util::RedisKeyType;

use super::RedisKey;

#[derive(Clone)]
pub enum RedisClient {
    Single(redis::Client),
    Cluster(redis::cluster::ClusterClient),
}

impl RedisClient {
    pub fn get_redis_connection(&self) -> RedisResult<RedisConnection> {
        return match self {
            RedisClient::Single(s) => {
                let conn = s.get_connection()?;
                Ok(RedisConnection::Single(conn))
            }
            RedisClient::Cluster(c) => {
                let conn = c.get_connection()?;
                Ok(RedisConnection::Cluster(conn))
            }
        };
    }
}

pub enum RedisConnection {
    Single(redis::Connection),
    Cluster(redis::cluster::ClusterConnection),
}

impl RedisConnection {
    pub fn get_dyn_connection(self) -> Box<dyn ConnectionLike> {
        let r: Box<dyn ConnectionLike> = match self {
            RedisConnection::Single(s) => Box::new(s),
            RedisConnection::Cluster(c) => Box::new(c),
        };
        r
    }
}

impl RedisClient {}

#[derive(Clone, Debug)]
pub enum InfoSection {
    Server,
    Clients,
    Memory,
    Persistence,
    Stats,
    Replication,
    Cpu,
    Commandstats,
    Latencystats,
    Cluster,
    Modules,
    Keyspace,
    Errorstats,
    All,
    Default,
    Everything,
}

impl Display for InfoSection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            InfoSection::Server => {
                write!(f, "server")
            }
            InfoSection::Clients => {
                write!(f, "clients")
            }
            InfoSection::Memory => {
                write!(f, "memory")
            }
            InfoSection::Persistence => {
                write!(f, "persistence")
            }
            InfoSection::Stats => {
                write!(f, "stats")
            }
            InfoSection::Replication => {
                write!(f, "replication")
            }
            InfoSection::Cpu => {
                write!(f, "cpu")
            }
            InfoSection::Commandstats => {
                write!(f, "commandstats")
            }
            InfoSection::Latencystats => {
                write!(f, "latencystats")
            }
            InfoSection::Cluster => {
                write!(f, "cluster")
            }
            InfoSection::Modules => {
                write!(f, "modules")
            }
            InfoSection::Keyspace => {
                write!(f, "keyspace")
            }
            InfoSection::Errorstats => {
                write!(f, "errorstats")
            }
            InfoSection::All => {
                write!(f, "all")
            }
            InfoSection::Default => {
                write!(f, "default")
            }
            InfoSection::Everything => {
                write!(f, "everything")
            }
        }
    }
}

pub fn info(
    section: InfoSection,
    conn: &mut dyn redis::ConnectionLike,
) -> Result<HashMap<String, HashMap<String, String>>> {
    let info: Value = redis::cmd("info")
        .arg(section.to_string())
        .query(conn)
        .map_err(|err| anyhow!("{}", err.to_string()))?;
    let mut info_map: HashMap<String, HashMap<String, String>> = HashMap::new();

    if let Value::Data(ref data) = info {
        let info_str = from_utf8(data).map_err(|err| anyhow!("{}", err.to_string()))?;
        let info_split = info_str.split("\r\n");
        let mut section = "".to_string();
        let mut vec_kv: Vec<String> = vec![];
        let mut hash_kv: HashMap<String, String> = HashMap::new();

        for item in info_split {
            if item.starts_with("#") {
                if !section.is_empty() {
                    info_map.insert(section.clone(), hash_kv.clone());
                    section.clear();
                    vec_kv.clear();
                    hash_kv.clear();
                }
                section = item.to_string();
                continue;
            }
            if !item.is_empty() {
                let kv = item.split(":");
                let mut key = "".to_string();
                let mut value = "".to_string();
                let mut count = 0;

                for element in kv {
                    if count == 0 {
                        key = element.to_string()
                    }
                    if count == 1 {
                        value = element.to_string();
                    }
                    count += 1;
                }
                hash_kv.insert(key, value);
            }
        }
        info_map.insert(section.clone(), hash_kv.clone());
    }
    Ok(info_map)
}

pub fn scan<T>(con: &mut dyn redis::ConnectionLike) -> RedisResult<Iter<'_, T>>
where
    T: FromRedisValue,
{
    let mut c = redis::cmd("SCAN");
    c.cursor_arg(0);
    c.iter(con)
}

// 获取key类型
pub fn key_type<T>(key: T, con: &mut dyn redis::ConnectionLike) -> RedisResult<RedisKeyType>
where
    T: ToRedisArgs,
{
    let key_type: RedisKeyType = redis::cmd("TYPE").arg(key).query(con)?;
    Ok(key_type)
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

// 获取redis实例配置参数
pub fn get_instance_parameters<C>(con: &mut C) -> RedisResult<HashMap<String, String>>
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
// 通过pipline 批量获取 key type
pub fn key_type_pipline(
    keys: Vec<String>,
    con: &mut dyn redis::ConnectionLike,
) -> RedisResult<Vec<RedisKey>> {
    let mut pip = redis::pipe();
    let mut vec_rediskeys: Vec<RedisKey> = vec![];
    for key in keys.clone() {
        let cmd_type = redis::cmd("TYPE").arg(key).to_owned();
        pip.add_command(cmd_type);
    }
    let r: Vec<Value> = pip.query(con)?;
    for (i, v) in r.iter().enumerate() {
        if let Value::Status(s) = v {
            match s.as_str() {
                "string" => {
                    let key = keys.get(i);
                    if let Some(k) = key {
                        let rediskey = RedisKey {
                            key: k.to_string(),
                            key_type: RedisKeyType::TypeString,
                        };
                        vec_rediskeys.push(rediskey);
                    }
                }
                "list" => {
                    let key = keys.get(i);
                    if let Some(k) = key {
                        let rediskey = RedisKey {
                            key: k.to_string(),
                            key_type: RedisKeyType::TypeList,
                        };
                        vec_rediskeys.push(rediskey);
                    }
                }
                "set" => {
                    let key = keys.get(i);
                    if let Some(k) = key {
                        let rediskey = RedisKey {
                            key: k.to_string(),
                            key_type: RedisKeyType::TypeSet,
                        };
                        vec_rediskeys.push(rediskey);
                    }
                }

                "zset" => {
                    let key = keys.get(i);
                    if let Some(k) = key {
                        let rediskey = RedisKey {
                            key: k.to_string(),
                            key_type: RedisKeyType::TypeZSet,
                        };
                        vec_rediskeys.push(rediskey);
                    }
                }
                "hash" => {
                    let key = keys.get(i);
                    if let Some(k) = key {
                        let rediskey = RedisKey {
                            key: k.to_string(),
                            key_type: RedisKeyType::TypeHash,
                        };
                        vec_rediskeys.push(rediskey);
                    }
                }

                _ => {}
            }
        }
    }

    Ok(vec_rediskeys)
}

#[cfg(test)]
mod test {
    use super::*;

    static S_URL: &str = "redis://:redistest0102@114.67.76.82:16377/?timeout=1s";

    //cargo test util::redis_util::test::test_info --  --nocapture
    #[test]
    fn test_info() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let info = info(InfoSection::All, &mut conn).unwrap();

        for item in info.iter() {
            println!("section is:{:?},value is {:?} ", item.0, item.1);
        }
    }

    //cargo test util::redis_util::test::test_key_type_pipline --  --nocapture
    #[test]
    fn test_key_type_pipline() {
        let client = redis::Client::open(S_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let vk = vec![
            "a".to_string(),
            "b".to_string(),
            "pfmerge_EciZ".to_string(),
            "lmove_Ibak".to_string(),
            "hset_iFV3".to_string(),
            "zadd_D&G7".to_string(),
            "srem_iFV3".to_string(),
        ];
        let r = key_type_pipline(vk, &mut conn);
        println!("{:?}", r);
    }
}
