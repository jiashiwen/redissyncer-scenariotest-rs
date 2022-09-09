use anyhow::{anyhow, Result};
use redis::Iter;
use redis::{FromRedisValue, RedisResult, ToRedisArgs, Value};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::str::from_utf8;

use crate::util::RedisKeyType;

use super::RedisKey;

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
