use std::fmt::{Display, Formatter};

use enum_iterator::Sequence;
use redis::{ErrorKind, FromRedisValue, RedisError, RedisResult, Value};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, Clone)]
pub struct RedisKey {
    pub key: String,
    pub key_type: RedisKeyType,
}
