use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

use crate::util::RedisKey;

/// 错误的类型
#[derive(Debug, Clone)]
pub enum CompareErrorType {
    TTLDiff,
    ExistsErr,
    ListLenDiff,
    ListIndexValueDiff,
    SetCardDiff,
    SetMemberNotIn,
    ZSetCardDiff,
    ZSetMemberScoreDiff,
    HashLenDiff,
    HashFieldValueDiff,
    StringValueNotEqual,
    RedisConnectionErr,
    KeyTypeNotString,
    KeyTypeNotList,
    KeyTypeNotSet,
    KeyTypeNotZSet,
    KeyTypeNotHash,
    /// 未知错误
    Unknown,
}

impl fmt::Display for CompareErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompareErrorType::TTLDiff => {
                write!(f, "TTL different")
            }
            CompareErrorType::ExistsErr => {
                write!(f, "Key not exists")
            }
            CompareErrorType::ListLenDiff => {
                write!(f, "List length different")
            }
            CompareErrorType::ListIndexValueDiff => {
                write!(f, "List index value different")
            }
            CompareErrorType::SetCardDiff => {
                write!(f, "Set cardinality different")
            }
            CompareErrorType::SetMemberNotIn => {
                write!(f, "mumber not in set")
            }
            CompareErrorType::ZSetCardDiff => {
                write!(f, "Sorted set cardinality different")
            }
            CompareErrorType::ZSetMemberScoreDiff => {
                write!(f, "mumber not in sorted set")
            }
            CompareErrorType::HashLenDiff => {
                write!(f, "Hash length different")
            }
            CompareErrorType::HashFieldValueDiff => {
                write!(f, "Hash field value different")
            }
            CompareErrorType::StringValueNotEqual => {
                write!(f, "String value not equal")
            }
            CompareErrorType::RedisConnectionErr => {
                write!(f, "Redis connection error")
            }
            CompareErrorType::KeyTypeNotString => {
                write!(f, "Key type not string")
            }
            CompareErrorType::KeyTypeNotList => {
                write!(f, "Key type not list")
            }
            CompareErrorType::KeyTypeNotSet => {
                write!(f, "Key type not set")
            }
            CompareErrorType::KeyTypeNotZSet => {
                write!(f, "Key type not zset")
            }
            CompareErrorType::KeyTypeNotHash => {
                write!(f, "Key type not hash")
            }
            CompareErrorType::Unknown => {
                write!(f, "Unknown")
            }
        }
    }
}

// 用于描述集合类型元素位置，list index；zset member；hash field
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Position {
    ListIndex(usize),
    ZsetMember(String),
    HashField(String),
}

// #[derive(Debug, Clone)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CompareErrorReason {
    pub redis_key: RedisKey,
    pub position: Option<Position>,
    pub source: Option<String>,
    pub target: Option<String>,
}

/// 应用错误
#[derive(Debug, Clone)]
pub struct CompareError {
    /// 错误信息
    pub message: Option<String>,
    /// 错误类型
    pub error_type: CompareErrorType,
    pub reason: Option<CompareErrorReason>,
}

impl CompareError {
    /// 错误代码
    #[allow(dead_code)]
    fn code(&self) -> i32 {
        match self.error_type {
            CompareErrorType::Unknown => 9999,
            CompareErrorType::TTLDiff => 1000,
            CompareErrorType::ExistsErr => 1001,
            CompareErrorType::StringValueNotEqual => 1002,
            CompareErrorType::ListLenDiff => 1003,
            CompareErrorType::ListIndexValueDiff => 1004,
            CompareErrorType::SetCardDiff => 1005,
            CompareErrorType::SetMemberNotIn => 1006,
            CompareErrorType::ZSetCardDiff => 1007,
            CompareErrorType::ZSetMemberScoreDiff => 1008,
            CompareErrorType::HashLenDiff => 1009,
            CompareErrorType::HashFieldValueDiff => 1010,
            CompareErrorType::RedisConnectionErr => 1011,
            CompareErrorType::KeyTypeNotString => 1011,
            CompareErrorType::KeyTypeNotList => 1012,
            CompareErrorType::KeyTypeNotSet => 1013,
            CompareErrorType::KeyTypeNotZSet => 1014,
            CompareErrorType::KeyTypeNotHash => 1015,
        }
    }

    /// 从字符串创建应用错误
    #[allow(dead_code)]
    pub fn from_str(msg: &str, error_type: CompareErrorType) -> Self {
        Self {
            message: Some(msg.to_string()),
            error_type,
            reason: None,
        }
    }

    pub fn from_reason(reason: CompareErrorReason, error_type: CompareErrorType) -> Self {
        Self {
            message: Some(error_type.to_string()),
            error_type,
            reason: Some(reason),
        }
    }
}

impl From<anyhow::Error> for CompareError {
    fn from(e: anyhow::Error) -> Self {
        e.into()
    }
}

impl std::error::Error for CompareError {}

impl Display for CompareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
