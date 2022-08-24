use std::fmt;
use std::fmt::{Display, Formatter, write};

/// 错误的类型
#[derive(Debug)]
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

    /// 未知错误
    Unknown,
}

impl fmt::Display for CompareErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompareErrorType::TTLDiff => { write!(f, "TTL different") }
            CompareErrorType::ExistsErr => { write!(f, "Key not exists") }
            CompareErrorType::ListLenDiff => { write!(f, "List length different") }
            CompareErrorType::ListIndexValueDiff => { write!(f, "List index value different") }
            CompareErrorType::SetCardDiff => { write!(f, "Set cardinality different") }
            CompareErrorType::SetMemberNotIn => { write!(f, "mumber not in set") }
            CompareErrorType::ZSetCardDiff => { write!(f, "Sorted set cardinality different") }
            CompareErrorType::ZSetMemberScoreDiff => { write!(f, "mumber not in sorted set") }
            CompareErrorType::HashLenDiff => { write!(f, "Hash length different") }
            CompareErrorType::HashFieldValueDiff => { write!(f, "Hash field value different") }
            CompareErrorType::StringValueNotEqual => { write!(f, "String value not equal") }
            CompareErrorType::Unknown => { write!(f, "Unknown") }
        }
    }
}

/// 应用错误
#[derive(Debug)]
pub struct CompareError {
    /// 错误信息
    pub message: Option<String>,
    /// 错误原因（上一级的错误）
    pub cause: Option<String>,
    /// 错误类型
    pub error_type: CompareErrorType,
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
            CompareErrorType::HashFieldValueDiff => 1010
        }
    }
    /// 从上级错误中创建应用错误
    pub(crate) fn from_err(err: impl ToString, error_type: CompareErrorType) -> Self {
        Self {
            message: None,
            cause: Some(err.to_string()),
            error_type: error_type,
        }
    }
    /// 从字符串创建应用错误
    #[allow(dead_code)]
    pub fn from_str(msg: &str, error_type: CompareErrorType) -> Self {
        Self {
            message: Some(msg.to_string()),
            cause: None,
            error_type: error_type,
        }
    }
}

impl std::error::Error for CompareError {}

impl Display for CompareError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}