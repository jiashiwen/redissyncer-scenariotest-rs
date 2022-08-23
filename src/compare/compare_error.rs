use std::fmt::Display;

/// 错误的类型
#[derive(Debug)]
pub enum CompareErrorType {
    TTLDiff,
    ExistsErr,
    ListLenDiff,
    ListIndexValueDiff,
    SetMembersDiff,
    SetMemberNotIn,
    ZSetMembersDiff,
    ZSetMemberScoreDiff,
    ValueNotEqual,

    /// 未知错误
    Unknow,
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
            CompareErrorType::Unknow => 9999,
            CompareErrorType::TTLDiff => 1000,
            CompareErrorType::ExistsErr => 1001,
            CompareErrorType::ValueNotEqual => 1002,
            CompareErrorType::ListLenDiff => 1003,
            CompareErrorType::ListIndexValueDiff => 1004,
            CompareErrorType::SetMembersDiff => 1005,
            CompareErrorType::SetMemberNotIn => 1006,
            CompareErrorType::ZSetMembersDiff => 1007,
            CompareErrorType::ZSetMemberScoreDiff => 1008
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
