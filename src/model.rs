use serde::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum DataValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
    Deleted, // 툼스톤 표시용
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub key: String,
    pub value: DataValue,
}

#[derive(Error, Debug)]
pub enum MyDbError {
    #[error("I/O 오류 발생: {0}")]
    Io(#[from] std::io::Error),

    #[error("데이터 파싱 실패 (JSON): {0}")]
    Parse(#[from] serde_json::Error),

    #[error("키를 찾을 수 없음: {0}")]
    KeyNotFound(String),

    #[error("데이터 폴더 접근 불가: {0}")]
    ConfigError(String),
}

pub type Result<T> = std::result::Result<T, MyDbError>;