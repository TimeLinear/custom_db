use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DataValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub key: String,
    pub value: DataValue,
}