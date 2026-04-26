use std::io;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use tokio::sync::Mutex;

mod model;
mod engine;

use model::DataValue;
use engine::MyDatabase;

use crate::model::MyDbError;


fn parse_input(input: &str) -> DataValue {
    if let Ok(i) = input.parse::<i64>() {
        DataValue::Integer(i)
    } else if let Ok(f) = input.parse::<f64>() {
        DataValue::Float(f)
    } else if let Ok(b) = input.parse::<bool>() {
        DataValue::Boolean(b)
    } else if input == "null" {
        DataValue::Null
    } else {
        DataValue::Text(input.to_string())
    }
}


#[tokio::main]
async fn main() -> io::Result<()> {
    let db = Arc::new(Mutex::new(MyDatabase::new("data_store").expect("DB 초기화 실패")));
    
    let listener = TcpListener::bind("127.0.0.1:16379").await?;
    println!("DB 서버가 127.0.0.1:16379 에서 가동 중입니다...");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let c_db: Arc<Mutex<MyDatabase>> = Arc::clone(&db);

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                // 클라이언트로부터 데이터 읽기
                let n = match socket.read(&mut buf).await {
                    Ok(n) if n == 0 => return, // 접속 종료
                    Ok(n) => n,
                    Err(_) => return,
                };

                let request = String::from_utf8_lossy(&buf[..n]);
                let parts: Vec<&str> = request.trim().split_whitespace().collect();

                if parts.is_empty() { continue; }

                // DB 작업 수행
                let mut l_db: tokio::sync::MutexGuard<MyDatabase> = c_db.lock().await; // 잠금 획득 or 대기
                let response = match parts[0] {
                    "set" if parts.len() == 3 => {
                        l_db.set(parts[1].to_string(), parse_input(parts[2])).ok();
                        "OK\n".to_string()
                    }
                    "get" if parts.len() == 2 => {
                        match l_db.get(parts[1]) {
                            Ok(v) => format!("\"{:?}\"\n", v),
                            Err(MyDbError::KeyNotFound(k)) => format!("(nil) - Key '{}' not found\n", k),
                            Err(e) => format!("Internal Error: {}\n", e), // 치명적 에러도 서버는 죽지 않음
                        }
                    }
                    "del" if parts.len() == 2 => {
                        match l_db.delete(parts[1].to_string()) {
                            Ok(_) => "OK\n".to_string(),
                            Err(e) => format!("Error: {}\n", e),
                        }
                    }
                    "compact" => {
                        match l_db.compact() {
                            Ok(_) => "Compaction completed successfully\n".to_string(),
                            Err(e) => format!("Error during compaction: {}\n", e),
                        }
                    }
                    "filter_gt" if parts.len() == 2 => {
                        if let Ok(threshold) = parts[1].parse::<i64>() {
                            // Integer 타입이면서 기준값보다 큰 데이터만 필터링
                            let results = l_db.filter(|val| {
                                if let DataValue::Integer(i) = val {
                                    *i > threshold
                                } else {
                                    false
                                }
                            });

                            if results.is_empty() {
                                "No results found\n".to_string()
                            } else {
                                let mut res_str = String::new();
                                for entry in results {
                                    res_str.push_str(&format!("{}: {:?}\n", entry.key, entry.value));
                                }
                                res_str
                            }
                        } else {
                            "Usage: filter_gt [number]\n".to_string()
                        }
                    }
                    "scan" if parts.len() == 3 => {
                        let results = l_db.get_range(parts[1], parts[2]);

                        if results.is_empty() {
                            "No results in range\n".to_string()
                        } else {
                            let mut res_str = String::new();
                            for entry in results {
                                res_str.push_str(&format!("{}: {:?}\n", entry.key, entry.value));
                            }
                            res_str
                        }
                    }
                    _ => "Error: Unknown Command\n".to_string(),
                };

                socket.write_all(response.as_bytes()).await.ok();
            }
        });
    }
}
