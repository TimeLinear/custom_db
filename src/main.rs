use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs::{OpenOptions, File};
use std::io::{self, BufRead, BufReader, Write};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Debug)]
struct LogEntry {
    key: String,
    value: String,
}

struct MyDatabase {
    storage: HashMap<String, String>,
    file: File,
}

impl MyDatabase {
    fn new(path: &str) -> Self {
        let mut storage = HashMap::new();

        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                if let Ok(l) = line {
                    if let Ok(entry) = serde_json::from_str::<LogEntry>(&l) {
                        storage.insert(entry.key, entry.value);
                    }
                }
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("파일을 열 수 없습니다!");

        Self { storage, file }
    }

    fn set(&mut self, key: String, value: String) -> io::Result<()> {
        let entry = LogEntry {
            key: key.clone(),
            value: value.clone()
        };

        let json_line = serde_json::to_string(&entry)?;
        writeln!(self.file, "{}", json_line)?;

        self.storage.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &str) -> Option<&String> {
        self.storage.get(key)
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let db = Arc::new(Mutex::new(MyDatabase::new("database.db")));
    
    let listener = TcpListener::bind("127.0.0.1:16379").await?;
    println!("DB 서버가 127.0.0.1:16379 에서 가동 중입니다...");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let c_db = Arc::clone(&db);

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
                let mut db = c_db.lock().await; // 잠금 획득 or 대기
                let response = match parts[0] {
                    "set" if parts.len() == 3 => {
                        db.set(parts[1].to_string(), parts[2].to_string()).ok();
                        "OK\n".to_string()
                    }
                    "get" if parts.len() == 2 => {
                        match db.get(parts[1]) {
                            Some(v) => format!("\"{}\"\n", v),
                            None => "(nil)\n".to_string(),
                        }
                    }
                    _ => "Error: Unknown Command\n".to_string(),
                };

                socket.write_all(response.as_bytes()).await.ok();
            }
        });
    }
}
