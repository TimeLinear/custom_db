use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write, Seek, SeekFrom};
use crate::model::{DataValue, LogEntry};

pub struct IndexEntry {
    pub offset: u64,
    pub len: usize,
}

pub struct MyDatabase {
    memtable: BTreeMap<String, DataValue>,
    index: HashMap<String, IndexEntry>,
    file: File,
    file_path: String,
}

impl MyDatabase {
    pub fn new(path: &str) -> Self {
        let mut index = HashMap::new();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(path)
            .expect("파일 열기 실패");

        // [복구 로직] 파일을 처음부터 읽으며 인덱스 재구성
        let mut reader = BufReader::new(&file);
        let mut current_offset = 0;
        let mut line = String::new();

        while reader.read_line(&mut line).unwrap() > 0 {
            if let Ok(entry) = serde_json::from_str::<LogEntry>(&line) {
                let len = line.len() as usize;
                index.insert(entry.key, IndexEntry { offset: current_offset, len });
                current_offset += len as u64;
            }
            line.clear();
        }

        Self { 
            memtable: BTreeMap::new(),
            index, 
            file, 
            file_path: path.to_string() 
        }
    }

    // 저장 시: 파일 끝에 쓰고, 위치를 인덱스에 기록
    pub fn set(&mut self, key: String, value: DataValue) -> io::Result<()> {
        self.memtable.insert(key.clone(), value.clone());

        let entry = LogEntry { key: key.clone(), value };
        let json_line = format!("{}\n", serde_json::to_string(&entry)?);
        
        let offset = self.file.seek(SeekFrom::End(0))?;
        let len = json_line.len();
        self.file.write_all(json_line.as_bytes())?;
        
        self.index.insert(key, IndexEntry { offset, len });
        Ok(())
    }

    // 조회 시: 메모리 조회 후 없으면 인덱스로 파일 조회
    pub fn get(&mut self, key: &str) -> Option<DataValue> {
        if let Some(val) = self.memtable.get(key) {
            return Some(val.clone());
        }

        let idx = self.index.get(key)?;
        self.file.seek(SeekFrom::Start(idx.offset)).ok()?;
        let mut buffer = vec![0; idx.len];
        self.file.read_exact(&mut buffer).ok()?;
        let entry: LogEntry = serde_json::from_slice(&buffer).ok()?;
        Some(entry.value)
    }

    // 전수 검사(추후 최적화 필요)
    pub fn filter<F>(&mut self, predicate: F) -> Vec<LogEntry>
    where
        F: Fn(&DataValue) -> bool, // 값(DataValue)을 검사하는 함수를 인자로 받음
    {
        let mut results = Vec::new();

        // 1. 파일을 처음부터 읽기 위해 위치 초기화
        let _ = self.file.seek(SeekFrom::Start(0));
        let reader = BufReader::new(&self.file);

        // 2. 한 줄씩 읽으며 조건 검사
        for line in reader.lines() {
            if let Ok(l) = line {
                if let Ok(entry) = serde_json::from_str::<LogEntry>(&l) {
                    // 사용자가 전달한 조건 함수(predicate)에 넣어봅니다.
                    if predicate(&entry.value) {
                        results.push(entry);
                    }
                }
            }
        }
        results
    }

    pub fn get_range(&mut self, start: &str, end: &str) -> Vec<LogEntry> {
        let mut results = Vec::new();

        for (key, value) in self.memtable.range(start.to_string()..=end.to_string()) {
            results.push(LogEntry {
                key: key.clone(),
                value: value.clone(),
            });
        }

        results
    }
}