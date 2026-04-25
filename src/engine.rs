use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write, Seek, SeekFrom};
use std::path::Path;
use crate::model::{DataValue, LogEntry, MyDbError, Result};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct IndexEntry {
    pub offset: u64,
    pub len: usize,
}

pub struct MyDatabase {
    memtable: BTreeMap<String, DataValue>,
    index: HashMap<String, (String, IndexEntry)>,
    data_dir: String,
}

impl MyDatabase {
    pub fn new(dir_path: &str) -> Result<Self> {
        fs::create_dir_all(dir_path).map_err(|e| MyDbError::ConfigError(e.to_string()))?;

        let mut index = HashMap::new();
        let paths = fs::read_dir(dir_path)?;

        let mut idx_files: Vec<_> = paths
            .filter_map(|r| r.ok())
            .filter(|e| {
                e.path().extension().map_or(false, |ext| ext == "idx")
            })
            .collect();

        idx_files.sort_by_key(|e| e.file_name());

        for entry in idx_files {
            let idx_path = entry.path();
            let file_stem = idx_path.file_stem().unwrap().to_str().unwrap().to_string();
            let data_filename = format!("{}.db", file_stem);

            if let Ok(file) = File::open(&idx_path) {
                let temp_idx: HashMap<String, IndexEntry> = serde_json::from_reader(file).unwrap_or_default();
                for (key, idx_entry) in temp_idx {
                    index.insert(key, (data_filename.clone(), idx_entry));
                }
            }
        }

        println!("Loaded {} entries from index files.", index.len());

        Ok(Self { 
            memtable: BTreeMap::new(),
            index,
            data_dir: dir_path.to_string(),
        })
    }

    // 저장 시: 파일 끝에 쓰고, 위치를 인덱스에 기록
    pub fn set(&mut self, key: String, value: DataValue) -> io::Result<()> {
        self.memtable.insert(key.clone(), value.clone());

        if self.memtable.len() >= 100 { // 예시로 100개 이상일 때 flush
            self.flush_to_disk()?;
        }
        Ok(())
    }

    // 조회 시: 메모리 조회 후 없으면 인덱스로 파일 조회
    pub fn get(&mut self, key: &str) -> Result<DataValue> {
        if let Some(val) = self.memtable.get(key) {
            if *val == DataValue::Deleted {
                return Err(MyDbError::KeyNotFound(key.to_string()));
            }
            return Ok(val.clone());
        }

        let (filename, idx_entry) = self.index.get(key)
            .ok_or_else(|| MyDbError::KeyNotFound(key.to_string()))?;

        let full_path = Path::new(&self.data_dir).join(filename);
        let mut file = File::open(full_path)?;
        file.seek(SeekFrom::Start(idx_entry.offset))?;
        
        let mut buffer = vec![0; idx_entry.len];
        file.read_exact(&mut buffer)?;
        
        let entry: LogEntry = serde_json::from_slice(&buffer)?;

        if entry.value == DataValue::Deleted {
            Err(MyDbError::KeyNotFound(key.to_string()))
        } else {
            Ok(entry.value)
        }
    }

    pub fn delete(&mut self, key: String) -> io::Result<()> {
        self.set(key, DataValue::Deleted)
    }

    pub fn compact(&mut self) -> io::Result<()> {
        println!("컴팩션 시작");

        // 임시 파일 생성
        let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        let tmp_data_path = Path::new(&self.data_dir).join(format!("compact_{}.db.tmp", timestamp));
        let tmp_idx_path = Path::new(&self.data_dir).join(format!("compact_{}.idx.tmp", timestamp));

        let mut tmp_data_file = File::create(&tmp_data_path)?;
        let mut new_idx_map = HashMap::new();
        let mut current_offset = 0;

        // 인덱스의 모든 키 수집 및 정렬
        let mut all_keys: Vec<String> = self.index.keys().cloned().collect();
        all_keys.sort();

        for key in all_keys {
            if let Ok(value) = self.get(&key) {
                // 삭제된 데이터(Tombstone)는 건너뛰기
                if value == DataValue::Deleted {
                    continue;
                }

                let entry = LogEntry { key: key.clone(), value };
                let json_line = format!("{}\n", serde_json::to_string(&entry)?);
                let len = json_line.len();

                tmp_data_file.write_all(json_line.as_bytes())?;
                new_idx_map.insert(key, IndexEntry { offset: current_offset, len });
                current_offset += len as u64;
            }
        }

        // 임시 인덱스 파일 저장
        let tmp_idx_file = File::create(&tmp_idx_path)?;
        serde_json::to_writer(tmp_idx_file, &new_idx_map)?;

        // 기존 파일 목록 확보
        let old_files: Vec<String> = self.index.values()
            .map(|(filename, _)| filename.clone())
            .collect::<std::collections::HashSet<_>>() // 중복 제거
            .into_iter()
            .collect();

        // 파일명 변경
        let final_data_path = tmp_data_path.with_extension(""); // .tmp 제거
        let final_idx_path = tmp_idx_path.with_extension(""); 
        fs::rename(&tmp_data_path, &final_data_path)?;
        fs::rename(&tmp_idx_path, &final_idx_path)?;

        // 메모리 인덱스 교체
        let final_data_name = final_data_path.file_name().unwrap().to_str().unwrap().to_string();
        self.index.clear();
        let new_idx_map_len = new_idx_map.len();
        for (k, v) in new_idx_map {
            self.index.insert(k, (final_data_name.clone(), v));
        }

        // 기존 파일 삭제
        let old_files_len = old_files.len();
        for filename in old_files {
            let path = Path::new(&self.data_dir).join(filename);
            let idx_path = path.with_extension("idx");
            let _ = fs::remove_file(path);
            let _ = fs::remove_file(idx_path);
        }

        println!("컴팩션 완료: {}개 파일이 삭제되고, {}개의 키가 새 파일로 이동되었습니다.", old_files_len, new_idx_map_len);
        Ok(())
    }

    // 전수 검사(추후 최적화 필요)
    pub fn filter<F>(&mut self, predicate: F) -> Vec<LogEntry>
    where
        F: Fn(&DataValue) -> bool, // 값(DataValue)을 검사하는 함수를 인자로 받음
    {
        let mut latest_data = HashMap::new();

        // 1. 디렉토리 내의 모든 .db 파일들을 순회하며 전수 검사
        if let Ok(entries) = fs::read_dir(&self.data_dir) {
            let mut paths: Vec<_> = entries.filter_map(|e| e.ok()).collect();
            paths.sort_by_key(|e| e.file_name());

            for entry in paths {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "db") {
                    if let Ok(file) = File::open(path) {
                        let reader = BufReader::new(file);
                        for line in reader.lines().filter_map(|l| l.ok()) {
                            if let Ok(log_entry) = serde_json::from_str::<LogEntry>(&line) {
                                latest_data.insert(log_entry.key, log_entry.value);
                            }
                        }
                    }
                }
            }
        }

        // 2. 메모리(memtable) 데이터로 덮어쓰기 (가장 최신)
        for (key, value) in &self.memtable {
            latest_data.insert(key.clone(), value.clone());
        }

        // 3. 필터링 및 결과 생성
        latest_data.into_iter()
            .filter(|(_, val)| *val != DataValue::Deleted && predicate(val))
            .map(|(key, value)| LogEntry { key, value })
            .collect()
    }

    pub fn get_range(&mut self, start: &str, end: &str) -> Vec<LogEntry> {
        let mut results = Vec::new();

        for (key, value) in self.memtable.range(start.to_string()..=end.to_string()) {
            if *value != DataValue::Deleted {
                results.push(LogEntry {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
        }

        results
    }

    fn flush_to_disk(&mut self) -> io::Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let file_prefix = format!("data_{}", timestamp);
        let data_path = Path::new(&self.data_dir).join(format!("{}.db", file_prefix));
        let idx_path = Path::new(&self.data_dir).join(format!("{}.idx", file_prefix));


        let mut data_file = File::create(&data_path)?;
        let mut idx_map = HashMap::new();
        let mut current_offset = 0;

        for (key, value) in &self.memtable {
            let entry = LogEntry { key: key.clone(), value: value.clone() };
            let json_line = format!("{}\n", serde_json::to_string(&entry)?);
            let len = json_line.len();

            data_file.write_all(json_line.as_bytes())?;
            idx_map.insert(key.clone(), IndexEntry { offset: current_offset, len });
            current_offset += len as u64;
        }

        let idx_file = File::create(&idx_path)?;
        serde_json::to_writer(idx_file, &idx_map)?;

        for (key, idx_entry) in idx_map {
            self.index.insert(key, (format!("{}.db", file_prefix), idx_entry));
        }

        println!("Successfully flushed {} entries to {}", self.memtable.len(), data_path.display());

        self.memtable.clear();
        Ok(())
    }
}