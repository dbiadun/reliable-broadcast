use crate::StableStorage;
use std::path::{PathBuf};
use std::fs::{copy, write, read};

const LENGTH_BUFFER_SIZE: usize = 8;
const NEW_ENDING: &str = "new";
const OLD_ENDING: &str = "old";

struct FileStableStorage {
    dir: PathBuf
}

impl FileStableStorage {
    fn new(dir: PathBuf) -> Self {
        Self {
            dir
        }
    }

    fn file_path(&self, base_name: &str, ending: &str) -> PathBuf {
        let mut buf = self.dir.clone();
        buf.push(Self::file_name(base_name, ending));
        buf
    }

    fn file_name(base_name: &str, ending: &str) -> String {
        let mut name = String::new();
        name.push_str(base_name);
        name.push_str("_");
        name.push_str(ending);
        name
    }

    fn write_value(path: PathBuf, value: &[u8]) {
        let value_with_legnth = Self::add_length_to_value(value);
        write(path, value_with_legnth).ok();
    }

    fn add_length_to_value(value: &[u8]) -> Vec<u8> {
        let length = value.len();
        let length_buffer: [u8; LENGTH_BUFFER_SIZE] = length.to_be_bytes();
        let mut buf = vec![];
        buf.append(& mut length_buffer.to_vec());
        buf.extend_from_slice(value);
        buf
    }

    fn read_value(path: PathBuf) -> Option<Vec<u8>> {
        let mut res = None;
        if let Ok(data) = read(path) {
            if data.len() >= 8 {
                let mut length_buffer: [u8; 8] = [0; 8];
                length_buffer.copy_from_slice(&data[..8]);
                let length = usize::from_be_bytes(length_buffer);
                if data.len() == length + 8 {
                    res = Some(data[8..].into())
                }
            }
        }

        res
    }
}

impl StableStorage for FileStableStorage {
    fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > 255 {
            return Err("Too long key".into());
        }
        if value.len() > 65536 {
            return Err("Too long value".into());
        }

        copy(self.file_path(key, NEW_ENDING), self.file_path(key, OLD_ENDING)).ok();
        Self::write_value(self.file_path(key, NEW_ENDING), value);

        Ok(())
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut res = None;

        if let Some(value) = Self::read_value(self.file_path(key, NEW_ENDING)) {
            res = Some(value);
        } else if let Some(value) = Self::read_value(self.file_path(key, OLD_ENDING)) {
            res = Some(value);
        }

        res
    }
}

pub fn build_file_stable_storage(_root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    Box::new(FileStableStorage::new(_root_storage_dir))
}