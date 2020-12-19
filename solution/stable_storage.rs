use crate::StableStorage;
use std::path::{PathBuf};
use std::fs::{copy, write, read, create_dir_all};

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
        let mut rel_path = String::new();
        rel_path.push_str(ending);
        rel_path.push_str("/");
        rel_path.push_str(base_name);
        buf.push(Self::fix_file_path(&rel_path));
        buf
    }

    fn fix_file_path(path: &String) -> String {
        path
            .chars()
            .flat_map(|c| {
                match c {
                    '.' => vec!['/', '.', '/'],
                    l => vec![l]
                }
            })
            .flat_map(|c| {
                match c {
                    '/' => vec!['x', '/', 'x'],
                    l => vec![l]
                }
            })
            .collect()
    }

    fn write_value(path: PathBuf, value: &[u8]) {
        let value_with_legnth = Self::add_length_to_value(value);
        Self::create_parent(&path);
        write(path, value_with_legnth).ok();
    }

    fn create_parent(path: &PathBuf) {
        if let Some(parent) = path.parent() {
            create_dir_all(parent).ok();
        }
    }

    fn add_length_to_value(value: &[u8]) -> Vec<u8> {
        let length = value.len();
        let length_buffer: [u8; LENGTH_BUFFER_SIZE] = length.to_be_bytes();
        let mut buf = vec![];
        buf.append(&mut length_buffer.to_vec());
        buf.extend_from_slice(value);
        buf
    }

    fn read_value(path: PathBuf) -> Option<Vec<u8>> {
        let mut res = None;
        if let Ok(data) = read(path) {
            if data.len() >= 8 {
                let mut length_buffer: [u8; LENGTH_BUFFER_SIZE] = [0; LENGTH_BUFFER_SIZE];
                length_buffer.copy_from_slice(&data[..LENGTH_BUFFER_SIZE]);
                let length = usize::from_be_bytes(length_buffer);
                if data.len() == length + LENGTH_BUFFER_SIZE {
                    res = Some(data[LENGTH_BUFFER_SIZE..].into())
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

        Self::create_parent(&self.file_path(key, OLD_ENDING));
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