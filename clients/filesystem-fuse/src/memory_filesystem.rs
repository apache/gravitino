/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use crate::filesystem::{
    FileReader, FileStat, FileSystemCapacity, FileWriter, PathFileSystem, Result,
};
use crate::opened_file::{OpenFileFlags, OpenedFile};
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

// Simple in-memory file implementation of MemoryFileSystem
struct MemoryFile {
    kind: FileType,
    data: Arc<Mutex<Vec<u8>>>,
}

// MemoryFileSystem is a simple in-memory filesystem implementation
// It is used for testing purposes
pub struct MemoryFileSystem {
    // file_map is a map of file name to file size
    file_map: RwLock<BTreeMap<PathBuf, MemoryFile>>,
}

impl MemoryFileSystem {
    pub(crate) async fn new() -> Self {
        Self {
            file_map: RwLock::new(Default::default()),
        }
    }

    fn create_file_stat(&self, path: &Path, file: &MemoryFile) -> FileStat {
        match file.kind {
            Directory => FileStat::new_dir_filestat_with_path(path),
            _ => {
                FileStat::new_file_filestat_with_path(path, file.data.lock().unwrap().len() as u64)
            }
        }
    }
}

#[async_trait]
impl PathFileSystem for MemoryFileSystem {
    async fn init(&self) -> Result<()> {
        let root_file = MemoryFile {
            kind: Directory,
            data: Arc::new(Mutex::new(Vec::new())),
        };
        let root_path = PathBuf::from("/");
        self.file_map.write().unwrap().insert(root_path, root_file);
        Ok(())
    }

    async fn stat(&self, path: &Path, _kind: FileType) -> Result<FileStat> {
        self.file_map
            .read()
            .unwrap()
            .get(path)
            .map(|x| self.create_file_stat(path, x))
            .ok_or(Errno::from(libc::ENOENT))
    }

    async fn lookup(&self, path: &Path) -> Result<FileStat> {
        self.stat(path, RegularFile).await
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        let file_map = self.file_map.read().unwrap();

        let results: Vec<FileStat> = file_map
            .iter()
            .filter(|x| path_in_dir(path, x.0))
            .map(|(k, v)| self.create_file_stat(k, v))
            .collect();

        Ok(results)
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let file_stat = self.stat(path, RegularFile).await?;
        let mut opened_file = OpenedFile::new(file_stat);
        match opened_file.file_stat.kind {
            Directory => Ok(opened_file),
            RegularFile => {
                let data = self
                    .file_map
                    .read()
                    .unwrap()
                    .get(&opened_file.file_stat.path)
                    .unwrap()
                    .data
                    .clone();
                if flags.is_read() {
                    opened_file.reader = Some(Box::new(MemoryFileReader { data: data.clone() }));
                }
                if flags.is_write() || flags.is_append() || flags.is_truncate() {
                    opened_file.writer = Some(Box::new(MemoryFileWriter { data: data.clone() }));
                }

                if flags.is_truncate() {
                    let mut data = data.lock().unwrap();
                    data.clear();
                }
                Ok(opened_file)
            }
            _ => Err(Errno::from(libc::EBADF)),
        }
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        self.open_file(path, flags).await
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        if self.file_map.read().unwrap().contains_key(path) && flags.is_exclusive() {
            return Err(Errno::from(libc::EEXIST));
        }

        self.file_map.write().unwrap().insert(
            path.to_path_buf(),
            MemoryFile {
                kind: RegularFile,
                data: Arc::new(Mutex::new(Vec::new())),
            },
        );
        self.open_file(path, flags).await
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        let mut file_map = self.file_map.write().unwrap();
        if file_map.contains_key(path) {
            return Err(Errno::from(libc::EEXIST));
        }

        let file = FileStat::new_dir_filestat_with_path(path);
        file_map.insert(
            file.path.clone(),
            MemoryFile {
                kind: Directory,
                data: Arc::new(Mutex::new(Vec::new())),
            },
        );

        Ok(file)
    }

    async fn set_attr(&self, _name: &Path, _file_stat: &FileStat, _flush: bool) -> Result<()> {
        Ok(())
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        let mut file_map = self.file_map.write().unwrap();
        if file_map.remove(path).is_none() {
            return Err(Errno::from(libc::ENOENT));
        }
        Ok(())
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        let mut file_map = self.file_map.write().unwrap();
        let count = file_map.iter().filter(|x| path_in_dir(path, x.0)).count();

        if count != 0 {
            return Err(Errno::from(libc::ENOTEMPTY));
        }

        if file_map.remove(path).is_none() {
            return Err(Errno::from(libc::ENOENT));
        }
        Ok(())
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        Ok(FileSystemCapacity {})
    }
}

pub(crate) struct MemoryFileReader {
    pub(crate) data: Arc<Mutex<Vec<u8>>>,
}

#[async_trait]
impl FileReader for MemoryFileReader {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes> {
        let v = self.data.lock().unwrap();
        let start = offset as usize;
        let end = usize::min(start + size as usize, v.len());
        if start >= v.len() {
            return Ok(Bytes::default());
        }
        Ok(v[start..end].to_vec().into())
    }
}

pub(crate) struct MemoryFileWriter {
    pub(crate) data: Arc<Mutex<Vec<u8>>>,
}

#[async_trait]
impl FileWriter for MemoryFileWriter {
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        let mut v = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + data.len();

        if v.len() < end {
            v.resize(end, 0);
        }
        v[start..end].copy_from_slice(data);
        Ok(data.len() as u32)
    }
}

fn path_in_dir(dir: &Path, path: &Path) -> bool {
    if let Ok(relative_path) = path.strip_prefix(dir) {
        relative_path.components().count() == 1
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::default_raw_filesystem::DefaultRawFileSystem;
    use crate::filesystem::tests::{TestPathFileSystem, TestRawFileSystem};
    use crate::filesystem::{FileSystemContext, RawFileSystem};

    #[test]
    fn test_path_in_dir() {
        let dir = Path::new("/parent");

        let path1 = Path::new("/parent/child1");
        let path2 = Path::new("/parent/a.txt");
        let path3 = Path::new("/parent/child1/grandchild");
        let path4 = Path::new("/other");

        assert!(!path_in_dir(dir, dir));
        assert!(path_in_dir(dir, path1));
        assert!(path_in_dir(dir, path2));
        assert!(!path_in_dir(dir, path3));
        assert!(!path_in_dir(dir, path4));

        let dir = Path::new("/");

        let path1 = Path::new("/child1");
        let path2 = Path::new("/a.txt");
        let path3 = Path::new("/child1/grandchild");

        assert!(!path_in_dir(dir, dir));
        assert!(path_in_dir(dir, path1));
        assert!(path_in_dir(dir, path2));
        assert!(!path_in_dir(dir, path3));
    }

    #[tokio::test]
    async fn test_memory_file_system() {
        let fs = MemoryFileSystem::new().await;
        let _ = fs.init().await;
        let mut tester = TestPathFileSystem::new(Path::new("/ab"), fs);
        tester.test_path_file_system().await;
    }

    #[tokio::test]
    async fn test_memory_file_system_with_raw_file_system() {
        let memory_fs = MemoryFileSystem::new().await;
        let raw_fs = DefaultRawFileSystem::new(
            memory_fs,
            &AppConfig::default(),
            &FileSystemContext::default(),
        );
        let _ = raw_fs.init().await;
        let mut tester = TestRawFileSystem::new(Path::new("/ab"), raw_fs);
        tester.test_raw_file_system().await;
    }
}
