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
    FileReader, FileStat, FileSystemCapacity, FileWriter, OpenFileFlags, OpenedFile,
    PathFileSystem, Result,
};
use crate::filesystem_metadata::DefaultFileSystemMetadata;
use crate::utils::join_file_path;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType};
use regex::Regex;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};

// MemoryFileSystem is a simple in-memory filesystem implementation
// It is used for testing purposes

struct MemoryFile {
    kind: FileType,
    data: Arc<Mutex<Vec<u8>>>,
}

pub(crate) struct MemoryFileSystem {
    // file_map is a map of file name to file size
    file_map: RwLock<BTreeMap<String, MemoryFile>>,
}

impl MemoryFileSystem {
    pub fn new() -> Self {
        Self {
            file_map: RwLock::new(Default::default()),
        }
    }

    fn create_file_stat(&self, path: &str, file: &MemoryFile) -> FileStat {
        match file.kind {
            Directory => FileStat::new_dir_with_path(path),
            _ => FileStat::new_file_with_path(path, file.data.lock().unwrap().len() as u64),
        }
    }
}

#[async_trait]
impl PathFileSystem for MemoryFileSystem {
    async fn init(&self) {
        let root = MemoryFile {
            kind: Directory,
            data: Arc::new(Mutex::new(Vec::new())),
        };
        self.file_map.write().unwrap().insert("".to_string(), root);

        let meta = MemoryFile {
            kind: RegularFile,
            data: Arc::new(Mutex::new(Vec::new())),
        };
        self.file_map.write().unwrap().insert(
            DefaultFileSystemMetadata::FS_META_FILE_NAME.to_string(),
            meta,
        );
    }

    async fn stat(&self, name: &str) -> Result<FileStat> {
        self.file_map
            .read()
            .unwrap()
            .get(name)
            .map(|x| self.create_file_stat(name, x))
            .ok_or(Errno::from(libc::ENOENT))
    }

    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat> {
        self.stat(&join_file_path(parent, name)).await
    }

    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>> {
        let file_map = self.file_map.read().unwrap();

        let results: Vec<FileStat> = file_map
            .iter()
            .filter(|x| dir_child_reg_expr(name).is_match(x.0))
            .map(|(k, v)| self.create_file_stat(k, v))
            .collect();

        Ok(results)
    }

    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        let file_stat = self.stat(name).await?;
        let mut file = OpenedFile::new(file_stat.clone());
        match file.file_stat.kind {
            Directory => Ok(file),
            RegularFile => {
                let data = self
                    .file_map
                    .read()
                    .unwrap()
                    .get(&file.file_stat.path)
                    .unwrap()
                    .data
                    .clone();
                file.reader = Some(Box::new(MemoryFileReader { data: data.clone() }));
                file.writer = Some(Box::new(MemoryFileWriter { data: data }));
                Ok(file)
            }
            _ => Err(Errno::from(libc::EBADF)),
        }
    }

    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        self.open_file(name, flags).await
    }

    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile> {
        {
            let file_map = self.file_map.read().unwrap();
            if file_map.contains_key(&join_file_path(parent, name)) {
                return Err(Errno::from(libc::EEXIST));
            }
        };

        let mut file = OpenedFile::new(FileStat::new_file(parent, name, 0));

        let data = Arc::new(Mutex::new(Vec::new()));
        self.file_map.write().unwrap().insert(
            file.file_stat.path.clone(),
            MemoryFile {
                kind: RegularFile,
                data: data.clone(),
            },
        );
        file.reader = Some(Box::new(MemoryFileReader { data: data.clone() }));
        file.writer = Some(Box::new(MemoryFileWriter { data: data }));

        Ok(file)
    }

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        {
            let mut file_map = self.file_map.read().unwrap();
            if file_map.contains_key(&join_file_path(parent, name)) {
                return Err(Errno::from(libc::EEXIST));
            }
        }

        let file = OpenedFile::new(FileStat::new_dir(parent, name));
        self.file_map.write().unwrap().insert(
            file.file_stat.path.clone(),
            MemoryFile {
                kind: Directory,
                data: Arc::new(Mutex::new(Vec::new())),
            },
        );

        Ok(file)
    }

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()> {
        Ok(())
    }

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()> {
        let mut file_map = self.file_map.write().unwrap();
        if file_map.remove(&join_file_path(parent, name)).is_none() {
            return Err(Errno::from(libc::ENOENT));
        }
        Ok(())
    }

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()> {
        let mut file_map = self.file_map.write().unwrap();
        let count = file_map
            .iter()
            .filter(|x| dir_child_reg_expr(name).is_match(x.0))
            .count();

        if count != 0 {
            return Err(Errno::from(libc::ENOTEMPTY));
        }

        file_map.remove(&join_file_path(parent, name));
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

fn dir_child_reg_expr(name: &str) -> Regex {
    let regex_pattern = if name.is_empty() {
        r"^[^/]+$".to_string()
    } else {
        format!(r"^{}/[^/]+$", name)
    };
    Regex::new(&regex_pattern).unwrap()
}
