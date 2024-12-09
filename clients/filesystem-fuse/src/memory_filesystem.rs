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
    FileReader, FileStat, FileWriter, OpenedFile, PathFileSystem, Result,
};
use async_trait::async_trait;
use dashmap::DashMap;
use fuse3::{Errno, FileType};
use regex::Regex;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, RwLock};
use bytes::Bytes;
use crate::utils::join_file_path;

// MemoryFileSystem is a simple in-memory filesystem implementation
// It is used for testing purposes
pub(crate) struct MemoryFileSystem {
    // file_map is a map of file stats
    file_map: RwLock<BTreeMap<String, FileStat>>,

    // file_data_map is a map of file data
    file_data_map: DashMap<String, Arc<Mutex<Vec<u8>>>>,
}

impl MemoryFileSystem {
    pub fn new() -> Self {
        Self {
            file_map: RwLock::new(Default::default()),
            file_data_map: Default::default(),
        }
    }

    pub fn init(&self) {}
}

#[async_trait]
impl PathFileSystem for MemoryFileSystem {
    async fn init(&self) {}

    async fn stat(&self, name: &str) -> Result<FileStat> {
        self.file_map
            .read()
            .unwrap()
            .get(name)
            .map(|x| x.clone())
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
            .map(|(_, v)| v.clone())
            .collect();

        Ok(results)
    }

    async fn open_file(&self, name: &str, flags : u32) -> Result<OpenedFile> {
        let file_stat = self.stat(name).await?;
        let mut file = OpenedFile::new(file_stat.clone());
        match file.file_stat.kind {
            FileType::Directory => {
                Ok(file)
            }
            FileType::RegularFile => {
                let data = self.file_data_map.get(&file.file_stat.path).unwrap().value().clone();
                file.reader = Some(Box::new(MemoryFileReader {
                    data: data.clone(),
                }));
                file.writer = Some(Box::new(MemoryFileWriter {
                    data: data,
                }));
                Ok(file)
            }
            _ => Err(Errno::from(libc::EBADF)),
        }
    }

    async fn create_file(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        let mut file_map = self.file_map.read().unwrap();
        if file_map.contains_key(&join_file_path(parent, name)) {
            return Err(Errno::from(libc::EEXIST));
        }

        let mut file = OpenedFile::new(FileStat::new_file(parent, name, 0));

        self.file_data_map
            .insert(file.file_stat.path.clone(), Arc::new(Mutex::new(Vec::new())));
        let data = self.file_data_map.get(&file.file_stat.path).unwrap().value().clone();
        file.reader = Some(Box::new(MemoryFileReader {
            data: data.clone(),
        }));
        file.writer = Some(Box::new(MemoryFileWriter {
            data: data,
        }));
        Ok(file)
    }

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        let mut file_map = self.file_map.read().unwrap();
        if file_map.contains_key(&join_file_path(parent, name)) {
            return Err(Errno::from(libc::EEXIST));
        }

        let file = OpenedFile::new(FileStat::new_dir(parent, name));
        Ok(file)
    }

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()> {
        let mut file_map = self.file_map.write().unwrap();
        file_map.insert(name.to_string(), file_stat.clone());
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
