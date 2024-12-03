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
use crate::file_handle_manager::FileHandleManager;
use crate::filesystem::{FileReader, FileStat, FileWriter, IFileSystem, OpenedFile};
use crate::filesystem_metadata::{DefaultFileSystemMetadata, IFileSystemMetadata};
use dashmap::DashMap;
use fuse3::Errno;
use std::sync::{Arc, Mutex, RwLock};

// MemoryFileSystem is a simple in-memory filesystem implementation
// It is used for testing purposes
pub(crate) struct MemoryFileSystem {
    // meta is the metadata of the filesystem
    meta: RwLock<DefaultFileSystemMetadata>,

    file_handle_manager: RwLock<FileHandleManager>,

    // file_data_map is a map of file data
    file_data_map: DashMap<u64, Arc<Mutex<Vec<u8>>>>,
}

impl MemoryFileSystem {
    const ROOT_DIR_PARENT_FILE_ID: u64 = 0;
    const ROOT_DIR_FILE_ID: u64 = 1;
    const FS_META_FILE_NAME: &'static str = ".gvfs_meta";

    pub fn new() -> Self {
        let fs = Self {
            meta: RwLock::new(DefaultFileSystemMetadata::new()),
            file_handle_manager: RwLock::new(FileHandleManager::new()),
            file_data_map: Default::default(),
        };

        FileStat::new_dir(
            "/".into(),
            MemoryFileSystem::ROOT_DIR_FILE_ID,
            MemoryFileSystem::ROOT_DIR_PARENT_FILE_ID,
        );

        {
            let mut meta = fs.meta.write().unwrap();
            meta.add_root_dir();
            meta.add_file(
                MemoryFileSystem::ROOT_DIR_FILE_ID,
                MemoryFileSystem::FS_META_FILE_NAME,
            );
        }
        fs
    }
}

impl IFileSystem for MemoryFileSystem {
    fn get_file_path(&self, file_id: u64) -> String {
        let meta = self.meta.read().unwrap();
        meta.get_file_path(file_id)
    }

    fn get_opened_file(&self, _file_id: u64, fh: u64) -> Option<OpenedFile> {
        let file_handle_map = self.file_handle_manager.read().unwrap();
        file_handle_map.get_file(fh)
    }

    fn stat(&self, file_id: u64) -> Option<FileStat> {
        let meta = self.meta.read().unwrap();
        meta.get_file(file_id)
    }

    fn lookup(&self, parent_file_id: u64, name: &str) -> Option<FileStat> {
        let meta = self.meta.read().unwrap();
        meta.get_file_from_dir(parent_file_id, name)
    }

    fn read_dir(&self, file_id: u64) -> Vec<FileStat> {
        let meta = self.meta.read().unwrap();
        meta.get_dir_childs(file_id)
    }

    fn open_file(&self, file_id: u64) -> Result<OpenedFile, Errno> {
        let meta = self.meta.read().unwrap();
        let file_stat = meta.get_file(file_id);
        match file_stat {
            Some(file_stat) => {
                let mut file_handle_map = self.file_handle_manager.write().unwrap();
                let file_handle = file_handle_map.open_file(&file_stat);
                Ok(file_handle)
            }
            None => Err(libc::ENOENT.into()),
        }
    }

    fn create_file(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile, Errno> {
        let mut meta = self.meta.write().unwrap();
        let file_stat = meta.add_file(parent_file_id, name);

        let mut file_handle_map = self.file_handle_manager.write().unwrap();
        let file_handle = file_handle_map.open_file(&file_stat);

        self.file_data_map
            .insert(file_stat.inode, Arc::new(Mutex::new(Vec::new())));
        Ok(file_handle)
    }

    fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile, Errno> {
        let mut meta = self.meta.write().unwrap();
        let file_stat = meta.add_dir(parent_file_id, name);

        let mut file_handle_map = self.file_handle_manager.write().unwrap();
        let file_handle = file_handle_map.open_file(&file_stat);
        Ok(file_handle)
    }

    fn set_attr(&self, file_id: u64, file_info: &FileStat) -> Result<(), Errno> {
        Ok(())
    }

    fn update_file_status(&self, file_id: u64, file_stat: &FileStat) {
        let mut meta = self.meta.write().unwrap();
        meta.update_file_stat(file_id, file_stat)
    }

    fn read(&self, file_id: u64, fh: u64) -> Box<dyn FileReader> {
        let file = {
            let file_handle_map = self.file_handle_manager.read().unwrap();
            file_handle_map.get_file(fh).unwrap()
        };

        let data = { self.file_data_map.get(&file_id).unwrap().clone() };

        Box::new(MemoryFileReader {
            file: file.clone(),
            data: data,
        })
    }

    fn write(&self, file_id: u64, fh: u64) -> Box<dyn FileWriter> {
        let file = {
            let file_handle_map = self.file_handle_manager.read().unwrap();
            file_handle_map.get_file(fh).unwrap()
        };

        let data = { self.file_data_map.get(&file_id).unwrap().clone() };

        Box::new(MemoryFileWriter {
            file: file.clone(),
            data: data,
        })
    }

    fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<(), Errno> {
        let mut meta = self.meta.write().unwrap();
        meta.remove_file(parent_file_id, name);
        Ok(())
    }

    fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<(), Errno> {
        let mut meta = self.meta.write().unwrap();
        meta.remove_dir(parent_file_id, name)
    }

    fn close_file(&self, _file_id: u64, fh: u64) -> Result<(), Errno> {
        let mut file_handle_manager = self.file_handle_manager.write().unwrap();
        file_handle_manager.remove_file(fh);
        Ok(())
    }
}

pub(crate) struct MemoryFileReader {
    pub(crate) file: OpenedFile,
    pub(crate) data: Arc<Mutex<Vec<u8>>>,
}

impl FileReader for MemoryFileReader {
    fn file(&self) -> &OpenedFile {
        &self.file
    }

    fn read(&mut self, offset: u64, size: u32) -> Vec<u8> {
        let v = self.data.lock().unwrap();
        let start = offset as usize;
        let end = usize::min(start + size as usize, v.len());
        if start >= v.len() {
            return Vec::new();
        }
        v[start..end].to_vec()
    }
}

pub(crate) struct MemoryFileWriter {
    pub(crate) file: OpenedFile,
    pub(crate) data: Arc<Mutex<Vec<u8>>>,
}

impl FileWriter for MemoryFileWriter {
    fn file(&self) -> &OpenedFile {
        &self.file
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> u32 {
        let mut v = self.data.lock().unwrap();
        let start = offset as usize;
        let end = start + data.len();
        if end > self.file.size as usize {
            self.file.size = end as u64;
        }
        if v.len() < end {
            v.resize(end, 0);
        }
        v[start..end].copy_from_slice(data);
        data.len() as u32
    }
}
