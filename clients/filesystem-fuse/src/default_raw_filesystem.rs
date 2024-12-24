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
use crate::filesystem::{FileStat, PathFileSystem, RawFileSystem, Result};
use crate::opened_file::{FileHandle, OpenFileFlags};
use crate::opened_file_manager::OpenedFileManager;
use crate::utils::join_file_path;
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::{Errno, FileType};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use tokio::sync::RwLock;

/// DefaultRawFileSystem is a simple implementation for the file system.
/// it is used to manage the file metadata and file handle.
/// The operations of the file system are implemented by the PathFileSystem.
pub struct DefaultRawFileSystem<T: PathFileSystem> {
    /// file entries
    file_entry_manager: RwLock<FileEntryManager>,
    /// opened files
    opened_file_manager: OpenedFileManager,
    /// file id generator
    file_id_generator: AtomicU64,

    /// real filesystem
    fs: T,
}

impl<T: PathFileSystem> DefaultRawFileSystem<T> {
    const INITIAL_FILE_ID: u64 = 10000;
    const ROOT_DIR_PARENT_FILE_ID: u64 = 1;
    const ROOT_DIR_FILE_ID: u64 = 1;
    const ROOT_DIR_NAME: &'static str = "";

    pub(crate) fn new(fs: T) -> Self {
        Self {
            file_entry_manager: RwLock::new(FileEntryManager::new()),
            opened_file_manager: OpenedFileManager::new(),
            file_id_generator: AtomicU64::new(Self::INITIAL_FILE_ID),
            fs,
        }
    }

    fn next_file_id(&self) -> u64 {
        self.file_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    async fn get_file_entry(&self, file_id: u64) -> Result<FileEntry> {
        self.file_entry_manager
            .read()
            .await
            .get_file_entry_by_id(file_id)
            .ok_or(Errno::from(libc::ENOENT))
    }

    async fn get_file_entry_by_path(&self, path: &str) -> Option<FileEntry> {
        self.file_entry_manager
            .read()
            .await
            .get_file_entry_by_path(path)
    }

    async fn resolve_file_id_to_filestat(&self, file_stat: &mut FileStat, parent_file_id: u64) {
        let mut file_manager = self.file_entry_manager.write().await;
        let file_entry = file_manager.get_file_entry_by_path(&file_stat.path);
        match file_entry {
            None => {
                // allocate new file id
                file_stat.set_file_id(parent_file_id, self.next_file_id());
                file_manager.insert(file_stat.parent_file_id, file_stat.file_id, &file_stat.path);
            }
            Some(file) => {
                // use the exist file id
                file_stat.set_file_id(file.parent_file_id, file.file_id);
            }
        }
    }

    async fn open_file_internal(
        &self,
        file_id: u64,
        flags: u32,
        kind: FileType,
    ) -> Result<FileHandle> {
        let file_entry = self.get_file_entry(file_id).await?;

        let mut opened_file = {
            match kind {
                FileType::Directory => {
                    self.fs
                        .open_dir(&file_entry.path, OpenFileFlags(flags))
                        .await?
                }
                FileType::RegularFile => {
                    self.fs
                        .open_file(&file_entry.path, OpenFileFlags(flags))
                        .await?
                }
                _ => return Err(Errno::from(libc::EINVAL)),
            }
        };
        // set the exists file id
        opened_file.set_file_id(file_entry.parent_file_id, file_id);
        let file = self.opened_file_manager.put(opened_file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }

    async fn remove_file_entry_locked(&self, path: &str) {
        let mut file_manager = self.file_entry_manager.write().await;
        file_manager.remove(path);
    }

    async fn insert_file_entry_locked(&self, parent_file_id: u64, file_id: u64, path: &str) {
        let mut file_manager = self.file_entry_manager.write().await;
        file_manager.insert(parent_file_id, file_id, path);
    }
}

#[async_trait]
impl<T: PathFileSystem> RawFileSystem for DefaultRawFileSystem<T> {
    async fn init(&self) -> Result<()> {
        // init root directory
        self.insert_file_entry_locked(
            Self::ROOT_DIR_PARENT_FILE_ID,
            Self::ROOT_DIR_FILE_ID,
            Self::ROOT_DIR_NAME,
        )
        .await;
        self.fs.init().await
    }

    async fn get_file_path(&self, file_id: u64) -> Result<String> {
        let file_entry = self.get_file_entry(file_id).await?;
        Ok(file_entry.path)
    }

    async fn valid_file_handle_id(&self, file_id: u64, fh: u64) -> Result<()> {
        let fh_file_id = self
            .opened_file_manager
            .get(fh)
            .ok_or(Errno::from(libc::EBADF))?
            .lock()
            .await
            .file_stat
            .file_id;

        (file_id == fh_file_id)
            .then_some(())
            .ok_or(Errno::from(libc::EBADF))
    }

    async fn stat(&self, file_id: u64) -> Result<FileStat> {
        let file_entry = self.get_file_entry(file_id).await?;
        let mut file_stat = self.fs.stat(&file_entry.path).await?;
        file_stat.set_file_id(file_entry.parent_file_id, file_entry.file_id);
        Ok(file_stat)
    }

    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat> {
        let parent_file_entry = self.get_file_entry(parent_file_id).await?;
        let mut file_stat = self.fs.lookup(&parent_file_entry.path, name).await?;
        // fill the file id to file stat
        self.resolve_file_id_to_filestat(&mut file_stat, parent_file_id)
            .await;
        Ok(file_stat)
    }

    async fn read_dir(&self, file_id: u64) -> Result<Vec<FileStat>> {
        let file_entry = self.get_file_entry(file_id).await?;
        let mut child_filestats = self.fs.read_dir(&file_entry.path).await?;
        for file_stat in child_filestats.iter_mut() {
            self.resolve_file_id_to_filestat(file_stat, file_stat.file_id)
                .await;
        }
        Ok(child_filestats)
    }

    async fn open_file(&self, file_id: u64, flags: u32) -> Result<FileHandle> {
        self.open_file_internal(file_id, flags, FileType::RegularFile)
            .await
    }

    async fn open_dir(&self, file_id: u64, flags: u32) -> Result<FileHandle> {
        self.open_file_internal(file_id, flags, FileType::Directory)
            .await
    }

    async fn create_file(&self, parent_file_id: u64, name: &str, flags: u32) -> Result<FileHandle> {
        let parent_file_entry = self.get_file_entry(parent_file_id).await?;
        let mut file_without_id = self
            .fs
            .create_file(&parent_file_entry.path, name, OpenFileFlags(flags))
            .await?;

        file_without_id.set_file_id(parent_file_id, self.next_file_id());

        // insert the new file to file entry manager
        self.insert_file_entry_locked(
            parent_file_id,
            file_without_id.file_stat.file_id,
            &file_without_id.file_stat.path,
        )
        .await;

        // put the openfile to the opened file manager and allocate a file handle id
        let file_with_id = self.opened_file_manager.put(file_without_id);
        let opened_file_with_file_handle_id = file_with_id.lock().await;
        Ok(opened_file_with_file_handle_id.file_handle())
    }

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<u64> {
        let parent_file_entry = self.get_file_entry(parent_file_id).await?;
        let mut filestat = self.fs.create_dir(&parent_file_entry.path, name).await?;

        filestat.set_file_id(parent_file_id, self.next_file_id());

        // insert the new file to file entry manager
        self.insert_file_entry_locked(parent_file_id, filestat.file_id, &filestat.path)
            .await;
        Ok(filestat.file_id)
    }

    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let file_entry = self.get_file_entry(file_id).await?;
        self.fs.set_attr(&file_entry.path, file_stat, true).await
    }

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_entry = self.get_file_entry(parent_file_id).await?;
        self.fs.remove_file(&parent_file_entry.path, name).await?;

        // remove the file from file entry manager
        self.remove_file_entry_locked(&join_file_path(&parent_file_entry.path, name))
            .await;
        Ok(())
    }

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_entry = self.get_file_entry(parent_file_id).await?;
        self.fs.remove_dir(&parent_file_entry.path, name).await?;

        // remove the dir from file entry manager
        self.remove_file_entry_locked(&join_file_path(&parent_file_entry.path, name))
            .await;
        Ok(())
    }

    async fn close_file(&self, _file_id: u64, fh: u64) -> Result<()> {
        let opened_file = self
            .opened_file_manager
            .remove(fh)
            .ok_or(Errno::from(libc::EBADF))?;
        let mut file = opened_file.lock().await;
        file.close().await
    }

    async fn read(
        &self,
        _file_id: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> crate::filesystem::Result<Bytes> {
        let (data, file_stat) = {
            let opened_file = self
                .opened_file_manager
                .get(fh)
                .ok_or(Errno::from(libc::EBADF))?;
            let mut opened_file = opened_file.lock().await;
            let data = opened_file.read(offset, size).await;
            (data, opened_file.file_stat.clone())
        };

        // update the file atime
        self.fs.set_attr(&file_stat.path, &file_stat, false).await?;

        data
    }

    async fn write(
        &self,
        _file_id: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
    ) -> crate::filesystem::Result<u32> {
        let (len, file_stat) = {
            let opened_file = self
                .opened_file_manager
                .get(fh)
                .ok_or(Errno::from(libc::EBADF))?;
            let mut opened_file = opened_file.lock().await;
            let len = opened_file.write(offset, data).await;
            (len, opened_file.file_stat.clone())
        };

        // update the file size, mtime and atime
        self.fs.set_attr(&file_stat.path, &file_stat, false).await?;

        len
    }
}

/// File entry is represent the abstract file.
#[derive(Debug, Clone)]
struct FileEntry {
    file_id: u64,
    parent_file_id: u64,
    path: String,
}

/// FileEntryManager is manage all the file entries in memory. it is used manger the file relationship and name mapping.
struct FileEntryManager {
    // file_id_map is a map of file_id to file entry.
    file_id_map: HashMap<u64, FileEntry>,

    // file_path_map is a map of file path to file entry.
    file_path_map: HashMap<String, FileEntry>,
}

impl FileEntryManager {
    fn new() -> Self {
        Self {
            file_id_map: HashMap::new(),
            file_path_map: HashMap::new(),
        }
    }

    fn get_file_entry_by_id(&self, file_id: u64) -> Option<FileEntry> {
        self.file_id_map.get(&file_id).cloned()
    }

    fn get_file_entry_by_path(&self, path: &str) -> Option<FileEntry> {
        self.file_path_map.get(path).cloned()
    }

    fn insert(&mut self, parent_file_id: u64, file_id: u64, path: &str) {
        let file_entry = FileEntry {
            file_id,
            parent_file_id,
            path: path.to_string(),
        };
        self.file_id_map.insert(file_id, file_entry.clone());
        self.file_path_map.insert(path.to_string(), file_entry);
    }

    fn remove(&mut self, path: &str) {
        if let Some(file) = self.file_path_map.remove(path) {
            self.file_id_map.remove(&file.file_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_entry_manager() {
        let mut manager = FileEntryManager::new();
        manager.insert(1, 2, "a/b");
        let file = manager.get_file_entry_by_id(2).unwrap();
        assert_eq!(file.file_id, 2);
        assert_eq!(file.parent_file_id, 1);
        assert_eq!(file.path, "a/b");

        let file = manager.get_file_entry_by_path("a/b").unwrap();
        assert_eq!(file.file_id, 2);
        assert_eq!(file.parent_file_id, 1);
        assert_eq!(file.path, "a/b");

        manager.remove("a/b");
        assert!(manager.get_file_entry_by_id(2).is_none());
        assert!(manager.get_file_entry_by_path("a/b").is_none());
    }
}
