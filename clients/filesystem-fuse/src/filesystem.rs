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
use crate::opened_file_manager::OpenedFileManager;
use crate::utils::{join_file_path, split_file_path};
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::{Errno, FileType, Timestamp};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::time::SystemTime;
use tokio::sync::RwLock;

pub(crate) type Result<T> = std::result::Result<T, Errno>;

/// RawFileSystem interface for the file system implementation. it use by FuseApiHandle,
/// it ues the file id to operate the file system apis
/// the `file_id` and `parent_file_id` it is the unique identifier for the file system,
/// it is used to identify the file or directory
/// the `handle_id` it is the file handle, it is used to identify the opened file,
/// it is used to read or write the file content
/// the `file id` and `handle_id` need to mapping the `ino`/`inode` and `fh` in the fuse3
#[async_trait]
pub(crate) trait RawFileSystem: Send + Sync {
    /// Init the file system
    async fn init(&self) -> Result<()>;

    /// Get the file path by file id, if the file id is valid, return the file path
    async fn get_file_path(&self, file_id: u64) -> String;

    /// Validate the file id and file handle, if file id and file handle is valid and it associated, return Ok
    async fn valid_file_id(&self, file_id: u64, fh: u64) -> Result<()>;

    /// Get the file stat by file id. if the file id is valid, return the file stat
    async fn stat(&self, file_id: u64) -> Result<FileStat>;

    /// Lookup the file by parent file id and file name, if the file is exist, return the file stat
    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat>;

    /// Read the directory by file id, if the file id is a valid directory, return the file stat list
    async fn read_dir(&self, dir_file_id: u64) -> Result<Vec<FileStat>>;

    /// Open the file by file id and flags, if the file id is a valid file, return the file handle
    async fn open_file(&self, file_id: u64, flags: u32) -> Result<FileHandle>;

    /// Open the directory by file id and flags, if successful, return the file handle
    async fn open_dir(&self, file_id: u64, flags: u32) -> Result<FileHandle>;

    /// Create the file by parent file id and file name and flags, if successful, return the file handle
    async fn create_file(&self, parent_file_id: u64, name: &str, flags: u32) -> Result<FileHandle>;

    /// Create the directory by parent file id and file name, if successful, return the file id
    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<u64>;

    /// Set the file attribute by file id and file stat
    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()>;

    /// Remove the file by parent file id and file name
    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()>;

    /// Remove the directory by parent file id and file name
    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()>;

    /// Close the file by file id and file handle, if successful
    async fn close_file(&self, file_id: u64, fh: u64) -> Result<()>;

    /// Read the file content by file id, file handle, offset and size, if successful, return the read result
    async fn read(&self, file_id: u64, fh: u64, offset: u64, size: u32) -> Result<Bytes>;

    /// Write the file content by file id, file handle, offset and data, if successful, return the written size
    async fn write(&self, file_id: u64, fh: u64, offset: u64, data: &[u8]) -> Result<u32>;
}

/// PathFileSystem is the interface for the file system implementation, it use to interact with other file system
/// it is used file path to operate the file system
#[async_trait]
pub(crate) trait PathFileSystem: Send + Sync {
    /// Init the file system
    async fn init(&self) -> Result<()>;

    /// Get the file stat by file path, if the file is exist, return the file stat
    async fn stat(&self, name: &str) -> Result<FileStat>;

    /// Get the file stat by parent file path and file name, if the file is exist, return the file stat
    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat>;

    /// Read the directory by file path, if the file is a valid directory, return the file stat list
    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>>;

    /// Open the file by file path and flags, if the file is exist, return the opened file
    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile>;

    /// Open the directory by file path and flags, if the file is exist, return the opened file
    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile>;

    /// Create the file by parent file path and file name and flags, if successful, return the opened file
    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile>;

    /// Create the directory by parent file path and file name, if successful, return the file stat
    async fn create_dir(&self, parent: &str, name: &str) -> Result<FileStat>;

    /// Set the file attribute by file path and file stat
    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()>;

    /// Remove the file by parent file path and file name
    async fn remove_file(&self, parent: &str, name: &str) -> Result<()>;

    /// Remove the directory by parent file path and file name
    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()>;
}

// FileSystemContext is the system environment for the fuse file system.
pub(crate) struct FileSystemContext {
    // system user id
    pub(crate) uid: u32,

    // system group id
    pub(crate) gid: u32,

    // default file permission
    pub(crate) default_file_perm: u16,

    // default idr permission
    pub(crate) default_dir_perm: u16,

    // io block size
    pub(crate) block_size: u32,
}

impl FileSystemContext {
    pub(crate) fn new(uid: u32, gid: u32) -> Self {
        FileSystemContext {
            uid,
            gid,
            default_file_perm: 0o644,
            default_dir_perm: 0o755,
            block_size: 4 * 1024,
        }
    }
}

// FileStat is the file metadata of the file
#[derive(Clone, Debug)]
pub struct FileStat {
    // file id for the file system.
    pub(crate) file_id: u64,

    // parent file id
    pub(crate) parent_file_id: u64,

    // file name
    pub(crate) name: String,

    // file path of the fuse file system root
    pub(crate) path: String,

    // file size
    pub(crate) size: u64,

    // file type like regular file or directory and so on
    pub(crate) kind: FileType,

    // file access time
    pub(crate) atime: Timestamp,

    // file modify time
    pub(crate) mtime: Timestamp,

    // file create time
    pub(crate) ctime: Timestamp,

    // file link count
    pub(crate) nlink: u32,
}

impl FileStat {
    pub fn new_file_with_path(path: &str, size: u64) -> Self {
        let (parent, name) = split_file_path(path);
        Self::new_file(parent, name, size)
    }

    pub fn new_dir_with_path(path: &str) -> Self {
        let (parent, name) = split_file_path(path);
        Self::new_dir(parent, name)
    }

    pub fn new_file(parent: &str, name: &str, size: u64) -> Self {
        Self::new_file_entry(parent, name, size, FileType::RegularFile)
    }

    pub fn new_dir(parent: &str, name: &str) -> Self {
        Self::new_file_entry(parent, name, 0, FileType::Directory)
    }

    pub fn new_file_entry(parent: &str, name: &str, size: u64, kind: FileType) -> Self {
        let atime = Timestamp::from(SystemTime::now());
        Self {
            file_id: 0,
            parent_file_id: 0,
            name: name.into(),
            path: join_file_path(parent, name),
            size: size,
            kind: kind,
            atime: atime,
            mtime: atime,
            ctime: atime,
            nlink: 1,
        }
    }

    pub(crate) fn set_file_id(&mut self, parent_file_id: u64, file_id: u64) {
        debug_assert!(file_id != 0 && parent_file_id != 0);
        self.parent_file_id = parent_file_id;
        self.file_id = file_id;
    }
}

/// Opened file for read or write, it is used to read or write the file content.
pub(crate) struct OpenedFile {
    pub(crate) file_stat: FileStat,

    pub(crate) handle_id: u64,

    pub reader: Option<Box<dyn FileReader>>,

    pub writer: Option<Box<dyn FileWriter>>,
}

impl OpenedFile {
    pub fn new(file_stat: FileStat) -> Self {
        OpenedFile {
            file_stat: file_stat,
            handle_id: 0,
            reader: None,
            writer: None,
        }
    }

    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes> {
        let reader = self.reader.as_mut().ok_or(Errno::from(libc::EBADF))?;
        let result = reader.read(offset, size).await?;

        // update the access time
        self.file_stat.atime = Timestamp::from(SystemTime::now());

        Ok(result)
    }

    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        let writer = self.writer.as_mut().ok_or(Errno::from(libc::EBADF))?;
        let written = writer.write(offset, data).await?;

        // update the file size
        let end = offset + data.len() as u64;
        if end > self.file_stat.size {
            self.file_stat.size = end;
        }
        self.file_stat.atime = Timestamp::from(SystemTime::now());
        self.file_stat.mtime = self.file_stat.atime;

        Ok(written)
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut reader) = self.reader.take() {
            reader.close().await?;
        }
        if let Some(mut writer) = self.writer.take() {
            self.flush().await?;
            writer.close().await?
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    fn file_handle(&self) -> FileHandle {
        debug_assert!(self.handle_id != 0);
        debug_assert!(self.file_stat.file_id != 0);
        FileHandle {
            file_id: self.file_stat.file_id,
            handle_id: self.handle_id,
        }
    }

    pub(crate) fn set_file_id(&mut self, parent_file_id: u64, file_id: u64) {
        debug_assert!(file_id != 0 && parent_file_id != 0);
        self.file_stat.set_file_id(parent_file_id, file_id)
    }
}

// FileHandle is the file handle for the opened file.
pub(crate) struct FileHandle {
    pub(crate) file_id: u64,

    pub(crate) handle_id: u64,
}

// OpenFileFlags is the open file flags for the file system.
pub struct OpenFileFlags(u32);

/// File reader interface  for read file content
#[async_trait]
pub(crate) trait FileReader: Sync + Send {
    /// read the file content by offset and size, if successful, return the read result
    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes>;

    /// close the file
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// File writer interface  for write file content
#[async_trait]
pub trait FileWriter: Sync + Send {
    /// write the file content by offset and data, if successful, return the written size
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32>;

    /// close the file
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// flush the file
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// SimpleFileSystem is a simple implementation for the file system.
/// it is used to manage the file metadata and file handle.
/// The operations of the file system are implemented by the PathFileSystem.
/// Note: This class is not use in the production code, it is used for the demo and testing
pub struct SimpleFileSystem<T: PathFileSystem> {
    /// file entries
    file_entry_manager: RwLock<FileEntryManager>,
    /// opened files
    opened_file_manager: OpenedFileManager,
    /// inode id generator
    file_id_generator: AtomicU64,

    /// real system
    fs: T,
}

impl<T: PathFileSystem> SimpleFileSystem<T> {
    const INITIAL_FILE_ID: u64 = 10000;
    const ROOT_DIR_PARENT_FILE_ID: u64 = 0;
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
            .get_file_by_id(file_id)
            .ok_or(Errno::from(libc::ENOENT))
    }

    async fn get_file_entry_by_path(&self, path: &str) -> Option<FileEntry> {
        self.file_entry_manager.read().await.get_file_by_name(path)
    }

    async fn fill_file_id(&self, file_stat: &mut FileStat, parent_file_id: u64) {
        let mut file_manager = self.file_entry_manager.write().await;
        let file = file_manager.get_file_by_name(&file_stat.path);
        match file {
            None => {
                // allocate new file id
                file_stat.set_file_id(parent_file_id, self.next_file_id());
                file_manager.insert(file_stat.parent_file_id, file_stat.file_id, &file_stat.path);
            }
            Some(file) => {
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

        let mut file = {
            match kind {
                FileType::Directory => {
                    self.fs
                        .open_dir(&file_entry.file_name, OpenFileFlags(flags))
                        .await?
                }
                FileType::RegularFile => {
                    self.fs
                        .open_file(&file_entry.file_name, OpenFileFlags(flags))
                        .await?
                }
                _ => return Err(Errno::from(libc::EINVAL)),
            }
        };
        file.set_file_id(file_entry.parent_file_id, file_id);
        let file = self.opened_file_manager.put_file(file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }
}

#[async_trait]
impl<T: PathFileSystem> RawFileSystem for SimpleFileSystem<T> {
    async fn init(&self) -> Result<()> {
        self.file_entry_manager.write().await.insert(
            Self::ROOT_DIR_PARENT_FILE_ID,
            Self::ROOT_DIR_FILE_ID,
            Self::ROOT_DIR_NAME,
        );
        self.fs.init().await
    }

    async fn get_file_path(&self, file_id: u64) -> String {
        let file = self.get_file_entry(file_id).await;
        file.map(|x| x.file_name).unwrap_or_else(|_| "".to_string())
    }

    async fn valid_file_id(&self, _file_id: u64, fh: u64) -> Result<()> {
        let file_id = self
            .opened_file_manager
            .get_file(fh)
            .ok_or(Errno::from(libc::EBADF))?
            .lock()
            .await
            .file_stat
            .file_id;

        (file_id == _file_id)
            .then_some(())
            .ok_or(Errno::from(libc::EBADF))
    }

    async fn stat(&self, file_id: u64) -> Result<FileStat> {
        let file = self.get_file_entry(file_id).await?;
        let mut stat = self.fs.stat(&file.file_name).await?;
        stat.set_file_id(file.parent_file_id, file.file_id);
        Ok(stat)
    }

    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat> {
        let parent_file = self.get_file_entry(parent_file_id).await?;
        let mut stat = self.fs.lookup(&parent_file.file_name, name).await?;
        self.fill_file_id(&mut stat, parent_file_id).await;
        Ok(stat)
    }

    async fn read_dir(&self, file_id: u64) -> Result<Vec<FileStat>> {
        let file = self.get_file_entry(file_id).await?;
        let mut files = self.fs.read_dir(&file.file_name).await?;
        for file in files.iter_mut() {
            self.fill_file_id(file, file.file_id).await;
        }
        Ok(files)
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
        let parent = self.get_file_entry(parent_file_id).await?;
        let mut file = self
            .fs
            .create_file(&parent.file_name, name, OpenFileFlags(flags))
            .await?;

        file.set_file_id(parent_file_id, self.next_file_id());
        {
            let mut file_manager = self.file_entry_manager.write().await;
            file_manager.insert(
                file.file_stat.parent_file_id,
                file.file_stat.file_id,
                &file.file_stat.path,
            );
        }
        let file = self.opened_file_manager.put_file(file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<u64> {
        let parent = self.get_file_entry(parent_file_id).await?;
        let mut file = self.fs.create_dir(&parent.file_name, name).await?;

        file.set_file_id(parent_file_id, self.next_file_id());
        {
            let mut file_manager = self.file_entry_manager.write().await;
            file_manager.insert(file.parent_file_id, file.file_id, &file.path);
        }
        Ok(file.file_id)
    }

    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let file = self.get_file_entry(file_id).await?;
        self.fs.set_attr(&file.file_name, file_stat, true).await
    }

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file = self.get_file_entry(parent_file_id).await?;
        self.fs.remove_file(&parent_file.file_name, name).await?;

        {
            let mut file_id_manager = self.file_entry_manager.write().await;
            file_id_manager.remove(&join_file_path(&parent_file.file_name, name));
        }
        Ok(())
    }

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file = self.get_file_entry(parent_file_id).await?;
        self.fs.remove_dir(&parent_file.file_name, name).await?;

        {
            let mut file_id_manager = self.file_entry_manager.write().await;
            file_id_manager.remove(&join_file_path(&parent_file.file_name, name));
        }
        Ok(())
    }

    async fn close_file(&self, _file_id: u64, fh: u64) -> Result<()> {
        let file = self
            .opened_file_manager
            .remove_file(fh)
            .ok_or(Errno::from(libc::EBADF))?;
        let mut file = file.lock().await;
        file.close().await?;
        Ok(())
    }

    async fn read(&self, _file_id: u64, fh: u64, offset: u64, size: u32) -> Result<Bytes> {
        let file_stat: FileStat;
        let data = {
            let opened_file = self
                .opened_file_manager
                .get_file(fh)
                .ok_or(Errno::from(libc::EBADF))?;
            let mut opened_file = opened_file.lock().await;
            file_stat = opened_file.file_stat.clone();
            opened_file.read(offset, size).await
        };

        self.fs.set_attr(&file_stat.path, &file_stat, false).await?;

        data
    }

    async fn write(&self, _file_id: u64, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let (len, file_stat) = {
            let opened_file = self
                .opened_file_manager
                .get_file(fh)
                .ok_or(Errno::from(libc::EBADF))?;
            let mut opened_file = opened_file.lock().await;
            let len = opened_file.write(offset, data).await;
            (len, opened_file.file_stat.clone())
        };

        self.fs.set_attr(&file_stat.path, &file_stat, false).await?;

        len
    }
}

/// File entry is represent the abstract file.
#[derive(Debug, Clone)]
struct FileEntry {
    file_id: u64,
    parent_file_id: u64,
    file_name: String,
}

/// FileEntryManager is manage all the file entries in memory. it is used manger the file relationship and name mapping.
struct FileEntryManager {
    // file_id_map is a map of file_id to file name.
    file_id_map: HashMap<u64, FileEntry>,

    // file_name_map is a map of file name to file id.
    file_name_map: HashMap<String, FileEntry>,
}

impl FileEntryManager {
    fn new() -> Self {
        Self {
            file_id_map: HashMap::new(),
            file_name_map: HashMap::new(),
        }
    }

    fn get_file_by_id(&self, file_id: u64) -> Option<FileEntry> {
        self.file_id_map.get(&file_id).cloned()
    }

    fn get_file_by_name(&self, file_name: &str) -> Option<FileEntry> {
        self.file_name_map.get(file_name).cloned()
    }

    fn insert(&mut self, parent_file_id: u64, file_id: u64, file_name: &str) {
        let file = FileEntry {
            file_id,
            parent_file_id,
            file_name: file_name.to_string(),
        };
        self.file_id_map.insert(file_id, file.clone());
        self.file_name_map.insert(file_name.to_string(), file);
    }

    fn remove(&mut self, file_name: &str) {
        if let Some(file) = self.file_name_map.remove(file_name) {
            self.file_id_map.remove(&file.file_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_file_stat() {
        //test new file
        let file_stat = FileStat::new_file("a", "b", 10);
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, "a/b");
        assert_eq!(file_stat.size, 10);
        assert_eq!(file_stat.kind, FileType::RegularFile);

        //test new dir
        let file_stat = FileStat::new_dir("a", "b");
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, "a/b");
        assert_eq!(file_stat.size, 0);
        assert_eq!(file_stat.kind, FileType::Directory);

        //test new file with path
        let file_stat = FileStat::new_file_with_path("a/b", 10);
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, "a/b");
        assert_eq!(file_stat.size, 10);
        assert_eq!(file_stat.kind, FileType::RegularFile);

        //test new dir with path
        let file_stat = FileStat::new_dir_with_path("a/b");
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, "a/b");
        assert_eq!(file_stat.size, 0);
        assert_eq!(file_stat.kind, FileType::Directory);
    }

    #[test]
    fn test_file_stat_set_file_id() {
        let mut file_stat = FileStat::new_file("a", "b", 10);
        file_stat.set_file_id(1, 2);
        assert_eq!(file_stat.file_id, 2);
        assert_eq!(file_stat.parent_file_id, 1);
    }

    #[test]
    #[should_panic(expected = "assertion failed: file_id != 0 && parent_file_id != 0")]
    fn test_file_stat_set_file_id_panic() {
        let mut file_stat = FileStat::new_file("a", "b", 10);
        file_stat.set_file_id(1, 0);
    }

    #[test]
    fn test_open_file() {
        let mut open_file = OpenedFile::new(FileStat::new_file("a", "b", 10));
        assert_eq!(open_file.file_stat.name, "b");
        assert_eq!(open_file.file_stat.size, 10);

        open_file.set_file_id(1, 2);

        assert_eq!(open_file.file_stat.file_id, 2);
        assert_eq!(open_file.file_stat.parent_file_id, 1);
    }

    #[test]
    fn test_file_entry_manager() {
        let mut manager = FileEntryManager::new();
        manager.insert(1, 2, "a/b");
        let file = manager.get_file_by_id(2).unwrap();
        assert_eq!(file.file_id, 2);
        assert_eq!(file.parent_file_id, 1);
        assert_eq!(file.file_name, "a/b");

        let file = manager.get_file_by_name("a/b").unwrap();
        assert_eq!(file.file_id, 2);
        assert_eq!(file.parent_file_id, 1);
        assert_eq!(file.file_name, "a/b");

        manager.remove("a/b");
        assert!(manager.get_file_by_id(2).is_none());
        assert!(manager.get_file_by_name("a/b").is_none());
    }
}
