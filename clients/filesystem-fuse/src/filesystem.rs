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
use crate::filesystem_metadata::DefaultFileSystemMetadata;
use crate::opened_file_manager::OpenedFileManager;
use crate::utils::join_file_path;
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::{Errno, FileType, Timestamp};
use futures_util::{FutureExt, TryFutureExt};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, RwLock};
use std::time::SystemTime;

pub type Result<T> = std::result::Result<T, Errno>;

pub struct FileHandle {
    pub(crate) file_id: u64,
    pub(crate) handle_id: u64,
}

/// RawFileSystem interface for the file system implementation. it use by FuseApiHandle
/// the `file_id` and `parent_file_id` it is the unique identifier for the file system, it is used to identify the file or directory
/// the `fh` it is the file handle, it is used to identify the opened file, it is used to read or write the file content

#[async_trait]
pub trait RawFileSystem: Send + Sync {
    async fn init(&self);

    async fn get_file_path(&self, file_id: u64) -> String;

    async fn valid_file_id(&self, file_id: u64, fh: u64) -> Result<()>;

    async fn stat(&self, file_id: u64) -> Result<FileStat>;

    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat>;

    async fn read_dir(&self, dir_file_id: u64) -> Result<Vec<FileStat>>;

    async fn open_file(&self, file_id: u64, flags: u32) -> Result<FileHandle>;

    async fn open_dir(&self, file_id: u64, flags: u32) -> Result<FileHandle>;

    async fn create_file(&self, parent_file_id: u64, name: &str, flags: u32) -> Result<FileHandle>;

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<FileHandle>;

    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()>;

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()>;

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()>;

    async fn close_file(&self, file_id: u64, fh: u64) -> Result<()>;

    async fn read(&self, file_id: u64, fh: u64, offset: u64, size: u32) -> Result<Bytes>;

    async fn write(&self, file_id: u64, fh: u64, offset: u64, data: &[u8]) -> Result<u32>;
}

/// PathFileSystem is the interface for the file system implementation, it use to interact with other file system
/// it is used file name or path to operate the file system
#[async_trait]
pub trait PathFileSystem: Send + Sync {
    async fn init(&self);

    async fn stat(&self, name: &str) -> Result<FileStat>;

    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat>;

    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>>;

    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile>;

    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile>;

    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile>;

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile>;

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()>;

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()>;

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()>;

    fn get_capacity(&self) -> Result<FileSystemCapacity>;
}

pub struct FileSystemContext {
    // system user id
    pub(crate) uid: u32,

    // system group id
    pub(crate) gid: u32,
}

impl FileSystemContext {
    pub(crate) fn new(uid: u32, gid: u32) -> Self {
        FileSystemContext { uid, gid }
    }
}

pub struct OpenFileFlags(u32);

impl OpenFileFlags {
    pub fn is_read(&self) -> bool {
        self.0 & libc::O_RDONLY as u32 == 0 || self.0 & libc::O_RDWR as u32 == 0
    }

    pub fn is_write(&self) -> bool {
        self.0 & libc::O_WRONLY as u32 == 0 || self.0 & libc::O_RDWR as u32 == 0
    }

    pub fn is_append(&self) -> bool {
        self.0 & libc::O_APPEND as u32 != 0
    }

    pub fn is_create(&self) -> bool {
        self.0 & libc::O_CREAT as u32 != 0
    }

    pub fn is_truncate(&self) -> bool {
        self.0 & libc::O_TRUNC as u32 != 0
    }

    pub fn is_exclusive(&self) -> bool {
        self.0 & libc::O_EXCL as u32 != 0
    }
}

pub struct FileSystemCapacity {}

#[derive(Clone, Debug)]
pub struct FileStat {
    // inode id for the file system, also call file id
    pub(crate) inode: u64,

    // parent inode id
    pub(crate) parent_inode: u64,

    // file name
    pub(crate) name: String,

    // file path of the fuse file system root
    pub(crate) path: String,

    // file size
    pub size: u64,

    // file type like regular file or directory and so on
    pub kind: FileType,

    // file permission
    pub(crate) perm: u16,

    // file access time
    pub atime: Timestamp,

    // file modify time
    pub mtime: Timestamp,

    // file create time
    pub ctime: Timestamp,

    // file link count
    pub(crate) nlink: u32,

    // filestat timestamp after retrieved from original file system
    pub(crate) timestamp: Timestamp,
}

impl FileStat {
    // TODO need to handle the file permission by config
    pub fn new_file_with_path(path: &str, size: u64) -> Self {
        let (parent, name) = match path.rfind('/') {
            Some(pos) => (&path[..pos], &path[pos + 1..]),
            None => ("", path),
        };

        Self::new_file(parent, name, size)
    }

    pub fn new_dir_with_path(path: &str) -> Self {
        let (parent, name) = match path.rfind('/') {
            Some(pos) => (&path[..pos], &path[pos + 1..]),
            None => ("", path),
        };

        Self::new_dir(parent, name)
    }

    pub fn new_file(parent: &str, name: &str, size: u64) -> Self {
        let atime = Timestamp::from(SystemTime::now());
        Self {
            inode: 0,
            parent_inode: 0,
            name: name.into(),
            path: join_file_path(parent, name),
            size: size,
            kind: FileType::RegularFile,
            perm: 0o664,
            atime: atime,
            mtime: atime,
            ctime: atime,
            nlink: 1,
            timestamp: atime,
        }
    }

    pub fn new_dir(parent: &str, name: &str) -> Self {
        let atime = Timestamp::from(SystemTime::now());
        Self {
            inode: 0,
            parent_inode: 0,
            name: name.into(),
            path: join_file_path(parent, name),
            size: 0,
            kind: FileType::Directory,
            perm: 0o755,
            atime: atime,
            mtime: atime,
            ctime: atime,
            nlink: 1,
            timestamp: atime,
        }
    }

    pub(crate) fn set_inode(&mut self, parent_file_id: u64, file_id: u64) {
        self.parent_inode = parent_file_id;
        self.inode = file_id;
    }
}

/// Opened file for read or write, it is used to read or write the file content.
pub struct OpenedFile {
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
        self.file_stat.atime = Timestamp::from(SystemTime::now());

        self.reader.as_mut().unwrap().read(offset, size).await
    }

    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        let end = offset + data.len() as u64;

        if end > self.file_stat.size {
            self.file_stat.size = end;
        }
        self.file_stat.atime = Timestamp::from(SystemTime::now());
        self.file_stat.mtime = self.file_stat.atime;

        self.writer.as_mut().unwrap().write(offset, data).await
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
        debug_assert!(self.file_stat.inode != 0);
        FileHandle {
            file_id: self.file_stat.inode,
            handle_id: self.handle_id,
        }
    }

    pub(crate) fn set_inode(&mut self, parent_file_id: u64, file_id: u64) {
        debug_assert!(file_id != 0 && parent_file_id != 0);
        self.file_stat.set_inode(parent_file_id, file_id)
    }
}

/// File reader interface  for read file content
#[async_trait]
pub trait FileReader: Sync + Send {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes>;
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// File writer interface  for write file content
#[async_trait]
pub trait FileWriter: Sync + Send {
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32>;
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct FileIdInfo {
    file_id: u64,
    parent_file_id: u64,
    file_name: String,
}
/// FileIdManager is a manager for file id and file name mapping.
struct FileNodeManager {
    // file_id_map is a map of file_id to file name.
    file_id_map: HashMap<u64, FileIdInfo>,

    // file_name_map is a map of file name to file id.
    file_name_map: HashMap<String, FileIdInfo>,
}

impl FileNodeManager {
    fn new() -> Self {
        Self {
            file_id_map: HashMap::new(),
            file_name_map: HashMap::new(),
        }
    }

    fn get_node_by_id(&self, file_id: u64) -> Option<FileIdInfo> {
        self.file_id_map.get(&file_id).map(|x| x.clone())
    }

    fn get_node_by_name(&self, file_name: &str) -> Option<FileIdInfo> {
        self.file_name_map.get(file_name).map(|x| x.clone())
    }

    fn insert(&mut self, parent_file_id: u64, file_id: u64, file_name: &str) {
        let file_node = FileIdInfo {
            file_id,
            parent_file_id,
            file_name: file_name.to_string(),
        };
        self.file_id_map.insert(file_id, file_node.clone());
        self.file_name_map.insert(file_name.to_string(), file_node);
    }

    fn remove(&mut self, file_name: &str) {
        if let Some(node) = self.file_name_map.remove(file_name) {
            self.file_id_map.remove(&node.file_id);
        }
    }
}

// SimpleFileSystem is a simple file system implementation for the file system.
// it is used to manage the file system metadata and file handle.
// The operations of the file system are implemented by the PathFileSystem.
pub struct SimpleFileSystem<T: PathFileSystem> {
    file_node_manager: RwLock<FileNodeManager>,
    opened_file_manager: OpenedFileManager,

    inode_id_generator: AtomicU64,

    fs: T,
}

impl<T: PathFileSystem> SimpleFileSystem<T> {
    pub(crate) fn new(fs: T) -> Self {
        Self {
            file_node_manager: RwLock::new(FileNodeManager::new()),
            opened_file_manager: OpenedFileManager::new(),
            inode_id_generator: AtomicU64::new(10000),
            fs,
        }
    }

    fn next_inode_id(&self) -> u64 {
        self.inode_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn get_file_node(&self, file_id: u64) -> Result<FileIdInfo> {
        self.file_node_manager
            .read()
            .unwrap()
            .get_node_by_id(file_id)
            .ok_or(Errno::from(libc::ENOENT))
    }

    fn get_file_node_by_path(&self, path: &str) -> Option<FileIdInfo> {
        self.file_node_manager
            .read()
            .unwrap()
            .get_node_by_name(path)
    }

    fn fill_file_node_id(&self, file_stat: &mut FileStat, parent_file_id: u64) {
        let mut node_manager = self.file_node_manager.write().unwrap();
        let file_node = node_manager.get_node_by_name(&file_stat.path);
        match file_node {
            None => {
                file_stat.set_inode(parent_file_id, self.next_inode_id());
                node_manager.insert(file_stat.parent_inode, file_stat.inode, &file_stat.path);
            }
            Some(file) => {
                file_stat.set_inode(file.parent_file_id, file.file_id);
            }
        }
    }

    async fn create_file_internal(
        &self,
        parent_file_id: u64,
        name: &str,
        flags: u32,
        kind: FileType,
    ) -> Result<FileHandle> {
        let parent_node = self.get_file_node(parent_file_id)?;
        let mut file = match kind {
            FileType::Directory => self.fs.create_dir(&parent_node.file_name, name).await?,
            FileType::RegularFile => {
                self.fs
                    .create_file(&parent_node.file_name, name, OpenFileFlags(flags))
                    .await?
            }
            _ => return Err(Errno::from(libc::EINVAL)),
        };

        file.set_inode(parent_file_id, self.next_inode_id());
        {
            let mut file_node_manager = self.file_node_manager.write().unwrap();
            file_node_manager.insert(
                file.file_stat.parent_inode,
                file.file_stat.inode,
                &file.file_stat.path,
            );
        }
        let file = self.opened_file_manager.put_file(file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }

    async fn open_file_internal(
        &self,
        file_id: u64,
        flags: u32,
        kind: FileType,
    ) -> Result<FileHandle> {
        let file_node = self.get_file_node(file_id)?;

        let mut file = {
            match kind {
                FileType::Directory => {
                    self.fs
                        .open_dir(&file_node.file_name, OpenFileFlags(flags))
                        .await?
                }
                FileType::RegularFile => {
                    self.fs
                        .open_file(&file_node.file_name, OpenFileFlags(flags))
                        .await?
                }
                _ => return Err(Errno::from(libc::EINVAL)),
            }
        };
        file.set_inode(file_node.parent_file_id, file_id);
        let file = self.opened_file_manager.put_file(file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }
}

#[async_trait]
impl<T: PathFileSystem> RawFileSystem for SimpleFileSystem<T> {
    async fn init(&self) {
        self.file_node_manager.write().unwrap().insert(
            DefaultFileSystemMetadata::ROOT_DIR_PARENT_FILE_ID,
            DefaultFileSystemMetadata::ROOT_DIR_FILE_ID,
            DefaultFileSystemMetadata::ROOT_DIR_NAME,
        );
        self.fs.init().await
    }

    async fn get_file_path(&self, file_id: u64) -> String {
        self.get_file_node(file_id)
            .map(|x| x.file_name)
            .unwrap_or_else(|_| "".to_string())
    }

    async fn valid_file_id(&self, _file_id: u64, fh: u64) -> Result<()> {
        let file_id = self
            .opened_file_manager
            .get_file(fh)
            .ok_or(Errno::from(libc::EBADF))?
            .lock()
            .await
            .file_stat
            .inode;

        (file_id == _file_id)
            .then(|| ())
            .ok_or(Errno::from(libc::EBADF))
    }

    async fn stat(&self, file_id: u64) -> Result<FileStat> {
        let file_node = self.get_file_node(file_id)?;
        let mut stat = self.fs.stat(&file_node.file_name).await?;
        stat.set_inode(file_node.parent_file_id, file_node.file_id);
        Ok(stat)
    }

    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat> {
        let parent_file_node = self.get_file_node(parent_file_id)?;
        let mut stat = self.fs.lookup(&parent_file_node.file_name, name).await?;
        self.fill_file_node_id(&mut stat, parent_file_id);
        Ok(stat)
    }

    async fn read_dir(&self, file_id: u64) -> Result<Vec<FileStat>> {
        let file_node = self.get_file_node(file_id)?;
        let mut files = self.fs.read_dir(&file_node.file_name).await?;
        for file in files.iter_mut() {
            self.fill_file_node_id(file, file_node.file_id);
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
        self.create_file_internal(parent_file_id, name, flags, FileType::RegularFile)
            .await
    }

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<FileHandle> {
        self.create_file_internal(parent_file_id, name, 0, FileType::Directory)
            .await
    }

    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let file_node = self.get_file_node(file_id)?;
        self.fs
            .set_attr(&file_node.file_name, file_stat, true)
            .await
    }

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_node = self.get_file_node(parent_file_id)?;
        self.fs
            .remove_file(&parent_file_node.file_name, name)
            .await?;

        {
            let mut file_id_manager = self.file_node_manager.write().unwrap();
            file_id_manager.remove(&join_file_path(&parent_file_node.file_name, name));
        }
        Ok(())
    }

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_node = self.get_file_node(parent_file_id)?;
        self.fs
            .remove_dir(&parent_file_node.file_name, name)
            .await?;

        {
            let mut file_id_manager = self.file_node_manager.write().unwrap();
            file_id_manager.remove(&join_file_path(&parent_file_node.file_name, name));
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

    async fn read(&self, file_id: u64, fh: u64, offset: u64, size: u32) -> Result<Bytes> {
        let mut file_stat: FileStat;
        let data = {
            let mut opened_file = self
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

    async fn write(&self, file_id: u64, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
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

/*
pub struct BasicFileSystem {
    // meta is the metadata of the filesystem
    meta: RwLock<DefaultFileSystemMetadata>,

    // file_handle_manager is a manager for opened files.
    file_handle_manager: RwLock<FileHandleManager>,

    inode_id_generator: AtomicU64,

    fs: Box<dyn PathFileSystem>,
}

impl BasicFileSystem {
    const FILE_STAT_EXPIRE_TIME: i32 = 0;

    pub fn new(fs: Box<dyn PathFileSystem>) -> Self {
        Self {
            meta: RwLock::new(DefaultFileSystemMetadata::new()),
            file_handle_manager: RwLock::new(FileHandleManager::new()),
            inode_id_generator: AtomicU64::new(10000),
            fs,
        }
    }

    fn next_inode_id(&self) -> u64 {
        self.inode_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn get_and_update_file_stat(&self, file_id: u64) -> Result<FileStat> {
        let file_stat = self
            .meta
            .read()
            .unwrap()
            .get_file(file_id)
            .ok_or(Errno::from(libc::ENOENT))?;

        if timestamp_diff_from_now(&file_stat.timestamp) < Self::FILE_STAT_EXPIRE_TIME {
            Ok(file_stat)
        } else {
            match file_stat.kind {
                FileType::Directory => self.handle_file_stat_expired(&file_stat),
                FileType::RegularFile => self.handle_file_stat_expired(&file_stat),
                _ => Err(Errno::from(libc::ENOSYS)),
            }
        }
    }

    fn handle_file_stat_expired(&self, old_file: &FileStat) -> Result<FileStat> {
        self.fs.stat(&old_file.path).map(|new_stat| {
            self.meta
                .write()
                .unwrap()
                .update_file(old_file.inode, &new_stat);
            new_stat
        })
    }

    fn handle_dir_stat_expired(&self, old_dir: &FileStat) -> Result<FileStat> {
        self.handle_file_stat_expired(old_dir);
        let childs = self.fs.read_dir(&old_dir.path)?;
        todo!()
    }
}

#[async_trait]
impl RawFileSystem for BasicFileSystem {
    async fn init(&self) {
        let mut meta = self.meta.write().unwrap();
        meta.init_root_dir();
    }

    async fn get_file_path(&self, file_id: u64) -> String {
        let meta = self.meta.read().unwrap();
        meta.get_file_path(file_id)
    }

    async fn get_opened_file(&self, _file_id: u64, fh: u64) -> Result<OpenedFile> {
        let file_handle_map = self.file_handle_manager.read().unwrap();
        file_handle_map
            .get_file(fh)
            .ok_or(Errno::from(libc::ENOENT))
    }

    async fn stat(&self, file_id: u64) -> Result<FileStat> {
        self.get_and_update_file_stat(file_id)
    }

    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat> {
        self.get_and_update_file_stat(parent_file_id)?;
        self.meta
            .read()
            .unwrap()
            .find_file(parent_file_id, name)
            .ok_or(Errno::from(libc::ENOENT))
    }

    async fn read_dir(&self, file_id: u64) -> Result<Vec<FileStat>> {
        self.get_and_update_file_stat(file_id)?;
        let meta = self.meta.read().unwrap();
        Ok(meta.get_dir_childs(file_id))
    }

    async fn open_file(&self, file_id: u64) -> Result<OpenedFile> {
        let meta = self.meta.read().unwrap();
        let file_stat = meta.get_file(file_id).ok_or(Errno::from(libc::ENOENT))?;
        let mut file_handle_map = self.file_handle_manager.write().unwrap();
        let file_handle = file_handle_map.create_file(&file_stat);
        Ok(file_handle)
    }

    async fn create_file(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile> {
        let dir_stat = self.get_and_update_file_stat(parent_file_id)?;

        let mut file_stat = self.fs.create_file(&dir_stat.path, name)?;
        file_stat.inode = self.next_inode_id();
        file_stat.parent_inode = parent_file_id;

        let mut meta = self.meta.write().unwrap();
        meta.put_file(&file_stat);

        let mut file_handle_map = self.file_handle_manager.write().unwrap();
        let file_handle = file_handle_map.create_file(&file_stat);

        Ok(file_handle)
    }

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile> {
        let dir_stat = self.get_and_update_file_stat(parent_file_id)?;

        let mut file_stat = self.fs.create_dir(&dir_stat.path, name)?;
        file_stat.parent_inode = parent_file_id;

        let mut meta = self.meta.write().unwrap();
        meta.put_dir(&file_stat);

        let mut file_handle_map = self.file_handle_manager.write().unwrap();
        let file_handle = file_handle_map.create_file(&file_stat);
        Ok(file_handle)
    }

    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let mut meta = self.meta.write().unwrap();
        meta.update_file(file_id, file_stat);
        self.fs.set_attr(&file_stat.name, file_stat, true)
    }

    async fn update_file_status(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let mut meta = self.meta.write().unwrap();
        meta.update_file(file_id, file_stat);
        self.fs.set_attr(&file_stat.name, file_stat, false)
    }

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let mut meta = self.meta.write().unwrap();
        let dir_stat = meta
            .get_file(parent_file_id)
            .ok_or(Errno::from(libc::ENOENT))?;
        meta.remove_file(parent_file_id, name);
        self.fs.remove_file(&dir_stat.path, name)
    }

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let mut meta = self.meta.write().unwrap();
        let dir_stat = meta
            .get_file(parent_file_id)
            .ok_or(Errno::from(libc::ENOENT))?;
        meta.remove_dir(parent_file_id, name);
        self.fs.remove_dir(&dir_stat.path, name)
    }

    async fn close_file(&self, _file_id: u64, fh: u64) -> Result<()> {
        let mut file_handle_manager = self.file_handle_manager.write().unwrap();
        file_handle_manager.remove_file(fh);
        Ok(())
    }

    async fn read(&self, file_id: u64, fh: u64) -> Box<dyn FileReader> {
        let file = {
            let file_handle_map = self.file_handle_manager.read().unwrap();
            file_handle_map.get_file(fh).unwrap()
        };

        self.fs.read(&file)
    }

    async fn write(&self, file_id: u64, fh: u64) -> Box<dyn FileWriter> {
        let file = {
            let file_handle_map = self.file_handle_manager.read().unwrap();
            file_handle_map.get_file(fh).unwrap()
        };

        self.fs.write(&file)
    }
}
 */
