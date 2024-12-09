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
use crate::filesystem_metadata::DefaultFileSystemMetadata;
use async_trait::async_trait;
use fuse3::{Errno, FileType, Timestamp};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, RwLock};
use std::time::SystemTime;
use bytes::Bytes;
use crate::utils::join_file_path;

pub type Result<T> = std::result::Result<T, Errno>;

pub struct FileHandle {
    pub(crate) file_id : u64,
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

    async fn create_file(&self, parent_file_id: u64, name: &str, flags: u32) -> Result<FileHandle>;

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<FileHandle>;

    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()>;

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()>;

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()>;

    async fn close_file(&self, file_id: u64, fh: u64) -> Result<()>;

    async fn read(&self, file_id: u64, fh: u64, offset: u64, size : u32) -> Result<Bytes>;

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

    async fn open_file(&self, name: &str, flags : u32) -> Result<OpenedFile>;

    async fn create_file(&self, parent: &str, name: &str) -> Result<OpenedFile>;

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile>;

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()>;

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()>;

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()>;
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

#[derive(Clone, Debug)]
pub(crate) struct FileStat {
    // inode id for the file system, also call file id
    pub(crate) inode: u64,

    // parent inode id
    pub(crate) parent_inode: u64,

    // file name
    pub(crate) name: String,

    // file path of the fuse file system root
    pub(crate) path: String,

    // file size
    pub(crate) size: u64,

    // file type like regular file or directory and so on
    pub(crate) kind: FileType,

    // file permission
    pub(crate) perm: u16,

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
    // TODO need to handle the file permission by config
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
        }
    }
}

/// Opened file for read or write, it is used to read or write the file content.
pub(crate) struct OpenedFile {
    pub(crate) file_stat: FileStat,
    pub(crate) handle_id: u64,

    pub(crate) reader : Option<Box<dyn FileReader>>,
    pub(crate) writer : Option<Box<dyn FileWriter>>,
}

impl OpenedFile {
    pub(crate) fn new(file_stat: FileStat) -> Self {
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

        if end > self.file_stat.size  {
            self.file_stat.size = end;
        }
        self.file_stat.atime = Timestamp::from(SystemTime::now());
        self.file_stat.mtime = self.file_stat.atime;

        self.writer.as_mut().unwrap().write(offset, data).await
    }

    fn file_handle(&self) -> FileHandle {
        FileHandle {
            file_id: self.file_stat.inode,
            handle_id: self.handle_id,
        }
    }

    pub(crate) fn set_inode(&mut self, parent_file_id: u64, file_id: u64) {
        self.file_stat.parent_inode = parent_file_id;
        self.file_stat.inode= file_id;
    }
}

/// File reader interface  for read file content

#[async_trait]
pub(crate) trait FileReader: Sync + Send {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes>;
}

/// File writer interface  for write file content
#[async_trait]
pub(crate) trait FileWriter: Sync + Send {
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32>;
}

/// FileIdManager is a manager for file id and file name mapping.
struct FileIdManager {
    // file_id_map is a map of file_id to file name.
    file_id_map: HashMap<u64, String>,

    // file_name_map is a map of file name to file id.
    file_name_map: HashMap<String, u64>,
}

impl FileIdManager {
    fn new() -> Self {
        Self {
            file_id_map: HashMap::new(),
            file_name_map: HashMap::new(),
        }
    }

    fn get_file_name(&self, file_id: u64) -> Option<String> {
        self.file_id_map.get(&file_id).map(|x| x.clone())
    }

    fn get_file_id(&self, file_name: &str) -> Option<u64> {
        self.file_name_map.get(file_name).map(|x| *x)
    }

    fn insert(&mut self, file_id: u64, file_name: &str) {
        self.file_id_map.insert(file_id, file_name.to_string());
        self.file_name_map.insert(file_name.to_string(), file_id);
    }

    fn remove(&mut self, file_name: &str) {
        if let Some(file_id) = self.file_name_map.remove(file_name) {
            self.file_id_map.remove(&file_id);
        }
    }
}

// SimpleFileSystem is a simple file system implementation for the file system.
// it is used to manage the file system metadata and file handle.
// The operations of the file system are implemented by the PathFileSystem.
pub struct SimpleFileSystem<T: PathFileSystem> {
    file_id_manager: RwLock<FileIdManager>,
    opened_file_manager: OpenedFileManager,

    inode_id_generator: AtomicU64,

    fs: T,
}

impl<T: PathFileSystem> SimpleFileSystem<T> {
    pub(crate) fn new(fs: T) -> Self {
        Self {
            file_id_manager: RwLock::new(FileIdManager::new()),
            opened_file_manager: OpenedFileManager::new(),
            inode_id_generator: AtomicU64::new(10000),
            fs,
        }
    }

    fn next_inode_id(&self) -> u64 {
        self.inode_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn get_file_path(&self, file_id: u64) -> Result<String> {
        self.file_id_manager
            .read()
            .unwrap()
            .get_file_name(file_id)
            .ok_or(Errno::from(libc::ENOENT))
    }


    async fn create_file_internal(&self, parent_file_id: u64, name: &str, kind: FileType) -> Result<FileHandle> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        let mut file = match kind {
            FileType::Directory => self.fs.create_dir(&parent_file_path, name).await?,
            FileType::RegularFile => self.fs.create_file(&parent_file_path, name).await?,
            _ => return Err(Errno::from(libc::EINVAL)),
        };

        file.set_inode(parent_file_id, self.next_inode_id());
        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.insert(file.file_stat.inode, &file.file_stat.path);
        }
        self.fs.set_attr(&file.file_stat.path, &file.file_stat, false).await?;
        let file= self.opened_file_manager.put_file(file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }
}

#[async_trait]
impl<T: PathFileSystem> RawFileSystem for SimpleFileSystem<T> {
    async fn init(&self) {
        self.fs.init();

        let mut root_dir = FileStat::new_dir("", "");
        root_dir.inode = 1;
        root_dir.parent_inode = 0;
        self.file_id_manager
            .write()
            .unwrap()
            .insert(root_dir.inode, &root_dir.path);
        self.fs
            .set_attr(&root_dir.path, &root_dir, true)
            .await
            .unwrap();

        // todo meta file does not need to be created in the original file system
        self.create_file(root_dir.inode, DefaultFileSystemMetadata::FS_META_FILE_NAME, 0)
            .await.unwrap();
    }

    async fn get_file_path(&self, file_id: u64) -> String {
        self.get_file_path(file_id).unwrap_or("".to_string())
    }

    async fn valid_file_id(&self, _file_id: u64, fh: u64) -> Result<()> {
        let file_id = self.opened_file_manager
            .get_file(fh).ok_or(Errno::from(libc::EBADF))?
            .lock().await
            .file_stat.inode;

        (file_id == _file_id).then(|| ()).ok_or(Errno::from(libc::EBADF))
    }

    async fn stat(&self, file_id: u64) -> Result<FileStat> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.stat(&file_path).await
    }

    async fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        self.fs.lookup(&parent_file_path, name).await
    }

    async fn read_dir(&self, file_id: u64) -> Result<Vec<FileStat>> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.read_dir(&file_path).await
    }

    async fn open_file(&self, file_id: u64, flags: u32) -> Result<FileHandle> {
        let file_path = self.get_file_path(file_id)?;
        let file = self.fs.open_file(&file_path, flags).await?;
        let file = self.opened_file_manager.put_file(file);
        let file = file.lock().await;
        Ok(file.file_handle())
    }

    async fn create_file(&self, parent_file_id: u64, name: &str, flags: u32) -> Result<FileHandle> {
        self.create_file_internal(parent_file_id, name, FileType::RegularFile).await
    }

    async fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<FileHandle> {
        self.create_file_internal(parent_file_id, name, FileType::Directory).await
    }


    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.set_attr(&file_path, file_stat, true).await
    }

    async fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        self.fs.remove_file(&parent_file_path, name).await?;

        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.remove(&join_file_path(&parent_file_path, name));
        }
        Ok(())
    }

    async fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        self.fs.remove_dir(&parent_file_path, name).await?;

        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.remove(&join_file_path(&parent_file_path, name));
        }
        Ok(())
    }

    async fn close_file(&self, _file_id: u64, fh: u64) -> Result<()> {
        self.opened_file_manager.remove_file(fh);
        Ok(())
    }

    async fn read(&self, file_id: u64, fh: u64, offset: u64, size: u32) -> Result<Bytes> {
        let mut file_stat: FileStat;
        let data = {
            let mut opened_file = self.opened_file_manager.get_file(fh).ok_or(Errno::from(libc::EBADF))?;
            let mut opened_file =  opened_file.lock().await;
            file_stat = opened_file.file_stat.clone();
            opened_file.read(offset, size).await
        };

        self.fs.set_attr(&file_stat.path, &file_stat,false).await?;

        data
    }

    async fn write(&self, file_id: u64, fh: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let (len, file_stat) = {
            let opened_file = self.opened_file_manager.get_file(fh).ok_or(Errno::from(libc::EBADF))?;
            let mut opened_file = opened_file.lock().await;
            let len = opened_file.write(offset, data).await;
            (len, opened_file.file_stat.clone())
        };

        self.fs.set_attr(&file_stat.path, &file_stat, false).await?;

        len
    }
}

