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
use crate::filesystem_metadata::DefaultFileSystemMetadata;
use fuse3::{Errno, FileType, Timestamp};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::RwLock;
use std::time::SystemTime;

pub type Result<T> = std::result::Result<T, Errno>;

/// RawFileSystem interface for the file system implementation. it use by FuseApiHandle
/// the `file_id` and `parent_file_id` it is the unique identifier for the file system, it is used to identify the file or directory
/// the `fh` it is the file handle, it is used to identify the opened file, it is used to read or write the file content
pub trait RawFileSystem: Send + Sync {
    fn init(&self);

    fn get_file_path(&self, file_id: u64) -> String;

    fn get_opened_file(&self, file_id: u64, fh: u64) -> Result<OpenedFile>;

    fn stat(&self, file_id: u64) -> Result<FileStat>;

    fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat>;

    fn read_dir(&self, dir_file_id: u64) -> Result<Vec<FileStat>>;

    fn open_file(&self, file_id: u64) -> Result<OpenedFile>;

    fn create_file(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile>;

    fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile>;

    fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()>;

    fn update_file_status(&self, file_id: u64, file_stat: &FileStat) -> Result<()>;

    fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()>;

    fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()>;

    fn close_file(&self, file_id: u64, fh: u64) -> Result<()>;

    fn read(&self, file_id: u64, fh: u64) -> Box<dyn FileReader>;

    fn write(&self, file_id: u64, fh: u64) -> Box<dyn FileWriter>;
}

/// PathFileSystem is the interface for the file system implementation, it use to interact with other file system
/// it is used file name or path to operate the file system
pub trait PathFileSystem: Send + Sync {
    fn init(&self);

    fn stat(&self, name: &str) -> Result<FileStat>;

    fn lookup(&self, parent: &str, name: &str) -> Result<FileStat>;

    fn read_dir(&self, name: &str) -> Result<Vec<FileStat>>;

    fn create_file(&self, parent: &str, name: &str) -> Result<FileStat>;

    fn create_dir(&self, parent: &str, name: &str) -> Result<FileStat>;

    fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()>;

    fn read(&self, file: &OpenedFile) -> Box<dyn FileReader>;

    fn write(&self, file: &OpenedFile) -> Box<dyn FileWriter>;

    fn remove_file(&self, parent: &str, name: &str) -> Result<()>;

    fn remove_dir(&self, parent: &str, name: &str) -> Result<()>;
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
#[derive(Clone, Debug)]
pub(crate) struct OpenedFile {
    // file id
    pub(crate) file_id: u64,

    // file path (full name)
    pub(crate) path: String,

    // file handle id, open a same file multiple times will have different handle id
    pub(crate) handle_id: u64,

    // file size
    pub(crate) size: u64,
}

/// File reader interface  for read file content
pub(crate) trait FileReader {
    fn file(&self) -> &OpenedFile;
    fn read(&mut self, offset: u64, size: u32) -> Vec<u8>;
}

/// File writer interface  for write file content
pub(crate) trait FileWriter {
    fn file(&self) -> &OpenedFile;
    fn write(&mut self, offset: u64, data: &[u8]) -> u32;
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
pub struct SimpleFileSystem {
    file_id_manager: RwLock<FileIdManager>,
    file_handle_manager: RwLock<FileHandleManager>,

    inode_id_generator: AtomicU64,

    fs: Box<dyn PathFileSystem>,
}

impl SimpleFileSystem {
    pub fn new(fs: Box<dyn PathFileSystem>) -> Self {
        Self {
            file_id_manager: RwLock::new(FileIdManager::new()),
            file_handle_manager: RwLock::new(FileHandleManager::new()),
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
}

impl RawFileSystem for SimpleFileSystem {
    fn init(&self) {
        self.fs.init();

        let mut root_dir = FileStat::new_dir("", "");
        root_dir.inode = 1;
        root_dir.parent_inode = 0;
        self.file_id_manager
            .write()
            .unwrap()
            .insert(root_dir.inode, &root_dir.path);
        self.fs.set_attr(&root_dir.path, &root_dir, true).unwrap();

        self.create_file(root_dir.inode, DefaultFileSystemMetadata::FS_META_FILE_NAME);
    }

    fn get_file_path(&self, file_id: u64) -> String {
        self.get_file_path(file_id).unwrap_or("".to_string())
    }

    fn get_opened_file(&self, _file_id: u64, fh: u64) -> Result<OpenedFile> {
        let file_handle_map = self.file_handle_manager.read().unwrap();
        file_handle_map
            .get_file(fh)
            .ok_or(Errno::from(libc::ENOENT))
    }

    fn stat(&self, file_id: u64) -> Result<FileStat> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.stat(&file_path)
    }

    fn lookup(&self, parent_file_id: u64, name: &str) -> Result<FileStat> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        self.fs.lookup(&parent_file_path, name)
    }

    fn read_dir(&self, file_id: u64) -> Result<Vec<FileStat>> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.read_dir(&file_path)
    }

    fn open_file(&self, file_id: u64) -> Result<OpenedFile> {
        let file_path = self.get_file_path(file_id)?;
        let file_stat = self.fs.stat(&file_path)?;
        let file_handle = {
            let mut file_handle_map = self.file_handle_manager.write().unwrap();
            file_handle_map.create_file(&file_stat)
        };
        Ok(file_handle)
    }

    fn create_file(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        let mut file = self.fs.create_file(&parent_file_path, name)?;

        file.inode = self.next_inode_id();
        file.parent_inode = parent_file_id;

        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.insert(file.inode, &file.path);
        }

        self.fs.set_attr(&file.path, &file, false)?;

        let file_handle = {
            let mut file_handle_map = self.file_handle_manager.write().unwrap();
            file_handle_map.create_file(&file)
        };

        Ok(file_handle)
    }

    fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        let mut dir = self.fs.create_dir(&parent_file_path, name)?;

        dir.inode = self.next_inode_id();
        dir.parent_inode = parent_file_id;

        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.insert(dir.inode, &dir.path);
        }

        self.fs.set_attr(&dir.path, &dir, false)?;

        let file_handle = {
            let mut file_handle_map = self.file_handle_manager.write().unwrap();
            file_handle_map.create_file(&dir)
        };

        Ok(file_handle)
    }

    fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.set_attr(&file_path, file_stat, true)
    }

    fn update_file_status(&self, file_id: u64, file_stat: &FileStat) -> Result<()> {
        let file_path = self.get_file_path(file_id)?;
        self.fs.set_attr(&file_path, file_stat, false)
    }

    fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        self.fs.remove_file(&parent_file_path, name)?;

        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.remove(&join_file_path(&parent_file_path, name));
        }
        Ok(())
    }

    fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<()> {
        let parent_file_path = self.get_file_path(parent_file_id)?;
        self.fs.remove_dir(&parent_file_path, name)?;

        {
            let mut file_id_manager = self.file_id_manager.write().unwrap();
            file_id_manager.remove(&join_file_path(&parent_file_path, name));
        }
        Ok(())
    }

    fn close_file(&self, _file_id: u64, fh: u64) -> Result<()> {
        let mut file_handle_manager = self.file_handle_manager.write().unwrap();
        file_handle_manager.remove_file(fh);
        Ok(())
    }

    fn read(&self, file_id: u64, fh: u64) -> Box<dyn FileReader> {
        let file = {
            let file_handle_map = self.file_handle_manager.read().unwrap();
            file_handle_map.get_file(fh).unwrap()
        };

        self.fs.read(&file)
    }

    fn write(&self, file_id: u64, fh: u64) -> Box<dyn FileWriter> {
        let file = {
            let file_handle_map = self.file_handle_manager.read().unwrap();
            file_handle_map.get_file(fh).unwrap()
        };

        self.fs.write(&file)
    }
}

pub fn join_file_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent, name)
    }
}
