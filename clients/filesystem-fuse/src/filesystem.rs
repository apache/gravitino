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
use crate::opened_file::{FileHandle, OpenFileFlags, OpenedFile};
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType, Timestamp};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

pub(crate) type Result<T> = std::result::Result<T, Errno>;

pub(crate) const ROOT_DIR_PARENT_FILE_ID: u64 = 1;
pub(crate) const ROOT_DIR_FILE_ID: u64 = 1;
pub(crate) const ROOT_DIR_PATH: &str = "/";
pub(crate) const INITIAL_FILE_ID: u64 = 10000;

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
    async fn valid_file_handle_id(&self, file_id: u64, fh: u64) -> Result<()>;

    /// Get the file stat by file id. if the file id is valid, return the file stat
    async fn stat(&self, file_id: u64) -> Result<FileStat>;

    /// Lookup the file by parent file id and file name, if the file exists, return the file stat
    async fn lookup(&self, parent_file_id: u64, name: &OsStr) -> Result<FileStat>;

    /// Read the directory by file id, if the file id is a valid directory, return the file stat list
    async fn read_dir(&self, dir_file_id: u64) -> Result<Vec<FileStat>>;

    /// Open the file by file id and flags, if the file id is a valid file, return the file handle
    async fn open_file(&self, file_id: u64, flags: u32) -> Result<FileHandle>;

    /// Open the directory by file id and flags, if successful, return the file handle
    async fn open_dir(&self, file_id: u64, flags: u32) -> Result<FileHandle>;

    /// Create the file by parent file id and file name and flags, if successful, return the file handle
    async fn create_file(
        &self,
        parent_file_id: u64,
        name: &OsStr,
        flags: u32,
    ) -> Result<FileHandle>;

    /// Create the directory by parent file id and file name, if successful, return the file id
    async fn create_dir(&self, parent_file_id: u64, name: &OsStr) -> Result<u64>;

    /// Set the file attribute by file id and file stat
    async fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<()>;

    /// Remove the file by parent file id and file name
    async fn remove_file(&self, parent_file_id: u64, name: &OsStr) -> Result<()>;

    /// Remove the directory by parent file id and file name
    async fn remove_dir(&self, parent_file_id: u64, name: &OsStr) -> Result<()>;

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

    /// Get the file stat by file path, if the file exists, return the file stat
    async fn stat(&self, path: &Path) -> Result<FileStat>;

    /// Read the directory by file path, if the directory exists, return the file stat list
    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>>;

    /// Open the file by file path and flags, if the file exists, return the opened file
    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile>;

    /// Open the directory by file path and flags, if the file exists, return the opened file
    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile>;

    /// Create the file by file path and flags, if successful, return the opened file
    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile>;

    /// Create the directory by file path , if successful, return the file stat
    async fn create_dir(&self, path: &Path) -> Result<FileStat>;

    /// Set the file attribute by file path and file stat
    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()>;

    /// Remove the file by file path
    async fn remove_file(&self, path: &Path) -> Result<()>;

    /// Remove the directory by file path
    async fn remove_dir(&self, path: &Path) -> Result<()>;
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
    pub(crate) name: OsString,

    // file path of the fuse file system root
    pub(crate) path: PathBuf,

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
    pub fn new_file_filestat_with_path(path: &Path, size: u64) -> Self {
        Self::new_filestat(path, size, RegularFile)
    }

    pub fn new_dir_filestat_with_path(path: &Path) -> Self {
        Self::new_filestat(path, 0, Directory)
    }

    pub fn new_file_filestat(parent: &Path, name: &OsStr, size: u64) -> Self {
        let path = parent.join(name);
        Self::new_filestat(&path, size, RegularFile)
    }

    pub fn new_dir_filestat(parent: &Path, name: &OsStr) -> Self {
        let path = parent.join(name);
        Self::new_filestat(&path, 0, Directory)
    }

    pub fn new_filestat(path: &Path, size: u64, kind: FileType) -> Self {
        let atime = Timestamp::from(SystemTime::now());
        let name = path.file_name().unwrap_or(OsStr::new(""));
        Self {
            file_id: 0,
            parent_file_id: 0,
            name: name.to_os_string(),
            path: path.into(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::default_raw_filesystem::DefaultRawFileSystem;
    use crate::memory_filesystem::MemoryFileSystem;
    use std::collections::HashMap;

    #[test]
    fn test_create_file_stat() {
        //test new file
        let file_stat = FileStat::new_file_filestat(Path::new("a"), "b".as_ref(), 10);
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 10);
        assert_eq!(file_stat.kind, FileType::RegularFile);

        //test new dir
        let file_stat = FileStat::new_dir_filestat("a".as_ref(), "b".as_ref());
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 0);
        assert_eq!(file_stat.kind, FileType::Directory);

        //test new file with path
        let file_stat = FileStat::new_file_filestat_with_path("a/b".as_ref(), 10);
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 10);
        assert_eq!(file_stat.kind, FileType::RegularFile);

        //test new dir with path
        let file_stat = FileStat::new_dir_filestat_with_path("a/b".as_ref());
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 0);
        assert_eq!(file_stat.kind, FileType::Directory);
    }

    #[test]
    fn test_file_stat_set_file_id() {
        let mut file_stat = FileStat::new_file_filestat("a".as_ref(), "b".as_ref(), 10);
        file_stat.set_file_id(1, 2);
        assert_eq!(file_stat.file_id, 2);
        assert_eq!(file_stat.parent_file_id, 1);
    }

    #[test]
    #[should_panic(expected = "assertion failed: file_id != 0 && parent_file_id != 0")]
    fn test_file_stat_set_file_id_panic() {
        let mut file_stat = FileStat::new_file_filestat("a".as_ref(), "b".as_ref(), 10);
        file_stat.set_file_id(1, 0);
    }

    #[tokio::test]
    async fn test_memory_file_system() {
        let fs = MemoryFileSystem::new().await;
        let _ = fs.init().await;
        test_path_file_system(&fs).await;
    }

    async fn test_path_file_system(fs: &impl PathFileSystem) {
        let mut root_dir_child_file_stats = HashMap::new();

        // test root file
        let root_dir_path = Path::new("/");
        let root_file_stat = fs.stat(root_dir_path).await;
        assert!(root_file_stat.is_ok());
        let root_file_stat = root_file_stat.unwrap();
        assert_file_stat(&root_file_stat, root_dir_path, Directory, 0);

        // test meta file
        let meta_file_path = Path::new("/.gvfs_meta");
        let meta_file_stat = fs.stat(meta_file_path).await;
        assert!(meta_file_stat.is_ok());
        let meta_file_stat = meta_file_stat.unwrap();
        assert_file_stat(&meta_file_stat, meta_file_path, FileType::RegularFile, 0);
        root_dir_child_file_stats.insert(meta_file_stat.path.clone(), meta_file_stat);

        // test create file
        let file_path = Path::new("/file1.txt");
        let opened_file = fs.create_file(file_path, OpenFileFlags(0)).await;
        assert!(opened_file.is_ok());
        let file = opened_file.unwrap();
        assert_file_stat(&file.file_stat, file_path, FileType::RegularFile, 0);
        root_dir_child_file_stats.insert(file.file_stat.path.clone(), file.file_stat.clone());

        // test create dir
        let dir_path = Path::new("/dir1");
        let dir_stat = fs.create_dir(dir_path).await;
        assert!(dir_stat.is_ok());
        let dir_stat = dir_stat.unwrap();
        assert_file_stat(&dir_stat, dir_path, Directory, 0);
        root_dir_child_file_stats.insert(dir_stat.path.clone(), dir_stat);

        // test list dir
        let list_dir = fs.read_dir(Path::new("/")).await;
        assert!(list_dir.is_ok());
        let list_dir = list_dir.unwrap();
        assert_eq!(list_dir.len(), root_dir_child_file_stats.len());
        for file_stat in list_dir {
            assert!(root_dir_child_file_stats.contains_key(&file_stat.path));
            let actual_file_stat = root_dir_child_file_stats.get(&file_stat.path).unwrap();
            assert_file_stat(
                &file_stat,
                &actual_file_stat.path,
                actual_file_stat.kind,
                actual_file_stat.size,
            );
        }

        // test remove file
        let remove_file = fs.remove_file(file_path).await;
        assert!(remove_file.is_ok());
        root_dir_child_file_stats.remove(file_path);

        // test remove dir
        let remove_dir = fs.remove_dir(dir_path).await;
        assert!(remove_dir.is_ok());
        root_dir_child_file_stats.remove(dir_path);

        // test list dir
        let list_dir = fs.read_dir(Path::new("/")).await;
        assert!(list_dir.is_ok());

        let list_dir = list_dir.unwrap();
        assert_eq!(list_dir.len(), root_dir_child_file_stats.len());
        for file_stat in list_dir {
            assert!(root_dir_child_file_stats.contains_key(&file_stat.path));
            let actual_file_stat = root_dir_child_file_stats.get(&file_stat.path).unwrap();
            assert_file_stat(
                &file_stat,
                &actual_file_stat.path,
                actual_file_stat.kind,
                actual_file_stat.size,
            );
        }

        // test file not found
        let not_found_file = fs.stat(Path::new("/not_found.txt")).await;
        assert!(not_found_file.is_err());
    }

    fn assert_file_stat(file_stat: &FileStat, path: &Path, kind: FileType, size: u64) {
        assert_eq!(file_stat.path, path);
        assert_eq!(file_stat.kind, kind);
        assert_eq!(file_stat.size, size);
    }

    #[tokio::test]
    async fn test_default_raw_file_system() {
        let memory_fs = MemoryFileSystem::new().await;
        let raw_fs = DefaultRawFileSystem::new(memory_fs);
        let _ = raw_fs.init().await;
        test_raw_file_system(&raw_fs).await;
    }

    async fn test_raw_file_system(fs: &impl RawFileSystem) {
        let _ = fs.init().await;
    }
}
