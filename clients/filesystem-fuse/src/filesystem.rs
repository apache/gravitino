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
use crate::config::{
    AppConfig, CONF_FILESYSTEM_BLOCK_SIZE, CONF_FUSE_DIR_MASK, CONF_FUSE_FILE_MASK,
};
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
pub(crate) const ROOT_DIR_NAME: &str = "";
pub(crate) const ROOT_DIR_PATH: &str = "/";
pub(crate) const INITIAL_FILE_ID: u64 = 10000;

// File system meta file is indicated the fuse filesystem is active.
pub(crate) const FS_META_FILE_PATH: &str = "/.gvfs_meta";
pub(crate) const FS_META_FILE_NAME: &str = ".gvfs_meta";
pub(crate) const FS_META_FILE_ID: u64 = 10;

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
    async fn get_file_path(&self, file_id: u64) -> Result<String>;

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

    /// flush the file with file id and file handle, if successful return Ok
    async fn flush_file(&self, file_id: u64, fh: u64) -> Result<()>;

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

    fn get_capacity(&self) -> Result<FileSystemCapacity>;
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
    pub(crate) fn new(uid: u32, gid: u32, config: &AppConfig) -> Self {
        FileSystemContext {
            uid,
            gid,
            default_file_perm: config.fuse.file_mask as u16,
            default_dir_perm: config.fuse.dir_mask as u16,
            block_size: config.filesystem.block_size,
        }
    }

    pub(crate) fn default() -> Self {
        FileSystemContext {
            uid: 0,
            gid: 0,
            default_file_perm: CONF_FUSE_FILE_MASK.default as u16,
            default_dir_perm: CONF_FUSE_DIR_MASK.default as u16,
            block_size: CONF_FILESYSTEM_BLOCK_SIZE.default,
        }
    }
}

// capacity of the file system
pub struct FileSystemCapacity {}

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
        // root directory name is ""
        let name = path.file_name().unwrap_or(OsStr::new(ROOT_DIR_NAME));
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
pub(crate) mod tests {
    use super::*;
    use libc::{O_CREAT, O_RDONLY, O_WRONLY};
    use std::collections::HashMap;
    use std::path::Component;

    pub(crate) struct TestPathFileSystem<F: PathFileSystem> {
        files: HashMap<PathBuf, FileStat>,
        fs: F,
        cwd: PathBuf,
    }

    impl<F: PathFileSystem> TestPathFileSystem<F> {
        pub(crate) fn new(cwd: &Path, fs: F) -> Self {
            Self {
                files: HashMap::new(),
                fs,
                cwd: cwd.into(),
            }
        }

        pub(crate) async fn test_path_file_system(&mut self) {
            // test root dir
            let resutl = self.fs.stat(Path::new("/")).await;
            assert!(resutl.is_ok());
            let root_file_stat = resutl.unwrap();
            self.assert_file_stat(&root_file_stat, Path::new("/"), Directory, 0);

            // test list root dir
            let result = self.fs.read_dir(Path::new("/")).await;
            assert!(result.is_ok());

            // Test create file
            self.test_create_file(&self.cwd.join("file1.txt")).await;

            // Test create dir
            self.test_create_dir(&self.cwd.join("dir1")).await;

            // Test list dir
            self.test_list_dir(&self.cwd).await;

            // Test remove file
            self.test_remove_file(&self.cwd.join("file1.txt")).await;

            // Test remove dir
            self.test_remove_dir(&self.cwd.join("dir1")).await;

            // Test file not found
            self.test_file_not_found(&self.cwd.join("unknown")).await;
        }

        async fn test_stat_file(&mut self, path: &Path, expect_kind: FileType, expect_size: u64) {
            let file_stat = self.fs.stat(path).await;
            assert!(file_stat.is_ok());
            let file_stat = file_stat.unwrap();
            self.assert_file_stat(&file_stat, path, expect_kind, expect_size);
            self.files.insert(file_stat.path.clone(), file_stat);
        }

        async fn test_create_file(&mut self, path: &Path) {
            let opened_file = self.fs.create_file(path, OpenFileFlags(0)).await;
            assert!(opened_file.is_ok());
            let file = opened_file.unwrap();
            self.assert_file_stat(&file.file_stat, path, RegularFile, 0);
            self.test_stat_file(path, RegularFile, 0).await;
        }

        async fn test_create_dir(&mut self, path: &Path) {
            let dir_stat = self.fs.create_dir(path).await;
            assert!(dir_stat.is_ok());
            let dir_stat = dir_stat.unwrap();
            self.assert_file_stat(&dir_stat, path, Directory, 0);
            self.test_stat_file(path, Directory, 0).await;
        }

        async fn test_list_dir(&self, path: &Path) {
            let list_dir = self.fs.read_dir(path).await;
            assert!(list_dir.is_ok());
            let list_dir = list_dir.unwrap();
            for file_stat in list_dir {
                assert!(self.files.contains_key(&file_stat.path));
                let actual_file_stat = self.files.get(&file_stat.path).unwrap();
                self.assert_file_stat(
                    &file_stat,
                    &actual_file_stat.path,
                    actual_file_stat.kind,
                    actual_file_stat.size,
                );
            }
        }

        async fn test_remove_file(&mut self, path: &Path) {
            let remove_file = self.fs.remove_file(path).await;
            assert!(remove_file.is_ok());
            self.files.remove(path);

            self.test_file_not_found(path).await;
        }

        async fn test_remove_dir(&mut self, path: &Path) {
            let remove_dir = self.fs.remove_dir(path).await;
            assert!(remove_dir.is_ok());
            self.files.remove(path);

            self.test_file_not_found(path).await;
        }

        async fn test_file_not_found(&self, path: &Path) {
            let not_found_file = self.fs.stat(path).await;
            assert!(not_found_file.is_err());
        }

        fn assert_file_stat(&self, file_stat: &FileStat, path: &Path, kind: FileType, size: u64) {
            assert_eq!(file_stat.path, path);
            assert_eq!(file_stat.kind, kind);
            assert_eq!(file_stat.size, size);
        }
    }

    pub(crate) struct TestRawFileSystem<F: RawFileSystem> {
        fs: F,
        files: HashMap<u64, FileStat>,
        cwd: PathBuf,
    }

    impl<F: RawFileSystem> TestRawFileSystem<F> {
        pub(crate) fn new(cwd: &Path, fs: F) -> Self {
            Self {
                fs,
                files: HashMap::new(),
                cwd: cwd.into(),
            }
        }

        pub(crate) async fn test_raw_file_system(&mut self) {
            // Test root dir
            self.test_root_dir().await;

            // test read root dir
            self.test_list_dir(ROOT_DIR_FILE_ID, false).await;

            // Test lookup meta file
            let file_id = self
                .test_lookup_file(ROOT_DIR_FILE_ID, ".gvfs_meta".as_ref(), RegularFile, 0)
                .await;

            // Test get meta file stat
            self.test_stat_file(file_id, Path::new("/.gvfs_meta"), RegularFile, 0)
                .await;

            // Test get file path
            self.test_get_file_path(file_id, "/.gvfs_meta").await;

            // get cwd file id
            let mut parent_file_id = ROOT_DIR_FILE_ID;
            for child in self.cwd.components() {
                if child == Component::RootDir {
                    continue;
                }
                let file_id = self.fs.create_dir(parent_file_id, child.as_os_str()).await;
                assert!(file_id.is_ok());
                parent_file_id = file_id.unwrap();
            }

            // Test create file
            let file_handle = self
                .test_create_file(
                    parent_file_id,
                    "file1.txt".as_ref(),
                    (O_CREAT | O_WRONLY) as u32,
                )
                .await;

            // Test write file
            self.test_write_file(&file_handle, "test").await;

            // Test close file
            self.test_close_file(&file_handle).await;

            // Test open file with read
            let file_handle = self
                .test_open_file(parent_file_id, "file1.txt".as_ref(), O_RDONLY as u32)
                .await;

            // Test read file
            self.test_read_file(&file_handle, "test").await;

            // Test close file
            self.test_close_file(&file_handle).await;

            // Test create dir
            self.test_create_dir(parent_file_id, "dir1".as_ref()).await;

            // Test list dir
            self.test_list_dir(parent_file_id, true).await;

            // Test remove file
            self.test_remove_file(parent_file_id, "file1.txt".as_ref())
                .await;

            // Test remove dir
            self.test_remove_dir(parent_file_id, "dir1".as_ref()).await;

            // Test list dir again
            self.test_list_dir(parent_file_id, true).await;

            // Test file not found
            self.test_file_not_found(23).await;
        }

        async fn test_root_dir(&self) {
            let root_file_stat = self.fs.stat(ROOT_DIR_FILE_ID).await;
            assert!(root_file_stat.is_ok());
            let root_file_stat = root_file_stat.unwrap();
            self.assert_file_stat(&root_file_stat, Path::new(ROOT_DIR_PATH), Directory, 0);
        }

        async fn test_lookup_file(
            &mut self,
            parent_file_id: u64,
            expect_name: &OsStr,
            expect_kind: FileType,
            expect_size: u64,
        ) -> u64 {
            let file_stat = self.fs.lookup(parent_file_id, expect_name).await;
            assert!(file_stat.is_ok());
            let file_stat = file_stat.unwrap();
            self.assert_file_stat(&file_stat, &file_stat.path, expect_kind, expect_size);
            assert_eq!(file_stat.name, expect_name);
            let file_id = file_stat.file_id;
            self.files.insert(file_stat.file_id, file_stat);
            file_id
        }

        async fn test_get_file_path(&mut self, file_id: u64, expect_path: &str) {
            let file_path = self.fs.get_file_path(file_id).await;
            assert!(file_path.is_ok());
            assert_eq!(file_path.unwrap(), expect_path);
        }

        async fn test_stat_file(
            &mut self,
            file_id: u64,
            expect_path: &Path,
            expect_kind: FileType,
            expect_size: u64,
        ) {
            let file_stat = self.fs.stat(file_id).await;
            assert!(file_stat.is_ok());
            let file_stat = file_stat.unwrap();
            self.assert_file_stat(&file_stat, expect_path, expect_kind, expect_size);
            self.files.insert(file_stat.file_id, file_stat);
        }

        async fn test_create_file(
            &mut self,
            root_file_id: u64,
            name: &OsStr,
            flags: u32,
        ) -> FileHandle {
            let file = self.fs.create_file(root_file_id, name, flags).await;
            assert!(file.is_ok());
            let file = file.unwrap();
            assert!(file.handle_id > 0);
            assert!(file.file_id >= INITIAL_FILE_ID);
            let file_stat = self.fs.stat(file.file_id).await;
            assert!(file_stat.is_ok());

            self.test_stat_file(file.file_id, &file_stat.unwrap().path, RegularFile, 0)
                .await;
            file
        }

        async fn test_open_file(&self, root_file_id: u64, name: &OsStr, flags: u32) -> FileHandle {
            let file = self.fs.lookup(root_file_id, name).await.unwrap();
            let file_handle = self.fs.open_file(file.file_id, flags).await;
            assert!(file_handle.is_ok());
            let file_handle = file_handle.unwrap();
            assert_eq!(file_handle.file_id, file.file_id);
            file_handle
        }

        async fn test_write_file(&mut self, file_handle: &FileHandle, content: &str) {
            let write_size = self
                .fs
                .write(
                    file_handle.file_id,
                    file_handle.handle_id,
                    0,
                    content.as_bytes(),
                )
                .await;

            assert!(write_size.is_ok());
            assert_eq!(write_size.unwrap(), content.len() as u32);

            let result = self
                .fs
                .flush_file(file_handle.file_id, file_handle.handle_id)
                .await;
            assert!(result.is_ok());

            self.files.get_mut(&file_handle.file_id).unwrap().size = content.len() as u64;
        }

        async fn test_read_file(&self, file_handle: &FileHandle, expected_content: &str) {
            let read_data = self
                .fs
                .read(
                    file_handle.file_id,
                    file_handle.handle_id,
                    0,
                    expected_content.len() as u32,
                )
                .await;
            assert!(read_data.is_ok());
            assert_eq!(read_data.unwrap(), expected_content.as_bytes());
        }

        async fn test_close_file(&self, file_handle: &FileHandle) {
            let close_file = self
                .fs
                .close_file(file_handle.file_id, file_handle.handle_id)
                .await;
            assert!(close_file.is_ok());
        }

        async fn test_create_dir(&mut self, parent_file_id: u64, name: &OsStr) {
            let dir = self.fs.create_dir(parent_file_id, name).await;
            assert!(dir.is_ok());
            let dir_file_id = dir.unwrap();
            assert!(dir_file_id >= INITIAL_FILE_ID);
            let dir_stat = self.fs.stat(dir_file_id).await;
            assert!(dir_stat.is_ok());

            self.test_stat_file(dir_file_id, &dir_stat.unwrap().path, Directory, 0)
                .await;
        }

        async fn test_list_dir(&self, root_file_id: u64, check_child: bool) {
            let list_dir = self.fs.read_dir(root_file_id).await;
            assert!(list_dir.is_ok());
            let list_dir = list_dir.unwrap();

            if !check_child {
                return;
            }
            for file_stat in list_dir {
                assert!(self.files.contains_key(&file_stat.file_id));
                let actual_file_stat = self.files.get(&file_stat.file_id).unwrap();
                self.assert_file_stat(
                    &file_stat,
                    &actual_file_stat.path,
                    actual_file_stat.kind,
                    actual_file_stat.size,
                );
            }
        }

        async fn test_remove_file(&mut self, root_file_id: u64, name: &OsStr) {
            let file_stat = self.fs.lookup(root_file_id, name).await;
            assert!(file_stat.is_ok());
            let file_stat = file_stat.unwrap();

            let remove_file = self.fs.remove_file(root_file_id, name).await;
            assert!(remove_file.is_ok());
            self.files.remove(&file_stat.file_id);

            self.test_file_not_found(file_stat.file_id).await;
        }

        async fn test_remove_dir(&mut self, root_file_id: u64, name: &OsStr) {
            let file_stat = self.fs.lookup(root_file_id, name).await;
            assert!(file_stat.is_ok());
            let file_stat = file_stat.unwrap();

            let remove_dir = self.fs.remove_dir(root_file_id, name).await;
            assert!(remove_dir.is_ok());
            self.files.remove(&file_stat.file_id);

            self.test_file_not_found(file_stat.file_id).await;
        }

        async fn test_file_not_found(&self, file_id: u64) {
            let not_found_file = self.fs.stat(file_id).await;
            assert!(not_found_file.is_err());
        }

        fn assert_file_stat(&self, file_stat: &FileStat, path: &Path, kind: FileType, size: u64) {
            assert_eq!(file_stat.path, path);
            assert_eq!(file_stat.kind, kind);
            assert_eq!(file_stat.size, size);
            if file_stat.file_id == ROOT_DIR_FILE_ID || file_stat.file_id == FS_META_FILE_ID {
                assert_eq!(file_stat.parent_file_id, 1);
            } else {
                assert!(file_stat.file_id >= INITIAL_FILE_ID);
                assert!(
                    file_stat.parent_file_id == 1 || file_stat.parent_file_id >= INITIAL_FILE_ID
                );
            }
        }
    }

    #[test]
    fn test_create_file_stat() {
        //test new file
        let file_stat = FileStat::new_file_filestat(Path::new("a"), "b".as_ref(), 10);
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 10);
        assert_eq!(file_stat.kind, RegularFile);

        //test new dir
        let file_stat = FileStat::new_dir_filestat("a".as_ref(), "b".as_ref());
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 0);
        assert_eq!(file_stat.kind, Directory);

        //test new file with path
        let file_stat = FileStat::new_file_filestat_with_path("a/b".as_ref(), 10);
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 10);
        assert_eq!(file_stat.kind, RegularFile);

        //test new dir with path
        let file_stat = FileStat::new_dir_filestat_with_path("a/b".as_ref());
        assert_eq!(file_stat.name, "b");
        assert_eq!(file_stat.path, Path::new("a/b"));
        assert_eq!(file_stat.size, 0);
        assert_eq!(file_stat.kind, Directory);
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
}
