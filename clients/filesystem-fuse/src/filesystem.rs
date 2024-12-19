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
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::{Errno, FileType, Timestamp};

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
#[derive(Debug)]
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

/// Opened file for read or write, it is used to read or write the file content.
pub(crate) struct OpenedFile {
    pub(crate) file_stat: FileStat,

    pub(crate) handle_id: u64,

    pub reader: Option<Box<dyn FileReader>>,

    pub writer: Option<Box<dyn FileWriter>>,
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
