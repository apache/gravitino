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
use fuse3::{Errno, FileType, Timestamp};
use std::time::SystemTime;

/// File system interface for the file system implementation. it use by FuseApiHandle
/// the `file_id` and `parent_file_id` it is the unique identifier for the file system, it is used to identify the file or directory
/// the `fh` it is the file handle, it is used to identify the opened file, it is used to read or write the file content
pub trait IFileSystem: Send + Sync {

    fn get_file_path(&self, file_id: u64) -> String;

    fn get_opened_file(&self, file_id: u64, fh: u64) -> Option<OpenedFile>;

    fn stat(&self, file_id: u64) -> Option<FileStat>;

    fn lookup(&self, parent_file_id: u64, name :&str) -> Option<FileStat>;

    fn read_dir(&self, dir_file_id: u64) -> Vec<FileStat>;

    fn open_file(&self, file_id: u64) -> Result<OpenedFile, Errno>;

    fn create_file(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile, Errno>;

    fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile, Errno>;

    fn set_attr(&self, file_id: u64, file_stat: &FileStat) -> Result<(), Errno>;

    fn update_file_status(&self, file_id: u64, file_stat: &FileStat);

    fn read(&self, file_id: u64, fh: u64) -> Box<dyn FileReader>;

    fn write(&self, file_id: u64, fh: u64) -> Box<dyn FileWriter>;

    fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<(), Errno>;

    fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<(), Errno>;

    fn close_file(&self, file_id: u64, fh: u64) -> Result<(), Errno>;
}

pub struct FileSystemContext {
    // system user id
    pub(crate) uid: u32,

    // system group id
    pub(crate) gid: u32,
}

impl FileSystemContext {
    pub(crate) fn new(uid: u32, gid: u32) -> Self {
        FileSystemContext {
            uid,
            gid,
        }
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
    pub fn new_file(name: &str, inode: u64, parent_inode: u64) -> Self {
        let atime = Timestamp::from(SystemTime::now());
        Self {
            inode: inode,
            parent_inode: parent_inode,
            name: name.into(),
            path: "".to_string(),
            size: 0,
            kind : FileType::RegularFile,
            perm : 0o664,
            atime: atime,
            mtime : atime,
            ctime: atime,
            nlink : 1,
        }
    }

    pub fn new_dir(name: &str, inode: u64, parent_inode: u64) -> Self {
        let atime = Timestamp::from(SystemTime::now());
        Self {
            inode: inode,
            parent_inode: parent_inode,
            name: name.into(),
            path: "".to_string(),
            size: 0,
            kind : FileType::Directory,
            perm : 0o755,
            atime: atime,
            mtime : atime,
            ctime: atime,
            nlink : 1,
        }
    }
}

/// Opened file for read or write, it is used to read or write the file content.
#[derive(Clone, Debug)]
pub(crate) struct OpenedFile {
    // file id
    pub(crate) file_id: u64,

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