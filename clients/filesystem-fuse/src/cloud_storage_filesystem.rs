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
use crate::filesystem::{
    FileReader, FileStat, FileSystemCapacity, FileWriter, OpenFileFlags, OpenedFile,
    PathFileSystem, Result,
};
use crate::filesystem_metadata::DefaultFileSystemMetadata;
use crate::utils::join_file_path;
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::{Errno, FileType, Timestamp};
use futures_util::{TryFutureExt, TryStreamExt};
use log::debug;
use opendal::layers::LoggingLayer;
use opendal::{services, EntryMode, ErrorKind, Metadata, Operator};
use std::ops::Range;
use std::sync::{Mutex, RwLock};
use std::time::SystemTime;

pub(crate) struct CloudStorageFileSystem {
    op: Operator,
}

impl CloudStorageFileSystem {
    pub fn new(op: Operator) -> Self {
        Self { op: op }
    }
}

#[async_trait]
impl PathFileSystem for CloudStorageFileSystem {
    async fn init(&self) {}

    async fn stat(&self, name: &str) -> Result<FileStat> {
        let meta = self.op.stat(name).await.map_err(opendal_error_to_errno)?;
        let mut file_stat = FileStat::new_file_with_path(name, 0);
        opdal_meta_to_file_stat(&meta, &mut file_stat);
        Ok(file_stat)
    }

    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat> {
        let path = join_file_path(parent, name);
        self.stat(&path).await
    }

    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>> {
        let entries = self.op.list(name).await.map_err(opendal_error_to_errno)?;
        entries
            .iter()
            .map(|entry| {
                let path = entry.path().trim_end_matches('/');
                let mut file_stat = FileStat::new_file_with_path(&path, 0);
                opdal_meta_to_file_stat(entry.metadata(), &mut file_stat);
                debug!("read dir file stat: {:?}", file_stat);
                Ok(file_stat)
            })
            .collect()
    }

    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        let file_stat = self.stat(name).await?;
        debug_assert!(file_stat.kind == FileType::RegularFile);
        let mut file = OpenedFile::new(file_stat);
        if flags.is_read() {
            let reader = self
                .op
                .reader_with(name)
                .await
                .map_err(opendal_error_to_errno)?;
            file.reader = Some(Box::new(FileReaderImpl { reader }));
        }
        if flags.is_write() {
            let writer = self
                .op
                .writer_with(name)
                .await
                .map_err(opendal_error_to_errno)?;
            file.writer = Some(Box::new(FileWriterImpl { writer }));
        }
        Ok(file)
    }

    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        let file_stat = self.stat(name).await?;
        debug_assert!(file_stat.kind == FileType::Directory);
        let mut file = OpenedFile::new(file_stat);
        Ok(file)
    }

    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile> {
        let mut file = OpenedFile::new(FileStat::new_file_with_path(name, 0));

        if flags.is_read() {
            let reader = self
                .op
                .reader_with(name)
                .await
                .map_err(opendal_error_to_errno)?;
            file.reader = Some(Box::new(FileReaderImpl { reader }));
        }
        if flags.is_write() {
            let writer = self
                .op
                .writer_with(name)
                .await
                .map_err(opendal_error_to_errno)?;
            file.writer = Some(Box::new(FileWriterImpl { writer }));
        }
        Ok(file)
    }

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        let path = join_file_path(parent, name);
        self.op
            .create_dir(&path)
            .await
            .map_err(opendal_error_to_errno)?;
        let file_stat = self.stat(&path).await?;
        Ok(OpenedFile::new(file_stat))
    }

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()> {
        Ok(())
    }

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()> {
        self.op
            .remove(vec![join_file_path(parent, name)])
            .await
            .map_err(opendal_error_to_errno)
    }

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()> {
        //todo:: need to consider keeping the behavior of posix remove dir when the dir is not empty
        self.op
            .remove_all(&join_file_path(parent, name))
            .await
            .map_err(opendal_error_to_errno)
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        Ok(FileSystemCapacity {})
    }
}

struct FileReaderImpl {
    reader: opendal::Reader,
}

#[async_trait]
impl FileReader for FileReaderImpl {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes> {
        let end = offset + size as u64;
        let v = self
            .reader
            .read(offset..end)
            .await
            .map_err(opendal_error_to_errno)?;
        Ok(v.to_bytes())
    }
}

struct FileWriterImpl {
    writer: opendal::Writer,
}

#[async_trait]
impl FileWriter for FileWriterImpl {
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        self.writer
            .write(data.to_vec())
            .await
            .map_err(opendal_error_to_errno)?;
        Ok(data.len() as u32)
    }

    async fn close(&mut self) -> Result<()> {
        self.writer.close().await.map_err(opendal_error_to_errno)?;
        Ok(())
    }
}

fn opendal_error_to_errno(err: opendal::Error) -> fuse3::Errno {
    debug!("opendal_error2errno: {:?}", err);
    match err.kind() {
        ErrorKind::Unsupported => Errno::from(libc::EOPNOTSUPP),
        ErrorKind::IsADirectory => Errno::from(libc::EISDIR),
        ErrorKind::NotFound => Errno::from(libc::ENOENT),
        ErrorKind::PermissionDenied => Errno::from(libc::EACCES),
        ErrorKind::AlreadyExists => Errno::from(libc::EEXIST),
        ErrorKind::NotADirectory => Errno::from(libc::ENOTDIR),
        ErrorKind::RateLimited => Errno::from(libc::EBUSY),
        _ => Errno::from(libc::ENOENT),
    }
}

fn opendal_filemode_to_filetype(mode: EntryMode) -> FileType {
    match mode {
        EntryMode::DIR => FileType::Directory,
        _ => FileType::RegularFile,
    }
}

fn opdal_meta_to_file_stat(meta: &Metadata, file_stat: &mut FileStat) {
    let now = SystemTime::now();
    let mtime = meta.last_modified().map(|x| x.into()).unwrap_or(now);

    file_stat.size = meta.content_length();
    file_stat.kind = opendal_filemode_to_filetype(meta.mode());
    file_stat.ctime = Timestamp::from(mtime);
    file_stat.atime = Timestamp::from(now);
    file_stat.mtime = Timestamp::from(mtime);
}
