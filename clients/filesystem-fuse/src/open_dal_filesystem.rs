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
use crate::config::AppConfig;
use crate::error::ErrorCode::OpenDalError;
use crate::filesystem::{
    FileReader, FileStat, FileSystemCapacity, FileSystemContext, FileWriter, PathFileSystem, Result,
};
use crate::gvfs_fuse::FileSystemSchema;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use crate::utils::GvfsResult;
use async_trait::async_trait;
use bytes::Bytes;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType, Timestamp};
use log::{debug, error};
use opendal::layers::LoggingLayer;
use opendal::services::S3;
use opendal::{Builder, EntryMode, ErrorKind, Metadata, Operator};
use std::path::Path;
use std::time::SystemTime;

pub(crate) struct OpenDalFileSystem {
    op: Operator,
}

impl OpenDalFileSystem {}

impl OpenDalFileSystem {
    fn new(op: Operator, _config: &AppConfig, _fs_context: &FileSystemContext) -> Self {
        Self { op: op }
    }

    pub(crate) fn create_file_system(
        schema: &FileSystemSchema,
        config: &AppConfig,
        fs_context: &FileSystemContext,
    ) -> GvfsResult<Box<dyn PathFileSystem>> {
        match schema {
            FileSystemSchema::S3 => {
                let builder = S3::from_map(config.extent_config.clone());

                let op = Operator::new(builder);
                if let Err(e) = op {
                    error!("opendal create failed: {:?}", e);
                    return Err(OpenDalError.to_error(format!("opendal create failed: {:?}", e)));
                }
                let op = op.unwrap().layer(LoggingLayer::default()).finish();
                Ok(Box::new(OpenDalFileSystem::new(op, config, fs_context)))
            }
        }
    }

    fn opendal_meta_to_file_stat(&self, meta: &Metadata, file_stat: &mut FileStat) {
        let now = SystemTime::now();
        let mtime = meta.last_modified().map(|x| x.into()).unwrap_or(now);

        file_stat.size = meta.content_length();
        file_stat.kind = opendal_filemode_to_filetype(meta.mode());
        file_stat.ctime = Timestamp::from(mtime);
        file_stat.atime = Timestamp::from(now);
        file_stat.mtime = Timestamp::from(mtime);
    }
}

#[async_trait]
impl PathFileSystem for OpenDalFileSystem {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        let file_name = path.to_string_lossy().to_string();
        let meta = self
            .op
            .stat_with(&file_name)
            .await
            .map_err(opendal_error_to_errno)?;

        let mut file_stat = FileStat::new_file_filestat_with_path(path, 0);
        self.opendal_meta_to_file_stat(&meta, &mut file_stat);
        Ok(file_stat)
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        let file_name = path.to_string_lossy().to_string();
        let entries = self
            .op
            .list(&file_name)
            .await
            .map_err(opendal_error_to_errno)?;
        entries
            .iter()
            .map(|entry| {
                let path = Path::new(entry.path());
                let mut file_stat = FileStat::new_file_filestat_with_path(path, 0);
                self.opendal_meta_to_file_stat(entry.metadata(), &mut file_stat);
                debug!("read dir file stat: {:?}", file_stat);
                Ok(file_stat)
            })
            .collect()
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let file_stat = self.stat(path).await?;
        debug_assert!(file_stat.kind == RegularFile);

        let mut file = OpenedFile::new(file_stat);
        let file_name = path.to_string_lossy().to_string();
        if flags.is_read() {
            let reader = self
                .op
                .reader_with(&file_name)
                .await
                .map_err(opendal_error_to_errno)?;
            file.reader = Some(Box::new(FileReaderImpl { reader }));
        }
        if flags.is_write() {
            let writer = self
                .op
                .writer_with(&file_name)
                .await
                .map_err(opendal_error_to_errno)?;
            file.writer = Some(Box::new(FileWriterImpl { writer }));
        }
        Ok(file)
    }

    async fn open_dir(&self, path: &Path, _flags: OpenFileFlags) -> Result<OpenedFile> {
        let file_stat = self.stat(path).await?;
        debug_assert!(file_stat.kind == Directory);

        let opened_file = OpenedFile::new(file_stat);
        Ok(opened_file)
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        self.open_file(path, flags).await
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        let file_name = path.to_string_lossy().to_string();
        self.op
            .create_dir(&file_name)
            .await
            .map_err(opendal_error_to_errno)?;
        let file_stat = self.stat(path).await?;
        Ok(file_stat)
    }

    async fn set_attr(&self, _path: &Path, _file_stat: &FileStat, _flush: bool) -> Result<()> {
        // no need to implement
        Ok(())
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        let file_name = path.to_string_lossy().to_string();
        self.op
            .remove(vec![file_name])
            .await
            .map_err(opendal_error_to_errno)
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        //todo:: need to consider keeping the behavior of posix remove dir when the dir is not empty
        self.remove_file(path).await
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
    async fn write(&mut self, _offset: u64, data: &[u8]) -> Result<u32> {
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

fn opendal_error_to_errno(err: opendal::Error) -> Errno {
    error!("opendal operator error {:?}", err);
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
        EntryMode::DIR => Directory,
        _ => RegularFile,
    }
}

#[cfg(test)]
mod test {
    use opendal::layers::LoggingLayer;
    use opendal::{services, Operator};

    async fn test_s3_stat() {
        let mut builder = services::S3::default();
        builder
            .access_key_id("<Your AWS Access Key>") // Replace with your AWS access key
            .secret_access_key("<Your AWS Secret Key>") // Replace with your AWS secret key
            .region("<Your AWS region") // Replace with your S3 bucket region
            .bucket("<Your S3 bucket name>"); // Replace with your S3 bucket name

        // Init an operator
        let op = Operator::new(builder)
            .expect("opendal create failed")
            .layer(LoggingLayer::default())
            .finish();

        let meta = op.stat_with("s1/fileset1/");
        println!("{:?}", meta.await);
    }
}
