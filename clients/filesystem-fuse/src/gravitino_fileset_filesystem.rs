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
use crate::filesystem::{FileStat, FileSystemCapacity, FileSystemContext, PathFileSystem, Result};
use crate::gravitino_client::GravitinoClient;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use async_trait::async_trait;
use fuse3::Errno;
use std::path::{Path, PathBuf};

/// GravitinoFileSystem is a filesystem that is associated with a fileset in Gravitino.
/// It mapping the fileset path to the original data storage path. and delegate the operation
/// to the inner filesystem like S3 GCS, JuiceFS.
pub(crate) struct GravitinoFilesetFileSystem {
    physical_fs: Box<dyn PathFileSystem>,
    client: GravitinoClient,
    fileset_location: PathBuf,
}

impl GravitinoFilesetFileSystem {
    pub async fn new(
        fs: Box<dyn PathFileSystem>,
        location: &Path,
        client: GravitinoClient,
        _config: &AppConfig,
        _context: &FileSystemContext,
    ) -> Self {
        Self {
            physical_fs: fs,
            client: client,
            fileset_location: location.into(),
        }
    }

    fn gvfs_path_to_raw_path(&self, path: &Path) -> PathBuf {
        self.fileset_location.join(path)
    }

    fn raw_path_to_gvfs_path(&self, path: &Path) -> Result<PathBuf> {
        path.strip_prefix(&self.fileset_location)
            .map_err(|_| Errno::from(libc::EBADF))?;
        Ok(path.into())
    }
}

#[async_trait]
impl PathFileSystem for GravitinoFilesetFileSystem {
    async fn init(&self) -> Result<()> {
        self.physical_fs.init().await
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        let mut file_stat = self.physical_fs.stat(&raw_path).await?;
        file_stat.path = self.raw_path_to_gvfs_path(&file_stat.path)?;
        Ok(file_stat)
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        let mut child_filestats = self.physical_fs.read_dir(&raw_path).await?;
        for file_stat in child_filestats.iter_mut() {
            file_stat.path = self.raw_path_to_gvfs_path(&file_stat.path)?;
        }
        Ok(child_filestats)
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        let mut opened_file = self.physical_fs.open_file(&raw_path, flags).await?;
        opened_file.file_stat.path = self.raw_path_to_gvfs_path(&opened_file.file_stat.path)?;
        Ok(opened_file)
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        let mut opened_file = self.physical_fs.open_dir(&raw_path, flags).await?;
        opened_file.file_stat.path = self.raw_path_to_gvfs_path(&opened_file.file_stat.path)?;
        Ok(opened_file)
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        let mut opened_file = self.physical_fs.create_file(&raw_path, flags).await?;
        opened_file.file_stat.path = self.raw_path_to_gvfs_path(&opened_file.file_stat.path)?;
        Ok(opened_file)
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        let mut file_stat = self.physical_fs.create_dir(&raw_path).await?;
        file_stat.path = self.raw_path_to_gvfs_path(&file_stat.path)?;
        Ok(file_stat)
    }

    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        self.physical_fs.set_attr(&raw_path, file_stat, flush).await
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        self.physical_fs.remove_file(&raw_path).await
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        let raw_path = self.gvfs_path_to_raw_path(path);
        self.physical_fs.remove_dir(&raw_path).await
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        self.physical_fs.get_capacity()
    }
}
