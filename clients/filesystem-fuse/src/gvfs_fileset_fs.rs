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
use crate::config::Config;
use crate::filesystem::{FileStat, FileSystemCapacity, FileSystemContext, PathFileSystem, Result};
use crate::gravitino_client::GravitinoClient;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use crate::storage_filesystem::StorageFileSystem;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

pub(crate) struct GravitinoFileSystemConfig {}

pub(crate) struct GvfsFilesetFs {
    fs: StorageFileSystem,
    client: GravitinoClient,
    fileset_location: PathBuf,
}

impl GvfsFilesetFs {
    pub async fn new(mount_from: &str, config: &Config, _context: &FileSystemContext) -> Self {
        todo!("GravitinoFileSystem::new")
    }

    fn map_to_raw_path(&self, path: &Path) -> PathBuf {
        if path == Path::new("/") {
            return self.fileset_location.clone();
        }
        self.fileset_location.join(path)
    }
}

#[async_trait]
impl PathFileSystem for GvfsFilesetFs {
    async fn init(&self) -> Result<()> {
        self.fs.init().await
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.stat(&raw_path).await
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.read_dir(&raw_path).await
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.open_file(&raw_path, flags).await
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.open_dir(&raw_path, flags).await
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.create_file(&raw_path, flags).await
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.create_dir(&raw_path).await
    }

    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.set_attr(&raw_path, file_stat, flush).await
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.remove_file(&raw_path).await
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        let raw_path = self.map_to_raw_path(path);
        self.fs.remove_dir(&raw_path).await
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        self.fs.get_capacity()
    }
}
