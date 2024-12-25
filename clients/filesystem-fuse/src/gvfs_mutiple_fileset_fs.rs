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
    FileStat, FileSystemCapacity, PathFileSystem, Result,
};
use crate::gravitino_client::GravitinoClient;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use crate::storage_filesystem::StorageFileSystem;
use async_trait::async_trait;
use dashmap::DashMap;
use std::path::Path;

pub(crate) struct GvfsMultiFilesetFs {
    // meta is the metadata of the filesystem
    client: GravitinoClient,
    inner_fs: DashMap<String, StorageFileSystem>,
}

impl GvfsMultiFilesetFs {
    pub fn new(client: GravitinoClient) -> Self {
        Self {
            client,
            inner_fs: Default::default(),
        }
    }
}

#[async_trait]
impl PathFileSystem for GvfsMultiFilesetFs {
    async fn init(&self) -> Result<()> {
        todo!()
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        todo!()
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        todo!()
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        todo!()
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        todo!()
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        todo!()
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        todo!()
    }

    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()> {
        todo!()
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        todo!()
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        todo!()
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        todo!()
    }
}