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
use crate::cloud_storage_filesystem::CloudStorageFileSystem;
use crate::config::Config;
use crate::error::{ErrorCode, GravitinoError};
use crate::filesystem::{FileStat, FileSystemCapacity, OpenFileFlags, OpenedFile, Result};
use crate::filesystem::{FileSystemContext, PathFileSystem};
use crate::memory_filesystem::MemoryFileSystem;
use crate::utils::{GravitinoResult, StorageFileSystemType};
use async_trait::async_trait;
use opendal::layers::LoggingLayer;
use opendal::{services, Builder, Operator};

pub(crate) enum StorageFileSystem {
    MemoryStorge(MemoryFileSystem),
    CloudStorage(CloudStorageFileSystem),
}

impl StorageFileSystem {
    pub(crate) async fn new(
        fs_type: &StorageFileSystemType,
        config: &Config,
        context: &FileSystemContext,
    ) -> GravitinoResult<Self> {
        match fs_type {
            StorageFileSystemType::S3 => {
                let builder = services::S3::from_map(config.extent_config.clone());
                let op = Operator::new(builder)
                    .expect("opendal create failed")
                    .layer(LoggingLayer::default())
                    .finish();

                let fs = CloudStorageFileSystem::new(op);
                Ok(StorageFileSystem::CloudStorage(fs))
            }
            _ => Err(ErrorCode::UnSupportedFilesystem
                .to_error(format!("Unsupported filesystem type: {}", fs_type))),
        }
    }
}

macro_rules! async_call_fun {
    ($self:expr, $fun:ident $(, $args:expr)* ) => {
        match $self {
            StorageFileSystem::MemoryStorge(fs) => fs.$fun($($args),*).await,
            StorageFileSystem::CloudStorage(fs) => fs.$fun($($args),*).await,
        }
    };
}

macro_rules! call_fun {
    ($self:expr, $fun:ident $(, $args:expr)* ) => {
        match $self {
            StorageFileSystem::MemoryStorge(fs) => fs.$fun($($args),*),
            StorageFileSystem::CloudStorage(fs) => fs.$fun($($args),*),
        }
    };
}

#[async_trait]
impl PathFileSystem for StorageFileSystem {
    async fn init(&self) {
        async_call_fun!(self, init)
    }

    async fn stat(&self, name: &str) -> Result<FileStat> {
        async_call_fun!(self, stat, name)
    }

    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat> {
        async_call_fun!(self, lookup, parent, name)
    }

    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>> {
        async_call_fun!(self, read_dir, name)
    }

    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        async_call_fun!(self, open_file, name, flags)
    }

    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        async_call_fun!(self, open_dir, name, flags)
    }

    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile> {
        async_call_fun!(self, create_file, parent, name, flags)
    }

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        async_call_fun!(self, create_dir, parent, name)
    }

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()> {
        async_call_fun!(self, set_attr, name, file_stat, flush)
    }

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()> {
        async_call_fun!(self, remove_file, parent, name)
    }

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()> {
        async_call_fun!(self, remove_dir, parent, name)
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        call_fun!(self, get_capacity)
    }
}
