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
use crate::filesystem::{FileStat, FileSystemCapacity, Result};
use crate::filesystem::{FileSystemContext, PathFileSystem};
use crate::memory_filesystem::MemoryFileSystem;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use crate::utils::GvfsResult;
use async_trait::async_trait;
use std::fmt;
use std::path::Path;

pub enum StorageFileSystemType {
    S3,
}

impl fmt::Display for StorageFileSystemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageFileSystemType::S3 => write!(f, "s3"),
        }
    }
}

pub(crate) enum StorageFileSystem {
    MemoryStorage(MemoryFileSystem),
    //OpenDalStorage(OpenDalFileSystem),
    //NasStorage(NasFileSystem),
}

impl StorageFileSystem {
    pub(crate) async fn new(
        fs_type: &StorageFileSystemType,
        config: &Config,
        context: &FileSystemContext,
        root: &str,
    ) -> GvfsResult<Self> {
        todo!()
    }
}

macro_rules! async_call_fun {
    ($self:expr, $fun:ident $(, $args:expr)* ) => {
        match $self {
            StorageFileSystem::MemoryStorage(fs) => fs.$fun($($args),*).await,
        }
    };
}

macro_rules! call_fun {
    ($self:expr, $fun:ident $(, $args:expr)* ) => {
        match $self {
            StorageFileSystem::MemoryStorage(fs) => fs.$fun($($args),*),
        }
    };
}

#[async_trait]
impl PathFileSystem for StorageFileSystem {
    async fn init(&self) -> Result<()> {
        async_call_fun!(self, init)
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        async_call_fun!(self, stat, path)
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        async_call_fun!(self, read_dir, path)
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        async_call_fun!(self, open_file, path, flags)
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        async_call_fun!(self, open_dir, path, flags)
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        async_call_fun!(self, create_file, path, flags)
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        async_call_fun!(self, create_dir, path)
    }

    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()> {
        async_call_fun!(self, set_attr, path, file_stat, flush)
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        async_call_fun!(self, remove_file, path)
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        async_call_fun!(self, remove_dir, path)
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        call_fun!(self, get_capacity)
    }
}
