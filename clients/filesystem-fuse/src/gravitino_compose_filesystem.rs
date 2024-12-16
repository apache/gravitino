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
use crate::gravitino_client::GravitinoClient;
use crate::storage_filesystem::StorageFileSystem;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;

pub(crate) struct GravitinoComposeFileSystem {
    // meta is the metadata of the filesystem
    client: GravitinoClient,
    inner_fs: DashMap<String, StorageFileSystem>,
}

impl GravitinoComposeFileSystem {
    pub fn new(client: GravitinoClient) -> Self {
        Self {
            client,
            inner_fs: Default::default(),
        }
    }
}

#[async_trait]
impl PathFileSystem for GravitinoComposeFileSystem {
    async fn init(&self) {
        todo!()
    }

    async fn stat(&self, name: &str) -> Result<FileStat> {
        todo!()
    }

    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat> {
        todo!()
    }

    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>> {
        todo!()
    }

    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        todo!()
    }

    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        todo!()
    }

    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile> {
        todo!()
    }

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        todo!()
    }

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()> {
        todo!()
    }

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()> {
        todo!()
    }

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()> {
        todo!()
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        todo!()
    }
}

struct FileReaderImpl {
    reader: opendal::Reader,
}

#[async_trait]
impl FileReader for FileReaderImpl {
    async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes> {
        todo!()
    }
}

struct FileWriterImpl {
    writer: opendal::Writer,
}

#[async_trait]
impl FileWriter for FileWriterImpl {
    async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        todo!()
    }

    async fn close(&mut self) -> Result<()> {
        todo!()
    }
}
