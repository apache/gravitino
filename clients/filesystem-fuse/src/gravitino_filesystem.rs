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
use crate::filesystem::{
    FileStat, FileSystemCapacity, FileSystemContext, OpenFileFlags, OpenedFile, PathFileSystem,
    Result,
};
use crate::gravitino_client::GravitinoClient;
use crate::storage_filesystem::StorageFileSystem;
use crate::utils::{extract_fileset, extract_storage_filesystem};
use async_trait::async_trait;

pub(crate) struct GravitinoFileSystemConfig {}

pub(crate) struct GravitinoFileSystem {
    fs: StorageFileSystem,
    client: GravitinoClient,
    fileset_location: String,
}

impl GravitinoFileSystem {
    pub async fn new(config: &Config, context: &FileSystemContext) -> Self {
        let client = GravitinoClient::new(&config.gravitino);
        let fileset_location = config.fuse.mount_from.clone();
        let (catalog, schema, fileset) = extract_fileset(&fileset_location).unwrap();
        let fileset = client
            .get_fileset(&catalog, &schema, &fileset)
            .await
            .unwrap();

        let (schema, location) = extract_storage_filesystem(&fileset.storage_location).unwrap();
        let fs = StorageFileSystem::new(&schema, &config, &context)
            .await
            .unwrap();

        Self {
            fs: fs,
            client: client,
            fileset_location: location,
        }
    }

    fn map_fileset_location(&self, name: &str) -> String {
        format!("{}/{}", self.fileset_location, name)
    }
}

#[async_trait]
impl PathFileSystem for GravitinoFileSystem {
    async fn init(&self) {
        self.fs.init().await;
    }

    async fn stat(&self, name: &str) -> Result<FileStat> {
        let name = self.map_fileset_location(name);
        self.fs.stat(&name).await
    }

    async fn lookup(&self, parent: &str, name: &str) -> Result<FileStat> {
        let parent = self.map_fileset_location(parent);
        self.fs.lookup(&parent, name).await
    }

    async fn read_dir(&self, name: &str) -> Result<Vec<FileStat>> {
        let name = self.map_fileset_location(name);
        self.fs.read_dir(&name).await
    }

    async fn open_file(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        let name = self.map_fileset_location(name);
        self.fs.open_file(&name, flags).await
    }

    async fn open_dir(&self, name: &str, flags: OpenFileFlags) -> Result<OpenedFile> {
        let name = self.map_fileset_location(name);
        self.fs.open_dir(&name, flags).await
    }

    async fn create_file(
        &self,
        parent: &str,
        name: &str,
        flags: OpenFileFlags,
    ) -> Result<OpenedFile> {
        let parent = self.map_fileset_location(parent);
        self.fs.create_file(&parent, name, flags).await
    }

    async fn create_dir(&self, parent: &str, name: &str) -> Result<OpenedFile> {
        let parent = self.map_fileset_location(parent);
        self.fs.create_dir(&parent, name).await
    }

    async fn set_attr(&self, name: &str, file_stat: &FileStat, flush: bool) -> Result<()> {
        let name = self.map_fileset_location(name);
        self.fs.set_attr(&name, file_stat, flush).await
    }

    async fn remove_file(&self, parent: &str, name: &str) -> Result<()> {
        let parent = self.map_fileset_location(parent);
        self.fs.remove_file(&parent, name).await
    }

    async fn remove_dir(&self, parent: &str, name: &str) -> Result<()> {
        let parent = self.map_fileset_location(parent);
        self.fs.remove_dir(&parent, name).await
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        self.fs.get_capacity()
    }
}
