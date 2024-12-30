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
use crate::filesystem::{FileStat, FileSystemCapacity, FileSystemContext, PathFileSystem, Result};
use crate::gravitino_client::{Catalog, Fileset};
use crate::open_dal_filesystem::OpenDalFileSystem;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use crate::utils::{extract_bucket, GvfsResult};
use async_trait::async_trait;
use log::error;
use opendal::layers::LoggingLayer;
use opendal::services::S3;
use opendal::{Builder, Operator};
use std::path::Path;

pub(crate) struct S3FileSystem {
    open_dal_fs: OpenDalFileSystem,
}

impl S3FileSystem {}

impl S3FileSystem {
    pub(crate) fn new(
        catalog: &Catalog,
        fileset: &Fileset,
        config: &AppConfig,
        _fs_context: &FileSystemContext,
    ) -> GvfsResult<Self> {
        let mut opendal_config = config.extend_config.clone();

        let bucket = extract_bucket(&fileset.storage_location)?;
        opendal_config.insert("bucket".to_string(), bucket);

        let region = catalog.properties.get("s3-endpoint");
        if region.is_none() {
            return Err(OpenDalError.to_error("s3-endpoint is not found in catalog"));
        }
        let region = region.unwrap();
        opendal_config.insert("region".to_string(), region.to_string());

        let builder = S3::from_map(config.extend_config.clone());

        let op = Operator::new(builder);
        if let Err(e) = op {
            error!("opendal create failed: {:?}", e);
            return Err(OpenDalError.to_error(format!("opendal create failed: {:?}", e)));
        }
        let op = op.unwrap().layer(LoggingLayer::default()).finish();
        let open_dal_fs = OpenDalFileSystem::new(op, config, _fs_context);
        Ok(Self {
            open_dal_fs: open_dal_fs,
        })
    }
}

#[async_trait]
impl PathFileSystem for S3FileSystem {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        self.open_dal_fs.stat(path).await
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        self.open_dal_fs.read_dir(path).await
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        self.open_dal_fs.open_file(path, flags).await
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        self.open_dal_fs.open_dir(path, flags).await
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        self.open_dal_fs.create_file(path, flags).await
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        self.open_dal_fs.create_dir(path).await
    }

    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()> {
        self.open_dal_fs.set_attr(path, file_stat, flush).await
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        self.open_dal_fs.remove_file(path).await
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        self.open_dal_fs.remove_file(path).await
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        self.open_dal_fs.get_capacity()
    }
}

#[cfg(test)]
mod test {}
