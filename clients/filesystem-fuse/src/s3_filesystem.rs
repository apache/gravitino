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
use crate::error::ErrorCode::{InvalidConfig, OpenDalError};
use crate::filesystem::{FileStat, FileSystemCapacity, FileSystemContext, PathFileSystem, Result};
use crate::gravitino_client::{Catalog, Fileset};
use crate::open_dal_filesystem::OpenDalFileSystem;
use crate::opened_file::{OpenFileFlags, OpenedFile};
use crate::utils::{parse_location, GvfsResult};
use async_trait::async_trait;
use fuse3::FileType;
use log::error;
use opendal::layers::LoggingLayer;
use opendal::services::S3;
use opendal::{Builder, Operator};
use std::collections::HashMap;
use std::path::Path;

pub(crate) struct S3FileSystem {
    open_dal_fs: OpenDalFileSystem,
}

impl S3FileSystem {}

impl S3FileSystem {
    const S3_CONFIG_PREFIX: &'static str = "s3-";

    pub(crate) async fn new(
        catalog: &Catalog,
        fileset: &Fileset,
        config: &AppConfig,
        _fs_context: &FileSystemContext,
    ) -> GvfsResult<Self> {
        let mut opendal_config = extract_s3_config(config);
        let bucket = extract_bucket(&fileset.storage_location)?;
        opendal_config.insert("bucket".to_string(), bucket.to_string());

        let endpoint = catalog.properties.get("s3-endpoint");
        if endpoint.is_none() {
            return Err(InvalidConfig.to_error("s3-endpoint is required".to_string()));
        }
        let endpoint = endpoint.unwrap();
        opendal_config.insert("endpoint".to_string(), endpoint.clone());

        let region = Self::get_s3_region(catalog, &bucket).await;
        if region.is_none() {
            return Err(InvalidConfig.to_error("s3-region is required".to_string()));
        }
        opendal_config.insert("region".to_string(), region.unwrap());

        let builder = S3::from_map(opendal_config);

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

    async fn get_s3_region(catalog: &Catalog, bucket: &str) -> Option<String> {
        if let Some(region) = catalog.properties.get("s3-region") {
            Some(region.clone())
        } else if let Some(endpoint) = catalog.properties.get("s3-endpoint") {
            S3::detect_region(endpoint, bucket).await
        } else {
            None
        }
    }
}

#[async_trait]
impl PathFileSystem for S3FileSystem {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn stat(&self, path: &Path, kind: FileType) -> Result<FileStat> {
        self.open_dal_fs.stat(path, kind).await
    }

    async fn lookup(&self, path: &Path) -> Result<FileStat> {
        self.open_dal_fs.lookup(path).await
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
        self.open_dal_fs.remove_dir(path).await
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        self.open_dal_fs.get_capacity()
    }
}

pub(crate) fn extract_bucket(location: &str) -> GvfsResult<String> {
    let url = parse_location(location)?;
    match url.host_str() {
        Some(host) => Ok(host.to_string()),
        None => Err(InvalidConfig.to_error(format!(
            "Invalid fileset location without bucket: {}",
            location
        ))),
    }
}

pub(crate) fn extract_region(location: &str) -> Option<String> {
    parse_location(location).ok().and_then(|url| {
        url.host_str()
            .and_then(|host| host.split('.').nth(1).map(|part| part.to_string()))
    })
}

pub fn extract_s3_config(config: &AppConfig) -> HashMap<String, String> {
    config
        .extend_config
        .clone()
        .into_iter()
        .filter_map(|(k, v)| {
            if k.starts_with(S3FileSystem::S3_CONFIG_PREFIX) {
                Some((
                    k.strip_prefix(S3FileSystem::S3_CONFIG_PREFIX)
                        .unwrap()
                        .to_string(),
                    v,
                ))
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::default_raw_filesystem::DefaultRawFileSystem;
    use crate::filesystem::tests::{TestPathFileSystem, TestRawFileSystem};
    use crate::filesystem::RawFileSystem;
    use crate::test_enable_with;
    use crate::RUN_TEST_WITH_S3;
    use opendal::layers::TimeoutLayer;
    use std::time::Duration;

    #[test]
    fn test_extract_bucket() {
        let location = "s3://bucket/path/to/file";
        let result = extract_bucket(location);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "bucket");
    }

    #[test]
    fn test_extract_region() {
        let location = "http://s3.ap-southeast-2.amazonaws.com";
        let result = extract_region(location);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "ap-southeast-2");
    }

    pub(crate) async fn delete_dir(op: &Operator, dir_name: &str) {
        let childs = op.list(dir_name).await.expect("list dir failed");
        for child in childs {
            let child_name = dir_name.to_string() + child.name();
            if child.metadata().is_dir() {
                Box::pin(delete_dir(op, &child_name)).await;
            } else {
                op.delete(&child_name).await.expect("delete file failed");
            }
        }
        op.delete(dir_name).await.expect("delete dir failed");
    }

    pub(crate) async fn cleanup_s3_fs(
        cwd: &Path,
        opendal_config: &HashMap<String, String>,
    ) -> Operator {
        let builder = S3::from_map(opendal_config.clone());
        let op = Operator::new(builder)
            .expect("opendal create failed")
            .layer(LoggingLayer::default())
            .layer(
                TimeoutLayer::new()
                    .with_timeout(Duration::from_secs(300))
                    .with_io_timeout(Duration::from_secs(300)),
            )
            .finish();

        // clean up the test directory
        let file_name = cwd.to_string_lossy().to_string() + "/";
        delete_dir(&op, &file_name).await;
        op.create_dir(&file_name)
            .await
            .expect("create test dir failed");
        op
    }

    async fn create_s3_fs(cwd: &Path, config: &AppConfig) -> S3FileSystem {
        let opendal_config = extract_s3_config(config);
        let op = cleanup_s3_fs(cwd, &opendal_config).await;

        let fs_context = FileSystemContext::default();
        let open_dal_fs = OpenDalFileSystem::new(op, config, &fs_context);

        S3FileSystem { open_dal_fs }
    }

    pub(crate) fn s3_test_config() -> AppConfig {
        let mut config_file_name = "target/conf/gvfs_fuse_s3.toml";
        let source_file_name = "tests/conf/gvfs_fuse_s3.toml";

        if !Path::new(config_file_name).exists() {
            config_file_name = source_file_name;
        }

        AppConfig::from_file(Some(config_file_name.to_string())).unwrap()
    }

    #[tokio::test]
    async fn s3_ut_test_s3_file_system() {
        test_enable_with!(RUN_TEST_WITH_S3);

        let config = s3_test_config();
        let cwd = Path::new("/gvfs_test1");
        let fs = create_s3_fs(cwd, &config).await;

        let _ = fs.init().await;
        let mut tester = TestPathFileSystem::new(cwd, fs);
        tester.test_path_file_system().await;
    }

    #[tokio::test]
    async fn s3_ut_test_s3_file_system_with_raw_file_system() {
        test_enable_with!(RUN_TEST_WITH_S3);

        let config = s3_test_config();
        let cwd = Path::new("/gvfs_test2");
        let s3_fs = create_s3_fs(cwd, &config).await;
        let raw_fs =
            DefaultRawFileSystem::new(s3_fs, &AppConfig::default(), &FileSystemContext::default());
        let _ = raw_fs.init().await;
        let mut tester = TestRawFileSystem::new(cwd, raw_fs);
        tester.test_raw_file_system().await;
    }
}
