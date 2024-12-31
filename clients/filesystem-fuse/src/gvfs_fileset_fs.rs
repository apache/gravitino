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

pub(crate) struct GravitinoFileSystemConfig {}

/// GravitinoFileSystem is a filesystem that is associated with a fileset in Gravitino.
/// It mapping the fileset path to the original data storage path. and delegate the operation
/// to the inner filesystem like S3 GCS, JuiceFS.
pub(crate) struct GvfsFilesetFs {
    fs: Box<dyn PathFileSystem>,
    client: GravitinoClient,
    target_path: PathBuf,
}

impl GvfsFilesetFs {
    pub async fn new(
        fs: Box<dyn PathFileSystem>,
        target_path: &Path,
        client: GravitinoClient,
        _config: &AppConfig,
        _context: &FileSystemContext,
    ) -> Self {
        Self {
            fs: fs,
            client: client,
            target_path: target_path.into(),
        }
    }

    fn map_fileset_path_to_raw_path(&self, path: &Path) -> PathBuf {
        let relation_path = path.strip_prefix("/").unwrap();
        if relation_path == Path::new("") {
            return self.target_path.clone();
        }
        self.target_path.join(relation_path)
    }

    fn map_raw_path_to_fileset_path(&self, path: &Path) -> Result<PathBuf> {
        let stripped_path = path
            .strip_prefix(&self.target_path)
            .map_err(|_| Errno::from(libc::EBADF))?;
        let mut result_path = PathBuf::from("/");
        result_path.push(stripped_path);
        Ok(result_path)
    }
}

#[async_trait]
impl PathFileSystem for GvfsFilesetFs {
    async fn init(&self) -> Result<()> {
        self.fs.init().await
    }

    async fn stat(&self, path: &Path) -> Result<FileStat> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        let mut file_stat = self.fs.stat(&raw_path).await?;
        file_stat.path = self.map_raw_path_to_fileset_path(&file_stat.path)?;
        Ok(file_stat)
    }

    async fn read_dir(&self, path: &Path) -> Result<Vec<FileStat>> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        let mut child_filestats = self.fs.read_dir(&raw_path).await?;
        for file_stat in child_filestats.iter_mut() {
            file_stat.path = self.map_raw_path_to_fileset_path(&file_stat.path)?;
        }
        Ok(child_filestats)
    }

    async fn open_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        let mut opened_file = self.fs.open_file(&raw_path, flags).await?;
        opened_file.file_stat.path =
            self.map_raw_path_to_fileset_path(&opened_file.file_stat.path)?;
        Ok(opened_file)
    }

    async fn open_dir(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        let mut opened_file = self.fs.open_dir(&raw_path, flags).await?;
        opened_file.file_stat.path =
            self.map_raw_path_to_fileset_path(&opened_file.file_stat.path)?;
        Ok(opened_file)
    }

    async fn create_file(&self, path: &Path, flags: OpenFileFlags) -> Result<OpenedFile> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        let mut opened_file = self.fs.create_file(&raw_path, flags).await?;
        opened_file.file_stat.path =
            self.map_raw_path_to_fileset_path(&opened_file.file_stat.path)?;
        Ok(opened_file)
    }

    async fn create_dir(&self, path: &Path) -> Result<FileStat> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        let mut file_stat = self.fs.create_dir(&raw_path).await?;
        file_stat.path = self.map_raw_path_to_fileset_path(&file_stat.path)?;
        Ok(file_stat)
    }

    async fn set_attr(&self, path: &Path, file_stat: &FileStat, flush: bool) -> Result<()> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        self.fs.set_attr(&raw_path, file_stat, flush).await
    }

    async fn remove_file(&self, path: &Path) -> Result<()> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        self.fs.remove_file(&raw_path).await
    }

    async fn remove_dir(&self, path: &Path) -> Result<()> {
        let raw_path = self.map_fileset_path_to_raw_path(path);
        self.fs.remove_dir(&raw_path).await
    }

    fn get_capacity(&self) -> Result<FileSystemCapacity> {
        self.fs.get_capacity()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::GravitinoConfig;
    use crate::memory_filesystem::MemoryFileSystem;
    use std::path::Path;

    #[tokio::test]
    async fn test_map_fileset_path_to_raw_path() {
        let fs = super::GvfsFilesetFs {
            fs: Box::new(MemoryFileSystem::new().await),
            client: super::GravitinoClient::new(&GravitinoConfig::default()),
            target_path: "/c1/fileset1".into(),
        };
        let path = fs.map_fileset_path_to_raw_path(Path::new("/a"));
        assert_eq!(path, Path::new("/c1/fileset1/a"));
        let path = fs.map_fileset_path_to_raw_path(Path::new("/"));
        assert_eq!(path, Path::new("/c1/fileset1"));
    }

    #[tokio::test]
    async fn test_map_raw_path_to_fileset_path() {
        let fs = super::GvfsFilesetFs {
            fs: Box::new(MemoryFileSystem::new().await),
            client: super::GravitinoClient::new(&GravitinoConfig::default()),
            target_path: "/c1/fileset1".into(),
        };
        let path = fs
            .map_raw_path_to_fileset_path(Path::new("/c1/fileset1/a"))
            .unwrap();
        assert_eq!(path, Path::new("/a"));
        let path = fs
            .map_raw_path_to_fileset_path(Path::new("/c1/fileset1"))
            .unwrap();
        assert_eq!(path, Path::new("/"));
    }
}
