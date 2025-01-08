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
use crate::error::ErrorCode::{InvalidConfig, UnSupportedFilesystem};
use crate::filesystem::{FileSystemContext, PathFileSystem};
use crate::gravitino_client::{Catalog, Fileset, GravitinoClient};
use crate::gravitino_fileset_filesystem::GravitinoFilesetFileSystem;
use crate::gvfs_fuse::{CreateFileSystemResult, FileSystemSchema};
use crate::s3_filesystem::S3FileSystem;
use crate::utils::{extract_root_path, parse_location, GvfsResult};

const GRAVITINO_FILESET_SCHEMA: &str = "gvfs";

pub async fn create_gvfs_filesystem(
    mount_from: &str,
    config: &AppConfig,
    fs_context: &FileSystemContext,
) -> GvfsResult<CreateFileSystemResult> {
    // Gvfs-fuse filesystem structure:
    // FuseApiHandle
    // ├─ DefaultRawFileSystem (RawFileSystem)
    // │ └─ FileSystemLog (PathFileSystem)
    // │    ├─ GravitinoComposedFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ S3FileSystem (PathFileSystem)
    // │    │  │     └─ OpenDALFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ HDFSFileSystem (PathFileSystem)
    // │    │  │     └─ OpenDALFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ JuiceFileSystem (PathFileSystem)
    // │    │  │     └─ NasFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ XXXFileSystem (PathFileSystem)
    //
    // `SimpleFileSystem` is a low-level filesystem designed to communicate with FUSE APIs.
    // It manages file and directory relationships, as well as file mappings.
    // It delegates file operations to the PathFileSystem
    //
    // `FileSystemLog` is a decorator that adds extra debug logging functionality to file system APIs.
    // Similar implementations include permissions, caching, and metrics.
    //
    // `GravitinoComposeFileSystem` is a composite file system that can combine multiple `GravitinoFilesetFileSystem`.
    // It use the part of catalog and schema of fileset path to a find actual GravitinoFilesetFileSystem. delegate the operation to the real storage.
    // If the user only mounts a fileset, this layer is not present. There will only be one below layer.
    //
    // `GravitinoFilesetFileSystem` is a file system that can access a fileset.It translates the fileset path to the real storage path.
    // and delegate the operation to the real storage.
    //
    // `OpenDALFileSystem` is a file system that use the OpenDAL to access real storage.
    // it can assess the S3, HDFS, gcs, azblob and other storage.
    //
    // `S3FileSystem` is a file system that use `OpenDALFileSystem` to access S3 storage.
    //
    // `HDFSFileSystem` is a file system that use `OpenDALFileSystem` to access HDFS storage.
    //
    // `NasFileSystem` is a filesystem that uses a locally accessible path mounted by NAS tools, such as JuiceFS.
    //
    // `JuiceFileSystem` is a file that use `NasFileSystem` to access JuiceFS storage.
    //
    // `XXXFileSystem is a filesystem that allows you to implement file access through your own extensions.

    let client = GravitinoClient::new(&config.gravitino);

    let (catalog_name, schema_name, fileset_name) = extract_fileset(mount_from)?;
    let catalog = client.get_catalog(&catalog_name).await?;
    if catalog.catalog_type != "fileset" {
        return Err(InvalidConfig.to_error(format!("Catalog {} is not a fileset", catalog_name)));
    }
    let fileset = client
        .get_fileset(&catalog_name, &schema_name, &fileset_name)
        .await?;

    let inner_fs = create_fs_with_fileset(&catalog, &fileset, config, fs_context).await?;

    let target_path = extract_root_path(fileset.storage_location.as_str())?;
    let fs =
        GravitinoFilesetFileSystem::new(inner_fs, &target_path, client, config, fs_context).await;
    Ok(CreateFileSystemResult::Gvfs(fs))
}

pub(crate) async fn create_fs_with_fileset(
    catalog: &Catalog,
    fileset: &Fileset,
    config: &AppConfig,
    fs_context: &FileSystemContext,
) -> GvfsResult<Box<dyn PathFileSystem>> {
    let schema = extract_filesystem_scheme(&fileset.storage_location)?;

    match schema {
        FileSystemSchema::S3 => Ok(Box::new(
            S3FileSystem::new(catalog, fileset, config, fs_context).await?,
        )),
    }
}

pub fn extract_fileset(path: &str) -> GvfsResult<(String, String, String)> {
    let path = parse_location(path)?;

    if path.scheme() != GRAVITINO_FILESET_SCHEMA {
        return Err(InvalidConfig.to_error(format!("Invalid fileset schema: {}", path)));
    }

    let split = path.path_segments();
    if split.is_none() {
        return Err(InvalidConfig.to_error(format!("Invalid fileset path: {}", path)));
    }
    let split = split.unwrap().collect::<Vec<&str>>();
    if split.len() != 4 {
        return Err(InvalidConfig.to_error(format!("Invalid fileset path: {}", path)));
    }

    let catalog = split[1].to_string();
    let schema = split[2].to_string();
    let fileset = split[3].to_string();
    Ok((catalog, schema, fileset))
}

pub fn extract_filesystem_scheme(path: &str) -> GvfsResult<FileSystemSchema> {
    let url = parse_location(path)?;
    let scheme = url.scheme();

    match scheme {
        "s3" => Ok(FileSystemSchema::S3),
        "s3a" => Ok(FileSystemSchema::S3),
        _ => Err(UnSupportedFilesystem.to_error(format!("Invalid storage schema: {}", path))),
    }
}

#[cfg(test)]
mod tests {
    use crate::gvfs_creator::extract_fileset;
    use crate::gvfs_fuse::FileSystemSchema;

    #[test]
    fn test_extract_fileset() {
        let location = "gvfs://fileset/test/c1/s1/fileset1";
        let (catalog, schema, fileset) = extract_fileset(location).unwrap();
        assert_eq!(catalog, "c1");
        assert_eq!(schema, "s1");
        assert_eq!(fileset, "fileset1");
    }

    #[test]
    fn test_extract_schema() {
        let location = "s3://bucket/path/to/file";
        let schema = super::extract_filesystem_scheme(location).unwrap();
        assert_eq!(schema, FileSystemSchema::S3);
    }
}
