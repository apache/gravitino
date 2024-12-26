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
use crate::error::ErrorCode::InvalidConfig;
use crate::filesystem::{FileSystemContext, PathFileSystem};
use crate::gravitino_client::GravitinoClient;
use crate::gvfs_fileset_fs::GvfsFilesetFs;
use crate::gvfs_fuse::{CreateFsResult, FileSystemSchema};
use crate::open_dal_filesystem::OpenDalFileSystem;
use crate::utils::GvfsResult;
use std::path::Path;

const FILESET_PREFIX: &str = "gvfs://fileset/";

pub async fn create_gvfs_filesystem(
    mount_from: &str,
    config: &AppConfig,
    fs_context: &FileSystemContext,
) -> GvfsResult<CreateFsResult> {
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

    let (catalog, schema, fileset) = extract_fileset(mount_from)?;
    let location = client
        .get_fileset(&catalog, &schema, &fileset)
        .await?
        .storage_location;
    let (schema, location) = extract_storage_filesystem(&location).unwrap();

    let inner_fs = create_fs_by_schema(&schema, config, fs_context)?;

    let fs = GvfsFilesetFs::new(inner_fs, Path::new(&location), client, config, fs_context).await;
    Ok(CreateFsResult::Gvfs(fs))
}

fn create_fs_by_schema(
    schema: &FileSystemSchema,
    config: &AppConfig,
    fs_context: &FileSystemContext,
) -> GvfsResult<Box<dyn PathFileSystem>> {
    match schema {
        FileSystemSchema::S3 => OpenDalFileSystem::create_file_system(schema, config, fs_context),
    }
}

pub fn extract_fileset(path: &str) -> GvfsResult<(String, String, String)> {
    if !path.starts_with(FILESET_PREFIX) {
        return Err(InvalidConfig.to_error("Invalid fileset path".to_string()));
    }

    let path_without_prefix = &path[FILESET_PREFIX.len()..];

    let parts: Vec<&str> = path_without_prefix.split('/').collect();

    if parts.len() != 3 {
        return Err(InvalidConfig.to_error("Invalid fileset path".to_string()));
    }
    // todo handle mount catalog or schema

    let catalog = parts[1].to_string();
    let schema = parts[2].to_string();
    let fileset = parts[3].to_string();

    Ok((catalog, schema, fileset))
}

pub fn extract_storage_filesystem(path: &str) -> Option<(FileSystemSchema, String)> {
    // todo need to improve the logic
    if let Some(pos) = path.find("://") {
        let protocol = &path[..pos];
        let location = &path[pos + 3..];
        let location = match location.find('/') {
            Some(index) => &location[index + 1..],
            None => "",
        };
        let location = match location.ends_with('/') {
            true => location.to_string(),
            false => format!("{}/", location),
        };

        match protocol {
            "s3" => Some((FileSystemSchema::S3, location.to_string())),
            "s3a" => Some((FileSystemSchema::S3, location.to_string())),
            _ => None,
        }
    } else {
        None
    }
}
