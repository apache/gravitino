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
use crate::default_raw_filesystem::DefaultRawFileSystem;
use crate::error::ErrorCode::{InvalidConfig, UnSupportedFilesystem};
use crate::filesystem::FileSystemContext;
use crate::fuse_api_handle::FuseApiHandle;
use crate::fuse_server::FuseServer;
use crate::gravitino_client::GravitinoClient;
use crate::gvfs_fileset_fs::GvfsFilesetFs;
use crate::memory_filesystem::MemoryFileSystem;
use crate::utils::GvfsResult;
use log::info;
use once_cell::sync::Lazy;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

const FILESET_PREFIX: &str = "gvfs://fileset/";

static SERVER: Lazy<Mutex<Option<Arc<FuseServer>>>> = Lazy::new(|| Mutex::new(None));

pub(crate) enum CreateFsResult {
    Memory(MemoryFileSystem),
    Gvfs(GvfsFilesetFs),
    FuseMemoryFs(FuseApiHandle<DefaultRawFileSystem<MemoryFileSystem>>),
    FuseGvfs(FuseApiHandle<DefaultRawFileSystem<GvfsFilesetFs>>),
    None,
}

pub enum FileSystemScheam {
    S3,
}

pub async fn mount(mount_to: &str, mount_from: &str, config: &AppConfig) -> GvfsResult<()> {
    info!("Starting gvfs-fuse server...");
    let svr = Arc::new(FuseServer::new(mount_to));
    {
        let mut server = SERVER.lock().await;
        *server = Some(svr.clone());
    }
    let fs = create_fuse_fs(mount_from, config).await?;
    match fs {
        CreateFsResult::FuseMemoryFs(vfs) => svr.start(vfs).await?,
        CreateFsResult::FuseGvfs(vfs) => svr.start(vfs).await?,
        _ => return Err(UnSupportedFilesystem.to_error("Unsupported filesystem type".to_string())),
    }
    Ok(())
}

pub async fn unmount() -> GvfsResult<()> {
    info!("Stop gvfs-fuse server...");
    let svr = {
        let mut server = SERVER.lock().await;
        if server.is_none() {
            info!("Server is already stopped.");
            return Ok(());
        }
        server.take().unwrap()
    };
    svr.stop().await
}

pub(crate) async fn create_fuse_fs(
    mount_from: &str,
    config: &AppConfig,
) -> GvfsResult<CreateFsResult> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let fs_context = FileSystemContext {
        uid: uid,
        gid: gid,
        default_file_perm: 0o644,
        default_dir_perm: 0o755,
        block_size: 4 * 1024,
    };
    let fs = create_path_fs(mount_from, config, &fs_context).await?;
    create_raw_fs(fs, config, fs_context).await
}

pub async fn create_raw_fs(
    path_fs: CreateFsResult,
    config: &AppConfig,
    fs_context: FileSystemContext,
) -> GvfsResult<CreateFsResult> {
    match path_fs {
        CreateFsResult::Memory(fs) => {
            let fs = FuseApiHandle::new(
                DefaultRawFileSystem::new(fs, config, &fs_context),
                config,
                fs_context,
            );
            Ok(CreateFsResult::FuseMemoryFs(fs))
        }
        CreateFsResult::Gvfs(fs) => {
            let fs = FuseApiHandle::new(
                DefaultRawFileSystem::new(fs, config, &fs_context),
                config,
                fs_context,
            );
            Ok(CreateFsResult::FuseGvfs(fs))
        }
        _ => Err(UnSupportedFilesystem.to_error("Unsupported filesystem type".to_string())),
    }
}

pub async fn create_path_fs(
    mount_from: &str,
    config: &AppConfig,
    fs_context: &FileSystemContext,
) -> GvfsResult<CreateFsResult> {
    if config.fuse.fs_type == "memory" {
        Ok(CreateFsResult::Memory(MemoryFileSystem::new().await))
    } else {
        create_gvfs_filesystem(mount_from, config, fs_context).await
    }
}

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
    let (_schema, location) = extract_storage_filesystem(&location).unwrap();

    let inner_fs = MemoryFileSystem::new().await;

    let fs = GvfsFilesetFs::new(
        Box::new(inner_fs),
        Path::new(&location),
        client,
        config,
        fs_context,
    )
    .await;
    Ok(CreateFsResult::Gvfs(fs))
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

    let catalog = parts[1].to_string();
    let schema = parts[2].to_string();
    let fileset = parts[3].to_string();

    Ok((catalog, schema, fileset))
}

pub fn extract_storage_filesystem(path: &str) -> Option<(FileSystemScheam, String)> {
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
            "s3" => Some((FileSystemScheam::S3, location.to_string())),
            "s3a" => Some((FileSystemScheam::S3, location.to_string())),
            _ => None,
        }
    } else {
        None
    }
}
