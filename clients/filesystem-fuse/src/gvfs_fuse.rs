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
use crate::error::ErrorCode::UnSupportedFilesystem;
use crate::filesystem::FileSystemContext;
use crate::fuse_api_handle::FuseApiHandle;
use crate::fuse_api_handle_debug::FuseApiHandleDebug;
use crate::fuse_server::FuseServer;
use crate::gravitino_fileset_filesystem::GravitinoFilesetFileSystem;
use crate::gvfs_creator::create_gvfs_filesystem;
use crate::memory_filesystem::MemoryFileSystem;
use crate::utils::GvfsResult;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

static SERVER: Lazy<Mutex<Option<Arc<FuseServer>>>> = Lazy::new(|| Mutex::new(None));

pub(crate) enum CreateFileSystemResult {
    Memory(MemoryFileSystem),
    Gvfs(GravitinoFilesetFileSystem),
    FuseMemoryFs(FuseApiHandle<DefaultRawFileSystem<MemoryFileSystem>>),
    FuseGvfs(FuseApiHandle<DefaultRawFileSystem<GravitinoFilesetFileSystem>>),
    FuseMemoryFsWithDebug(FuseApiHandleDebug<DefaultRawFileSystem<MemoryFileSystem>>),
    FuseGvfsWithDebug(FuseApiHandleDebug<DefaultRawFileSystem<GravitinoFilesetFileSystem>>),
    None,
}

#[derive(Debug, PartialEq)]
pub enum FileSystemSchema {
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
        CreateFileSystemResult::FuseMemoryFs(vfs) => svr.start(vfs).await?,
        CreateFileSystemResult::FuseMemoryFsWithDebug(vfs) => svr.start(vfs).await?,
        CreateFileSystemResult::FuseGvfs(vfs) => svr.start(vfs).await?,
        CreateFileSystemResult::FuseGvfsWithDebug(vfs) => svr.start(vfs).await?,
        _ => return Err(UnSupportedFilesystem.to_error("Unsupported filesystem type".to_string())),
    }
    Ok(())
}

pub async fn unmount() -> GvfsResult<()> {
    info!("Stopping gvfs-fuse server...");
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
) -> GvfsResult<CreateFileSystemResult> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let fs_context = FileSystemContext::new(uid, gid, config);
    let fs = create_path_fs(mount_from, config, &fs_context).await?;
    create_raw_fs(fs, config, fs_context).await
}

pub async fn create_raw_fs(
    path_fs: CreateFileSystemResult,
    config: &AppConfig,
    fs_context: FileSystemContext,
) -> GvfsResult<CreateFileSystemResult> {
    match path_fs {
        CreateFileSystemResult::Memory(fs) => {
            if config.fuse.fuse_debug {
                let fs = FuseApiHandleDebug::new(
                    DefaultRawFileSystem::new(fs, config, &fs_context),
                    config,
                    fs_context,
                );
                return Ok(CreateFileSystemResult::FuseMemoryFsWithDebug(fs));
            }
            let fs = FuseApiHandle::new(
                DefaultRawFileSystem::new(fs, config, &fs_context),
                config,
                fs_context,
            );
            Ok(CreateFileSystemResult::FuseMemoryFs(fs))
        }
        CreateFileSystemResult::Gvfs(fs) => {
            if config.fuse.fuse_debug {
                let fs = FuseApiHandleDebug::new(
                    DefaultRawFileSystem::new(fs, config, &fs_context),
                    config,
                    fs_context,
                );
                return Ok(CreateFileSystemResult::FuseGvfsWithDebug(fs));
            }
            let fs = FuseApiHandle::new(
                DefaultRawFileSystem::new(fs, config, &fs_context),
                config,
                fs_context,
            );
            Ok(CreateFileSystemResult::FuseGvfs(fs))
        }
        _ => Err(UnSupportedFilesystem.to_error("Unsupported filesystem type".to_string())),
    }
}

pub async fn create_path_fs(
    mount_from: &str,
    config: &AppConfig,
    fs_context: &FileSystemContext,
) -> GvfsResult<CreateFileSystemResult> {
    if config.fuse.fs_type == "memory" {
        Ok(CreateFileSystemResult::Memory(
            MemoryFileSystem::new().await,
        ))
    } else {
        create_gvfs_filesystem(mount_from, config, fs_context).await
    }
}
