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
use crate::fuse_server::FuseServer;
use crate::gvfs_creator::create_gvfs_filesystem;
use crate::gvfs_fileset_fs::GvfsFilesetFs;
use crate::memory_filesystem::MemoryFileSystem;
use crate::utils::GvfsResult;
use log::info;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;

static SERVER: Lazy<Mutex<Option<Arc<FuseServer>>>> = Lazy::new(|| Mutex::new(None));

pub(crate) enum CreateFsResult {
    Memory(MemoryFileSystem),
    Gvfs(GvfsFilesetFs),
    FuseMemoryFs(FuseApiHandle<DefaultRawFileSystem<MemoryFileSystem>>),
    FuseGvfs(FuseApiHandle<DefaultRawFileSystem<GvfsFilesetFs>>),
    None,
}

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
        CreateFsResult::FuseMemoryFs(vfs) => svr.start(vfs).await?,
        CreateFsResult::FuseGvfs(vfs) => svr.start(vfs).await?,
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
