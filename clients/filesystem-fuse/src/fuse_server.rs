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
use crate::fuse_api_handle::FuseApiHandle;
use crate::memory_filesystem::MemoryFileSystem;
use fuse3::raw::{MountHandle, Session};
use fuse3::{MountOptions, Result};
use log::info;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::timeout;
use crate::filesystem::FileSystemContext;

/// FuseServer is a struct that represents a FUSE server.
/// It initializes a new FileSystem and FuseApiHandle. and provides methods to start and stop the server.
pub struct FuseServer {
    close_notify: Arc<Notify>,
}

impl FuseServer {
    pub fn new() -> Self {
        Self {
            close_notify: Arc::new(Notify::new()),
        }
    }

    pub fn stop(&self) {
        self.close_notify.notify_one()
    }

    pub async fn start(&self) -> Result<()> {
        let fs = Box::new(MemoryFileSystem::new());
        let fs_context = FileSystemContext{ uid: 1000, gid: 1000 };
        let fuse_fs = FuseApiHandle::new(fs, fs_context);
        let mount_path = "gvfs";
        info!("Starting fuse filesystem and mounted to {}", mount_path);

        let mount_options = MountOptions::default();
        let mut mount_handle = Session::new(mount_options.clone())
            .mount_with_unprivileged(fuse_fs, mount_path)
            .await?;
        let handle = &mut mount_handle;

        tokio::select! {
            res = handle => res?,
            _ = self.close_notify.notified() => {
                FuseServer::unmount_with_timeout(mount_handle).await;
            }
        }
        Ok(())
    }

    async fn unmount_with_timeout(mount_handle: MountHandle) {
        let timeout_duration = Duration::from_secs(5);
        let res = timeout(timeout_duration, mount_handle.unmount()).await;
        match res {
            Ok(Ok(())) => {
                info!("Successfully unmounted the FileSystem");
            }
            Ok(Err(e)) => {
                info!("Failed to unmount the FileSystem: {:?}", e);
                exit(1);
            }
            Err(_) => {
                info!("Umount FileSystem timed out");
                exit(1);
            }
        }
    }
}
