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
use crate::cloud_storage_filesystem::CloudStorageFileSystem;
use crate::config::{Config, FilesystemConfig, FuseConfig, GravitinoConfig};
use crate::filesystem::{FileSystemContext, SimpleFileSystem};
use crate::fuse_api_handle::FuseApiHandle;
use crate::gravitino_filesystem::GravitinoFileSystem;
use crate::log_fuse_api_handle::LogFuseApiHandle;
use crate::memory_filesystem::MemoryFileSystem;
use fuse3::raw::{MountHandle, Session};
use fuse3::{MountOptions, Result};
use log::{error, info};
use opendal::layers::LoggingLayer;
use opendal::{services, Operator};
use serde::__private::ser::constrain;
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;

/// Represents a FUSE server capable of starting and stopping the FUSE filesystem.
pub struct FuseServer {
    // Notification for stop
    close_notify: Arc<Notify>,

    // Shared handle to manage FUSE unmounting
    mount_handle: Arc<Mutex<Option<MountHandle>>>, // Shared handle to manage FUSE unmounting

    // Mount point of the FUSE filesystem
    mount_point: String,
}

impl FuseServer {
    /// Creates a new instance of `FuseServer`.
    pub fn new(mount_point: &str) -> Self {
        Self {
            close_notify: Arc::new(Notify::new()),
            mount_handle: Arc::new(Mutex::new(None)),
            mount_point: mount_point.to_string(),
        }
    }

    /// Starts the FUSE filesystem and blocks until it is stopped.
    pub async fn start(&self, config: &Config) -> Result<()> {
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let fs_context = FileSystemContext { uid: uid, gid: gid };

        let gvfs = GravitinoFileSystem::new(&config, &fs_context).await;
        let fs = SimpleFileSystem::new(gvfs);

        //let fs = SimpleFileSystem::new(MemoryFileSystem::new());
        let fuse_fs = LogFuseApiHandle::new(FuseApiHandle::new(fs, fs_context));
        //let fuse_fs = FuseApiHandle::new(fs, fs_context);

        //check if the mount point exists
        if !std::path::Path::new(&self.mount_point).exists() {
            error!("Mount point {} does not exist", self.mount_point);
            exit(libc::ENOENT);
        }

        info!(
            "Starting FUSE filesystem and mounting at {}",
            self.mount_point
        );

        let mount_options = MountOptions::default();
        let mount_handle = Session::new(mount_options)
            .mount_with_unprivileged(fuse_fs, &self.mount_point)
            .await?;

        {
            let mut handle_guard = self.mount_handle.lock().await;
            *handle_guard = Some(mount_handle);
        }

        // Wait for stop notification
        self.close_notify.notified().await;

        info!("Received stop notification, FUSE filesystem will be unmounted.");
        Ok(())
    }

    /// Stops the FUSE filesystem and waits for unmounting to complete.
    pub async fn stop(&self) -> Result<()> {
        // Notify stop
        self.close_notify.notify_one();

        info!("Stopping FUSE filesystem...");
        let timeout_duration = Duration::from_secs(5);

        let handle = {
            let mut handle_guard = self.mount_handle.lock().await;
            handle_guard.take() // Take the handle out to unmount
        };

        if let Some(mount_handle) = handle {
            let res = timeout(timeout_duration, mount_handle.unmount()).await;

            match res {
                Ok(Ok(())) => {
                    info!("FUSE filesystem unmounted successfully.");
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Failed to unmount FUSE filesystem: {:?}", e);
                    Err(e.into())
                }
                Err(_) => {
                    error!("Unmount timed out.");
                    Err(libc::ETIMEDOUT.into())
                }
            }
        } else {
            error!("No active mount handle to unmount.");
            Err(libc::EBADF.into())
        }
    }
}
