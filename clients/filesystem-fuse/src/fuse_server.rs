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
use log::{error, info};
use std::process::exit;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;
use crate::filesystem::FileSystemContext;

/// Represents a FUSE server capable of starting and stopping the FUSE filesystem.
pub struct FuseServer {
    // Notification for stop
    close_notify: Arc<Notify>,

    // Shared handle to manage FUSE unmounting
    mount_handle: Arc<Mutex<Option<MountHandle>>>, // Shared handle to manage FUSE unmounting
}

impl FuseServer {
    /// Creates a new instance of `FuseServer`.
    pub fn new() -> Self {
        Self {
            close_notify: Arc::new(Notify::new()),
            mount_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the FUSE filesystem and blocks until it is stopped.
    pub async fn start(&self) -> Result<()> {
        let fs = Box::new(MemoryFileSystem::new());
        let fs_context = crate::filesystem::FileSystemContext { uid: 1000, gid: 1000 };
        let fuse_fs = FuseApiHandle::new(fs, fs_context);
        let mount_path = "gvfs";

        info!("Starting FUSE filesystem and mounting at {}", mount_path);

        let mount_options = MountOptions::default();
        let mount_handle = Session::new(mount_options)
            .mount_with_unprivileged(fuse_fs, mount_path)
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