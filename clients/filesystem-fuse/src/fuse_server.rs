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
use crate::utils::GvfsResult;
use fuse3::raw::{Filesystem, Session};
use fuse3::MountOptions;
use log::{error, info};
use std::path::Path;
use std::process::exit;
use std::sync::Arc;
use tokio::select;
use tokio::sync::Notify;

/// Represents a FUSE server capable of starting and stopping the FUSE filesystem.
pub struct FuseServer {
    // Notification for stop
    close_notify: Arc<Notify>,

    // Mount point of the FUSE filesystem
    mount_point: String,
}

impl FuseServer {
    /// Creates a new instance of `FuseServer`.
    pub fn new(mount_point: &str) -> Self {
        Self {
            close_notify: Arc::new(Default::default()),
            mount_point: mount_point.to_string(),
        }
    }

    /// Starts the FUSE filesystem and blocks until it is stopped.
    pub async fn start(&self, fuse_fs: impl Filesystem + Sync + 'static) -> GvfsResult<()> {
        //check if the mount point exists
        if !Path::new(&self.mount_point).exists() {
            error!("Mount point {} does not exist", self.mount_point);
            exit(libc::ENOENT);
        }

        info!(
            "Starting FUSE filesystem and mounting at {}",
            self.mount_point
        );

        let mount_options = MountOptions::default();
        let mut mount_handle = Session::new(mount_options)
            .mount_with_unprivileged(fuse_fs, &self.mount_point)
            .await?;

        let handle = &mut mount_handle;

        select! {
            res = handle => {
                if res.is_err() {
                    error!("Failed to mount FUSE filesystem: {:?}", res.err());
                }
            },
            _ = self.close_notify.notified() => {
               if let Err(e) = mount_handle.unmount().await {
                    error!("Failed to unmount FUSE filesystem: {:?}", e);
                } else {
                    info!("FUSE filesystem unmounted successfully.");
                }
            }
        }

        // notify that the filesystem is stopped
        self.close_notify.notify_one();
        Ok(())
    }

    /// Stops the FUSE filesystem.
    pub async fn stop(&self) -> GvfsResult<()> {
        info!("Stopping FUSE filesystem...");
        self.close_notify.notify_one();

        // wait for the filesystem to stop
        self.close_notify.notified().await;
        Ok(())
    }
}
