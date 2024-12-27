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
use fuse3::Errno;
use gvfs_fuse::config::AppConfig;
use gvfs_fuse::{gvfs_mount, gvfs_unmount};
use log::{error, info};
use tokio::signal;

#[tokio::main]
async fn main() -> fuse3::Result<()> {
    tracing_subscriber::fmt().init();

    //todo(read config file from args)
    let config = AppConfig::from_file(Some("conf/gvfs_fuse.toml)"));
    if let Err(e) = &config {
        error!("Failed to load config: {:?}", e);
        return Err(Errno::from(libc::EINVAL));
    }
    let config = config.unwrap();
    let handle = tokio::spawn(async move { gvfs_mount("gvfs", "", &config).await });

    let _ = signal::ctrl_c().await;
    info!("Received Ctrl+C, Unmounting gvfs...");

    if let Err(e) = handle.await {
        error!("Failed to mount gvfs: {:?}", e);
        return Err(Errno::from(libc::EINVAL));
    }

    let _ = gvfs_unmount().await;
    Ok(())
}
