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
    tracing_subscriber::fmt::init();

    // todo need inmprove the args parsing
    let args: Vec<String> = std::env::args().collect();
    let (mount_point, mount_from, config_path) = match args.len() {
        4 => (args[1].clone(), args[2].clone(), args[3].clone()),
        _ => {
            error!("Usage: {} <mount_point> <mount_from> <config>", args[0]);
            return Err(Errno::from(libc::EINVAL));
        }
    };

    //todo(read config file from args)
    let config = AppConfig::from_file(Some(&config_path));
    if let Err(e) = &config {
        error!("Failed to load config: {:?}", e);
        return Err(Errno::from(libc::EINVAL));
    }
    let config = config.unwrap();
    let handle = tokio::spawn(async move {
        let result = gvfs_mount(&mount_point, &mount_from, &config).await;
        if let Err(e) = result {
            error!("Failed to mount gvfs: {:?}", e);
            return Err(Errno::from(libc::EINVAL));
        }
        Ok(())
    });

    tokio::select! {
        _ = handle => {}
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, unmounting gvfs...");
        }
    }

    let _ = gvfs_unmount().await;
    Ok(())
}
