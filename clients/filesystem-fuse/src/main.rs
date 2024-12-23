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
use log::{error, info};
use tokio::signal;
use gvfs_fuse::{gvfs_mount, gvfs_unmount};

#[tokio::main]
async fn main() -> fuse3::Result<()> {
    tracing_subscriber::fmt().init();
    let handle = tokio::spawn(async {
        gvfs_mount("gvfs").await
    });

    tokio::select! {
        hd = handle => {
            match hd {
                Ok(Ok(_)) => info!("Mount succeeded."),
                Ok(Err(e)) => error!("Mount failed: {:?}", e),
                Err(e) => error!("Mount failed: {:?}", e),
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received Ctrl+C, stopping server...");
            info!("Unmounting gvfs...");
            gvfs_unmount().await;
        }
    }
    Ok(())
}
