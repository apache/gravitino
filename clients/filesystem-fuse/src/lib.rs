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
use crate::utils::GvfsResult;

pub mod config;
mod default_raw_filesystem;
mod error;
mod filesystem;
mod fuse_api_handle;
mod fuse_server;
mod gravitino_client;
mod gravitino_fileset_filesystem;
mod gvfs_creator;
mod gvfs_fuse;
mod memory_filesystem;
mod open_dal_filesystem;
mod opened_file;
mod opened_file_manager;
mod s3_filesystem;
mod utils;

#[macro_export]
macro_rules! test_enable_with {
    ($env_var:expr) => {
        if std::env::var($env_var).is_err() {
            println!("Test skipped because {} is not set", $env_var);
            return;
        }
    };
}

pub const RUN_TEST_WITH_S3: &str = "RUN_TEST_WITH_S3";
pub const RUN_TEST_WITH_FUSE: &str = "RUN_TEST_WITH_FUSE";

pub const LOG_FILE_NAME: &str = "gvfs-fuse.log";
pub const PID_FILE_NAME: &str = "gvfs-fuse.pid";

pub async fn gvfs_mount(mount_to: &str, mount_from: &str, config: &AppConfig) -> GvfsResult<()> {
    gvfs_fuse::mount(mount_to, mount_from, config).await
}

pub async fn gvfs_unmount() -> GvfsResult<()> {
    gvfs_fuse::unmount().await
}
