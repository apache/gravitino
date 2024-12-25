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
use crate::config::Config;
use crate::utils::GvfsResult;

mod default_raw_filesystem;
mod filesystem;
mod fuse_api_handle;
mod fuse_server;
mod memory_filesystem;
mod mount;
mod opened_file;
mod opened_file_manager;
mod utils;
pub mod config;
mod error;
mod gvfs_fileset_fs;
mod gvfs_mutiple_fileset_fs;
mod gravitino_client;
mod storage_filesystem;

pub async fn gvfs_mount(mount_to: &str, mount_from: &str, config: &Config) -> GvfsResult<()> {
    mount::mount(mount_to, mount_from, &config).await
}

pub async fn gvfs_unmount() -> GvfsResult<()>{
    mount::unmount().await
}
