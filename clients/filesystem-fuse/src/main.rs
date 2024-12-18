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
mod filesystem;
mod fuse_api_handle;
mod opened_file_manager;
mod utils;

use log::debug;
use log::info;
use std::process::exit;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().init();
    info!("Starting filesystem...");
    debug!("Shutdown filesystem...");
    exit(0);
}

async fn create_gvfs_fuse_filesystem() {
    // Gvfs-fuse filesystem structure:
    // FuseApiHandle
    // ├─ SimpleFileSystem (RawFileSystem)
    // │  └─ FileSystemLog (PathFileSystem)
    //      ├─ GravitinoComposedFileSystem (PathFileSystem)
    //      │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    //      │  │  └─ OpenDALFileSystem (PathFileSystem)
    //      │  │     └─ S3FileSystem (PathFileSystem)
    //      │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    //      │  │  └─ OpenDALFileSystem (PathFileSystem)
    //      │  │     └─ HDFSFileSystem (PathFileSystem)
    //      │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    //      │  │  └─ NasFileSystem (PathFileSystem)
    //      │  │     └─ JuiceFileSystem (PathFileSystem)
    //      │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    //      │  │  └─ XXXFileSystem (PathFileSystem)
    //
    // `SimpleFileSystem` is a low-level filesystem designed to communicate with FUSE APIs.
    // It manages file and directory relationships, as well as file mappings.
    // It delegates file operations to the PathFileSystem
    //
    // `FileSystemLog` is a decorator that adds extra debug logging functionality to file system APIs.
    // Similar implementations include permissions, caching, and metrics.
    //
    // `GravitinoComposeFileSystem` is a composite file system that can combine multiple `GravitinoFilesetFileSystem`.
    //  It translates the part of catalog and schema of fileset path to a signal GravitinoFilesetFileSystem path.
    // If the user only mounts a fileset, this layer is not present. There will only be one below layer.
    //
    // `GravitinoFilesetFileSystem` is a file system that can access a fileset.It translates the fileset path to the real storage path.
    // and delegate the operation to the real storage.
    //
    // `OpenDALFileSystem` is a file system that use the OpenDAL to access real storage.
    // it can assess the S3, HDFS, gcs, azblob and other storage.
    //
    // `S3FileSystem` is a file system that use to access S3 storage.
    //
    // `HDFSFileSystem` is a file system that use to access HDFS storage.

    // `NasFileSystem` is a filesystem that uses a locally accessible path mounted by NAS tools, such as JuiceFS.

    // `JuiceFileSystem` is a file system that use to manage JuiceFS mount and access JuiceFS storage.

    // `XXXFileSystem is a filesystem that allows you to implement file access through your own extensions.

    todo!("Implement the createGvfsFuseFileSystem function");
}
