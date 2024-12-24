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
use crate::default_raw_filesystem::DefaultRawFileSystem;
use crate::filesystem::FileSystemContext;
use crate::fuse_api_handle::FuseApiHandle;
use crate::fuse_server::FuseServer;
use crate::memory_filesystem::MemoryFileSystem;
use fuse3::raw::Filesystem;
use log::info;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::Mutex;

static SERVER: Lazy<Mutex<Option<Arc<FuseServer>>>> = Lazy::new(|| Mutex::new(None));

pub async fn mount(mount_point: &str) -> fuse3::Result<()> {
    info!("Starting gvfs-fuse server...");
    let svr = Arc::new(FuseServer::new(mount_point));
    {
        let mut server = SERVER.lock().await;
        *server = Some(svr.clone());
    }
    let fs = create_fuse_fs().await;
    svr.start(fs).await
}

pub async fn unmount() {
    info!("Stop gvfs-fuse server...");
    let svr = {
        let mut server = SERVER.lock().await;
        if server.is_none() {
            info!("Server is already stopped.");
            return;
        }
        server.take().unwrap()
    };
    let _ = svr.stop().await;
}

pub async fn create_fuse_fs() -> impl Filesystem + Sync + 'static {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let fs_context = FileSystemContext {
        uid: uid,
        gid: gid,
        default_file_perm: 0o644,
        default_dir_perm: 0o755,
        block_size: 4 * 1024,
    };

    let gvfs = MemoryFileSystem::new().await;
    let fs = DefaultRawFileSystem::new(gvfs);
    FuseApiHandle::new(fs, fs_context)
}

pub async fn create_gvfs_filesystem() {
    // Gvfs-fuse filesystem structure:
    // FuseApiHandle
    // ├─ DefaultRawFileSystem (RawFileSystem)
    // │ └─ FileSystemLog (PathFileSystem)
    // │    ├─ GravitinoComposedFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ S3FileSystem (PathFileSystem)
    // │    │  │     └─ OpenDALFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ HDFSFileSystem (PathFileSystem)
    // │    │  │     └─ OpenDALFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ JuiceFileSystem (PathFileSystem)
    // │    │  │     └─ NasFileSystem (PathFileSystem)
    // │    │  ├─ GravitinoFilesetFileSystem (PathFileSystem)
    // │    │  │  └─ XXXFileSystem (PathFileSystem)
    //
    // `SimpleFileSystem` is a low-level filesystem designed to communicate with FUSE APIs.
    // It manages file and directory relationships, as well as file mappings.
    // It delegates file operations to the PathFileSystem
    //
    // `FileSystemLog` is a decorator that adds extra debug logging functionality to file system APIs.
    // Similar implementations include permissions, caching, and metrics.
    //
    // `GravitinoComposeFileSystem` is a composite file system that can combine multiple `GravitinoFilesetFileSystem`.
    // It use the part of catalog and schema of fileset path to a find actual GravitinoFilesetFileSystem. delegate the operation to the real storage.
    // If the user only mounts a fileset, this layer is not present. There will only be one below layer.
    //
    // `GravitinoFilesetFileSystem` is a file system that can access a fileset.It translates the fileset path to the real storage path.
    // and delegate the operation to the real storage.
    //
    // `OpenDALFileSystem` is a file system that use the OpenDAL to access real storage.
    // it can assess the S3, HDFS, gcs, azblob and other storage.
    //
    // `S3FileSystem` is a file system that use `OpenDALFileSystem` to access S3 storage.
    //
    // `HDFSFileSystem` is a file system that use `OpenDALFileSystem` to access HDFS storage.
    //
    // `NasFileSystem` is a filesystem that uses a locally accessible path mounted by NAS tools, such as JuiceFS.
    //
    // `JuiceFileSystem` is a file that use `NasFileSystem` to access JuiceFS storage.
    //
    // `XXXFileSystem is a filesystem that allows you to implement file access through your own extensions.

    todo!("Implement the createGvfsFuseFileSystem function");
}
