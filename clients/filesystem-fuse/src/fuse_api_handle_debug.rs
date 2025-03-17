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
use crate::filesystem::{FileSystemContext, RawFileSystem};
use crate::fuse_api_handle::FuseApiHandle;
use chrono::DateTime;
use fuse3::path::prelude::{ReplyData, ReplyOpen, ReplyStatFs, ReplyWrite};
use fuse3::path::Request;
use fuse3::raw::prelude::{
    FileAttr, ReplyAttr, ReplyCreated, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit,
};
use fuse3::raw::reply::{DirectoryEntry, DirectoryEntryPlus};
use fuse3::raw::Filesystem;
use fuse3::{Inode, SetAttr, Timestamp};
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use std::ffi::OsStr;
use std::fmt::Write;
use tracing::{debug, error};

/// Log the result without printing the reply
macro_rules! log_status {
    ($method_call:expr, $method_name:expr, $req:ident) => {
        match $method_call.await {
            Ok(reply) => {
                debug!($req.unique, "{} completed", $method_name.to_uppercase());
                Ok(reply)
            }
            Err(e) => {
                error!($req.unique, ?e, "{} failed", $method_name.to_uppercase());
                Err(e)
            }
        }
    };
}

/// Log the result with default Debug formatting
macro_rules! log_value {
    ($method_call:expr, $method_name:expr, $req:ident) => {
        match $method_call.await {
            Ok(reply) => {
                debug!(
                    $req.unique,
                    ?reply,
                    "{} completed",
                    $method_name.to_uppercase()
                );
                Ok(reply)
            }
            Err(e) => {
                error!($req.unique, ?e, "{} failed", $method_name.to_uppercase());
                Err(e)
            }
        }
    };
}
/// Log the result with custom formatting
macro_rules! log_value_custom {
    ($method_call:expr, $method_name:expr, $req:ident, $format_reply_fn:ident) => {
        match $method_call.await {
            Ok(reply) => {
                debug!(
                    $req.unique,
                    reply = $format_reply_fn(&reply),
                    "{} completed",
                    $method_name.to_uppercase()
                );
                Ok(reply)
            }
            Err(e) => {
                error!($req.unique, ?e, "{} failed", $method_name.to_uppercase());
                Err(e)
            }
        }
    };
}

/// Log the result for readdir operations
macro_rules! log_readdir {
    ($method_call:expr, $method_name:expr, $req:ident, $entry_to_desc_str:expr, $reply_type:ident) => {{
        match $method_call.await {
            Ok(mut reply_dir) => {
                let mut entries = Vec::new();

                while let Some(entry_result) = reply_dir.entries.next().await {
                    match entry_result {
                        Ok(entry) => {
                            entries.push(entry);
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }

                let entries_info = format!(
                    "[{}]",
                    entries
                        .iter()
                        .map(|entry| $entry_to_desc_str(entry))
                        .collect::<Vec<String>>()
                        .join(", ")
                );

                debug!(
                    $req.unique,
                    entries = entries_info,
                    "{} completed",
                    $method_name.to_uppercase()
                );

                Ok($reply_type {
                    entries: stream::iter(entries.into_iter().map(Ok)).boxed(),
                })
            }
            Err(e) => {
                error!($req.unique, ?e, "READDIR failed");
                Err(e)
            }
        }
    }};
}

/// Convert `ReplyAttr` to descriptive string.
///
/// Example (pretty-printed for readability):
/// ```text
/// ttl: 1s,
/// FileAttr: {
///     ino: 10000,
///     size: 0,
///     blocks: 0,
///     atime: 2025-01-16 02:42:52.600436,
///     mtime: 2025-01-16 02:42:52.600436,
///     ctime: 2025-01-16 02:42:52.600436,
///     crtime: 2025-01-16 02:42:52.600436,
///     kind: RegularFile,
///     perm: 600,
///     nlink: 1,
///     uid: 501,
///     gid: 20,
///     rdev: 0,
///     flags: 0,
///     blksize: 8192
/// }
/// ```
fn reply_attr_to_desc_str(reply_attr: &ReplyAttr) -> String {
    let mut output = String::new();

    write!(
        output,
        "ttl: {:?}, FileAttr: {}",
        reply_attr.ttl,
        file_attr_to_desc_str(&reply_attr.attr)
    )
    .unwrap();

    output
}

/// Convert `ReplyEntry` to descriptive string.
///
/// Example (pretty-printed for readability):
/// ```text
/// ttl: 1s,
/// FileAttr: {
///     ino: 10001,
///     size: 0,
///     blocks: 1,
///     atime: 2025-01-16 02:42:52.606512,
///     mtime: 2025-01-16 02:42:52.606512,
///     ctime: 2025-01-16 02:42:52.606512,
///     crtime: 2025-01-16 02:42:52.606512,
///     kind: Directory,
///     perm: 700,
///     nlink: 0,
///     uid: 501,
///     gid: 20,
///     rdev: 0,
///     flags: 0,
///     blksize: 8192
/// },
/// generation: 0
/// ```
fn reply_entry_to_desc_str(reply_entry: &ReplyEntry) -> String {
    let mut output = String::new();

    write!(
        output,
        "ttl: {:?}, FileAttr: {}, generation: {}",
        reply_entry.ttl,
        file_attr_to_desc_str(&reply_entry.attr),
        reply_entry.generation
    )
    .unwrap();

    output
}

/// Convert `ReplyCreated` to descriptive string.
///
/// Example (pretty-printed for readability):
/// ```text
/// ttl: 1s,
/// FileAttr: {
///     ino: 10000,
///     size: 0,
///     blocks: 1,
///     atime: 2025-01-16 02:47:32.126592,
///     mtime: 2025-01-16 02:47:32.126592,
///     ctime: 2025-01-16 02:47:32.126592,
///     crtime: 2025-01-16 02:47:32.126592,
///     kind: RegularFile,
///     perm: 600,
///     nlink: 0,
///     uid: 501,
///     gid: 20,
///     rdev: 0,
///     flags: 0,
///     blksize: 8192
/// },
/// generation: 0,
/// fh: 1
/// ```
fn reply_created_to_desc_str(reply_created: &ReplyCreated) -> String {
    let mut output = String::new();

    write!(
        output,
        "ttl: {:?}, FileAttr: {}, generation: {}, fh: {}",
        reply_created.ttl,
        file_attr_to_desc_str(&reply_created.attr),
        reply_created.generation,
        reply_created.fh
    )
    .unwrap();

    output
}

/// Convert `FileAttr` to descriptive string.
///
/// Example (pretty-printed for readability):
/// ```text
/// {
///     ino: 10000,
///     size: 0,
///     blocks: 1,
///     atime: 2025-01-10 22:53:45.491337,
///     mtime: 2025-01-10 22:53:45.491337,
///     ctime: 2025-01-10 22:53:45.491337,
///     crtime: 2025-01-10 22:53:45.491337,
///     kind: RegularFile,
///     perm: 384,
///     nlink: 0,
///     uid: 501,
///     gid: 20,
///     rdev: 0,
///     flags: 0,
///     blksize: 8192
/// }
/// ```
fn file_attr_to_desc_str(attr: &FileAttr) -> String {
    let mut output = String::new();

    write!(
        output,
        "{{ ino: {}, size: {}, blocks: {}, atime: {}, mtime: {}, ctime: {}, ",
        attr.ino,
        attr.size,
        attr.blocks,
        timestamp_to_desc_string(attr.atime),
        timestamp_to_desc_string(attr.mtime),
        timestamp_to_desc_string(attr.ctime)
    )
    .unwrap();
    #[cfg(target_os = "macos")]
    {
        write!(
            output,
            "crtime: {}, ",
            timestamp_to_desc_string(attr.crtime)
        )
        .unwrap();
    }
    write!(
        output,
        "kind: {:?}, perm: {:o}, nlink: {}, uid: {}, gid: {}, rdev: {}, ",
        attr.kind, attr.perm, attr.nlink, attr.uid, attr.gid, attr.rdev
    )
    .unwrap();
    #[cfg(target_os = "macos")]
    {
        write!(output, "flags: {}, ", attr.flags).unwrap();
    }
    write!(output, "blksize: {} }}", attr.blksize).unwrap();

    output
}

/// Convert `Timestamp` to descriptive string.
///
/// Example output: "2025-01-07 23:01:30.531699"
fn timestamp_to_desc_string(tstmp: Timestamp) -> String {
    DateTime::from_timestamp(tstmp.sec, tstmp.nsec)
        .unwrap()
        .naive_utc()
        .to_string()
}

/// Convert `DirectoryEntry` to descriptive string.
///
/// Example: `{ inode: 1234, kind: RegularFile, name: "file.txt", offset: 1 }`
fn directory_entry_to_desc_str(entry: &DirectoryEntry) -> String {
    let mut output = String::new();
    write!(
        output,
        "{{ inode: {}, kind: {:?}, name: {}, offset: {} }}",
        entry.inode,
        entry.kind,
        entry.name.to_string_lossy(),
        entry.offset
    )
    .unwrap();
    output
}

/// Convert `DirectoryEntryPlus` to descriptive string.
///
/// Example (pretty-printed for readability):
/// ```text
/// {
///     inode: 1234,
///     generation: 0,
///     kind: RegularFile,
///     name: "file.txt",
///     offset: 1,
///     attr: {...},
///     entry_ttl: 1s,
///     attr_ttl: 1s
/// }
/// ```
fn directory_entry_plus_to_desc_str(entry: &DirectoryEntryPlus) -> String {
    let mut output = String::new();
    write!(
        output,
        "{{ inode: {}, generation: {}, kind: {:?}, name: {}, offset: {}, \
         attr: {}, entry_ttl: {:?}, attr_ttl: {:?} }}",
        entry.inode,
        entry.generation,
        entry.kind,
        entry.name.to_string_lossy(),
        entry.offset,
        file_attr_to_desc_str(&entry.attr),
        entry.entry_ttl,
        entry.attr_ttl
    )
    .unwrap();

    output
}

pub(crate) struct FuseApiHandleDebug<T: RawFileSystem> {
    inner: FuseApiHandle<T>,
}

impl<T: RawFileSystem> FuseApiHandleDebug<T> {
    pub fn new(fs: T, _config: &AppConfig, context: FileSystemContext) -> Self {
        Self {
            inner: FuseApiHandle::new(fs, _config, context),
        }
    }

    /// Wrapper for get_file_path that returns an empty string on error
    /// instead of propagating the error
    async fn get_file_path_or_empty(&self, inode: Inode) -> String {
        self.inner
            .get_file_path(inode)
            .await
            .unwrap_or_else(|_| "".to_string())
    }
}

impl<T: RawFileSystem> Filesystem for FuseApiHandleDebug<T> {
    async fn init(&self, req: Request) -> fuse3::Result<ReplyInit> {
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "INIT started"
        );

        log_value!(self.inner.init(req), "init", req)
    }

    async fn destroy(&self, req: Request) {
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "DESTROY started"
        );
        self.inner.destroy(req).await;
        debug!(req.unique, "DESTROY completed");
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "parent" = ?parent_path_name,
            "parent_id" = parent,
            ?name,
            "LOOKUP started"
        );

        log_value_custom!(
            self.inner.lookup(req, parent, name),
            "lookup",
            req,
            reply_entry_to_desc_str
        )
    }

    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            ?fh,
            ?flags,
            "GETATTR started"
        );

        log_value_custom!(
            self.inner.getattr(req, inode, fh, flags),
            "GETATTR",
            req,
            reply_attr_to_desc_str
        )
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            ?fh,
            ?set_attr,
            "SETATTR started"
        );

        log_value_custom!(
            self.inner.setattr(req, inode, fh, set_attr),
            "SETATTR",
            req,
            reply_attr_to_desc_str
        )
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "parent" = ?parent_path_name,
            "parent_id" = parent,
            ?name,
            mode,
            umask,
            "MKDIR started"
        );

        log_value_custom!(
            self.inner.mkdir(req, parent, name, mode, umask),
            "mkdir",
            req,
            reply_entry_to_desc_str
        )
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "parent" = ?parent_path_name,
            "parent_id" = parent,
            ?name,
            "UNLINK started"
        );

        log_status!(self.inner.unlink(req, parent, name), "unlink", req)
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            parent = ?parent_path_name,
            ?name,
            "RMDIR started"
        );

        log_status!(self.inner.rmdir(req, parent, name), "rmdir", req)
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            "file_id" = inode,
            ?flags,
            "OPEN started"
        );

        log_value!(self.inner.open(req, inode, flags), "open", req)
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            ?req,
            "filename" = ?filename,
            ?fh,
            ?offset,
            ?size,
            "READ started"
        );

        log_status!(self.inner.read(req, inode, fh, offset, size), "read", req)
    }

    async fn write(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        data: &[u8],
        write_flags: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            "file_id" = inode,
            ?fh,
            ?offset,
            data_len = data.len(),
            ?write_flags,
            ?flags,
            "WRITE started"
        );

        log_status!(
            self.inner
                .write(req, inode, fh, offset, data, write_flags, flags),
            "write",
            req
        )
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            "file_id" = inode,
            "STATFS started"
        );

        log_value!(self.inner.statfs(req, inode), "statfs", req)
    }

    async fn release(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
        lock_owner: u64,
        flush: bool,
    ) -> fuse3::Result<()> {
        let filename = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            "file_id" = inode,
            ?fh,
            ?flags,
            ?lock_owner,
            ?flush,
            "RELEASE started"
        );

        log_status!(
            self.inner.release(req, inode, fh, flags, lock_owner, flush),
            "release",
            req
        )
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let parent_path_name = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "dirname" = ?parent_path_name,
            ?flags,
            "OPENDIR started"
        );

        log_value!(self.inner.opendir(req, inode, flags), "opendir", req)
    }

    type DirEntryStream<'a>
    = BoxStream<'a, fuse3::Result<DirectoryEntry>>
    where
        T: 'a;

    #[allow(clippy::needless_lifetimes)]
    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "parent" = ?parent_path_name,
            ?fh,
            ?offset,
            "READDIR started"
        );

        log_readdir!(
            self.inner.readdir(req, parent, fh, offset),
            "READDIR",
            req,
            directory_entry_to_desc_str,
            ReplyDirectory
        )
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        let parent_path_name = self.get_file_path_or_empty(inode).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "dirname" = ?parent_path_name,
            ?fh,
            ?flags,
            "RELEASEDIR started"
        );

        log_status!(
            self.inner.releasedir(req, inode, fh, flags),
            "releasedir",
            req
        )
    }

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;

        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "parent" = ?parent_path_name,
            "parent_id" = parent,
            "filename" = ?name,
            ?mode,
            ?flags,
            "CREATE started"
        );

        log_value_custom!(
            self.inner.create(req, parent, name, mode, flags),
            "create",
            req,
            reply_created_to_desc_str
        )
    }

    type DirEntryPlusStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntryPlus>>
    where
        T: 'a;

    #[allow(clippy::needless_lifetimes)]
    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> fuse3::Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let parent_path_name = self.get_file_path_or_empty(parent).await;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "parent" = ?parent_path_name,
            ?fh,
            ?offset,
            ?lock_owner,
            "READDIRPLUS started"
        );

        log_readdir!(
            self.inner.readdirplus(req, parent, fh, offset, lock_owner),
            "READDIRPLUS",
            req,
            directory_entry_plus_to_desc_str,
            ReplyDirectoryPlus
        )
    }
}
