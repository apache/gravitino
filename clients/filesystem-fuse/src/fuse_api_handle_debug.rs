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

/// A macro to log the result of an asynchronous method call in the context of FUSE operations.
///
/// This macro provides six variants for logging:
/// 1. Without logging the reply (logs only the method status).
/// 2. With default `Debug` formatting for the reply.
/// 3. With a customizable formatting function for the reply.
/// 4. With both Debug and custom formatting for the reply.
/// 5. Special stream handling for `readdir` operations.
/// 6. Special stream handling for `readdirplus` operations.
///
/// # Usage
///
/// ```ignore
/// // No reply printing
/// log_result!(method_call, "method_name", req);
///
/// // With default Debug formatting for the reply
/// log_result!(method_call, "method_name", req, debug);
///
/// // With a custom formatting function for the reply
/// log_result!(method_call, "method_name", req, custom_format_fn);
///
/// // With both Debug and custom formatting
/// log_result!(method_call, "method_name", req, debug, custom_format_fn);
///
/// // Special handling for readdir stream
/// log_result!(method_call, "readdir", req, stream);
///
/// // Special handling for readdirplus stream
/// log_result!(method_call, "readdirplus", req, stream);
/// ```
///
/// # Arguments
///
/// * `$method_call` - The asynchronous method call to execute and log.
/// * `$method_name` - A string representing the name of the method for logging purposes.
/// * `$req` - The incoming FUSE request associated with the method call.
/// * Format Options (Optional):
///     * No format option - Only logs method status
///     * `debug` - Uses default Debug formatting for the reply
///     * (`debug`, custom_format_fn) - Combines Debug output with custom formatted output
///     * `stream` - Special handling for directory streams (only for "readdir"/"readdirplus")
macro_rules! log_result {
    // No reply printing
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

    // Default Debug formatting
    ($method_call:expr, $method_name:expr, $req:ident, debug) => {
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

    // Format stream for readdir
    ($method_call:expr, "readdir", $req:ident, stream) => {{
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
                        .map(|entry| directory_entry_to_desc_str(entry))
                        .collect::<Vec<String>>()
                        .join(", ")
                );

                debug!($req.unique, entries = entries_info, "READDIR completed");

                Ok(ReplyDirectory {
                    entries: stream::iter(entries.into_iter().map(Ok)).boxed(),
                })
            }
            Err(e) => {
                error!($req.unique, ?e, "READDIR failed");
                Err(e)
            }
        }
    }};

    // Format stream for readdirplus
    ($method_call:expr, "readdirplus", $req:ident, stream) => {{
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
                        .map(|entry| directory_entry_plus_to_desc_str(entry))
                        .collect::<Vec<String>>()
                        .join(", ")
                );

                debug!($req.unique, entries = entries_info, "READDIRPLUS completed");

                Ok(ReplyDirectoryPlus {
                    entries: stream::iter(entries.into_iter().map(Ok)).boxed(),
                })
            }
            Err(e) => {
                error!($req.unique, ?e, "READDIRPLUS failed");
                Err(e)
            }
        }
    }};

    // For debug and custom formatting
    ($method_call:expr, $method_name:expr, $req:ident, debug, $format_reply_fn:ident) => {
        match $method_call.await {
            Ok(reply) => {
                debug!(
                    $req.unique,
                    ?reply,
                    reply_formatted = $format_reply_fn(&reply),
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

/// Convert `ReplyAttr` to descriptive string.
///
/// Example: `ttl: 1s, FileAttr: { ino: 10000, size: 0, blocks: 0, atime: "2025-01-16 02:42:52.600436", mtime: "2025-01-16 02:42:52.600436", ctime: "2025-01-16 02:42:52.600436", crtime: "2025-01-16 02:42:52.600436", kind: RegularFile, perm: 600, nlink: 1, uid: 501, gid: 20, rdev: 0, flags: 0, blksize: 8192 }`
fn reply_attr_to_desc_str(reply_attr: &ReplyAttr) -> String {
    let mut output = String::new();

    write!(output, "ttl: {:?}, ", reply_attr.ttl).unwrap();
    write!(
        output,
        "FileAttr: {}",
        file_attr_to_desc_str(&reply_attr.attr)
    )
    .unwrap();

    output
}

/// Convert `ReplyEntry` to descriptive string.
///
/// Example: `ttl: 1s, FileAttr: { ino: 10001, size: 0, blocks: 1, atime: "2025-01-16 02:42:52.606512", mtime: "2025-01-16 02:42:52.606512", ctime: "2025-01-16 02:42:52.606512", crtime: "2025-01-16 02:42:52.606512", kind: Directory, perm: 700, nlink: 0, uid: 501, gid: 20, rdev: 0, flags: 0, blksize: 8192 }, generation: 0`
fn reply_entry_to_desc_str(reply_entry: &ReplyEntry) -> String {
    let mut output = String::new();

    write!(output, "ttl: {:?}, ", reply_entry.ttl).unwrap();
    write!(
        output,
        "FileAttr: {}, ",
        file_attr_to_desc_str(&reply_entry.attr)
    )
    .unwrap();
    write!(output, "generation: {}", reply_entry.generation).unwrap();

    output
}

/// Convert `ReplyCreated` to descriptive string.
///
/// Example: `ttl: 1s, FileAttr: { ino: 10000, size: 0, blocks: 1, atime: "2025-01-16 02:47:32.126592", mtime: "2025-01-16 02:47:32.126592", ctime: "2025-01-16 02:47:32.126592", crtime: "2025-01-16 02:47:32.126592", kind: RegularFile, perm: 600, nlink: 0, uid: 501, gid: 20, rdev: 0, flags: 0, blksize: 8192 }, generation: 0, fh: 1`
fn reply_created_to_desc_str(reply_created: &ReplyCreated) -> String {
    let mut output = String::new();

    write!(output, "ttl: {:?}, ", reply_created.ttl).unwrap();
    write!(
        output,
        "FileAttr: {}, ",
        file_attr_to_desc_str(&reply_created.attr)
    )
    .unwrap();
    write!(output, "generation: {}, ", reply_created.generation).unwrap();
    write!(output, "fh: {}", reply_created.fh).unwrap();

    output
}

/// Convert `FileAttr` to descriptive string.
///
/// Example: `{ ino: 10000, size: 0, blocks: 1, atime: "2025-01-10 22:53:45.491337", mtime: "2025-01-10 22:53:45.491337", ctime: "2025-01-10 22:53:45.491337", crtime: "2025-01-10 22:53:45.491337", kind: RegularFile, perm: 384, nlink: 0, uid: 501, gid: 20, rdev: 0, flags: 0, blksize: 8192 }`
fn file_attr_to_desc_str(attr: &FileAttr) -> String {
    let mut output = String::new();

    write!(output, "{{ ").unwrap();

    write!(output, "ino: {}, ", attr.ino).unwrap();
    write!(output, "size: {}, ", attr.size).unwrap();
    write!(output, "blocks: {}, ", attr.blocks).unwrap();
    write!(
        output,
        "atime: {:?}, ",
        timestamp_to_desc_string(attr.atime)
    )
    .unwrap();
    write!(
        output,
        "mtime: {:?}, ",
        timestamp_to_desc_string(attr.mtime)
    )
    .unwrap();
    write!(
        output,
        "ctime: {:?}, ",
        timestamp_to_desc_string(attr.ctime)
    )
    .unwrap();
    #[cfg(target_os = "macos")]
    {
        write!(
            output,
            "crtime: {:?}, ",
            timestamp_to_desc_string(attr.crtime)
        )
        .unwrap();
    }
    write!(output, "kind: {:?}, ", attr.kind).unwrap();
    write!(output, "perm: {:o}, ", attr.perm).unwrap();
    write!(output, "nlink: {}, ", attr.nlink).unwrap();
    write!(output, "uid: {}, ", attr.uid).unwrap();
    write!(output, "gid: {}, ", attr.gid).unwrap();
    write!(output, "rdev: {}, ", attr.rdev).unwrap();
    #[cfg(target_os = "macos")]
    {
        write!(output, "flags: {}, ", attr.flags).unwrap();
    }
    write!(output, "blksize: {}", attr.blksize).unwrap();

    write!(output, " }}").unwrap();

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
    write!(output, "{{ ").unwrap();
    write!(output, "inode: {}, ", entry.inode).unwrap();
    write!(output, "kind: {:?}, ", entry.kind).unwrap();
    write!(output, "name: {}, ", entry.name.to_string_lossy()).unwrap();
    write!(output, "offset: {} }}", entry.offset).unwrap();
    output
}

/// Convert `DirectoryEntryPlus` to descriptive string.
///
/// Example: `{ inode: 1234, generation: 0, kind: RegularFile, name: "file.txt", offset: 1, attr: {...}, entry_ttl: 1s, attr_ttl: 1s }`
fn directory_entry_plus_to_desc_str(entry: &DirectoryEntryPlus) -> String {
    let mut output = String::new();
    write!(output, "{{ ").unwrap();
    write!(output, "inode: {}, ", entry.inode).unwrap();
    write!(output, "generation: {}, ", entry.generation).unwrap();
    write!(output, "kind: {:?}, ", entry.kind).unwrap();
    write!(output, "name: {}, ", entry.name.to_string_lossy()).unwrap();
    write!(output, "offset: {}, ", entry.offset).unwrap();
    write!(output, "attr: {}, ", file_attr_to_desc_str(&entry.attr)).unwrap();
    write!(output, "entry_ttl: {:?}, ", entry.entry_ttl).unwrap();
    write!(output, "attr_ttl: {:?} }}", entry.attr_ttl).unwrap();
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

        log_result!(self.inner.init(req), "init", req, debug)
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
        let parent_path_name = self.inner.get_file_path(parent).await?;
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

        log_result!(
            self.inner.lookup(req, parent, name),
            "lookup",
            req,
            debug,
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
        let filename = self.inner.get_file_path(inode).await?;
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

        log_result!(
            self.inner.getattr(req, inode, fh, flags),
            "GETATTR",
            req,
            debug,
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
        let filename = self.inner.get_file_path(inode).await?;
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

        log_result!(
            self.inner.setattr(req, inode, fh, set_attr),
            "SETATTR",
            req,
            debug,
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
        let parent_path_name = self.inner.get_file_path(parent).await?;
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

        log_result!(
            self.inner.mkdir(req, parent, name, mode, umask),
            "mkdir",
            req,
            debug,
            reply_entry_to_desc_str
        )
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let parent_path_name = self.inner.get_file_path(parent).await?;
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

        log_result!(self.inner.unlink(req, parent, name), "unlink", req)
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let parent_path_name = self.inner.get_file_path(parent).await?;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            parent = ?parent_path_name,
            ?name,
            "RMDIR started"
        );

        log_result!(self.inner.rmdir(req, parent, name), "rmdir", req)
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let filename = self.inner.get_file_path(inode).await?;
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

        log_result!(self.inner.open(req, inode, flags), "open", req, debug)
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        let filename = self.inner.get_file_path(inode).await?;
        debug!(
            req.unique,
            ?req,
            "filename" = ?filename,
            ?fh,
            ?offset,
            ?size,
            "READ started"
        );

        log_result!(self.inner.read(req, inode, fh, offset, size), "read", req)
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
        let filename = self.inner.get_file_path(inode).await?;
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

        log_result!(
            self.inner
                .write(req, inode, fh, offset, data, write_flags, flags),
            "write",
            req
        )
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        let filename = self.inner.get_file_path(inode).await?;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "filename" = ?filename,
            "file_id" = inode,
            "STATFS started"
        );

        log_result!(self.inner.statfs(req, inode), "statfs", req, debug)
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
        let filename = self.inner.get_file_path(inode).await?;
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

        log_result!(
            self.inner.release(req, inode, fh, flags, lock_owner, flush),
            "release",
            req
        )
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let parent_path_name = self.inner.get_file_path(inode).await?;
        debug!(
            req.unique,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            "dirname" = ?parent_path_name,
            ?flags,
            "OPENDIR started"
        );

        log_result!(self.inner.opendir(req, inode, flags), "opendir", req, debug)
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
        let parent_path_name = self.inner.get_file_path(parent).await?;
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

        log_result!(
            self.inner.readdir(req, parent, fh, offset),
            "readdir",
            req,
            stream
        )
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        let parent_path_name = self.inner.get_file_path(inode).await?;
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

        log_result!(
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
        let parent_path_name = self.inner.get_file_path(parent).await?;

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

        log_result!(
            self.inner.create(req, parent, name, mode, flags),
            "create",
            req,
            debug,
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
        let parent_path_name = self.inner.get_file_path(parent).await?;
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

        log_result!(
            self.inner.readdirplus(req, parent, fh, offset, lock_owner),
            "readdirplus",
            req,
            stream
        )
    }
}
