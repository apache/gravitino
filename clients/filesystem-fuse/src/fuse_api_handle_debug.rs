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
use futures_util::stream::{BoxStream};
use futures_util::{StreamExt};
use std::ffi::{OsStr};
use std::fmt::Write;
use tracing::{debug, error};

/// A macro to log the result of an asynchronous method call in the context of FUSE operations.
///
/// This macro provides three variants for logging:
/// 1. Without logging the reply (logs only the method status).
/// 2. With default `Debug` formatting for the reply.
/// 3. With a customizable formatting function for the reply.
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
/// ```
///
/// # Arguments
///
/// - `$method_call`: The asynchronous method call to execute and log.
/// - `$method_name`: A string representing the name of the method for logging purposes.
/// - `$req`: The incoming FUSE request associated with the method call.
/// - `$format_reply_fn`: (Optional) A custom formatting function to describe the reply more specifically.
macro_rules! log_result {
   // No reply printing
   ($method_call:expr, $method_name:expr, $req:ident) => {
        match $method_call.await {
            Ok(reply) => {
                debug!($req.unique, concat!($method_name, " completed"));
                Ok(reply)
            }
            Err(e) => {
                error!($req.unique, ?e, concat!($method_name, " failed"));
                Err(e)
            }
        }
    };

    // Default Debug formatting
    ($method_call:expr, $method_name:expr, $req:ident, debug) => {
        match $method_call.await {
            Ok(reply) => {
                debug!($req.unique, ?reply, concat!($method_name, " completed"));
                Ok(reply)
            }
            Err(e) => {
                error!($req.unique, ?e, concat!($method_name, " failed"));
                Err(e)
            }
        }
    };

    // Format reply with custom formatting function
    ($method_call:expr, $method_name:expr, $req:ident, $format_reply_fn:ident) => {
            match $method_call.await {
            Ok(reply) => {
                debug!($req.unique, reply = %$format_reply_fn(&reply), concat!($method_name, " completed"));
                Ok(reply)
            }
            Err(e) => {
                error!($req.unique, ?e, concat!($method_name, " failed"));
                Err(e)
            }
        }
    };

    // Format stream with custom formatting function
    ($method_call:expr, $method_name:expr, $req:ident, stream, $format_reply_fn:expr) => {{
        match $method_call.await {
            Ok(reply_dir) => {
                let mut stream = reply_dir.entries.peekable();
                let mut debug_output = String::new();
                while let Some(entry_result) = stream.peek().await {
                    match entry_result {
                        Ok(entry) => {
                            debug_output.push_str(format!("Directory entry: {:?}\n", entry).as_str());
                        }
                        Err(e) => {
                            debug_output.push_str(format!("Error reading directory entry: {:?}\n", e).as_str());
                        }
                    }
                }

                Ok(reply_dir)
            }
            Err(e) => {
                error!(
                    $req.unique,
                    ?e,
                    concat!($method_name, " failed")
                );
                Err(e)
            }
        }
    }};
}

// async fn stream_to_desc_str<S>(mut res: ReplyDirectory<>) -> String {
//     let mut s = String::new();
//     while let Some(entry_result) = res.entries.next().await {
//         match entry_result {
//             Ok(entry) => s.push_str(format!("Directory entry: {:?}", entry).as_str()),
//             Err(e) => s.push_str(format!("Error reading directory entry: {:?}", e).as_str()),
//         }
//     }
//     s
// }

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
        debug!(req.unique, ?req, "init");
        log_result!(self.inner.init(req), "init", req, debug)
    }

    async fn destroy(&self, req: Request) {
        debug!(req.unique, ?req, "destroy started");
        self.inner.destroy(req).await;
        debug!(req.unique, "destroy completed");
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        let parent_stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!("req_id" = req.unique, parent = ?parent_stat.name, ?name, ?req, "LOOKUP started");

        log_result!(
            self.inner.lookup(req, parent, name),
            "LOOKUP",
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
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            "filename" = ?stat.name,
            "file_id" = inode,
            "uid" = req.uid,
            "gid" = req.gid,
            "pid" = req.pid,
            ?fh,
            flags,
            "GETATTR started"
        );

        log_result!(
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
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, "filename" = ?stat.name, ?fh, ?set_attr, "setattr started");

        log_result!(
            self.inner.setattr(req, inode, fh, set_attr),
            "setattr",
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
        let parent_stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            parent = ?parent_stat.name,
            ?name,
            mode,
            umask,
            "mkdir started"
        );

        log_result!(
            self.inner.mkdir(req, parent, name, mode, umask),
            "mkdir",
            req,
            reply_entry_to_desc_str
        )
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let parent_stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, parent = ?parent_stat.name, ?name, "unlink started");

        log_result!(self.inner.unlink(req, parent, name), "unlink", req)
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let parent_stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, parent = ?parent_stat.name, ?name, "rmdir started");

        log_result!(self.inner.rmdir(req, parent, name), "rmdir", req)
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, "filename" = ?stat.name, ?flags, "open started");

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
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            "filename" = ?stat.name,
            ?fh,
            ?offset,
            ?size,
            "read started"
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
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            "filename" = ?stat.name,
            ?fh,
            ?offset,
            data_len = data.len(),
            ?write_flags,
            ?flags,
            "write started"
        );

        log_result!(
            self.inner
                .write(req, inode, fh, offset, data, write_flags, flags),
            "write",
            req
        )
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, "filename" = ?stat.name, "statfs started");

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
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            "pathname" = ?stat.name,
            ?fh,
            ?flags,
            ?lock_owner,
            ?flush,
            "release started"
        );

        log_result!(
            self.inner.release(req, inode, fh, flags, lock_owner, flush),
            "release",
            req
        )
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, "dirname" = ?stat.name, ?flags, "opendir started");

        log_result!(self.inner.opendir(req, inode, flags), "opendir", req, debug)
    }

    type DirEntryStream<'a>
    = BoxStream<'a, fuse3::Result<DirectoryEntry>>
    where
        T: 'a;

    // type DirEntryStream = Stream<Item = fuse3::Result<DirectoryEntry>>;

    // type DirEntryStream<'a> = Stream<'a, fuse3::Result<DirectoryEntry>>
    // where
    //     T: 'a;
    //
    // type DirEntryStream<'a> = Stream<Item = fuse3::Result<DirectoryEntry>> + Send + 'a;

    #[allow(clippy::needless_lifetimes)]
    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            parent = ?stat.name,
            ?fh,
            ?offset,
            "readdir started"
        );

        let mut debug_output = String::new();

        match self.inner.readdir(req, parent, fh, offset).await {
            Ok(mut reply_dir) => {
               // Chain the `inspect` method to the existing stream
                reply_dir.entries = Box::pin(reply_dir.entries.inspect(|entry_result| {
                    match entry_result {
                        Ok(entry) => {
                            debug!("Directory entry: {:?}", entry);
                        }
                        Err(e) => {
                            debug!("Error reading directory entry: {:?}", e);
                        }
                    }
                }));

                Ok(reply_dir)


                // let stream = &mut reply_dir.entries;
                //
                // let ins = stream.inspect(|entry_result| match entry_result {
                //     Ok(entry) => {
                //         // debug_output.push_str(format!("Directory entry: {:?}\n", entry).as_str());
                //         debug!("{}", format!("Directory entry: {:?}\n", entry).as_str());
                //     }
                //
                //     Err(e) => {
                //         // debug_output
                //         //     .push_str(format!("Error reading directory entry: {:?}\n", e).as_str());
                //         debug!("{}",format!("Error reading directory entry: {:?}\n", e).as_str());
                //     }
                // });

                // let e: Vec<_> = ins.collect().await;
                // debug!("xxx collected {:?}", e);

                // reply_dir.entries = Box::pin(stream);

                // Ok(reply_dir)
            }
            Err(e) => {
                debug_output
                    .push_str(format!("Error reading directory entries: {:?}\n", e).as_str());
                Err(e)
            }
        }

        // while let Some(entry_result) = res.entries.collect() {
        //
        //
        // }
        // while let Some(entry_result) = res.entries.next().await {
        //     match entry_result {
        //         Ok(entry) => debug!(?entry, "READDIR result"),
        //         Err(e) => error!("Error reading directory entry: {:?}", e),
        //     }
        // }

        // log_result!(
        //     self.inner.readdir(req, parent, fh, offset),
        //     "READDIR",
        //     req,
        //     stream,
        //     // |entry: &DirectoryEntry| { debug!("Hello") }
        //     |entry: &DirectoryEntry| {
        //         format!(
        //             "{{ inode: {}, kind: {}, name: \"{}\", offset: {} }}",
        //             entry.inode,
        //             match entry.kind {
        //                 FileType::Directory => "Directory",
        //                 FileType::RegularFile => "File",
        //                 FileType::Symlink => "Symlink",
        //                 _ => "Other",
        //             },
        //             entry.name.to_string_lossy(),
        //             entry.offset
        //         )
        //     }
        // )

        // log_result!(self.inner.readdir(req, parent, fh, offset), "READDIR", req)
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        let stat = self
            .inner
            .get_modified_file_stat(inode, Option::None, Option::None, Option::None)
            .await?;
        debug!(req.unique, ?req, "dirname" = ?stat.name, ?fh, ?flags, "releasedir started");

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
        let parent_stat = self
            .inner
            .get_modified_file_stat(parent, Option::None, Option::None, Option::None)
            .await?;
        debug!(
            req.unique,
            ?req,
            parent = ?parent_stat.name,
            "filename" = ?name,
            ?mode,
            ?flags,
            "create started"
        );

        log_result!(
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
        debug!(
            req.unique,
            ?req,
            ?parent,
            ?fh,
            ?offset,
            ?lock_owner,
            "readdirplus started"
        );

        let mut res = self
            .inner
            .readdirplus(req, parent, fh, offset, lock_owner)
            .await?;
        while let Some(entry_result) = res.entries.next().await {
            match entry_result {
                Ok(entry) => println!("Directory entry plus: {:?}", entry),
                Err(e) => eprintln!("Error reading directory entry plus: {:?}", e),
            }
        }

        log_result!(
            self.inner.readdirplus(req, parent, fh, offset, lock_owner),
            "readdirplus",
            req
        )
    }
}
