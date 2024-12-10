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

use crate::filesystem::{FileStat, FileSystemContext, RawFileSystem};
use crate::fuse_api_handle::FuseApiHandle;
use fuse3::path::prelude::{ReplyData, ReplyOpen, ReplyStatFs, ReplyWrite};
use fuse3::path::Request;
use fuse3::raw::prelude::{
    FileAttr, ReplyAttr, ReplyCreated, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit,
};
use fuse3::raw::reply::{DirectoryEntry, DirectoryEntryPlus};
use fuse3::raw::Filesystem;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType, Inode, SetAttr, Timestamp};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::{stream, FutureExt};
use log::{debug, error, info};
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

pub(crate) struct LogFuseApiHandle<T: RawFileSystem> {
    handle: FuseApiHandle<T>,
}

impl<T: RawFileSystem> LogFuseApiHandle<T> {
    pub fn new(handle: FuseApiHandle<T>) -> Self {
        Self { handle: handle }
    }
}

impl<T: RawFileSystem> Filesystem for LogFuseApiHandle<T> {
    async fn init(&self, req: Request) -> fuse3::Result<ReplyInit> {
        debug!(
            "INIT req_id={} pid={} [{},{}]",
            req.unique, req.pid, req.uid, req.gid
        );

        let result = self.handle.init(req.clone()).await;
        match &result {
            Ok(reply) => {
                debug!(
                    "INIT reply: req_id={}, max_write={}",
                    req.unique, reply.max_write
                );
            }
            Err(e) => {
                debug!("INIT failed: req_id={} error={:?}", req.unique, e);
            }
        }
        result
    }

    async fn destroy(&self, req: Request) {
        debug!(
            "DESTROY req_id={} pid={} [{},{}]",
            req.unique, req.pid, req.uid, req.gid
        );
        self.handle.destroy(req).await;
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        debug!(
            "LOOKUP req_id={} pid={} [{}, {}] parent={}({}) name={}",
            req.unique,
            req.pid,
            req.uid,
            req.gid,
            self.handle.get_file_path(parent).await,
            parent,
            name.to_string_lossy(),
        );

        let result = self.handle.lookup(req.clone(), parent, name).await;

        match &result {
            Ok(reply) => {
                debug!(
                    "LOOKUP reply: req_id={}, [ino={} gen={} attr={:?}]",
                    req.unique, reply.attr.ino, reply.generation, reply.attr
                );
            }
            Err(e) => {
                error!("LOOKUP failed: req_id={}, [error={:?}]", req.unique, e);
            }
        }
        result
    }

    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        debug!(
            "GETATTR req_id={} pid={} [{}, {}] inode={} fh={:?} flags={}",
            req.unique, req.pid, req.uid, req.gid, inode, fh, flags
        );

        let result = self.handle.getattr(req.clone(), inode, fh, flags).await;

        match &result {
            Ok(reply) => {
                debug!(
                    "GETATTR reply: req_id={} [attr={:?}]",
                    req.unique, reply.attr
                );
            }
            Err(e) => {
                error!("GETATTR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        debug!(
            "SETATTR req_id={} pid={} [{}, {}] inode={} fh={:?} set_attr={:?}",
            req.unique, req.pid, req.uid, req.gid, inode, fh, set_attr
        );

        let result = self.handle.setattr(req.clone(), inode, fh, set_attr).await;

        match &result {
            Ok(reply) => {
                debug!(
                    "SETATTR reply: req_id={} [attr={:?}]",
                    req.unique, reply.attr
                );
            }
            Err(e) => {
                error!("SETATTR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        debug!(
            "MKDIR req_id={} pid={} [{}, {}] parent={}({}) name={} mode={} umask={}",
            req.unique,
            req.pid,
            req.uid,
            req.gid,
            self.handle.get_file_path(parent).await,
            parent,
            name.to_string_lossy(),
            mode,
            umask
        );

        let result = self
            .handle
            .mkdir(req.clone(), parent, name, mode, umask)
            .await;

        match &result {
            Ok(reply) => {
                debug!(
                    "MKDIR reply: req_id={} [ino={} gen={} attr={:?}]",
                    req.unique, reply.attr.ino, reply.generation, reply.attr
                );
            }
            Err(e) => {
                error!("MKDIR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        debug!(
            "UNLINK req_id={} pid={} [{}, {}] parent={}({}) name={}",
            req.unique,
            req.pid,
            req.uid,
            req.gid,
            self.handle.get_file_path(parent).await,
            parent,
            name.to_string_lossy()
        );

        let result = self.handle.unlink(req.clone(), parent, name).await;

        match &result {
            Ok(_) => {
                debug!("UNLINK reply: req_id={} [status=success]", req.unique);
            }
            Err(e) => {
                error!("UNLINK failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        debug!(
            "RMDIR req_id={} pid={} [{}, {}] parent={}({}) name={}",
            req.unique,
            req.pid,
            req.uid,
            req.gid,
            self.handle.get_file_path(parent).await,
            parent,
            name.to_string_lossy()
        );

        let result = self.handle.rmdir(req.clone(), parent, name).await;

        match &result {
            Ok(_) => {
                debug!("RMDIR reply: req_id={} [status=success]", req.unique);
            }
            Err(e) => {
                error!("RMDIR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        debug!(
            "OPEN req_id={} pid={} [{}, {}] inode={} flags={}",
            req.unique, req.pid, req.uid, req.gid, inode, flags
        );

        let result = self.handle.open(req.clone(), inode, flags).await;

        match &result {
            Ok(reply) => {
                debug!(
                    "OPEN reply: req_id={} [fh={} flags={}]",
                    req.unique, reply.fh, reply.flags
                );
            }
            Err(e) => {
                error!("OPEN failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        debug!(
            "READ req_id={} pid={} [{}, {}] inode={} fh={} offset={} size={}",
            req.unique, req.pid, req.uid, req.gid, inode, fh, offset, size
        );

        let result = self.handle.read(req.clone(), inode, fh, offset, size).await;

        match &result {
            Ok(_) => {
                debug!(
                    "READ reply: req_id={} [status=success size={}]",
                    req.unique, size
                );
            }
            Err(e) => {
                error!("READ failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
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
        debug!(
        "WRITE req_id={} pid={} [{}, {}] inode={} fh={} offset={} size={} write_flags={} flags={}",
        req.unique,
        req.pid,
        req.uid,
        req.gid,
        inode,
        fh,
        offset,
        data.len(),
        write_flags,
        flags
    );

        let result = self
            .handle
            .write(req.clone(), inode, fh, offset, data, write_flags, flags)
            .await;

        match &result {
            Ok(reply) => {
                debug!(
                    "WRITE reply: req_id={} [written_bytes={}]",
                    req.unique, reply.written,
                );
            }
            Err(e) => {
                error!("WRITE failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        debug!(
            "STATFS req_id={} pid={} [{}, {}] inode={}",
            req.unique, req.pid, req.uid, req.gid, inode
        );

        let result = self.handle.statfs(req.clone(), inode).await;

        match &result {
            Ok(reply) => {
                debug!(
                    "STATFS reply: req_id={} [blocks={} bfree={} bavail={} files={} ffree={}]",
                    req.unique, reply.blocks, reply.bfree, reply.bavail, reply.files, reply.ffree
                );
            }
            Err(e) => {
                error!("STATFS failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
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
        debug!(
            "RELEASE req_id={} pid={} [{}, {}] inode={} fh={} flags={} lock_owner={} flush={}",
            req.unique, req.pid, req.uid, req.gid, inode, fh, flags, lock_owner, flush
        );

        let result = self
            .handle
            .release(req.clone(), inode, fh, flags, lock_owner, flush)
            .await;

        match &result {
            Ok(_) => {
                debug!("RELEASE reply: req_id={} [status=success]", req.unique);
            }
            Err(e) => {
                error!("RELEASE failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        debug!(
            "OPENDIR req_id={} pid={} [{}, {}] inode={} flags={}",
            req.unique, req.pid, req.uid, req.gid, inode, flags
        );

        let result = self.handle.opendir(req.clone(), inode, flags).await;

        match &result {
            Ok(reply) => {
                debug!(
                    "OPENDIR reply: req_id={} [fh={} flags={}]",
                    req.unique, reply.fh, reply.flags
                );
            }
            Err(e) => {
                error!("OPENDIR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    type DirEntryStream<'a>
        = BoxStream<'a, fuse3::Result<DirectoryEntry>>
    where
        T: 'a;

    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        debug!(
            "READDIR req_id={} pid={} [{}, {}] parent={} fh={} offset={}",
            req.unique, req.pid, req.uid, req.gid, parent, fh, offset
        );

        let result = self.handle.readdir(req.clone(), parent, fh, offset).await;

        match &result {
            Ok(_) => {
                debug!("READDIR reply: req_id={} [status=success]", req.unique);
            }
            Err(e) => {
                error!("READDIR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        debug!(
            "RELEASEDIR req_id={} pid={} [{}, {}] inode={} fh={} flags={}",
            req.unique, req.pid, req.uid, req.gid, inode, fh, flags
        );

        let result = self.handle.releasedir(req.clone(), inode, fh, flags).await;

        match &result {
            Ok(_) => {
                debug!("RELEASEDIR reply: req_id={} [status=success]", req.unique);
            }
            Err(e) => {
                error!("RELEASEDIR failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        debug!(
            "CREATE req_id={} pid={} [{}, {}] parent={}({}) name={} mode={} flags={}",
            req.unique,
            req.pid,
            req.uid,
            req.gid,
            self.handle.get_file_path(parent).await,
            parent,
            name.to_string_lossy(),
            mode,
            flags
        );

        let result = self
            .handle
            .create(req.clone(), parent, name, mode, flags)
            .await;

        match &result {
            Ok(reply) => {
                debug!(
                    "CREATE reply: req_id={} [ino={} gen={} fh={} flags={}]",
                    req.unique, reply.attr.ino, reply.generation, reply.fh, reply.flags
                );
            }
            Err(e) => {
                error!("CREATE failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }

    type DirEntryPlusStream<'a>
        = BoxStream<'a, fuse3::Result<DirectoryEntryPlus>>
    where
        T: 'a;

    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> fuse3::Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        debug!(
            "READDIRPLUS req_id={} pid={} [{}, {}] parent={} fh={} offset={} lock_owner={}",
            req.unique, req.pid, req.uid, req.gid, parent, fh, offset, lock_owner
        );

        let result = self
            .handle
            .readdirplus(req.clone(), parent, fh, offset, lock_owner)
            .await;

        match &result {
            Ok(_) => {
                debug!("READDIRPLUS reply: req_id={} [status=success]", req.unique);
            }
            Err(e) => {
                error!("READDIRPLUS failed: req_id={} [error={:?}]", req.unique, e);
            }
        }

        result
    }
}
