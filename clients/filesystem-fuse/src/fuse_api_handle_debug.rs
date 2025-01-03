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
use fuse3::path::prelude::{ReplyData, ReplyOpen, ReplyStatFs, ReplyWrite};
use fuse3::path::Request;
use fuse3::raw::prelude::{
    FileAttr, ReplyAttr, ReplyCreated, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit,
};
use fuse3::raw::reply::{DirectoryEntry, DirectoryEntryPlus};
use fuse3::raw::Filesystem;
use fuse3::FileType::{Directory, RegularFile};
use fuse3::{Errno, FileType, Inode, SetAttr, Timestamp};
use futures_util::stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use log::debug;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};
use crate::fuse_api_handle::FuseApiHandle;

/// Wrapper Struct for `Timestamp` to enable custom Display implementation
pub struct TimestampDebug(pub Timestamp);

impl fmt::Display for TimestampDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ts = &self.0; // Access the inner `Timestamp`
        write!(f, "{}.{:09}", ts.sec, ts.nsec) // Nanoseconds padded to 9 digits
    }
}

// Optional Debug implementation for `TimestampDebug`
impl fmt::Debug for TimestampDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Timestamp({})", self) // Reuses `Display` formatting
    }
}

pub struct FileAttrDebug<'a> {
    pub file_attr: &'a FileAttr,
}

impl<'a> std::fmt::Debug for FileAttrDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let attr = &self.file_attr;
        let mut struc = f.debug_struct("FileAttr");

        struc
            .field("ino", &attr.ino)
            .field("size", &attr.size)
            .field("blocks", &attr.blocks)
            .field("atime", &TimestampDebug(attr.atime))
            .field("mtime", &TimestampDebug(attr.mtime))
            .field("ctime", &TimestampDebug(attr.ctime));

        // Conditionally add the "crtime" field only for macOS
        #[cfg(target_os = "macos")]
        {
            struc.field("crtime", &TimestampDebug(attr.crtime));
        }

        struc
            .field("kind", &attr.kind)
            .field("perm", &attr.perm)
            .field("nlink", &attr.nlink)
            .field("uid", &attr.uid)
            .field("gid", &attr.gid)
            .field("rdev", &attr.rdev)
            .finish()
    }
}

pub(crate) struct FuseApiHandleDebug<T: RawFileSystem> {
    inner: FuseApiHandle<T>,
}

impl<T: RawFileSystem> FuseApiHandleDebug<T> {
    pub fn new(fs: T, context: FileSystemContext) -> Self {
        Self {
            inner: FuseApiHandle::new(fs, context),
        }
    }
}

impl<T: RawFileSystem> Filesystem for FuseApiHandleDebug<T> {
    async fn init(&self, req: Request) -> fuse3::Result<ReplyInit> {
        debug!("init [id={}]: req: {:?}", req.unique, req);
        let res = self.inner.init(req).await;
        match res {
            Ok(reply) => {
                debug!("init [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("init [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn destroy(&self, req: Request) {
        debug!("destroy [id={}]: req: {:?}", req.unique, req);
        self.inner.destroy(req).await;
        debug!("destroy [id={}]: completed", req.unique);
    }

    async fn lookup(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        debug!(
            "lookup [id={}]: req: {:?}, parent: {:?}, name: {:?}",
            req.unique, req, parent, name
        );
        let result = self.inner.lookup(req, parent, name).await;
        match result {
            Ok(reply) => {
                debug!("lookup [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("lookup [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn getattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        debug!(
            "getattr [id={}]: req: {:?}, inode: {:?}, fh: {:?}, flags: {:?}",
            req.unique, req, inode, fh, flags
        );
        let result = self.inner.getattr(req, inode, fh, flags).await;
        match result {
            Ok(reply) => {
                debug!(
                    "getattr [id={}]: reply: {:?}",
                    req.unique,
                    FileAttrDebug {
                        file_attr: &reply.attr
                    }
                );
                Ok(reply)
            }
            Err(e) => {
                debug!("getattr [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        debug!(
            "setattr [id={}]: req: {:?}, inode: {:?}, fh: {:?}, set_attr: {:?}",
            req.unique, req, inode, fh, set_attr
        );
        let result = self.inner.setattr(req, inode, fh, set_attr).await;
        match result {
            Ok(reply) => {
                debug!("setattr [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("setattr [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
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
            "mkdir [id={}]: req: {:?}, parent: {:?}, name: {:?}, mode: {}, umask: {}",
            req.unique, req, parent, name, mode, umask
        );
        let result = self.inner.mkdir(req, parent, name, mode, umask).await;
        match result {
            Ok(reply) => {
                debug!("mkdir [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("mkdir [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        debug!(
            "unlink: req: {:?}, parent: {:?}, name: {:?}",
            req, parent, name
        );
        let result = self.inner.unlink(req, parent, name).await;
        match result {
            Ok(()) => {
                debug!("unlink [id={}]: success", req.unique);
                Ok(())
            }
            Err(e) => {
                debug!("unlink [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        debug!(
            "rmdir [id={}]: req: {:?}, parent: {:?}, name: {:?}",
            req.unique, req, parent, name
        );
        let result = self.inner.rmdir(req, parent, name).await;
        match result {
            Ok(()) => {
                debug!("rmdir [id={}]: success", req.unique);
                Ok(())
            }
            Err(e) => {
                debug!("rmdir [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        debug!(
            "open [id={}]: req: {:?}, inode: {:?}, flags: {:?}",
            req.unique, req, inode, flags
        );
        let result = self.inner.open(req, inode, flags).await;
        match result {
            Ok(reply) => {
                debug!("open [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("open [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
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
            "read [id={}]: req: {:?}, inode: {:?}, fh: {:?}, offset: {:?}, size: {:?}",
            req.unique, req, inode, fh, offset, size
        );
        let result = self.inner.read(req, inode, fh, offset, size).await;
        match result {
            Ok(reply) => {
                debug!("read [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("read [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
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
            "write [id={}]: req: {:?}, inode: {:?}, fh: {:?}, offset: {:?}, data_len: {}, write_flags: {:?}, flags: {:?}",
            req.unique, req, inode, fh, offset, data.len(), write_flags, flags
        );
        let result = self
            .inner
            .write(req, inode, fh, offset, data, write_flags, flags)
            .await;
        match result {
            Ok(reply) => {
                debug!("write [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("write [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        debug!(
            "statfs [id={}]: req: {:?}, inode: {:?}",
            req.unique, req, inode
        );
        let result = self.inner.statfs(req, inode).await;
        match result {
            Ok(reply) => {
                debug!("statfs [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("statfs [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
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
            "release [id={}]: req: {:?}, inode: {:?}, fh: {:?}, flags: {:?}, lock_owner: {:?}, flush: {:?}",
            req.unique, req, inode, fh, flags, lock_owner, flush
        );
        let result = self
            .inner
            .release(req, inode, fh, flags, lock_owner, flush)
            .await;
        match result {
            Ok(()) => {
                debug!("release [id={}]: success", req.unique);
                Ok(())
            }
            Err(e) => {
                debug!("release [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        debug!(
            "opendir [id={}]: req: {:?}, inode: {:?}, flags: {:?}",
            req.unique, req, inode, flags
        );
        let result = self.inner.opendir(req, inode, flags).await;
        match result {
            Ok(reply) => {
                debug!("opendir [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("opendir [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    type DirEntryStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntry>>
    where
        T: 'a;

    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let stat = self.inner.get_modified_file_stat(parent, Option::None, Option::None, Option::None).await?;
        debug!(
            "readdir [id={}]: req: {:?}, parent: {:?}, fh: {:?}, offset: {:?}",
            req.unique,
            req,
            stat,
            fh,
            offset
        );
        let result = self.inner.readdir(req, parent, fh, offset).await;
        match result {
            Ok(reply) => {
                debug!("readdir [id={}]: success", req.unique);
                Ok(reply)
            }
            Err(e) => {
                debug!("readdir [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        debug!(
            "releasedir [id={}]: req: {:?}, inode: {:?}, fh: {:?}, flags: {:?}",
            req.unique, req, inode, fh, flags
        );
        let result = self.inner.releasedir(req, inode, fh, flags).await;
        match result {
            Ok(()) => {
                debug!("releasedir [id={}]: success", req.unique);
                Ok(())
            }
            Err(e) => {
                debug!("releasedir [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
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
            "create [id={}]: req: {:?}, parent: {:?}, name: {:?}, mode: {:?}, flags: {:?}",
            req.unique, req, parent, name, mode, flags
        );
        let result = self.inner.create(req, parent, name, mode, flags).await;
        match result {
            Ok(reply) => {
                debug!("create [id={}]: reply: {:?}", req.unique, reply);
                Ok(reply)
            }
            Err(e) => {
                debug!("create [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }

    type DirEntryPlusStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntryPlus>>
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
            "readdirplus [id={}]: req: {:?}, parent: {:?}, fh: {:?}, offset: {:?}, lock_owner: {:?}",
            req.unique, req, parent, fh, offset, lock_owner
        );
        let result = self
            .inner
            .readdirplus(req, parent, fh, offset, lock_owner)
            .await;
        match result {
            Ok(reply) => {
                debug!("readdirplus [id={}]: success", req.unique);
                Ok(reply)
            }
            Err(e) => {
                debug!("readdirplus [id={}]: error: {:?}", req.unique, e);
                Err(e)
            }
        }
    }
}
