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

use crate::filesystem::{FileStat, FileSystemContext, RawFileSystem, SimpleFileSystem};
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
use std::ffi::{OsStr, OsString};
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

pub(crate) struct FuseApiHandle<T: RawFileSystem> {
    local_fs: T,
    default_ttl: Duration,
    fs_context: FileSystemContext,
}

impl<T: RawFileSystem> FuseApiHandle<T> {
    pub fn new(fs: T, context: FileSystemContext) -> Self {
        FuseApiHandle {
            local_fs: fs,
            default_ttl: Duration::from_secs(1),
            fs_context: context,
        }
    }

    pub async fn get_file_path(&self, inode: u64) -> String {
        self.local_fs.get_file_path(inode).await
    }

    async fn get_modified_file_stat(
        &self,
        inode: u64,
        size: Option<u64>,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
    ) -> Result<FileStat, Errno> {
        let file_stat = self.local_fs.stat(inode).await?;
        let mut nf = FileStat::clone(&file_stat);
        size.map(|size| {
            nf.size = size;
        });
        atime.map(|atime| {
            nf.atime = atime;
        });
        mtime.map(|mtime| {
            nf.mtime = mtime;
        });
        Ok(nf)
    }
}

impl<T: RawFileSystem> Filesystem for FuseApiHandle<T> {
    async fn init(&self, _req: Request) -> fuse3::Result<ReplyInit> {
        self.local_fs.init().await;
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(
        &self,
        _req: Request,
        parent: Inode,
        name: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
        let file_stat = self.local_fs.lookup(parent, name.to_str().unwrap()).await?;
        Ok(ReplyEntry {
            ttl: self.default_ttl,
            attr: fstat_to_file_attr(&file_stat, &self.fs_context),
            generation: 0,
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: Inode,
        fh: Option<u64>,
        _flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        // check the opened file inode is the same as the inode
        if let Some(fh) = fh {
            self.local_fs.valid_file_id(inode, fh).await?;
        }

        let file_stat = self.local_fs.stat(inode).await?;
        Ok(ReplyAttr {
            ttl: self.default_ttl,
            attr: fstat_to_file_attr(&file_stat, &self.fs_context),
        })
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        let new_file_stat = self
            .get_modified_file_stat(inode, set_attr.size, set_attr.atime, set_attr.mtime)
            .await?;
        let attr = fstat_to_file_attr(&new_file_stat, &self.fs_context);
        self.local_fs.set_attr(inode, &new_file_stat).await?;
        Ok(ReplyAttr {
            ttl: self.default_ttl,
            attr: attr,
        })
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        let handle_id = self
            .local_fs
            .create_dir(parent, name.to_str().unwrap())
            .await?;
        Ok(ReplyEntry {
            ttl: self.default_ttl,
            attr: dummy_file_attr(
                handle_id.file_id,
                Directory,
                Timestamp::from(SystemTime::now()),
                &self.fs_context,
            ),
            generation: 0,
        })
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let result = self
            .local_fs
            .remove_file(parent, name.to_str().unwrap())
            .await?;
        Ok(())
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        self.local_fs
            .remove_dir(parent, name.to_str().unwrap())
            .await?;
        Ok(())
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let file_handle = self.local_fs.open_file(inode, flags).await?;
        Ok(ReplyOpen {
            fh: file_handle.handle_id,
            flags: flags,
        })
    }

    async fn read(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        let data = self.local_fs.read(inode, fh, offset, size).await?;
        Ok(ReplyData { data: data })
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
        let mut written = self.local_fs.write(inode, fh, offset, data).await?;
        Ok(ReplyWrite { written: written })
    }

    async fn statfs(&self, req: Request, inode: Inode) -> fuse3::Result<ReplyStatFs> {
        Ok(ReplyStatFs {
            blocks: 1000000,
            bfree: 1000000,
            bavail: 1000000,
            files: 1000000,
            ffree: 1000000,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
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
        self.local_fs.close_file(inode, fh).await
    }

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let file_handle = self.local_fs.open_dir(inode, flags).await?;
        Ok(ReplyOpen {
            fh: file_handle.handle_id,
            flags: flags,
        })
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
        let current = self.local_fs.stat(parent).await?;
        let files = self.local_fs.read_dir(parent).await?;
        let entries_stream =
            stream::iter(files.into_iter().enumerate().map(|(index, file_stat)| {
                Ok(DirectoryEntry {
                    inode: file_stat.inode,
                    name: file_stat.name.clone().into(),
                    kind: file_stat.kind,
                    offset: (index + 3) as i64,
                })
            }));

        let mut relative_paths = stream::iter([
            Ok(DirectoryEntry {
                inode: current.inode,
                name: ".".into(),
                kind: Directory,
                offset: 1,
            }),
            Ok(DirectoryEntry {
                inode: current.parent_inode,
                name: "..".into(),
                kind: Directory,
                offset: 2,
            }),
        ]);

        let combined_stream = relative_paths.chain(entries_stream);
        Ok(ReplyDirectory {
            entries: combined_stream.skip(offset as usize).boxed(),
        })
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        self.local_fs.close_file(inode, fh).await
    }

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        let file_handle = self
            .local_fs
            .create_file(parent, name.to_str().unwrap(), flags)
            .await?;
        Ok(ReplyCreated {
            ttl: self.default_ttl,
            attr: dummy_file_attr(
                file_handle.file_id,
                RegularFile,
                Timestamp::from(SystemTime::now()),
                &self.fs_context,
            ),
            generation: 0,
            fh: file_handle.handle_id,
            flags: flags,
        })
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
        let current = self.local_fs.stat(parent).await?;
        let files = self.local_fs.read_dir(parent).await?;
        let entries_stream =
            stream::iter(files.into_iter().enumerate().map(|(index, file_stat)| {
                Ok(DirectoryEntryPlus {
                    inode: file_stat.inode,
                    name: file_stat.name.clone().into(),
                    kind: file_stat.kind,
                    offset: (index + 3) as i64,
                    attr: fstat_to_file_attr(&file_stat, &self.fs_context),
                    generation: 0,
                    entry_ttl: self.default_ttl,
                    attr_ttl: self.default_ttl,
                })
            }));

        let mut relative_paths = stream::iter([
            Ok(DirectoryEntryPlus {
                inode: current.inode,
                name: OsString::from("."),
                kind: Directory,
                offset: 1,
                attr: fstat_to_file_attr(&current, &self.fs_context),
                generation: 0,
                entry_ttl: self.default_ttl,
                attr_ttl: self.default_ttl,
            }),
            Ok(DirectoryEntryPlus {
                inode: current.parent_inode,
                name: OsString::from(".."),
                kind: Directory,
                offset: 2,
                attr: dummy_file_attr(
                    current.parent_inode,
                    Directory,
                    Timestamp::from(SystemTime::now()),
                    &self.fs_context,
                ),
                generation: 0,
                entry_ttl: self.default_ttl,
                attr_ttl: self.default_ttl,
            }),
        ]);

        let combined_stream = relative_paths.chain(entries_stream);
        Ok(ReplyDirectoryPlus {
            entries: combined_stream.skip(offset as usize).boxed(),
        })
    }
}

const fn fstat_to_file_attr(file_st: &FileStat, context: &FileSystemContext) -> FileAttr {
    FileAttr {
        ino: file_st.inode,
        size: file_st.size,
        blocks: 1,
        atime: file_st.atime,
        mtime: file_st.mtime,
        ctime: file_st.ctime,
        kind: file_st.kind,
        perm: file_st.perm,
        nlink: file_st.nlink,
        uid: context.uid,
        gid: context.gid,
        rdev: 0,
        blksize: 0,
        #[cfg(target_os = "macos")]
        crtime: file_st.ctime,
        #[cfg(target_os = "macos")]
        flags: 0,
    }
}

const fn dummy_file_attr(
    inode: u64,
    kind: FileType,
    now: Timestamp,
    fs_context: &FileSystemContext,
) -> FileAttr {
    FileAttr {
        ino: inode,
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        kind,
        perm: fuse3::perm_from_mode_and_kind(kind, 0o775),
        nlink: 0,
        uid: fs_context.uid,
        gid: fs_context.gid,
        rdev: 0,
        blksize: 4096,
        #[cfg(target_os = "macos")]
        crtime: now,
        #[cfg(target_os = "macos")]
        flags: 0,
    }
}
