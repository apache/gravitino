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

use crate::filesystem::{FileStat, FileSystemContext, IFileSystem};
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
use std::future::Future;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};

pub(crate) struct FuseApiHandle {
    local_fs: Box<dyn IFileSystem>,
    default_ttl: Duration,
    fs_context: FileSystemContext,
}

impl FuseApiHandle {
    pub fn new(fs: Box<dyn IFileSystem>, context: FileSystemContext) -> Self {
        FuseApiHandle {
            local_fs: fs,
            default_ttl: Duration::from_secs(1),
            fs_context: context,
        }
    }

    fn update_file_status(
        &self,
        inode: u64,
        size: Option<u64>,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
    ) {
        self.get_modified_file_stat(inode, size, atime, mtime)
            .map(|stat| {
                self.local_fs.update_file_status(inode, &stat);
            });
    }

    fn get_modified_file_stat(
        &self,
        inode: u64,
        size: Option<u64>,
        atime: Option<Timestamp>,
        mtime: Option<Timestamp>,
    ) -> Option<FileStat> {
        let file_stat = self.local_fs.stat(inode);
        match file_stat {
            Some(f) => {
                let mut nf = FileStat::clone(&f);
                size.map(|size| {
                    nf.size = size;
                });
                atime.map(|atime| {
                    nf.atime = atime;
                });
                mtime.map(|mtime| {
                    nf.mtime = mtime;
                });
                Some(nf)
            }
            _ => None,
        }
    }
}

impl Filesystem for FuseApiHandle {
    async fn init(&self, _req: Request) -> fuse3::Result<ReplyInit> {
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
        let file_stat = self.local_fs.lookup(parent, name.to_str().unwrap());

        match file_stat {
            Some(f) => Ok(ReplyEntry {
                ttl: self.default_ttl,
                attr: fstat_to_file_attr(&f, &self.fs_context),
                generation: 0,
            }),
            None => Err(libc::ENOENT.into()),
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: Inode,
        fh: Option<u64>,
        _flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        // check the opened file inode is the same as the inode
        if let Some(f) = fh.and_then(|fh| self.local_fs.get_opened_file(inode, fh)) {
            if f.file_id != inode {
                return Err(libc::EBADF.into());
            }
        }

        let file_stat = self.local_fs.stat(inode);
        match file_stat {
            Some(f) => Ok(ReplyAttr {
                ttl: self.default_ttl,
                attr: fstat_to_file_attr(&f, &self.fs_context),
            }),
            None => Err(libc::ENOENT.into()),
        }
    }

    async fn setattr(
        &self,
        req: Request,
        inode: Inode,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        let new_file_stat =
            self.get_modified_file_stat(inode, set_attr.size, set_attr.atime, set_attr.mtime);
        match new_file_stat {
            Some(stat) => {
                let attr = fstat_to_file_attr(&stat, &self.fs_context);
                self.local_fs.set_attr(inode, &stat);
                Ok(ReplyAttr {
                    ttl: self.default_ttl,
                    attr: attr,
                })
            }
            None => Err(libc::ENOENT.into()),
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
        let file_handle = self.local_fs.create_dir(parent, name.to_str().unwrap());
        match file_handle {
            Ok(fh) => Ok(ReplyEntry {
                ttl: self.default_ttl,
                attr: dummy_file_attr(
                    fh.file_id,
                    Directory,
                    Timestamp::from(SystemTime::now()),
                    &self.fs_context,
                ),
                generation: 0,
            }),
            Err(e) => Err(e),
        }
    }

    async fn open(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let file_handle = self.local_fs.open_file(inode);
        match file_handle {
            Ok(fh) => Ok(ReplyOpen {
                fh: fh.handle_id,
                flags: flags,
            }),
            Err(e) => Err(e),
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
        let mut reader = self.local_fs.read(inode, fh);
        let data = reader.read(offset, size);
        self.update_file_status(inode, None, Some(Timestamp::from(SystemTime::now())), None);
        Ok(ReplyData { data: data.into() })
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
        let mut writer = self.local_fs.write(inode, fh);
        writer.write(offset, data);
        let file_size = writer.file().size;
        let now = Timestamp::from(SystemTime::now());
        self.update_file_status(inode, Some(file_size), Some(now), Some(now));

        Ok(ReplyWrite {
            written: data.len() as u32,
        })
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

    async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> fuse3::Result<ReplyOpen> {
        let file_handle = self.local_fs.open_file(inode);
        match file_handle {
            Ok(fh) => Ok(ReplyOpen {
                fh: fh.handle_id,
                flags: flags,
            }),
            Err(e) => Err(Errno::from(e)),
        }
    }

    type DirEntryStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntry>>;

    async fn readdir<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let current = self.local_fs.stat(parent);
        let current = match current {
            Some(file) => file,
            None => return Err(libc::ENOENT.into()),
        };

        let files = self.local_fs.read_dir(parent);
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

    async fn create(
        &self,
        req: Request,
        parent: Inode,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        let file_handle = self.local_fs.create_file(parent, name.to_str().unwrap());
        match file_handle {
            Ok(fh) => Ok(ReplyCreated {
                ttl: self.default_ttl,
                attr: dummy_file_attr(
                    fh.file_id,
                    RegularFile,
                    Timestamp::from(SystemTime::now()),
                    &self.fs_context,
                ),
                generation: 0,
                fh: fh.handle_id,
                flags: flags,
            }),
            Err(e) => Err(Errno::from(e)),
        }
    }

    async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let result = self.local_fs.remove_file(parent, name.to_str().unwrap());
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Errno::from(e)),
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
        self.local_fs.close_file(inode, fh);
        Ok(())
    }

    async fn releasedir(
        &self,
        req: Request,
        inode: Inode,
        fh: u64,
        flags: u32,
    ) -> fuse3::Result<()> {
        self.local_fs.close_file(inode, fh);
        Ok(())
    }

    async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> fuse3::Result<()> {
        let result = self.local_fs.remove_dir(parent, name.to_str().unwrap());
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Errno::from(e)),
        }
    }

    type DirEntryPlusStream<'a> = BoxStream<'a, fuse3::Result<DirectoryEntryPlus>>;

    async fn readdirplus<'a>(
        &'a self,
        req: Request,
        parent: Inode,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> fuse3::Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        let current = self.local_fs.stat(parent);
        let current = match current {
            Some(file) => file,
            None => return Err(libc::ENOENT.into()),
        };

        let files = self.local_fs.read_dir(parent);
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
