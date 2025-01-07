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
use crate::filesystem::{FileReader, FileStat, FileWriter, Result};
use bytes::Bytes;
use fuse3::{Errno, Timestamp};
use std::time::SystemTime;

/// Opened file for read or write, it is used to read or write the file content.
pub(crate) struct OpenedFile {
    pub(crate) file_stat: FileStat,

    pub(crate) handle_id: u64,

    pub reader: Option<Box<dyn FileReader>>,

    pub writer: Option<Box<dyn FileWriter>>,
}

impl OpenedFile {
    pub(crate) fn new(file_stat: FileStat) -> Self {
        OpenedFile {
            file_stat: file_stat,
            handle_id: 0,
            reader: None,
            writer: None,
        }
    }

    pub(crate) async fn read(&mut self, offset: u64, size: u32) -> Result<Bytes> {
        let reader = self.reader.as_mut().ok_or(Errno::from(libc::EBADF))?;
        let result = reader.read(offset, size).await?;

        // update the atime
        self.file_stat.atime = Timestamp::from(SystemTime::now());

        Ok(result)
    }

    pub(crate) async fn write(&mut self, offset: u64, data: &[u8]) -> Result<u32> {
        let writer = self.writer.as_mut().ok_or(Errno::from(libc::EBADF))?;
        let written = writer.write(offset, data).await?;

        // update the file size ,mtime and atime
        let end = offset + written as u64;
        if end > self.file_stat.size {
            self.file_stat.size = end;
        }
        self.file_stat.atime = Timestamp::from(SystemTime::now());
        self.file_stat.mtime = self.file_stat.atime;

        Ok(written)
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        let mut errors = Vec::new();
        if let Some(mut reader) = self.reader.take() {
            if let Err(e) = reader.close().await {
                errors.push(e);
            }
        }

        if let Some(mut writer) = self.writer.take() {
            if let Err(e) = self.flush().await {
                errors.push(e);
            }
            if let Err(e) = writer.close().await {
                errors.push(e);
            }
        }

        if !errors.is_empty() {
            return Err(errors.remove(0));
        }
        Ok(())
    }

    pub(crate) async fn flush(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush().await?;
        }
        Ok(())
    }

    pub(crate) fn file_handle(&self) -> FileHandle {
        debug_assert!(self.handle_id != 0);
        debug_assert!(self.file_stat.file_id != 0);
        FileHandle {
            file_id: self.file_stat.file_id,
            handle_id: self.handle_id,
        }
    }

    pub(crate) fn set_file_id(&mut self, parent_file_id: u64, file_id: u64) {
        debug_assert!(file_id != 0 && parent_file_id != 0);
        self.file_stat.set_file_id(parent_file_id, file_id)
    }
}

// FileHandle is the file handle for the opened file.
pub(crate) struct FileHandle {
    pub(crate) file_id: u64,

    pub(crate) handle_id: u64,
}

// OpenFileFlags is the open file flags for the file system.
pub(crate) struct OpenFileFlags(pub(crate) u32);

impl OpenFileFlags {
    pub fn is_read(&self) -> bool {
        (self.0 & libc::O_WRONLY as u32) == 0
    }

    pub fn is_write(&self) -> bool {
        (self.0 & libc::O_WRONLY as u32) != 0 || (self.0 & libc::O_RDWR as u32) != 0
    }

    pub fn is_append(&self) -> bool {
        (self.0 & libc::O_APPEND as u32) != 0
    }

    pub fn is_create(&self) -> bool {
        (self.0 & libc::O_CREAT as u32) != 0
    }

    pub fn is_truncate(&self) -> bool {
        (self.0 & libc::O_TRUNC as u32) != 0
    }

    pub fn is_exclusive(&self) -> bool {
        (self.0 & libc::O_EXCL as u32) != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filesystem::FileStat;
    use std::path::Path;

    #[test]
    fn test_open_file() {
        let mut open_file = OpenedFile::new(FileStat::new_file_filestat(
            Path::new("a"),
            "b".as_ref(),
            10,
        ));
        assert_eq!(open_file.file_stat.name, "b");
        assert_eq!(open_file.file_stat.size, 10);

        open_file.set_file_id(1, 2);

        assert_eq!(open_file.file_stat.file_id, 2);
        assert_eq!(open_file.file_stat.parent_file_id, 1);
    }
}
