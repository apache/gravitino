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
use crate::filesystem::{FileReader, FileStat, FileWriter, IFileSystem, OpenedFile};
use fuse3::Errno;

/// The LogFileSystem is used to log the file system operations.
/// It is a wrapper around the actual file system.
/// TODO: Implement logging of file system operations.
pub(crate) struct LogFileSystem {
    inner_fs: Box<dyn IFileSystem>,
}

impl LogFileSystem {
    pub fn new(fs: Box<dyn IFileSystem>) -> Self {
        Self { inner_fs: fs }
    }
}

impl IFileSystem for LogFileSystem {
    fn get_file_path(&self, file_id: u64) -> String {
        self.inner_fs.get_file_path(file_id)
    }

    fn get_opened_file(&self, file_id: u64, fh: u64) -> Option<OpenedFile> {
        self.inner_fs.get_opened_file(file_id, fh)
    }

    fn stat(&self, file_id: u64) -> Option<FileStat> {
        self.inner_fs.stat(file_id)
    }

    fn lookup(&self, parent_file_id: u64, name: &str) -> Option<FileStat> {
        self.inner_fs.lookup(parent_file_id, name)
    }

    fn read_dir(&self, file_id: u64) -> Vec<FileStat> {
        self.inner_fs.read_dir(file_id)
    }

    fn open_file(&self, file_id: u64) -> Result<OpenedFile, Errno> {
        self.inner_fs.open_file(file_id)
    }

    fn create_file(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile, Errno> {
        self.inner_fs.create_file(parent_file_id, name)
    }

    fn create_dir(&self, parent_file_id: u64, name: &str) -> Result<OpenedFile, Errno> {
        self.inner_fs.create_dir(parent_file_id, name)
    }

    fn set_attr(&self, file_id: u64, file_info: &FileStat) -> Result<(), Errno> {
        self.inner_fs.set_attr(file_id, file_info)
    }

    fn update_file_status(&self, file_id: u64, file_stat: &FileStat) {
        self.inner_fs.update_file_status(file_id, file_stat)
    }

    fn read(&self, file_id: u64, fh: u64) -> Box<dyn FileReader> {
        self.inner_fs.read(file_id, fh)
    }

    fn write(&self, file_id: u64, fh: u64) -> Box<dyn FileWriter> {
        self.inner_fs.write(file_id, fh)
    }

    fn remove_file(&self, parent_file_id: u64, name: &str) -> Result<(), Errno> {
        self.inner_fs.remove_file(parent_file_id, name)
    }

    fn remove_dir(&self, parent_file_id: u64, name: &str) -> Result<(), Errno> {
        self.inner_fs.remove_dir(parent_file_id, name)
    }

    fn close_file(&self, file_id: u64, fh: u64) -> Result<(), Errno> {
        self.inner_fs.close_file(file_id, fh)
    }
}

pub(crate) struct LogFileReader {
    pub(crate) inner_reader: Box<dyn FileReader>,
}

impl FileReader for LogFileReader {
    fn file(&self) -> &OpenedFile {
        &self.inner_reader.file()
    }

    fn read(&mut self, offset: u64, size: u32) -> Vec<u8> {
        self.inner_reader.read(offset, size)
    }
}

pub(crate) struct LogFileWriter {
    pub(crate) inner_writer: Box<dyn FileWriter>,
}

impl FileWriter for LogFileWriter {
    fn file(&self) -> &OpenedFile {
        &self.inner_writer.file()
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> u32 {
        self.inner_writer.write(offset, data)
    }
}
