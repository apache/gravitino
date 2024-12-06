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
use crate::filesystem::{FileStat, OpenedFile};
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;

// FileHandleManager is a manager for opened files.
pub(crate) struct FileHandleManager {
    // file_handle_map is a map of file_handle_id to opned file.
    file_handle_map: HashMap<u64, OpenedFile>,

    // file_handle_id_generator is used to generate unique file handle IDs.
    handle_id_generator: AtomicU64,
}

impl FileHandleManager {
    pub fn new() -> Self {
        Self {
            file_handle_map: Default::default(),
            handle_id_generator: AtomicU64::new(1),
        }
    }

    pub(crate) fn next_handle_id(&self) -> u64 {
        self.handle_id_generator
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn create_file(&mut self, file: &FileStat) -> OpenedFile {
        let file_handle = OpenedFile {
            file_id: file.inode,
            path: file.path.clone(),
            handle_id: self.next_handle_id(),
            size: file.size,
        };
        self.file_handle_map
            .insert(file_handle.handle_id, file_handle.clone());
        file_handle
    }

    pub(crate) fn get_file(&self, handle_id: u64) -> Option<OpenedFile> {
        self.file_handle_map.get(&handle_id).map(|x| x.clone())
    }

    pub(crate) fn remove_file(&mut self, handle_id: u64) {
        self.file_handle_map.remove(&handle_id);
    }
}
