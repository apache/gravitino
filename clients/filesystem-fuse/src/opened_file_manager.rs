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
use crate::filesystem::OpenedFile;
use dashmap::DashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::sync::Mutex;

// OpenedFileManager is a manager for opened files.
pub(crate) struct OpenedFileManager {
    // file_handle_map is a map of file_handle_id to opned file.
    file_handle_map: DashMap<u64, Arc<Mutex<OpenedFile>>>,

    // file_handle_id_generator is used to generate unique file handle IDs.
    handle_id_generator: AtomicU64,
}

impl OpenedFileManager {
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

    pub(crate) fn put_file(&self, mut file: OpenedFile) -> Arc<Mutex<OpenedFile>> {
        let file_handle_id = self.next_handle_id();
        file.handle_id = file_handle_id;
        let file_handle = Arc::new(Mutex::new(file));
        self.file_handle_map
            .insert(file_handle_id, file_handle.clone());
        file_handle
    }

    pub(crate) fn get_file(&self, handle_id: u64) -> Option<Arc<Mutex<OpenedFile>>> {
        self.file_handle_map
            .get(&handle_id)
            .map(|x| x.value().clone())
    }

    pub(crate) fn remove_file(&self, handle_id: u64) -> Option<Arc<Mutex<OpenedFile>>> {
        self.file_handle_map.remove(&handle_id).map(|x| x.1)
    }
}
