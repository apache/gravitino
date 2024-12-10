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
use crate::filesystem::{FileStat, Result};
use crate::utils::join_file_path;
use std::collections::HashMap;

/// DefaultFileSystemMetadata is a simple implementation of FileSystemMetadata
/// that stores file metadata in memory.
pub struct DefaultFileSystemMetadata {
    // file_stat_map stores the metadata of all files.
    file_stat_map: HashMap<u64, FileStat>,

    // dir_name_map stores the inode of a directory by its full path of mount point.
    dir_name_map: HashMap<String, u64>,

    // dir_child_map stores the children of a directory.
    dir_child_map: HashMap<u64, HashMap<String, u64>>,
}

impl DefaultFileSystemMetadata {
    pub const ROOT_DIR_PARENT_FILE_ID: u64 = 0;
    pub const ROOT_DIR_NAME: &'static str = "";
    pub const ROOT_DIR_FILE_ID: u64 = 1;
    pub const FS_META_FILE_NAME: &'static str = ".gvfs_meta";

    pub fn new() -> Self {
        Self {
            file_stat_map: Default::default(),
            dir_name_map: Default::default(),
            dir_child_map: Default::default(),
        }
    }
}

impl DefaultFileSystemMetadata {
    pub(crate) fn get_file_path(&self, file_id: u64) -> String {
        self.file_stat_map
            .get(&file_id)
            .map(|x| x.path.clone())
            .unwrap_or_else(|| "".to_string())
    }

    pub(crate) fn put_file(&mut self, file_stat: &FileStat) {
        self.file_stat_map
            .insert(file_stat.inode, FileStat::clone(&file_stat));
        self.dir_child_map
            .entry(file_stat.parent_inode)
            .or_insert(HashMap::new())
            .insert(file_stat.name.clone(), file_stat.inode);
    }

    pub(crate) fn init_root_dir(&mut self) -> FileStat {
        let mut file_stat = FileStat::new_dir("", "");
        file_stat.inode = DefaultFileSystemMetadata::ROOT_DIR_FILE_ID;
        file_stat.parent_inode = DefaultFileSystemMetadata::ROOT_DIR_PARENT_FILE_ID;

        self.file_stat_map
            .insert(file_stat.inode, FileStat::clone(&file_stat));
        self.dir_child_map.insert(file_stat.inode, HashMap::new());

        self.dir_name_map
            .insert(file_stat.name.clone(), file_stat.inode);
        file_stat
    }

    pub(crate) fn put_dir(&mut self, file_stat: &FileStat) {
        self.file_stat_map
            .insert(file_stat.inode, FileStat::clone(&file_stat));
        self.dir_child_map.insert(file_stat.inode, HashMap::new());
        self.dir_child_map
            .get_mut(&file_stat.parent_inode)
            .unwrap()
            .insert(file_stat.name.clone(), file_stat.inode);

        let _ = self
            .file_stat_map
            .get(&file_stat.parent_inode)
            .unwrap()
            .clone();
        self.dir_name_map
            .insert(file_stat.path.clone(), file_stat.inode);
    }

    pub(crate) fn get_file(&self, inode: u64) -> Option<FileStat> {
        self.file_stat_map.get(&inode).map(|x| FileStat::clone(x))
    }

    pub(crate) fn find_file(&self, parent_inode: u64, name: &str) -> Option<FileStat> {
        self.dir_child_map
            .get(&parent_inode)
            .and_then(|x| x.get(name))
            .and_then(|x| self.get_file(*x))
    }

    pub(crate) fn get_dir_childs(&self, inode: u64) -> Vec<FileStat> {
        self.dir_child_map
            .get(&inode)
            .map(|child_map| {
                child_map
                    .iter()
                    .filter_map(|entry| self.file_stat_map.get(entry.1).map(|stat| stat.clone()))
                    .collect()
            })
            .unwrap_or_else(Vec::new)
    }

    pub(crate) fn update_file(&mut self, file_id: u64, file_stat: &FileStat) {
        self.file_stat_map
            .insert(file_id, FileStat::clone(file_stat));
    }

    pub(crate) fn remove_file(&mut self, parent_file_id: u64, name: &str) -> Result<()> {
        let dir_map = self.dir_child_map.get_mut(&parent_file_id);
        if let Some(dir_map) = dir_map {
            if let Some(file_id) = dir_map.remove(name) {
                self.file_stat_map.remove(&file_id);
                Ok(())
            } else {
                Err(libc::ENOENT.into())
            }
        } else {
            Err(libc::ENOENT.into())
        }
    }

    pub(crate) fn remove_dir(&mut self, parent_file_id: u64, name: &str) -> Result<()> {
        if let Some(dir_file) = self.find_file(parent_file_id, name) {
            if let Some(dir_child_map) = self.dir_child_map.get_mut(&dir_file.inode) {
                if dir_child_map.is_empty() {
                    self.dir_child_map.remove(&dir_file.inode);
                    self.file_stat_map.remove(&dir_file.inode);
                    let full_name = join_file_path(&dir_file.path, name);
                    self.dir_name_map.remove(&full_name);
                    return Ok(());
                } else {
                    return Err(libc::ENOTEMPTY.into());
                }
            }
        }
        Err(libc::ENOENT.into())
    }
}
