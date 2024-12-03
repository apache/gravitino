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
use crate::filesystem::FileStat;
use fuse3::Errno;
use std::collections::HashMap;

/// IFileSystemMetadata is an interface that defines the operations to manage file metadata.
pub trait IFileSystemMetadata {

    fn add_file(&mut self, parent_inode: u64, name :&str) -> FileStat;

    fn add_file_with_name(&mut self, name :&str, parent: &str) -> FileStat;

    fn add_root_dir(&mut self) -> FileStat;

    fn add_dir(&mut self, parent_inode: u64, name :&str) -> FileStat;

    fn add_dir_with_name(&mut self, parent :&str, name: &str) -> FileStat;

    fn get_file(&self, inode: u64) -> Option<FileStat>;

    fn get_file_from_dir(&self, parent_inode:u64,  name: &str) -> Option<FileStat>;

    fn get_dir_childs(&self, inode: u64) -> Vec<FileStat>;

    fn update_file_stat(&mut self, file_id: u64, file_stat: &FileStat);

    fn remove_file(&mut self, parent_file_id: u64, name: &str) -> Result<(), Errno>;

    fn remove_dir(&mut self, parent_file_id: u64, name: &str) -> Result<(), Errno>;

    fn get_file_path(&self, file_id: u64) -> String;
}

/// DefaultFileSystemMetadata is a simple implementation of FileSystemMetadata
/// that stores file metadata in memory.
pub struct DefaultFileSystemMetadata {

    // file_stat_map stores the metadata of all files.
    file_stat_map: HashMap<u64, FileStat>,

    // dir_name_map stores the inode of a directory by its full path of mount point.
    dir_name_map: HashMap<String, u64>,

    // dir_child_map stores the children of a directory.
    dir_child_map: HashMap<u64, HashMap<String, u64>>,

    // inode_id_generator is used to generate unique inode IDs.
    inode_id_generator: u64,
}

impl DefaultFileSystemMetadata{
    pub fn new() -> Self {
        Self {
            file_stat_map: Default::default(),
            dir_name_map: Default::default(),
            dir_child_map: Default::default(),
            inode_id_generator: 1000000,
        }
    }

    pub(crate) fn next_inode_id(&mut self) -> u64 {
        self.inode_id_generator += 1;
        self.inode_id_generator
    }
}

impl IFileSystemMetadata for DefaultFileSystemMetadata{

    fn get_file_path(&self, file_id: u64) -> String {
        self.file_stat_map.get(&file_id).map(|x| x.path.clone()).unwrap_or_else(|| "".to_string())
    }

    fn add_file_with_name(&mut self, name :&str, parent: &str) -> FileStat {
        let parent_inode = self.dir_name_map.get(parent).unwrap();
        self.add_file(*parent_inode, name)
    }

    fn add_file(&mut self, parent_inode: u64, name :&str) -> FileStat {
        let file_stat = FileStat::new_file(name, self.next_inode_id(), parent_inode);
        self.file_stat_map.insert(file_stat.inode, FileStat::clone(&file_stat));
        self.dir_child_map.entry(parent_inode).or_insert(HashMap::new()).insert(name.to_string(), file_stat.inode);
        file_stat
    }

    fn add_root_dir(&mut self) -> FileStat {
        let file_stat = FileStat::new_dir("", 1, 0);
        self.file_stat_map.insert(file_stat.inode, FileStat::clone(&file_stat));
        self.dir_child_map.insert(file_stat.inode, HashMap::new());

        self.dir_name_map.insert(file_stat.name.clone(), file_stat.inode);
        file_stat
    }

    fn add_dir(&mut self, parent_inode: u64, name :&str) -> FileStat {
        let file_stat = FileStat::new_dir(name, self.next_inode_id(), parent_inode);
        self.file_stat_map.insert(file_stat.inode, FileStat::clone(&file_stat));
        self.dir_child_map.insert(file_stat.inode, HashMap::new());
        self.dir_child_map.get_mut(&parent_inode).unwrap().insert(name.to_string(), file_stat.inode);


        let parent = self.file_stat_map.get(&parent_inode).unwrap().clone();
        let full_name= format!("{}/{}", parent.path, name);
        self.dir_name_map.insert(full_name, file_stat.inode);
        file_stat
    }

    fn add_dir_with_name(&mut self, parent :&str, name: &str) -> FileStat {
        let parent_inode = self.dir_name_map.get(parent).unwrap().clone();
        self.add_dir(parent_inode, name)
    }

    fn get_file(&self, inode: u64) -> Option<FileStat> {
        self.file_stat_map.get(&inode).map(|x| FileStat::clone(x))
    }

    fn get_file_from_dir(&self, parent_inode:u64,  name: &str) -> Option<FileStat> {
        self.dir_child_map.get(&parent_inode).and_then(|x| x.get(name)).and_then(|x| self.get_file(*x))
    }

    fn get_dir_childs(&self, inode: u64) -> Vec<FileStat> {
        self.dir_child_map
            .get(&inode)
            .map(|child_map| {
                child_map
                    .iter()
                    .filter_map(|entry| {
                        self.file_stat_map
                            .get(entry.1)
                            .map(|stat| stat.clone())
                    })
                    .collect()
            })
            .unwrap_or_else(Vec::new)
    }

    fn update_file_stat(&mut self, file_id: u64, file_stat: &FileStat) {
        self.file_stat_map.insert(file_id, FileStat::clone(file_stat));
    }

    fn remove_file(&mut self, parent_file_id: u64, name: &str) -> Result<(), Errno>{
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

    fn remove_dir(&mut self, parent_file_id: u64, name: &str) -> Result<(), Errno> {
        if let Some(dir_file) = self.get_file_from_dir(parent_file_id, name) {
            if let Some(dir_child_map) = self.dir_child_map.get_mut(&dir_file.inode) {
                if dir_child_map.is_empty() {
                    self.dir_child_map.remove(&dir_file.inode);
                    self.file_stat_map.remove(&dir_file.inode);
                    let full_name = format!("{}/{}", dir_file.path, name);
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