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
mod default_raw_filesystem;
mod filesystem;
mod fuse_api_handle;
mod fuse_server;
mod memory_filesystem;
mod mount;
mod opened_file;
mod opened_file_manager;
mod utils;

pub async fn gvfs_mount() -> fuse3::Result<()> {
    mount::mount().await
}

pub fn gvfs_unmount() {}


#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::path::Path;
    use fuse3::FileType;
    use fuse3::FileType::Directory;
    use libc::thread_identifier_info;
    use crate::filesystem::{FileStat, PathFileSystem, INITIAL_FILE_ID, ROOT_DIR_FILE_ID, ROOT_DIR_PARENT_FILE_ID, ROOT_DIR_PATH};
    use crate::memory_filesystem::MemoryFileSystem;
    use crate::opened_file::OpenFileFlags;

    #[tokio::test]
    async fn test_memory_file_system() {
        let fs = MemoryFileSystem::new().await;
        test_file_system(&fs).await;
    }

    async fn test_file_system(fs : &impl PathFileSystem) {
        let mut test_file_stats = HashSet::new();
        // test root file
        let root_dir_path = Path::new("/");
        let root_file_stat = fs.stat(root_dir_path).await;
        assert_eq!(root_file_stat.is_ok(), true);
        let root_file_stat = root_file_stat.unwrap();
        assert_file_stat(&root_file_stat,root_dir_path, Directory, 0);
        test_file_stats.push(root_file_stat);

        // test meta file
        let meta_file_path = Path::new("/.gvfs_meta");
        let meta_file_stat = fs.stat(meta_file_path).await;
        assert_eq!(meta_file_stat.is_ok(), true);
        let meta_file_stat = meta_file_stat.unwrap();
        assert_file_stat(&meta_file_stat, meta_file_path, FileType::RegularFile, 0);
        test_file_stats.push(meta_file_stat);

        // test create file
        let file_path = Path::new("/file1.txt");
        let opened_file = fs.create_file(file_path, OpenFileFlags(0)).await;
        assert_eq!(opened_file.is_ok(), true);
        let file = opened_file.unwrap();
        assert!(file.handle_id > 0);
        assert_file_stat(&file.file_stat, file_path, FileType::RegularFile, 0);
        test_file_stats.push(file.file_stat.clone());

        // test create dir
        let dir_path = Path::new("/dir1");
        let dir_stat = fs.create_dir(dir_path).await;
        assert_eq!(dir_stat.is_ok(), true);
        let dir_stat = dir_stat.unwrap();
        assert_file_stat(dir_stat, dir_path, FileType::Directory, 0);
        test_file_stats.push(dir_path);

        // test list dir
        let list_dir = fs.read_dir(Path::new("/")).await;
        assert_eq!(list_dir.is_ok(), true);
        let list_dir = list_dir.unwrap();
        assert_eq!(list_dir.len(), test_file_stats.len());
        for file_stat in list_dir {
            assert!(test_file_stats.contains(file_stat.path));
            let actual_file_stat = test_file_stats.get(&file_stat.path).unwrap();
            assert_file_stat(&file_stat, actual_file_stat.path, actual_file_stat.kind, actual_file_stat.size);
        }

        // test remove file
        let remove_file = fs.remove_file(file_path).await;
        assert_eq!(remove_file.is_ok(), true);

        // test remove dir
        let remove_dir = fs.remove_dir(dir_path).await;
        assert_eq!(remove_dir.is_ok(), true);

        // test list dir
        let list_dir = fs.read_dir(Path::new("/")).await;
        assert_eq!(list_dir.is_ok(), true);

        let list_dir = list_dir.unwrap();
        assert_eq!(list_dir.len(), test_file_stats.len() - 2);
        for file_stat in list_dir {
            assert!(test_file_stats.contains(file_stat.path));
            let actual_file_stat = test_file_stats.get(&file_stat.path).unwrap();
            assert_file_stat(&file_stat, actual_file_stat.path, actual_file_stat.kind, actual_file_stat.size);
        }

        // test file not found
        let not_found_file = fs.stat(Path::new("/not_found.txt")).await;
        assert_eq!(not_found_file.is_err(), true);

    }

    fn assert_file_stat(file_stat: &FileStat, path: &Path, kind: FileType, size: u64) {
        assert_eq!(file_stat.path, path);
        assert_eq!(file_stat.kind, kind);
        assert_eq!(file_stat.size, size);
        if file_stat.file_id == 1 {
            // root dir
            assert_eq!(file_stat.parent_file_id, ROOT_DIR_PARENT_FILE_ID);
        } else {
            assert!(file_stat.file_id >= INITIAL_FILE_ID);
            assert!(file_stat.parent_file_id >= INITIAL_FILE_ID);
        }
    }

}