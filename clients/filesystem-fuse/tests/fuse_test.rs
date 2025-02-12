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

use fuse3::Errno;
use gvfs_fuse::config::AppConfig;
use gvfs_fuse::RUN_TEST_WITH_FUSE;
use gvfs_fuse::{gvfs_mount, gvfs_unmount, test_enable_with};
use log::{error, info};
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{env, fs};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio::time::interval;

static ASYNC_RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

struct FuseTestEnv {
    mount_point: String,
    gvfs_mount: Option<JoinHandle<fuse3::Result<()>>>,
}

impl FuseTestEnv {
    pub fn setup(&mut self) {
        info!("Start gvfs fuse server");
        let mount_point = self.mount_point.clone();

        let config = AppConfig::from_file(Some("tests/conf/gvfs_fuse_memory.toml".to_string()))
            .expect("Failed to load config");
        ASYNC_RUNTIME.spawn(async move {
            let result = gvfs_mount(&mount_point, "", &config).await;
            if let Err(e) = result {
                error!("Failed to mount gvfs: {:?}", e);
                return Err(Errno::from(libc::EINVAL));
            }
            Ok(())
        });
        let success = Self::wait_for_fuse_server_ready(&self.mount_point, Duration::from_secs(15));
        assert!(success, "Fuse server cannot start up at 15 seconds");
    }

    pub fn shutdown(&mut self) {
        ASYNC_RUNTIME.block_on(async {
            let _ = gvfs_unmount().await;
        });
    }

    fn wait_for_fuse_server_ready(path: &str, timeout: Duration) -> bool {
        let test_file = format!("{}/.gvfs_meta", path);
        AwaitUtil::wait(timeout, Duration::from_millis(500), || {
            file_exists(&test_file)
        })
    }
}

struct AwaitUtil();

impl AwaitUtil {
    pub(crate) fn wait(
        max_wait: Duration,
        poll_interval: Duration,
        check_fn: impl Fn() -> bool + Send,
    ) -> bool {
        ASYNC_RUNTIME.block_on(async {
            let start = Instant::now();
            let mut interval = interval(poll_interval);

            while start.elapsed() < max_wait {
                interval.tick().await;
                if check_fn() {
                    return true;
                }
            }
            false
        })
    }
}

impl Drop for FuseTestEnv {
    fn drop(&mut self) {
        info!("Shutdown fuse server");
        self.shutdown();
    }
}

struct SequenceFileOperationTest {
    test_dir: PathBuf,
    weak_consistency: bool,
}

impl SequenceFileOperationTest {
    fn new(test_dir: &Path) -> Self {
        let args: Vec<String> = env::args().collect();
        let weak_consistency = args.contains(&"weak_consistency".to_string());

        SequenceFileOperationTest {
            test_dir: test_dir.to_path_buf(),
            weak_consistency: weak_consistency,
        }
    }
    fn test_create_file(&self, name: &str, open_options: Option<&OpenOptions>) -> File {
        let path = self.test_dir.join(name);
        let file = {
            match open_options {
                None => File::create(&path)
                    .unwrap_or_else(|_| panic!("Failed to create file: {:?}", path)),
                Some(options) => options.open(&path).unwrap_or_else(|_| {
                    panic!(
                        "Failed to create file: {:?},
                        options {:?}",
                        path, options
                    )
                }),
            }
        };
        let file_metadata = file
            .metadata()
            .unwrap_or_else(|_| panic!("Failed to get file metadata: {:?}", path));
        assert!(file_exists(path));
        if !self.weak_consistency {
            assert_eq!(file_metadata.len(), 0);
        }
        file
    }

    fn test_open_file(&self, name: &str, open_options: Option<&OpenOptions>) -> File {
        let path = self.test_dir.join(name);
        let file = {
            match open_options {
                None => {
                    File::open(&path).unwrap_or_else(|_| panic!("Failed to open file: {:?}", path))
                }
                Some(options) => options.open(&path).unwrap_or_else(|_| {
                    panic!(
                        "Failed to open file: {:?},
                        options {:?}",
                        path, options
                    )
                }),
            }
        };
        let file_metadata = file
            .metadata()
            .unwrap_or_else(|_| panic!("Failed to get file metadata: {:?}", path));
        assert!(file_metadata.is_file());
        assert!(file_exists(path));
        file
    }

    fn test_read_file(&self, file: &mut File, expect: &[u8]) {
        let mut content = vec![0; expect.len()];
        file.read_exact(&mut content).expect("Failed to read file");
        assert_eq!(content, *expect, "File content mismatch");
    }

    fn test_read_data(&self, file: &mut File, len: usize) -> Vec<u8> {
        let mut content = vec![0; len];
        file.read_exact(&mut content).expect("Failed to read file");
        content
    }

    fn test_append_file(&self, file: &mut File, content: &[u8]) {
        let old_len = file.metadata().unwrap().len();
        let size = content.len();
        file.write_all(content).expect("Failed to write file");

        if !self.weak_consistency {
            let new_len = file.metadata().unwrap().len();
            assert_eq!(new_len, old_len + size as u64, "File size mismatch");
        }
    }

    fn test_remove_file(&self, name: &str) {
        let path = self.test_dir.join(name);
        fs::remove_file(&path).unwrap_or_else(|_| panic!("Failed to remove file: {:?}", path));
        assert!(!file_exists(path));
    }

    fn test_create_dir(&self, name: &str) {
        let path = self.test_dir.join(name);
        fs::create_dir(&path).unwrap_or_else(|_| panic!("Failed to create directory: {:?}", path));
        assert!(file_exists(path));
    }

    fn test_list_dir_with_expect(&self, name: &str, expect_childs: &Vec<&str>) {
        self.test_list_dir(name, expect_childs, &vec![]);
    }

    fn test_list_dir_with_unexpected(&self, name: &str, unexpected_childs: &Vec<&str>) {
        self.test_list_dir(name, &vec![], unexpected_childs);
    }

    fn test_list_dir(&self, name: &str, expect_childs: &Vec<&str>, unexpected_childs: &Vec<&str>) {
        let path = self.test_dir.join(name);
        let dir_childs =
            fs::read_dir(&path).unwrap_or_else(|_| panic!("Failed to list directory: {:?}", path));
        let mut childs_set: HashSet<String> = HashSet::default();
        for child in dir_childs {
            let entry = child.expect("Failed to get entry");
            childs_set.insert(entry.file_name().to_string_lossy().to_string());
        }
        for expect_child in expect_childs {
            assert!(
                childs_set.contains(*expect_child),
                "Expect child not found: {}",
                expect_child
            );
        }

        for unexpected_child in unexpected_childs {
            assert!(
                !childs_set.contains(*unexpected_child),
                "Unexpected child found: {}",
                unexpected_child
            );
        }
    }

    fn test_remove_dir(&self, name: &str) {
        let path = self.test_dir.join(name);
        fs::remove_dir(&path).unwrap_or_else(|_| panic!("Failed to remove directory: {:?}", path));
        assert!(!file_exists(path));
    }

    // some file storage can't sync file immediately, so we need to sync file to make sure the file is written to disk
    fn sync_file(&self, file: File, name: &str, expect_len: u64) -> Result<(), ()> {
        if !self.weak_consistency {
            return Ok(());
        }
        drop(file);

        let path = self.test_dir.join(name);
        let success = AwaitUtil::wait(Duration::from_secs(3), Duration::from_millis(200), || {
            let file =
                File::open(&path).unwrap_or_else(|_| panic!("Failed to open file: {:?}", path));
            let file_len = file.metadata().unwrap().len();
            file_len >= expect_len
        });
        if !success {
            return Err(());
        }
        Ok(())
    }

    fn test_basic_filesystem(fs_test: &SequenceFileOperationTest) {
        let file_name1 = "test_create";
        //test create file
        let mut file1 = fs_test.test_create_file(file_name1, None);

        //test write file
        let content = "write test".as_bytes();
        fs_test.test_append_file(&mut file1, content);
        fs_test
            .sync_file(file1, file_name1, content.len() as u64)
            .expect("Failed to sync file");

        //test read file
        let mut file1 = fs_test.test_open_file(file_name1, None);
        fs_test.test_read_file(&mut file1, content);

        //test delete file
        fs_test.test_remove_file(file_name1);

        //test create directory
        let dir_name1 = "test_dir";
        fs_test.test_create_dir(dir_name1);

        //test create file in directory
        let test_file2 = "test_dir/test_file";
        let mut file2 = fs_test.test_create_file(test_file2, None);

        //test write file in directory
        fs_test.test_append_file(&mut file2, content);
        fs_test
            .sync_file(file2, test_file2, content.len() as u64)
            .expect("Failed to sync file");

        //test read file in directory
        let mut file2 = fs_test.test_open_file(test_file2, None);
        fs_test.test_read_file(&mut file2, content);

        //test list directory
        fs_test.test_list_dir_with_expect(dir_name1, &vec!["test_file"]);

        //test delete file in directory
        fs_test.test_remove_file(test_file2);

        //test list directory after delete file
        fs_test.test_list_dir_with_unexpected(dir_name1, &vec!["test_file"]);

        //test delete directory
        fs_test.test_remove_dir(dir_name1);
    }

    #[allow(clippy::needless_range_loop)]
    fn test_big_file(fs_test: &SequenceFileOperationTest) {
        let test_file = "test_big_file";
        let round_size: usize = 1024 * 1024;
        let round: u8 = 1;

        //test write big file
        {
            let mut file = fs_test.test_create_file(test_file, None);

            for i in 0..round {
                let mut content = vec![0; round_size];
                for j in 0..round_size {
                    content[j] = (i as usize + j) as u8;
                }

                fs_test.test_append_file(&mut file, &content);
            }
            fs_test
                .sync_file(file, test_file, round_size as u64 * round as u64)
                .expect("Failed to sync file");
        }

        //test read big file
        {
            let mut file = fs_test.test_open_file(test_file, None);
            for i in 0..round {
                let buffer = fs_test.test_read_data(&mut file, round_size);

                for j in 0..round_size {
                    assert_eq!(buffer[j], (i as usize + j) as u8, "File content mismatch");
                }
            }
        }

        fs_test.test_remove_file(test_file);
    }

    fn test_open_file_flag(fs_test: &SequenceFileOperationTest) {
        let write_content = "write content";
        {
            // test open file with read and write create flag
            let file_name = "test_open_file";
            let mut file = fs_test.test_create_file(
                file_name,
                Some(OpenOptions::new().read(true).write(true).create(true)),
            );

            // test write can be done
            fs_test.test_append_file(&mut file, write_content.as_bytes());

            // test read end of file
            let result = file.read_exact(&mut [1]);
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(e.to_string(), "failed to fill whole buffer");
            }
        }

        {
            // test open file with write flag
            let file_name = "test_open_file2";
            let mut file = fs_test
                .test_create_file(file_name, Some(OpenOptions::new().write(true).create(true)));

            // test write can be done
            fs_test.test_append_file(&mut file, write_content.as_bytes());

            // test read can be have error
            let result = file.read(&mut [0; 10]);
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(e.to_string(), "Bad file descriptor (os error 9)");
            }
        }

        {
            // test open file with read flag
            let file_name = "test_open_file2";
            let mut file = fs_test.test_open_file(file_name, Some(OpenOptions::new().read(true)));

            // test read can be done
            fs_test.test_read_file(&mut file, write_content.as_bytes());

            // test write can be have error
            let result = file.write_all(write_content.as_bytes());
            assert!(result.is_err());
            if let Err(e) = result {
                assert_eq!(e.to_string(), "Bad file descriptor (os error 9)");
            }
        }

        {
            // test open file with truncate file
            let file_name = "test_open_file2";
            let file = fs_test.test_open_file(
                file_name,
                Some(OpenOptions::new().write(true).truncate(true)),
            );

            // test file size is 0
            assert_eq!(file.metadata().unwrap().len(), 0);
        }

        {
            // test open file with append flag
            let file_name = "test_open_file";

            // opendal_fs does not support open and appand
            let result = OpenOptions::new()
                .append(true)
                .open(fs_test.test_dir.join(file_name));
            if let Err(e) = result {
                assert_eq!(e.to_string(), "Invalid argument (os error 22)");
                return;
            }

            let mut file = fs_test.test_open_file(file_name, Some(OpenOptions::new().append(true)));

            assert_eq!(file.metadata().unwrap().len(), write_content.len() as u64);

            // test append
            fs_test.test_append_file(&mut file, write_content.as_bytes());
            let file_len = file.metadata().unwrap().len();
            assert_eq!(file_len, 2 * write_content.len() as u64);
        }
    }
}

fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::metadata(path).is_ok()
}

fn run_tests(test_dir: &Path) {
    fs::create_dir_all(test_dir).expect("Failed to create test dir");

    let fs_test = SequenceFileOperationTest::new(test_dir);

    info!("test_fuse_filesystem started");
    SequenceFileOperationTest::test_basic_filesystem(&fs_test);
    info!("testtest_fuse_filesystem finished");

    info!("test_big_file started");
    SequenceFileOperationTest::test_big_file(&fs_test);
    info!("test_big_file finished");

    info!("test_open_file_flag started");
    SequenceFileOperationTest::test_open_file_flag(&fs_test);
    info!("test_open_file_flag finished");
}

fn test_manually() {
    let mount_point = Path::new("target/gvfs");
    let test_dir = mount_point.join("test_dir");
    run_tests(&test_dir);
}

#[test]
fn fuse_it_test_fuse() {
    test_enable_with!(RUN_TEST_WITH_FUSE);
    tracing_subscriber::fmt().init();

    let mount_point = Path::new("target/gvfs");
    let test_dir = mount_point.join("test_dir");

    run_tests(&test_dir);
}

#[test]
fn test_fuse_with_memory_fs() {
    tracing_subscriber::fmt().init();

    let mount_point = "target/gvfs";
    let _ = fs::create_dir_all(mount_point);

    let mut test = FuseTestEnv {
        mount_point: mount_point.to_string(),
        gvfs_mount: None,
    };

    test.setup();

    let test_dir = Path::new(&test.mount_point).join("test_dir");
    run_tests(&test_dir);
}
