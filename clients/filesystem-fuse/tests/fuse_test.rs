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
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{fs, panic, process};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

struct FuseTest {
    runtime: Arc<Runtime>,
    mount_point: String,
    gvfs_mount: Option<JoinHandle<fuse3::Result<()>>>,
}

impl FuseTest {
    pub fn setup(&mut self) {
        info!("Start gvfs fuse server");
        let mount_point = self.mount_point.clone();

        let config = AppConfig::from_file(Some("tests/conf/gvfs_fuse_memory.toml"))
            .expect("Failed to load config");
        self.runtime.spawn(async move {
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
        self.runtime.block_on(async {
            let _ = gvfs_unmount().await;
        });
    }

    fn wait_for_fuse_server_ready(path: &str, timeout: Duration) -> bool {
        let test_file = format!("{}/.gvfs_meta", path);
        let start_time = Instant::now();

        while start_time.elapsed() < timeout {
            if file_exists(&test_file) {
                info!("Fuse server is ready",);
                return true;
            }
            info!("Wait for fuse server ready",);
            sleep(Duration::from_secs(1));
        }
        false
    }
}

impl Drop for FuseTest {
    fn drop(&mut self) {
        info!("Shutdown fuse server");
        self.shutdown();
    }
}

#[test]
fn test_fuse_with_memory_fs() {
    tracing_subscriber::fmt().init();

    let mount_point = "target/gvfs";
    let _ = fs::create_dir_all(mount_point);

    let mut test = FuseTest {
        runtime: Arc::new(Runtime::new().unwrap()),
        mount_point: mount_point.to_string(),
        gvfs_mount: None,
    };

    test.setup();

    let test_dir = Path::new(&test.mount_point).join("test_dir");
    run_tests(&test_dir);
}

fn run_tests(test_dir: &Path) {
    fs::create_dir_all(test_dir).expect("Failed to create test dir");
    test_fuse_filesystem(test_dir);
    test_big_file(test_dir);
    test_open_file_flag(test_dir);
}

#[test]
fn fuse_it_test_fuse() {
    test_enable_with!(RUN_TEST_WITH_FUSE);
    let mount_point = Path::new("target/gvfs");
    let test_dir = mount_point.join("test_dir");

    run_tests(&test_dir);
}

fn test_fuse_filesystem(test_path: &Path) {
    info!("Test startup");
    //test create file
    let test_file = test_path.join("test_create");
    let file = File::create(&test_file).expect("Failed to create file");
    assert!(file.metadata().is_ok(), "Failed to get file metadata");
    assert!(file_exists(&test_file));

    //test write file
    fs::write(&test_file, "read test").expect("Failed to write file");

    //test read file
    let content = fs::read_to_string(&test_file).expect("Failed to read file");
    assert_eq!(content, "read test", "File content mismatch");

    //test delete file
    fs::remove_file(&test_file).expect("Failed to delete file");
    assert!(!file_exists(&test_file));

    //test create directory
    let test_dir = test_path.join("test_dir");
    fs::create_dir(&test_dir).expect("Failed to create directory");

    //test create file in directory
    let test_file = test_path.join("test_dir/test_file");
    let file = File::create(&test_file).expect("Failed to create file");
    assert!(file.metadata().is_ok(), "Failed to get file metadata");

    //test write file in directory
    let test_file = test_path.join("test_dir/test_read");
    fs::write(&test_file, "read test").expect("Failed to write file");

    //test read file in directory
    let content = fs::read_to_string(&test_file).expect("Failed to read file");
    assert_eq!(content, "read test", "File content mismatch");

    //test delete file in directory
    fs::remove_file(&test_file).expect("Failed to delete file");
    assert!(!file_exists(&test_file));

    //test delete directory
    fs::remove_dir_all(&test_dir).expect("Failed to delete directory");
    assert!(!file_exists(&test_dir));

    info!("Success test");
}

#[allow(clippy::needless_range_loop)]
fn test_big_file(test_dir: &Path) {
    if !file_exists(test_dir) {
        fs::create_dir_all(test_dir).expect("Failed to create test dir");
    }

    let test_file = test_dir.join("test_big_file");
    let mut file = File::create(&test_file).expect("Failed to create file");
    assert!(file.metadata().is_ok(), "Failed to get file metadata");
    assert!(file_exists(&test_file));

    let round_size: usize = 1024 * 1024;
    let round: u8 = 10;

    for i in 0..round {
        let mut content = vec![0; round_size];
        for j in 0..round_size {
            content[j] = (i as usize + j) as u8;
        }

        file.write_all(&content).expect("Failed to write file");
    }
    file.flush().expect("Failed to flush file");

    file = File::open(&test_file).expect("Failed to open file");
    for i in 0..round {
        let mut buffer = vec![0; round_size];
        file.read_exact(&mut buffer).unwrap();

        for j in 0..round_size {
            assert_eq!(buffer[j], (i as usize + j) as u8, "File content mismatch");
        }
    }

    fs::remove_file(&test_file).expect("Failed to delete file");
    assert!(!file_exists(&test_file));
}

fn test_open_file_flag(test_dir: &Path) {
    // test open file with read and write create flag
    let file_path = test_dir.join("test_open_file");
    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .open(&file_path)
        .expect("Failed to open file");
    // test file offset is 0
    let offset = file.stream_position().expect("Failed to seek file");
    assert_eq!(offset, 0, "File offset mismatch");

    // test write can be done
    let write_content = "write content";
    file.write_all(write_content.as_bytes())
        .expect("Failed to write file");
    let mut content = vec![0; write_content.len()];

    // test read can be done
    file.seek(SeekFrom::Start(0)).expect("Failed to seek file");
    file.read_exact(&mut content).expect("Failed to read file");
    assert_eq!(content, write_content.as_bytes(), "File content mismatch");

    // test open file with read flag
    let mut file = OpenOptions::new()
        .read(true)
        .open(&file_path)
        .expect("Failed to open file");
    // test reaad can be done
    let mut content = vec![0; write_content.len()];
    file.read_exact(&mut content).expect("Failed to read file");
    assert_eq!(content, write_content.as_bytes(), "File content mismatch");

    // test write can be have error
    let result = file.write_all(write_content.as_bytes());
    if let Err(e) = result {
        assert_eq!(e.to_string(), "Bad file descriptor (os error 9)");
    }

    // test open file with truncate file
    // test file size is not 0
    let old_file_size = file.metadata().expect("Failed to get file metadata").len();
    assert_eq!(old_file_size, write_content.len() as u64);

    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&file_path)
        .expect("Failed to open file");
    // validate file size is 0
    let file_size = file.metadata().expect("Failed to get file metadata").len();
    assert_eq!(file_size, 0, "File size mismatch");
    // validate file offset is 0
    let offset = file.stream_position().expect("Failed to seek file");
    assert_eq!(offset, 0, "File offset mismatch");

    file.write_all(write_content.as_bytes())
        .expect("Failed to write file");

    // test open file with append flag
    let mut file = OpenOptions::new()
        .append(true)
        .open(&file_path)
        .expect("Failed to open file");
    // test append
    file.write_all(write_content.as_bytes())
        .expect("Failed to write file");
    let file_len = file.metadata().expect("Failed to get file metadata").len();
    // validate file size is 2 * write_content.len()
    assert_eq!(
        file_len,
        2 * write_content.len() as u64,
        "File size mismatch"
    );
}

fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::metadata(path).is_ok()
}
