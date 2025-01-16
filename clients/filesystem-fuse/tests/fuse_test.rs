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
use std::fs::File;
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

        let config = AppConfig::from_file(Some("tests/conf/gvfs_fuse_memory.toml".to_string()))
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

    panic::set_hook(Box::new(|info| {
        error!("A panic occurred: {:?}", info);
        process::exit(1);
    }));

    let mount_point = "target/gvfs";
    let _ = fs::create_dir_all(mount_point);

    let mut test = FuseTest {
        runtime: Arc::new(Runtime::new().unwrap()),
        mount_point: mount_point.to_string(),
        gvfs_mount: None,
    };

    test.setup();
    test_fuse_filesystem(mount_point);
}

#[test]
fn fuse_it_test_fuse() {
    test_enable_with!(RUN_TEST_WITH_FUSE);

    test_fuse_filesystem("target/gvfs/gvfs_test");
}

fn test_fuse_filesystem(mount_point: &str) {
    info!("Test startup");
    let base_path = Path::new(mount_point);

    if !file_exists(base_path) {
        fs::create_dir_all(base_path).expect("Failed to create test dir");
    }

    //test create file
    let test_file = base_path.join("test_create");
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
    let test_dir = base_path.join("test_dir");
    fs::create_dir(&test_dir).expect("Failed to create directory");

    //test create file in directory
    let test_file = base_path.join("test_dir/test_file");
    let file = File::create(&test_file).expect("Failed to create file");
    assert!(file.metadata().is_ok(), "Failed to get file metadata");

    //test write file in directory
    let test_file = base_path.join("test_dir/test_read");
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

fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    fs::metadata(path).is_ok()
}
