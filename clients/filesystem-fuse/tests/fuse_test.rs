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

use gvfs_fuse::fuse_server::FuseServer;
use log::info;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

struct FuseTest {
    server: Arc<FuseServer>,
    runtime: Arc<Runtime>,
    mount_point: String,
}

impl FuseTest {
    pub fn setup(&self) {
        info!("Start gvfs fuse server");
        let svr = self.server.clone();
        self.runtime.spawn(async move { svr.start().await });

        let success = Self::wait_for_fuse_server_ready(&self.mount_point, Duration::from_secs(15));
        assert!(success, "Fuse server cannot start up at 15 seconds");
    }

    pub fn shutdown(&self) {
        self.runtime.block_on(async {
            let _ = self.server.stop().await;
        });
    }

    fn wait_for_fuse_server_ready(path: &str, timeout: Duration) -> bool {
        let test_file = format!("{}/.gvfs_meta", path);
        let start_time = Instant::now();

        while start_time.elapsed() < timeout {
            if let Ok(exists) = fs::exists(&test_file) {
                info!("Wait for fuse server ready: {}", exists);
                if exists {
                    return true;
                }
            }
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
fn test_fuse_system_with_auto() {
    tracing_subscriber::fmt()
        .with_env_filter("gvfs_fuse=debug,info")
        .init();

    let mount_point = "build/gvfs";
    let _ = fs::create_dir_all(mount_point);

    let test = FuseTest {
        server: Arc::new(FuseServer::new(mount_point)),
        runtime: Arc::new(Runtime::new().expect("")),
        mount_point: mount_point.to_string(),
    };
    test.setup();

    test_fuse_filesystem(mount_point);
}

fn test_fuse_system_with_manual() {
    test_fuse_filesystem("build/gvfs");
}

fn test_fuse_filesystem(mount_point: &str) {
    info!("Test startup");
    let base_path = Path::new(mount_point);

    //test create file
    let test_file = base_path.join("test_create");
    let file = File::create(&test_file).expect("Failed to create file");
    assert!(file.metadata().is_ok(), "Failed to get file metadata");
    assert!(fs::exists(&test_file).expect("File is not created"));

    //test write file
    fs::write(&test_file, "read test").expect("Failed to write file");

    //test read file
    let content = fs::read_to_string(test_file.clone()).expect("Failed to read file");
    assert_eq!(content, "read test", "File content mismatch");

    //test delete file
    fs::remove_file(test_file.clone()).expect("Failed to delete file");
    assert!(!fs::exists(test_file).expect("File is not deleted"));

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
    assert!(!fs::exists(&test_file).expect("File is not deleted"));

    //test delete directory
    fs::remove_dir_all(&test_dir).expect("Failed to delete directory");
    assert!(!fs::exists(&test_dir).expect("Directory is not deleted"));

    info!("Success test")
}
