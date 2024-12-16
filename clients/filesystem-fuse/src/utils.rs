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
use crate::error::GravitinoError;
use fuse3::Timestamp;
use std::fmt;
use std::time::SystemTime;

pub type GravitinoResult<T> = Result<T, GravitinoError>;

pub enum StorageFileSystemType {
    S3,
}

impl fmt::Display for StorageFileSystemType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageFileSystemType::S3 => write!(f, "s3"),
        }
    }
}

pub fn join_file_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", parent, name)
    }
}

pub fn timestamp_diff_from_now(timestamp: Timestamp) -> i64 {
    let now = Timestamp::from(SystemTime::now());
    timestamp.sec - now.sec
}

pub fn extract_fileset(path: &str) -> Option<(String, String, String)> {
    let prefix = "gvfs://fileset/";
    if !path.starts_with(prefix) {
        return None;
    }

    let path_without_prefix = &path[prefix.len()..];

    let parts: Vec<&str> = path_without_prefix.split('/').collect();

    if parts.len() < 3 {
        return None;
    }

    let catalog = parts[0].to_string();
    let schema = parts[1].to_string();
    let fileset = parts[2].to_string();

    Some((catalog, schema, fileset))
}

pub fn extract_storage_filesystem(path: &str) -> Option<(StorageFileSystemType, String)> {
    if let Some(pos) = path.find("://") {
        let protocol = &path[..pos];
        let location = &path[pos + 3..];
        match protocol {
            "s3" => Some((StorageFileSystemType::S3, location.to_string())),
            _ => None,
        }
    } else {
        None
    }
}
