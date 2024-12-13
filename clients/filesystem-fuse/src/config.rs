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
use std::collections::{BTreeMap, HashMap};

pub(crate) struct Config {
    pub(crate) fuse: FuseConfig,
    pub(crate) filesystem: FilesystemConfig,
    pub(crate) gravitino: GravitinoConfig,
    pub(crate) extent_config: HashMap<String, String>,
}

impl Config {
    pub(crate) fn new() -> Config {
        Config {
            fuse: FuseConfig {
                mount_to: "/mnt/gravitino".to_string(),
                mount_from: "/mnt/gravitino".to_string(),
                default_mask: 0o777,
                properties: HashMap::new(),
            },
            filesystem: FilesystemConfig {
                block_size: 4096,
            },
            gravitino: GravitinoConfig {
                gravitino_url: "http://localhost:8080".to_string(),
                metalake: "http://localhost:8080".to_string(),
            },
            extent_config: HashMap::new(),
        }
    }
}

pub(crate) struct  FuseConfig {
    pub(crate) mount_to: String,
    pub(crate) mount_from: String,
    pub(crate) default_mask:u32,
    pub(crate) properties: HashMap<String, String>,
}

pub(crate) struct FilesystemConfig {
    pub(crate) block_size: u32
}

pub(crate) struct GravitinoConfig {
    pub(crate) gravitino_url: String,
    pub(crate) metalake: String,
}