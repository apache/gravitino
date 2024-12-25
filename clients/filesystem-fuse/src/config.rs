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
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub fuse: FuseConfig,
    pub filesystem: FilesystemConfig,
    pub gravitino: GravitinoConfig,
    pub extent_config: HashMap<String, String>,
}

impl Config {
    pub fn from_file(file: &str) -> Config {
        let config_content = std::fs::read_to_string(file).unwrap();
        let config = toml::from_str::<Config>(&config_content).unwrap();
        config
    }

    pub fn default() -> Config {
        Config {
            fuse: FuseConfig {
                default_mask: 0o600,
                fs_type: "memory".to_string(),
                properties: HashMap::new(),
            },
            filesystem: FilesystemConfig { block_size: 4096 },
            gravitino: GravitinoConfig {
                gravitino_url: "http://localhost:8090".to_string(),
                metalake: "test".to_string(),
            },
            extent_config: HashMap::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct FuseConfig {
    pub default_mask: u32,
    pub fs_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct FilesystemConfig {
    pub block_size: u32,
}

#[derive(Debug, Deserialize)]
pub struct GravitinoConfig {
    pub gravitino_url: String,
    pub metalake: String,
}

#[cfg(test)]
mod test {
    use crate::config::Config;

    #[test]
    fn test_config_from_file() {
        let config = Config::from_file("conf/gvfs_test.toml");
        assert_eq!(config.fuse.default_mask, 0o600);
        assert_eq!(config.filesystem.block_size, 8192);
        assert_eq!(config.gravitino.gravitino_url, "http://localhost:8090");
        assert_eq!(config.gravitino.metalake, "test");
        assert_eq!(
            config.extent_config.get("access_key"),
            Some(&"XXX_access_key".to_string())
        );
        assert_eq!(
            config.extent_config.get("secret_key"),
            Some(&"XXX_secret_key".to_string())
        );
    }
}
