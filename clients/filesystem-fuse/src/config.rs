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
use crate::error::ErrorCode::{ConfigNotFound, InvalidConfig};
use crate::utils::GvfsResult;
use config::{builder, Config};
use log::{info, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;

const FUSE_DEFAULT_FILE_MASK: ConfigEntity<u32> = ConfigEntity::new(
    FuseConfig::MODULE_NAME,
    "default_file_mask",
    "The default file mask for the FUSE filesystem",
    0o600,
);

const FUSE_DEFAULT_DIR_MASK: ConfigEntity<u32> = ConfigEntity::new(
    FuseConfig::MODULE_NAME,
    "default_dir_mask",
    "The default directory mask for the FUSE filesystem",
    0o700,
);

const FUSE_FS_TYPE: ConfigEntity<&'static str> = ConfigEntity::new(
    FuseConfig::MODULE_NAME,
    "fs_type",
    "The type of the FUSE filesystem",
    "memory",
);

const FUSE_CONFIG_PATH: ConfigEntity<&'static str> = ConfigEntity::new(
    FuseConfig::MODULE_NAME,
    "config_path",
    "The path of the FUSE configuration file",
    "/etc/gvfs/gvfs.toml",
);

const FILESYSTEM_BLOCK_SIZE: ConfigEntity<u32> = ConfigEntity::new(
    FilesystemConfig::MODULE_NAME,
    "block_size",
    "The block size of the gvfs fuse filesystem",
    4096,
);

const GRAVITINO_URL: ConfigEntity<&'static str> = ConfigEntity::new(
    GravitinoConfig::MODULE_NAME,
    "gravitino_url",
    "The URL of the Gravitino server",
    "http://localhost:8090",
);

const GRAVITINO_METALAKE: ConfigEntity<&'static str> = ConfigEntity::new(
    GravitinoConfig::MODULE_NAME,
    "metalake",
    "The metalake of the Gravitino server",
    "",
);

struct ConfigEntity<T: 'static> {
    module: &'static str,
    name: &'static str,
    description: &'static str,
    default: T,
}

impl<T> ConfigEntity<T> {
    const fn new(
        module: &'static str,
        name: &'static str,
        description: &'static str,
        default: T,
    ) -> Self {
        ConfigEntity {
            module: module,
            name: name,
            description: description,
            default: default,
        }
    }
}

enum ConfigValue {
    I32(ConfigEntity<i32>),
    U32(ConfigEntity<u32>),
    String(ConfigEntity<&'static str>),
    Bool(ConfigEntity<bool>),
    Float(ConfigEntity<f64>),
}

struct DefaultConfig {
    configs: HashMap<String, ConfigValue>,
}

impl Default for DefaultConfig {
    fn default() -> Self {
        let mut configs = HashMap::new();

        configs.insert(
            Self::compose_key(FUSE_DEFAULT_FILE_MASK),
            ConfigValue::U32(FUSE_DEFAULT_FILE_MASK),
        );
        configs.insert(
            Self::compose_key(FUSE_DEFAULT_DIR_MASK),
            ConfigValue::U32(FUSE_DEFAULT_DIR_MASK),
        );
        configs.insert(
            Self::compose_key(FUSE_FS_TYPE),
            ConfigValue::String(FUSE_FS_TYPE),
        );
        configs.insert(
            Self::compose_key(FUSE_CONFIG_PATH),
            ConfigValue::String(FUSE_CONFIG_PATH),
        );
        configs.insert(
            Self::compose_key(GRAVITINO_URL),
            ConfigValue::String(GRAVITINO_URL),
        );
        configs.insert(
            Self::compose_key(GRAVITINO_METALAKE),
            ConfigValue::String(GRAVITINO_METALAKE),
        );
        configs.insert(
            Self::compose_key(FILESYSTEM_BLOCK_SIZE),
            ConfigValue::U32(FILESYSTEM_BLOCK_SIZE),
        );

        DefaultConfig { configs }
    }
}

impl DefaultConfig {
    fn compose_key<T>(entity: ConfigEntity<T>) -> String {
        format!("{}.{}", entity.module, entity.name)
    }
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub fuse: FuseConfig,
    #[serde(default)]
    pub filesystem: FilesystemConfig,
    #[serde(default)]
    pub gravitino: GravitinoConfig,
    #[serde(default)]
    pub extent_config: HashMap<String, String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        let builder = Self::crete_default_config_builder();
        let conf = builder
            .build()
            .expect("Failed to build default configuration");
        conf.try_deserialize::<AppConfig>()
            .expect("Failed to deserialize default AppConfig")
    }
}

type ConfigBuilder = builder::ConfigBuilder<builder::DefaultState>;

impl AppConfig {
    fn crete_default_config_builder() -> ConfigBuilder {
        let default = DefaultConfig::default();

        default
            .configs
            .values()
            .fold(
                Config::builder(),
                |builder, config_entity| match config_entity {
                    ConfigValue::I32(entity) => Self::add_config(builder, entity),
                    ConfigValue::U32(entity) => Self::add_config(builder, entity),
                    ConfigValue::String(entity) => Self::add_config(builder, entity),
                    ConfigValue::Bool(entity) => Self::add_config(builder, entity),
                    ConfigValue::Float(entity) => Self::add_config(builder, entity),
                },
            )
    }

    fn add_config<T: Clone + Into<config::Value>>(
        builder: ConfigBuilder,
        entity: &ConfigEntity<T>,
    ) -> ConfigBuilder {
        let name = format!("{}.{}", entity.module, entity.name);
        builder
            .set_default(&name, entity.default.clone().into())
            .unwrap_or_else(|e| panic!("Failed to set default for {}: {}", entity.name, e))
    }

    pub fn from_file(config_file_path: Option<&str>) -> GvfsResult<AppConfig> {
        let builder = Self::crete_default_config_builder();

        let config_path = {
            if config_file_path.is_some() {
                let path = config_file_path.unwrap();
                //check config file exists
                if fs::metadata(path).is_err() {
                    return Err(
                        ConfigNotFound.to_error("The configuration file not found".to_string())
                    );
                }
                info!("Use configuration file: {}", path);
                path
            } else {
                //use default config
                if fs::metadata(FUSE_CONFIG_PATH.default).is_err() {
                    warn!(
                        "The default configuration file is not found, using the default configuration"
                    );
                    return Ok(AppConfig::default());
                } else {
                    warn!(
                        "Use the default config file of {}",
                        FUSE_CONFIG_PATH.default
                    );
                }
                FUSE_CONFIG_PATH.default
            }
        };
        let config = builder
            .add_source(config::File::with_name(config_path).required(true))
            .build();
        if config.is_err() {
            return Err(InvalidConfig.to_error("Failed to build configuration".to_string()));
        }

        let conf = config.unwrap();
        let app_config = conf.try_deserialize::<AppConfig>();

        if app_config.is_err() {
            return Err(InvalidConfig.to_error("Failed to deserialize configuration".to_string()));
        }
        Ok(app_config.unwrap())
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct FuseConfig {
    #[serde(default)]
    pub default_file_mask: u32,
    #[serde(default)]
    pub default_dir_mask: u32,
    #[serde(default)]
    pub fs_type: String,
    #[serde(default)]
    pub config_path: String,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl FuseConfig {
    const MODULE_NAME: &'static str = "fuse";
}

#[derive(Debug, Deserialize, Default)]
pub struct FilesystemConfig {
    #[serde(default)]
    pub block_size: u32,
}

impl FilesystemConfig {
    const MODULE_NAME: &'static str = "filesystem";
}

#[derive(Debug, Deserialize, Default)]
pub struct GravitinoConfig {
    #[serde(default)]
    pub gravitino_url: String,
    #[serde(default)]
    pub metalake: String,
}

impl GravitinoConfig {
    const MODULE_NAME: &'static str = "gravitino";
}

#[cfg(test)]
mod test {
    use crate::config::AppConfig;

    #[test]
    fn test_config_from_file() {
        let config = AppConfig::from_file(Some("tests/conf/gvfs_fuse_test.toml")).unwrap();
        assert_eq!(config.fuse.default_file_mask, 0o600);
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

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.fuse.default_file_mask, 0o600);
        assert_eq!(config.filesystem.block_size, 4096);
        assert_eq!(config.gravitino.gravitino_url, "http://localhost:8090");
        assert_eq!(config.gravitino.metalake, "");
    }
}