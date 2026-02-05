--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file--
--  distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"). You may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

CREATE TABLE IF NOT EXISTS `function_meta` (
    `function_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'function id',
    `function_name` VARCHAR(128) NOT NULL COMMENT 'function name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `function_type` VARCHAR(64) NOT NULL COMMENT 'function type',
    `deterministic` TINYINT(1) DEFAULT 1 COMMENT 'whether the function result is deterministic',
    `return_type` TEXT NOT NULL COMMENT 'function return type',
    `function_current_version` INT UNSIGNED DEFAULT 1 COMMENT 'function current version',
    `function_latest_version` INT UNSIGNED DEFAULT 1 COMMENT 'function latest version',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'function audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'function deleted at',
    PRIMARY KEY (`function_id`),
    UNIQUE KEY `uk_sid_fn_del` (`schema_id`, `function_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'function metadata';

CREATE TABLE IF NOT EXISTS `function_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `function_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'function id',
    `version` INT UNSIGNED NOT NULL COMMENT 'function version',
    `function_comment` TEXT DEFAULT NULL COMMENT 'function version comment',
    `definitions` MEDIUMTEXT NOT NULL COMMENT 'function definitions details',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'function version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'function version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_fid_ver_del` (`function_id`, `version`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`),
    KEY `idx_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'function version info';

CREATE TABLE IF NOT EXISTS `view_meta` (
    `view_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'view id',
    `view_name` VARCHAR(128) NOT NULL COMMENT 'view name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'view current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'view last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'view deleted at',
    PRIMARY KEY (`view_id`),
    UNIQUE KEY `uk_sid_vn_del` (`schema_id`, `view_name`, `deleted_at`),
    KEY `idx_vemid` (`metalake_id`),
    KEY `idx_vecid` (`catalog_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'view metadata';

-- Add partition statistics storage support
CREATE TABLE IF NOT EXISTS `partition_statistic_meta` (
    `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id from table_meta',
    `partition_name` VARCHAR(1024) NOT NULL COMMENT 'partition name',
    `statistic_name` VARCHAR(128) NOT NULL COMMENT 'statistic name',
    `statistic_value` MEDIUMTEXT NOT NULL COMMENT 'statistic value as JSON',
    `audit_info` TEXT NOT NULL COMMENT 'audit information as JSON',
    `created_at` BIGINT(20) UNSIGNED NOT NULL COMMENT 'creation timestamp in milliseconds',
    `updated_at` BIGINT(20) UNSIGNED NOT NULL COMMENT 'last update timestamp in milliseconds',
    PRIMARY KEY (`table_id`, `partition_name`(255), `statistic_name`),
    KEY `idx_table_partition` (`table_id`, `partition_name`(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'partition statistics metadata';
