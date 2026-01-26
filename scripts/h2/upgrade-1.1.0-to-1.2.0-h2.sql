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
    `return_type` CLOB NOT NULL COMMENT 'function return type',
    `function_current_version` INT UNSIGNED DEFAULT 1 COMMENT 'function current version',
    `function_latest_version` INT UNSIGNED DEFAULT 1 COMMENT 'function latest version',
    `audit_info` CLOB NOT NULL COMMENT 'function audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'function deleted at',
    PRIMARY KEY (`function_id`),
    UNIQUE KEY `uk_sid_fun_del` (`schema_id`, `function_name`, `deleted_at`),
    KEY `idx_funmid` (`metalake_id`),
    KEY `idx_funcid` (`catalog_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `function_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `function_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'function id',
    `version` INT UNSIGNED NOT NULL COMMENT 'function version',
    `function_comment` CLOB DEFAULT NULL COMMENT 'function version comment',
    `definitions` CLOB NOT NULL COMMENT 'function definitions details',
    `audit_info` CLOB NOT NULL COMMENT 'function version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'function version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_funid_ver_del` (`function_id`, `version`, `deleted_at`),
    KEY `idx_funvmid` (`metalake_id`),
    KEY `idx_funvcid` (`catalog_id`),
    KEY `idx_funvsid` (`schema_id`)
) ENGINE=InnoDB;
