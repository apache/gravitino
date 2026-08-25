--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
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

ALTER TABLE `user_meta` ADD COLUMN `external_id` VARCHAR(256) DEFAULT NULL COMMENT 'external identifier from an upstream identity system' AFTER `metalake_id`;
ALTER TABLE `user_meta` ADD COLUMN `enabled` TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'whether the user is enabled, 0 is disabled, 1 is enabled' AFTER `external_id`;

ALTER TABLE `group_meta` ADD COLUMN `external_id` VARCHAR(256) DEFAULT NULL COMMENT 'external identifier from an upstream identity system' AFTER `metalake_id`;

CREATE UNIQUE INDEX IF NOT EXISTS `uk_mid_ueid_del` ON `user_meta` (`metalake_id`, `external_id`, `deleted_at`);
CREATE UNIQUE INDEX IF NOT EXISTS `uk_mid_geid_del` ON `group_meta` (`metalake_id`, `external_id`, `deleted_at`);

ALTER TABLE `table_column_version_info`
    ALTER COLUMN `column_comment` VARCHAR(4096) DEFAULT '';

ALTER TABLE `tag_meta` ADD COLUMN `allowed_values` CLOB DEFAULT NULL COMMENT 'tag allowed values as a JSON string array, NULL allows any value, [] allows no value' AFTER `properties`;

ALTER TABLE `tag_relation_meta` DROP INDEX `uk_ti_mi_del`;

ALTER TABLE `tag_relation_meta` ADD COLUMN `tag_value` VARCHAR(256) NOT NULL DEFAULT '' COMMENT 'tag assignment value, empty string means no value' AFTER `metadata_object_type`;

ALTER TABLE `idp_group_meta` ADD COLUMN `group_comment` VARCHAR(1024) DEFAULT '' COMMENT 'idp group comment' AFTER `group_name`;

CREATE UNIQUE INDEX IF NOT EXISTS `uk_ti_mi_mo_tv_del` ON `tag_relation_meta` (`tag_id`, `metadata_object_id`, `metadata_object_type`, `tag_value`, `deleted_at`);
CREATE INDEX IF NOT EXISTS `idx_tid_value` ON `tag_relation_meta` (`tag_id`, `tag_value`);

ALTER TABLE `job_run_meta` ADD COLUMN `job_started_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job started at' AFTER `job_run_status`;

CREATE TABLE IF NOT EXISTS `policy_tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `selector` CLOB DEFAULT NULL COMMENT 'policy tag selector JSON, NULL matches tag presence',
    `audit_info` CLOB NOT NULL COMMENT 'policy tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy tag relation deleted at',
    `tombstone_id` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'unique discriminator for deleted policy tag relations',
    PRIMARY KEY (`id`),
    UNIQUE KEY `policy_tag_relation_meta_uk_pid_tid_del` (`policy_id`, `tag_id`, `tombstone_id`),
    KEY `policy_tag_relation_meta_idx_tag_id` (`tag_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `semantic_model_meta` (
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `audit_info` CLOB NOT NULL COMMENT 'semantic model identity audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'last allocated version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'semantic model deleted at',
    PRIMARY KEY (`semantic_model_id`),
    UNIQUE KEY `uk_sid_smn_del` (`schema_id`, `semantic_model_name`, `deleted_at`),
    KEY `idx_smm_mid` (`metalake_id`),
    KEY `idx_smm_cid` (`catalog_id`)
) ENGINE=InnoDB COMMENT 'semantic model metadata';

CREATE TABLE IF NOT EXISTS `semantic_model_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `version` INT UNSIGNED NOT NULL COMMENT 'semantic model version',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name snapshot',
    `semantic_model_comment` CLOB DEFAULT NULL COMMENT 'semantic model comment snapshot',
    `semantic_model_definition` CLOB NOT NULL COMMENT 'structured definition snapshot (JSON)',
    `properties` CLOB DEFAULT NULL COMMENT 'semantic model properties snapshot (JSON)',
    `audit_info` CLOB NOT NULL COMMENT 'semantic model version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_smid_ver_del` (`semantic_model_id`, `version`, `deleted_at`),
    KEY `idx_smvi_mid` (`metalake_id`),
    KEY `idx_smvi_cid` (`catalog_id`),
    KEY `idx_smvi_sid` (`schema_id`)
) ENGINE=InnoDB COMMENT 'semantic model version information';
