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

ALTER TABLE `table_column_version_info`
    MODIFY COLUMN `column_comment` VARCHAR(4096) DEFAULT '' COMMENT 'column comment';

ALTER TABLE `tag_meta`
    ADD COLUMN `allowed_values` MEDIUMTEXT DEFAULT NULL COMMENT 'tag allowed values as a JSON string array, NULL allows any value, [] allows no value' AFTER `properties`;

ALTER TABLE `tag_relation_meta`
    DROP INDEX `uk_ti_mi_mo_del`;

ALTER TABLE `tag_relation_meta`
    ADD COLUMN `tag_value` VARCHAR(256) NOT NULL DEFAULT '' COMMENT 'tag assignment value, empty string means no value' AFTER `metadata_object_type`;

ALTER TABLE `idp_user_meta`
    ADD COLUMN `enabled` TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'whether the user is enabled, 0 is disabled, 1 is enabled' AFTER `password_hash`;

ALTER TABLE `idp_group_meta`
    ADD COLUMN `group_comment` VARCHAR(1024) DEFAULT '' COMMENT 'idp group comment' AFTER `group_name`;

-- add audit_info as nullable first for MySQL 5.7 compatibility (TEXT cannot have defaults)
ALTER TABLE `idp_user_meta`
    ADD COLUMN `audit_info` MEDIUMTEXT COMMENT 'idp user audit info' AFTER `enabled`;

UPDATE `idp_user_meta`
    SET `audit_info` = '{}'
    WHERE `audit_info` IS NULL;

ALTER TABLE `idp_user_meta`
    MODIFY COLUMN `audit_info` MEDIUMTEXT NOT NULL COMMENT 'idp user audit info' AFTER `enabled`;

ALTER TABLE `idp_group_meta`
    ADD COLUMN `audit_info` MEDIUMTEXT COMMENT 'idp group audit info' AFTER `group_comment`;

UPDATE `idp_group_meta`
    SET `audit_info` = '{}'
    WHERE `audit_info` IS NULL;

ALTER TABLE `idp_group_meta`
    MODIFY COLUMN `audit_info` MEDIUMTEXT NOT NULL COMMENT 'idp group audit info' AFTER `group_comment`;

ALTER TABLE `idp_user_group_rel`
    ADD COLUMN `audit_info` MEDIUMTEXT COMMENT 'idp user group relation audit info' AFTER `group_id`;

UPDATE `idp_user_group_rel`
    SET `audit_info` = '{}'
    WHERE `audit_info` IS NULL;

ALTER TABLE `idp_user_group_rel`
    MODIFY COLUMN `audit_info` MEDIUMTEXT NOT NULL COMMENT 'idp user group relation audit info' AFTER `group_id`;

CREATE UNIQUE INDEX `uk_ti_mi_mo_tv_del` ON `tag_relation_meta` (`tag_id`, `metadata_object_id`, `metadata_object_type`, `tag_value`, `deleted_at`);
CREATE INDEX `idx_tid_value` ON `tag_relation_meta` (`tag_id`, `tag_value`);

-- Index names are only scoped per-table in MySQL, so the same name could be
-- reused across tables. Prefix each reused name with its table name so that
-- every index name is unique across the whole schema. A database upgraded
-- from 1.3.0 ends up with the same index names as a fresh 2.0.0 install.
ALTER TABLE `schema_meta` RENAME INDEX `idx_mid` TO `schema_meta_idx_mid`;
ALTER TABLE `table_meta` RENAME INDEX `uk_sid_tn_del` TO `table_meta_uk_sid_tn_del`;
ALTER TABLE `table_meta` RENAME INDEX `idx_mid` TO `table_meta_idx_mid`;
ALTER TABLE `table_meta` RENAME INDEX `idx_cid` TO `table_meta_idx_cid`;
ALTER TABLE `table_column_version_info` RENAME INDEX `idx_mid` TO `table_column_version_info_idx_mid`;
ALTER TABLE `table_column_version_info` RENAME INDEX `idx_cid` TO `table_column_version_info_idx_cid`;
ALTER TABLE `table_column_version_info` RENAME INDEX `idx_sid` TO `table_column_version_info_idx_sid`;
ALTER TABLE `fileset_meta` RENAME INDEX `uk_sid_fn_del` TO `fileset_meta_uk_sid_fn_del`;
ALTER TABLE `fileset_meta` RENAME INDEX `idx_mid` TO `fileset_meta_idx_mid`;
ALTER TABLE `fileset_meta` RENAME INDEX `idx_cid` TO `fileset_meta_idx_cid`;
ALTER TABLE `fileset_version_info` RENAME INDEX `idx_mid` TO `fileset_version_info_idx_mid`;
ALTER TABLE `fileset_version_info` RENAME INDEX `idx_cid` TO `fileset_version_info_idx_cid`;
ALTER TABLE `fileset_version_info` RENAME INDEX `idx_sid` TO `fileset_version_info_idx_sid`;
ALTER TABLE `topic_meta` RENAME INDEX `uk_sid_tn_del` TO `topic_meta_uk_sid_tn_del`;
ALTER TABLE `topic_meta` RENAME INDEX `idx_mid` TO `topic_meta_idx_mid`;
ALTER TABLE `topic_meta` RENAME INDEX `idx_cid` TO `topic_meta_idx_cid`;
ALTER TABLE `user_role_rel` RENAME INDEX `idx_rid` TO `user_role_rel_idx_rid`;
ALTER TABLE `group_role_rel` RENAME INDEX `idx_rid` TO `group_role_rel_idx_rid`;
ALTER TABLE `tag_relation_meta` RENAME INDEX `idx_mid` TO `tag_relation_meta_idx_mid`;
ALTER TABLE `model_meta` RENAME INDEX `idx_mid` TO `model_meta_idx_mid`;
ALTER TABLE `model_meta` RENAME INDEX `idx_cid` TO `model_meta_idx_cid`;

ALTER TABLE `model_meta`
    ADD COLUMN `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'model current version' AFTER `model_latest_version`,
    ADD COLUMN `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'model last allocated version' AFTER `current_version`;
ALTER TABLE `model_version_info` RENAME INDEX `idx_mid` TO `model_version_info_idx_mid`;
ALTER TABLE `model_version_info` RENAME INDEX `idx_cid` TO `model_version_info_idx_cid`;
ALTER TABLE `model_version_info` RENAME INDEX `idx_sid` TO `model_version_info_idx_sid`;
ALTER TABLE `policy_version_info` RENAME INDEX `idx_mid` TO `policy_version_info_idx_mid`;
ALTER TABLE `policy_relation_meta` RENAME INDEX `idx_mid` TO `policy_relation_meta_idx_mid`;
ALTER TABLE `function_meta` RENAME INDEX `uk_sid_fn_del` TO `function_meta_uk_sid_fn_del`;
ALTER TABLE `function_meta` RENAME INDEX `idx_mid` TO `function_meta_idx_mid`;
ALTER TABLE `function_meta` RENAME INDEX `idx_cid` TO `function_meta_idx_cid`;
ALTER TABLE `function_version_info` RENAME INDEX `idx_mid` TO `function_version_info_idx_mid`;
ALTER TABLE `function_version_info` RENAME INDEX `idx_cid` TO `function_version_info_idx_cid`;
ALTER TABLE `function_version_info` RENAME INDEX `idx_sid` TO `function_version_info_idx_sid`;

-- `table_version_info` had no primary key. `version` and `deleted_at` become
-- NOT NULL and the existing unique key is promoted to the primary key, which
-- also makes it the InnoDB clustered index.
--
-- Both columns are always supplied on insert by TableVersionBaseSQLProvider,
-- so no row should hold NULL. Verify before running if you are unsure:
--   SELECT COUNT(*) FROM `table_version_info`
--    WHERE `version` IS NULL OR `deleted_at` IS NULL;
-- The statement below fails rather than silently coercing NULL to 0.
ALTER TABLE `table_version_info`
    MODIFY COLUMN `version` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table current version',
    MODIFY COLUMN `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'table deletion timestamp, 0 means not deleted',
    DROP INDEX `uk_table_id_version_deleted_at`,
    ADD PRIMARY KEY (`table_id`, `version`, `deleted_at`);

ALTER TABLE `job_run_meta`
    ADD COLUMN `job_started_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job started at' AFTER `job_run_status`;

CREATE TABLE IF NOT EXISTS `policy_tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `selector` MEDIUMTEXT DEFAULT NULL COMMENT 'policy tag selector JSON, NULL matches tag presence',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'policy tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy tag relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `policy_tag_relation_meta_uk_pid_tid_del` (`policy_id`, `tag_id`, `deleted_at`),
    KEY `policy_tag_relation_meta_idx_tag_id` (`tag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy tag relation';

ALTER TABLE `job_run_meta`
    ADD COLUMN `runtime_job_template` MEDIUMTEXT DEFAULT NULL COMMENT 'job run runtime job template' AFTER `job_finished_at`;

CREATE TABLE IF NOT EXISTS `semantic_model_meta` (
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'semantic model identity audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'last allocated version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'semantic model deleted at',
    PRIMARY KEY (`semantic_model_id`),
    UNIQUE KEY `uk_sid_smn_del` (`schema_id`, `semantic_model_name`, `deleted_at`),
    KEY `idx_smm_mid` (`metalake_id`),
    KEY `idx_smm_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'semantic model metadata';

CREATE TABLE IF NOT EXISTS `semantic_model_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `semantic_model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'semantic model id',
    `version` INT UNSIGNED NOT NULL COMMENT 'semantic model version',
    `semantic_model_name` VARCHAR(128) NOT NULL COMMENT 'semantic model name snapshot',
    `semantic_model_comment` TEXT DEFAULT NULL COMMENT 'semantic model comment snapshot',
    `semantic_model_definition` MEDIUMTEXT NOT NULL COMMENT 'structured definition snapshot (JSON)',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'semantic model properties snapshot (JSON)',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'semantic model version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_smid_ver_del` (`semantic_model_id`, `version`, `deleted_at`),
    KEY `idx_smvi_mid` (`metalake_id`),
    KEY `idx_smvi_cid` (`catalog_id`),
    KEY `idx_smvi_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'semantic model version information';
