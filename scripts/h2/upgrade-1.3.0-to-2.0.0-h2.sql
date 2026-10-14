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




-- Preserve policy_relation_meta from pre-2.0 installations, including its existing data.
-- The 2.0 server no longer reads direct object-policy assignments from this table.

ALTER TABLE `table_column_version_info`
    ALTER COLUMN `column_comment` VARCHAR(4096) DEFAULT '';

ALTER TABLE `model_meta` ADD COLUMN IF NOT EXISTS `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'model current version' AFTER `model_latest_version`;
ALTER TABLE `model_meta` ADD COLUMN IF NOT EXISTS `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'model last allocated version' AFTER `current_version`;

ALTER TABLE `tag_meta` ADD COLUMN IF NOT EXISTS `allowed_values` CLOB DEFAULT NULL COMMENT 'tag allowed values as a JSON string array, NULL allows any value, [] allows no value' AFTER `properties`;

ALTER TABLE `tag_relation_meta` DROP CONSTRAINT IF EXISTS `uk_ti_mi_del`;

ALTER TABLE `tag_relation_meta` ADD COLUMN IF NOT EXISTS `tag_value` VARCHAR(256) NOT NULL DEFAULT '' COMMENT 'tag assignment value, empty string means no value' AFTER `metadata_object_type`;

ALTER TABLE `idp_user_meta` ADD COLUMN IF NOT EXISTS `enabled` TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'whether the user is enabled, 0 is disabled, 1 is enabled' AFTER `password_hash`;

ALTER TABLE `idp_group_meta` ADD COLUMN IF NOT EXISTS `group_comment` VARCHAR(1024) DEFAULT '' COMMENT 'idp group comment' AFTER `group_name`;

ALTER TABLE `idp_user_meta` ADD COLUMN IF NOT EXISTS `audit_info` CLOB NOT NULL DEFAULT '{}' COMMENT 'idp user audit info' AFTER `enabled`;

ALTER TABLE `idp_group_meta` ADD COLUMN IF NOT EXISTS `audit_info` CLOB NOT NULL DEFAULT '{}' COMMENT 'idp group audit info' AFTER `group_comment`;

ALTER TABLE `idp_user_group_rel` ADD COLUMN IF NOT EXISTS `audit_info` CLOB NOT NULL DEFAULT '{}' COMMENT 'idp user group relation audit info' AFTER `group_id`;

CREATE UNIQUE INDEX IF NOT EXISTS `uk_ti_mi_mo_tv_del` ON `tag_relation_meta` (`tag_id`, `metadata_object_id`, `metadata_object_type`, `tag_value`, `deleted_at`);
CREATE INDEX IF NOT EXISTS `idx_tid_value` ON `tag_relation_meta` (`tag_id`, `tag_value`);

ALTER TABLE `job_run_meta` ADD COLUMN IF NOT EXISTS `job_started_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job started at' AFTER `job_run_status`;

CREATE TABLE IF NOT EXISTS `policy_tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `selector` CLOB DEFAULT NULL COMMENT 'policy tag selector JSON, NULL matches tag presence',
    `audit_info` CLOB NOT NULL COMMENT 'policy tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy tag relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `policy_tag_relation_meta_uk_pid_tid_del` (`policy_id`, `tag_id`, `deleted_at`),
    KEY `policy_tag_relation_meta_idx_tag_id` (`tag_id`)
) ENGINE=InnoDB;

ALTER TABLE `job_run_meta` ADD COLUMN IF NOT EXISTS `runtime_job_template` CLOB DEFAULT NULL COMMENT 'job run runtime job template' AFTER `job_finished_at`;

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

CREATE TABLE IF NOT EXISTS `scheduled_expand_tasks` (
    `task_name` VARCHAR(100) NOT NULL COMMENT 'task name',
    `task_instance` VARCHAR(100) NOT NULL COMMENT 'task instance',
    `task_data` BLOB COMMENT 'serialized task data',
    `execution_time` TIMESTAMP(6) NOT NULL COMMENT 'next execution time',
    `picked` BOOLEAN NOT NULL COMMENT 'whether the task is currently picked',
    `picked_by` VARCHAR(50) COMMENT 'scheduler instance that picked the task',
    `last_heartbeat` TIMESTAMP(6) COMMENT 'last heartbeat from the picker',
    `version` BIGINT NOT NULL COMMENT 'optimistic lock version',
    `consecutive_failures` INT COMMENT 'consecutive failure count',
    `last_success` TIMESTAMP(6) COMMENT 'last successful execution time',
    `last_failure` TIMESTAMP(6) COMMENT 'last failed execution time',
    PRIMARY KEY (`task_name`, `task_instance`)
) COMMENT='table maintenance expand schedule metadata';
CREATE INDEX IF NOT EXISTS `scheduled_expand_tasks_execution_time_idx` ON `scheduled_expand_tasks`(`execution_time`);
CREATE INDEX IF NOT EXISTS `scheduled_expand_tasks_last_heartbeat_idx` ON `scheduled_expand_tasks`(`last_heartbeat`);

CREATE TABLE IF NOT EXISTS `scheduled_cron_tasks` (
    `task_name` VARCHAR(100) NOT NULL COMMENT 'task name',
    `task_instance` VARCHAR(100) NOT NULL COMMENT 'task instance',
    `task_data` BLOB COMMENT 'serialized task data',
    `execution_time` TIMESTAMP(6) NOT NULL COMMENT 'next execution time',
    `picked` BOOLEAN NOT NULL COMMENT 'whether the task is currently picked',
    `picked_by` VARCHAR(50) COMMENT 'scheduler instance that picked the task',
    `last_heartbeat` TIMESTAMP(6) COMMENT 'last heartbeat from the picker',
    `version` BIGINT NOT NULL COMMENT 'optimistic lock version',
    `consecutive_failures` INT COMMENT 'consecutive failure count',
    `last_success` TIMESTAMP(6) COMMENT 'last successful execution time',
    `last_failure` TIMESTAMP(6) COMMENT 'last failed execution time',
    PRIMARY KEY (`task_name`, `task_instance`)
) COMMENT='table maintenance cron schedule metadata';
CREATE INDEX IF NOT EXISTS `scheduled_cron_tasks_execution_time_idx` ON `scheduled_cron_tasks`(`execution_time`);
CREATE INDEX IF NOT EXISTS `scheduled_cron_tasks_last_heartbeat_idx` ON `scheduled_cron_tasks`(`last_heartbeat`);

CREATE TABLE IF NOT EXISTS `scheduled_commit_tasks` (
    `task_name` VARCHAR(100) NOT NULL COMMENT 'task name',
    `task_instance` VARCHAR(100) NOT NULL COMMENT 'task instance',
    `task_data` BLOB COMMENT 'serialized task data',
    `execution_time` TIMESTAMP(6) NOT NULL COMMENT 'next execution time',
    `picked` BOOLEAN NOT NULL COMMENT 'whether the task is currently picked',
    `picked_by` VARCHAR(50) COMMENT 'scheduler instance that picked the task',
    `last_heartbeat` TIMESTAMP(6) COMMENT 'last heartbeat from the picker',
    `version` BIGINT NOT NULL COMMENT 'optimistic lock version',
    `consecutive_failures` INT COMMENT 'consecutive failure count',
    `last_success` TIMESTAMP(6) COMMENT 'last successful execution time',
    `last_failure` TIMESTAMP(6) COMMENT 'last failed execution time',
    PRIMARY KEY (`task_name`, `task_instance`)
) COMMENT='table maintenance commit schedule metadata';
CREATE INDEX IF NOT EXISTS `scheduled_commit_tasks_execution_time_idx` ON `scheduled_commit_tasks`(`execution_time`);
CREATE INDEX IF NOT EXISTS `scheduled_commit_tasks_last_heartbeat_idx` ON `scheduled_commit_tasks`(`last_heartbeat`);

CREATE TABLE IF NOT EXISTS `table_maintenance_action` (
    `id` BIGINT AUTO_INCREMENT NOT NULL COMMENT 'auto increment id',
    `job_id` BIGINT COMMENT 'job run id, maps to job_run_meta.job_run_id',
    `table_id` BIGINT NOT NULL COMMENT 'table id from table_meta',
    `policy_id` BIGINT NOT NULL COMMENT 'policy id from policy_meta',
    `before_snapshot_id` BIGINT COMMENT 'Iceberg snapshot id before maintenance, null until sampled',
    `after_snapshot_id` BIGINT COMMENT 'Iceberg snapshot id after maintenance, null while pending',
    `finished_at` BIGINT NOT NULL DEFAULT 0 COMMENT 'finish millis, 0 = soft in-flight slot (one per table/policy)',
    `created_at` BIGINT NOT NULL COMMENT 'creation timestamp in milliseconds',
    PRIMARY KEY (`id`)
) COMMENT='table maintenance action metadata';
CREATE UNIQUE INDEX IF NOT EXISTS `uk_tma_job_id` ON `table_maintenance_action`(`job_id`);
-- Soft slot: finished_at=0 is exclusive per table/policy (one in-flight finish).
CREATE UNIQUE INDEX IF NOT EXISTS `uk_tma_table_policy_finished` ON `table_maintenance_action`(`table_id`, `policy_id`, `finished_at`);
CREATE INDEX IF NOT EXISTS `idx_tma_before_sid` ON `table_maintenance_action`(`table_id`, `before_snapshot_id`);
CREATE INDEX IF NOT EXISTS `idx_tma_after_sid` ON `table_maintenance_action`(`table_id`, `after_snapshot_id`);
CREATE INDEX IF NOT EXISTS `idx_tma_created_at` ON `table_maintenance_action`(`created_at`);

CREATE TABLE IF NOT EXISTS `table_snapshot_metrics` (
    `id` BIGINT AUTO_INCREMENT NOT NULL COMMENT 'auto increment id',
    `table_id` BIGINT NOT NULL COMMENT 'table id from table_meta',
    `policy_id` BIGINT NOT NULL COMMENT 'policy id from policy_meta',
    `snapshot_id` BIGINT NOT NULL COMMENT 'Iceberg snapshot id',
    `metrics_value` CLOB NOT NULL COMMENT 'metrics value as JSON',
    `created_at` BIGINT NOT NULL COMMENT 'creation timestamp in milliseconds',
    PRIMARY KEY (`id`)
) COMMENT='table snapshot metrics metadata';
CREATE UNIQUE INDEX IF NOT EXISTS `uk_tid_pid_sid` ON `table_snapshot_metrics`(`table_id`, `policy_id`, `snapshot_id`);
CREATE INDEX IF NOT EXISTS `idx_tsm_created_at` ON `table_snapshot_metrics`(`created_at`);

-- Merge duplicate live owners left by concurrent assignments: the newest live row
-- (largest id) wins, and older ones are soft-deleted.
UPDATE `owner_meta`
    SET `deleted_at` = ((UNIX_TIMESTAMP() * 1000.0) + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000),
        `updated_at` = ((UNIX_TIMESTAMP() * 1000.0) + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000)
    WHERE `deleted_at` = 0
      AND `id` < (
        SELECT MAX(d.`id`) FROM `owner_meta` d
        WHERE d.`deleted_at` = 0
          AND d.`metadata_object_id` = `owner_meta`.`metadata_object_id`
          AND d.`metadata_object_type` = `owner_meta`.`metadata_object_type`
      );

-- Separate the optimistic-concurrency token from the history version for fileset and policy.
-- Until now `current_version` served as both: it is the join key into `*_version_info` and the
-- value the CAS compares, so every alter had to advance it and write a snapshot even when nothing
-- in that snapshot changed. `occ_version` takes over the CAS; `current_version` again advances
-- only when the stored snapshot changes. The default is the whole backfill, because `occ_version`
-- is only ever compared against itself on the same row.
ALTER TABLE `fileset_meta`
    ADD COLUMN IF NOT EXISTS `occ_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'fileset optimistic concurrency version' AFTER `last_version`;

ALTER TABLE `policy_meta`
    ADD COLUMN IF NOT EXISTS `occ_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy optimistic concurrency version' AFTER `last_version`;
