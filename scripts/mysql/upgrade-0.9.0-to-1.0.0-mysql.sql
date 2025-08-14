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

CREATE TABLE IF NOT EXISTS `policy_meta` (
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `policy_name` VARCHAR(128) NOT NULL COMMENT 'policy name',
    `policy_type` VARCHAR(64) NOT NULL COMMENT 'policy type',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'policy audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy deleted at',
    PRIMARY KEY (`policy_id`),
    UNIQUE KEY `uk_mi_pn_del` (`metalake_id`, `policy_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy metadata';

CREATE TABLE IF NOT EXISTS `policy_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `version` INT UNSIGNED NOT NULL COMMENT 'policy info version',
    `policy_comment` TEXT DEFAULT NULL COMMENT 'policy info comment',
    `enabled` TINYINT(1) DEFAULT 1 COMMENT 'whether the policy is enabled, 0 is disabled, 1 is enabled',
    `content` MEDIUMTEXT DEFAULT NULL COMMENT 'policy content',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_pod_ver_del` (`policy_id`, `version`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy version info';

CREATE TABLE IF NOT EXISTS `policy_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'policy relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_pi_mi_mo_del` (`policy_id`, `metadata_object_id`, `metadata_object_type`, `deleted_at`),
    KEY `idx_pid` (`policy_id`),
    KEY `idx_mid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy metadata object relation';

-- using default 'unknown' to fill in the new column for compatibility
ALTER TABLE `model_version_info` ADD COLUMN `model_version_uri_name` VARCHAR(256) NOT NULL DEFAULT 'unknown' COMMENT 'model version uri name' AFTER `model_version_properties`;
ALTER TABLE `model_version_info` DROP INDEX `uk_mid_ver_del`;
ALTER TABLE `model_version_info` ADD CONSTRAINT `uk_mid_ver_uri_del` UNIQUE KEY (`model_id`, `version`, `model_version_uri_name`, `deleted_at`);
-- remove the default value for model_version_uri_name
ALTER TABLE `model_version_info` ALTER COLUMN `model_version_uri_name` DROP DEFAULT;

CREATE TABLE IF NOT EXISTS `statistic_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `statistic_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'statistic id',
    `statistic_name` VARCHAR(128) NOT NULL COMMENT 'statistic name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `statistic_value` MEDIUMTEXT NOT NULL COMMENT 'statistic value',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'statistic audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'statistic current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'statistic last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'statistic deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_si_mi_mo_del` (`statistic_name`, `metadata_object_id`, `deleted_at`),
    KEY `idx_stid` (`statistic_id`),
    KEY `idx_moid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'statistic metadata';

CREATE TABLE IF NOT EXISTS `job_template_meta` (
    `job_template_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job template id',
    `job_template_name` VARCHAR(128) NOT NULL COMMENT 'job template name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `job_template_comment` TEXT DEFAULT NULL COMMENT 'job template comment',
    `job_template_content` MEDIUMTEXT NOT NULL COMMENT 'job template content',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'job template audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job template current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job template last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job template deleted at',
    PRIMARY KEY (`job_template_id`),
    UNIQUE KEY `uk_mid_jtn_del` (`metalake_id`, `job_template_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'job template metadata';

CREATE TABLE IF NOT EXISTS `job_run_meta` (
    `job_run_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job run id',
    `job_template_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job template id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `job_execution_id` varchar(256) NOT NULL COMMENT 'job execution id',
    `job_run_status` varchar(64) NOT NULL COMMENT 'job run status',
    `job_finished_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job finished at',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'job run audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job run current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job run last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job run deleted at',
    PRIMARY KEY (`job_run_id`),
    UNIQUE KEY `uk_mid_jei_del` (`metalake_id`, `job_execution_id`, `deleted_at`),
    KEY `idx_job_template_id` (`job_template_id`),
    KEY `idx_job_execution_id` (`job_execution_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'job run metadata';
