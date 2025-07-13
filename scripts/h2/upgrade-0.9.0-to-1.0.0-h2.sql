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
    `inheritable` TINYINT(1) NOT NULL COMMENT 'whether the policy is inheritable, 0 is not inheritable, 1 is inheritable',
    `exclusive` TINYINT(1) NOT NULL COMMENT 'whether the policy is exclusive, 0 is not exclusive, 1 is exclusive',
    `supported_object_types` TEXT NOT NULL COMMENT 'supported object types',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'policy audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy deleted at',
    PRIMARY KEY (`policy_id`),
    UNIQUE KEY `uk_mi_pn_del` (`metalake_id`, `policy_name`, `deleted_at`)
) ENGINE=InnoDB;

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
    KEY `idx_mid` (`metalake_id`),
) ENGINE=InnoDB;

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
) ENGINE=InnoDB;

-- using default 'unknown' to fill in the new column for compatibility
ALTER TABLE `model_version_info` ADD COLUMN `model_version_uri_name` VARCHAR(256) NOT NULL DEFAULT 'unknown' COMMENT 'model version uri name';
ALTER TABLE `model_version_info` DROP INDEX `uk_mid_ver_del`;
ALTER TABLE `model_version_info` ADD CONSTRAINT `uk_mid_ver_uri_del` UNIQUE (`model_id`, `version`, `model_version_uri_name`, `deleted_at`);
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
    UNIQUE KEY `uk_si_mi_mo_del` (`statistic_name`, `metalake_id`, `metadata_object_id`, `deleted_at`),
    KEY `idx_stid` (`statistic_id`),
    KEY `idx_moid` (`metadata_object_id`),
    ) ENGINE=InnoDB;