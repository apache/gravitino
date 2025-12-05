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
ALTER TABLE `role_meta_securable_object` ALTER COLUMN `privilege_names` CLOB(81920);
ALTER TABLE `role_meta_securable_object` ALTER COLUMN `privilege_conditions` CLOB(81920);

CREATE TABLE IF NOT EXISTS `model_meta` (
    `model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'model id',
    `model_name` VARCHAR(128) NOT NULL COMMENT 'model name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `model_comment` CLOB DEFAULT NULL COMMENT 'model comment',
    `model_properties` CLOB DEFAULT NULL COMMENT 'model properties',
    `model_latest_version` INT UNSIGNED DEFAULT 0 COMMENT 'model latest version',
    `audit_info` CLOB NOT NULL COMMENT 'model audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'model deleted at',
    PRIMARY KEY (`model_id`),
    UNIQUE KEY `uk_sid_mn_del` (`schema_id`, `model_name`, `deleted_at`),
    KEY `idx_mmid` (`metalake_id`),
    KEY `idx_mcid` (`catalog_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `model_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'model id',
    `version` INT UNSIGNED NOT NULL COMMENT 'model version',
    `model_version_comment` CLOB DEFAULT NULL COMMENT 'model version comment',
    `model_version_properties` CLOB DEFAULT NULL COMMENT 'model version properties',
    `model_version_uri` CLOB NOT NULL COMMENT 'model storage uri',
    `audit_info` CLOB NOT NULL COMMENT 'model version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'model version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_mid_ver_del` (`model_id`, `version`, `deleted_at`),
    KEY `idx_vmid` (`metalake_id`),
    KEY `idx_vcid` (`catalog_id`),
    KEY `idx_vsid` (`schema_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `model_version_alias_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'model id',
    `model_version` INT UNSIGNED NOT NULL COMMENT 'model version',
    `model_version_alias` VARCHAR(128) NOT NULL COMMENT 'model version alias',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'model version alias deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_mi_mva_del` (`model_id`, `model_version_alias`, `deleted_at`),
    KEY `idx_mva` (`model_version_alias`)
) ENGINE=InnoDB;
