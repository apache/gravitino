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
ALTER TABLE `role_meta` MODIFY COLUMN `securable_object_full_name` VARCHAR(256) NOT NULL DEFAULT '' COMMENT 'deprecated';
ALTER TABLE `role_meta` MODIFY COLUMN `securable_object_type` VARCHAR(32) NOT NULL DEFAULT '' COMMENT 'deprecated';
ALTER TABLE `role_meta` MODIFY COLUMN `privileges`  VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'deprecated';
ALTER TABLE `role_meta` MODIFY COLUMN `privilege_conditions` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'deprecated';

CREATE TABLE IF NOT EXISTS `role_meta_securable_object` (
    `id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'securable object id',
    `role_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'role id',
    `entity_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'The entity id of securable object',
    `type`  VARCHAR(128) NOT NULL COMMENT 'securable object type',
    `privilege_names` VARCHAR(256) NOT NULL COMMENT 'securable object privilege names',
    `privilege_conditions` VARCHAR(256) NOT NULL COMMENT 'securable object privilege conditions',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'securable objectcurrent version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'securable object last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'securable object deleted at',
    PRIMARY KEY (`id`),
    KEY `idx_obj_rid` (`role_id`),
    KEY `idx_obj_eid` (`entity_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'securable object meta';

CREATE TABLE IF NOT EXISTS `tag_meta` (
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `tag_name` VARCHAR(128) NOT NULL COMMENT 'tag name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `tag_comment` VARCHAR(256) DEFAULT '' COMMENT 'tag comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'tag properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'tag audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag deleted at',
    PRIMARY KEY (`tag_id`),
    UNIQUE KEY `uk_mn_tn_del` (`metalake_id`, `tag_name`, `deleted_at`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'tag metadata';

CREATE TABLE IF NOT EXISTS `tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ti_mi_mo_del` (`tag_id`, `metadata_object_id`, `metadata_object_type`, `deleted_at`),
    KEY `idx_tid` (`tag_id`),
    KEY `idx_mid` (`metadata_object_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'tag metadata object relation';

CREATE TABLE IF NOT EXISTS `owner_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `owner_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'owner id',
    `owner_type` VARCHAR(64) NOT NULL COMMENT 'owner type',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'owner relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'owner relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'owner relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owner relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ow_me_del` (`owner_id`, `metadata_object_id`, `metadata_object_type`, `deleted_at`),
    KEY `idx_oid` (`owner_id`),
    KEY `idx_meid` (`metadata_object_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'owner relation';
