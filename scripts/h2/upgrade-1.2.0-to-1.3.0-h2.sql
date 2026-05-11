--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
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

ALTER TABLE `user_meta`  ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'updated at';
ALTER TABLE `role_meta` ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'updated at';
ALTER TABLE `owner_meta` ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'updated at';
ALTER TABLE `group_meta` ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'updated at';

CREATE INDEX IF NOT EXISTS `idx_user_meta_name_del_upd` ON `user_meta`(`metalake_id`, `user_name`, `deleted_at`, `updated_at`);
CREATE INDEX IF NOT EXISTS `idx_owner_meta_del_upd_obj` ON `owner_meta`(`deleted_at`, `updated_at`, `metadata_object_id`);
CREATE INDEX IF NOT EXISTS `idx_group_meta_name_del_upd` ON `group_meta`(`metalake_id`, `group_name`, `deleted_at`, `updated_at`);

CREATE TABLE IF NOT EXISTS `entity_change_log` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
  `metalake_name` VARCHAR(128)    NOT NULL COMMENT 'metalake name',
  `entity_type`   VARCHAR(32)     NOT NULL COMMENT 'METALAKE | CATALOG | SCHEMA | TABLE | FILESET | TOPIC | MODEL | VIEW',
  `entity_full_name` VARCHAR(512) NOT NULL COMMENT 'Dot-separated full name of the affected entity. For ALTER, stores the old name. For DROP, stores the entity name.',
  `operate_type`  TINYINT UNSIGNED NOT NULL COMMENT 'Operate type code: 1=ALTER, 2=DROP, 3=INSERT. Codes are stable and never re-used.',
  `created_at`    BIGINT          NOT NULL COMMENT 'timestamp of the change in millis',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='Append-only log of entity structural changes for targeted metadataIdCache invalidation';
CREATE INDEX IF NOT EXISTS `idx_ecl_created_at` ON `entity_change_log`(`created_at`);

-- using default '{}' to fill in the new column for compatibility
ALTER TABLE `view_meta`
    ADD COLUMN `audit_info` CLOB NOT NULL DEFAULT '{}' COMMENT 'view audit info';

-- remove the default value for audit_info
ALTER TABLE `view_meta`
    ALTER COLUMN `audit_info` DROP DEFAULT;

CREATE TABLE IF NOT EXISTS `view_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `view_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'view id',
    `version` INT UNSIGNED NOT NULL COMMENT 'view version',
    `view_comment` CLOB DEFAULT NULL COMMENT 'view version comment',
    `columns` CLOB NOT NULL COMMENT 'view columns snapshot (JSON)',
    `properties` CLOB DEFAULT NULL COMMENT 'view properties (JSON)',
    `default_catalog` VARCHAR(128) DEFAULT NULL COMMENT 'default catalog for view SQL resolution',
    `default_schema` VARCHAR(128) DEFAULT NULL COMMENT 'default schema for view SQL resolution',
    `representations` CLOB NOT NULL COMMENT 'view representations (JSON array)',
    `audit_info` CLOB NOT NULL COMMENT 'view version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'view version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_vid_ver_del` (`view_id`, `version`, `deleted_at`),
    KEY `idx_vvmid` (`metalake_id`),
    KEY `idx_vvcid` (`catalog_id`),
    KEY `idx_vvsid` (`schema_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `idp_user_meta` (
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp user id',
    `user_name` VARCHAR(128) NOT NULL COMMENT 'idp username',
    `password_hash` VARCHAR(1024) NOT NULL COMMENT 'idp user password hash',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp user current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp user last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'idp user deleted at',
    PRIMARY KEY (`user_id`),
    CONSTRAINT `uk_iun_del` UNIQUE (`user_name`, `deleted_at`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `idp_group_meta` (
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp group id',
    `group_name` VARCHAR(128) NOT NULL COMMENT 'idp group name',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp group current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp group last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'idp group deleted at',
    PRIMARY KEY (`group_id`),
    CONSTRAINT `uk_ign_del` UNIQUE (`group_name`, `deleted_at`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `idp_group_user_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp group id',
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp user id',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'idp relation deleted at',
    PRIMARY KEY (`id`),
    CONSTRAINT `uk_igiu_del` UNIQUE (`group_id`, `user_id`, `deleted_at`),
    KEY `idx_iug_gid` (`group_id`),
    KEY `idx_iug_uid` (`user_id`)
) ENGINE=InnoDB;
