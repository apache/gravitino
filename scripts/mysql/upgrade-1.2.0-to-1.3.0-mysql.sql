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

ALTER TABLE `user_meta`
    ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'updated at';

ALTER TABLE `role_meta`
    ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'updated at';

ALTER TABLE `owner_meta`
    ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'updated at';

ALTER TABLE `group_meta`
    ADD COLUMN `updated_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0
    COMMENT 'updated at';

CREATE INDEX idx_user_meta_name_del_upd
    ON user_meta (metalake_id, user_name, deleted_at, updated_at);
CREATE INDEX idx_owner_meta_del_upd_obj
    ON owner_meta (deleted_at, updated_at, metadata_object_id);
CREATE INDEX idx_group_meta_name_del_upd
    ON group_meta (metalake_id, group_name, deleted_at, updated_at);

CREATE TABLE IF NOT EXISTS `entity_change_log` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
  `metalake_name` VARCHAR(128)    NOT NULL COMMENT 'metalake name',
  `entity_type`   VARCHAR(32)     NOT NULL COMMENT 'METALAKE | CATALOG | SCHEMA | TABLE | FILESET | TOPIC | MODEL | VIEW',
  `entity_full_name` VARCHAR(512) NOT NULL COMMENT 'Dot-separated full name of the affected entity. For ALTER, stores the old name. For DROP, stores the entity name.',
  `operate_type`  TINYINT UNSIGNED NOT NULL COMMENT 'Operate type code: 1=ALTER, 2=DROP, 3=INSERT. Codes are stable and never re-used.',
  `created_at`    BIGINT          NOT NULL COMMENT 'timestamp of the change in millis',
  PRIMARY KEY (`id`),
  KEY `idx_ecl_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'Append-only log of entity structural changes for targeted metadataIdCache invalidation';

-- add audit_info as nullable first for MySQL 5.7 compatibility
ALTER TABLE `view_meta`
    ADD COLUMN `audit_info` MEDIUMTEXT COMMENT 'view audit info' AFTER `schema_id`;

-- backfill existing rows before enforcing NOT NULL
UPDATE `view_meta`
    SET `audit_info` = '{}'
    WHERE `audit_info` IS NULL;

ALTER TABLE `view_meta`
    MODIFY COLUMN `audit_info` MEDIUMTEXT NOT NULL COMMENT 'view audit info' AFTER `schema_id`;

ALTER TABLE `table_column_version_info`
    MODIFY COLUMN `column_comment` VARCHAR(4096) DEFAULT '' COMMENT 'column comment';

CREATE TABLE IF NOT EXISTS `view_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `view_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'view id',
    `version` INT UNSIGNED NOT NULL COMMENT 'view version',
    `view_comment` TEXT DEFAULT NULL COMMENT 'view version comment',
    `columns` MEDIUMTEXT NOT NULL COMMENT 'view columns snapshot (JSON)',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'view properties (JSON)',
    `default_catalog` VARCHAR(128) DEFAULT NULL COMMENT 'default catalog for view SQL resolution',
    `default_schema` VARCHAR(128) DEFAULT NULL COMMENT 'default schema for view SQL resolution',
    `representations` MEDIUMTEXT NOT NULL COMMENT 'view representations (JSON array)',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'view version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'view version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_vid_ver_del` (`view_id`, `version`, `deleted_at`),
    KEY `idx_vvmid` (`metalake_id`),
    KEY `idx_vvcid` (`catalog_id`),
    KEY `idx_vvsid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'view version info';

CREATE TABLE IF NOT EXISTS `idp_user_meta` (
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp user id',
    `user_name` VARCHAR(128) NOT NULL COMMENT 'idp username',
    `password_hash` VARCHAR(1024) NOT NULL COMMENT 'idp user password hash',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp user current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp user last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'idp user deleted at',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uk_iun_del` (`user_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'local IdP user metadata';

CREATE TABLE IF NOT EXISTS `idp_group_meta` (
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp group id',
    `group_name` VARCHAR(128) NOT NULL COMMENT 'idp group name',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp group current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp group last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'idp group deleted at',
    PRIMARY KEY (`group_id`),
    UNIQUE KEY `uk_ign_del` (`group_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'local IdP group metadata';

CREATE TABLE IF NOT EXISTS `idp_user_group_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp user id',
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'idp group id',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'idp relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'idp relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_iuig_del` (`user_id`, `group_id`, `deleted_at`),
    KEY `idx_iuig_uid` (`user_id`),
    KEY `idx_iuig_gid` (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'local IdP user group relation';

CREATE TABLE IF NOT EXISTS `iceberg_cleanup_job` (
  `id`                BIGINT(20)    UNSIGNED NOT NULL COMMENT 'globally unique cleanup job id',
  `catalog_id`        BIGINT(20)    UNSIGNED NOT NULL COMMENT 'globally unique id of the owning catalog, stable across catalog rename',
  `namespace`         VARCHAR(512)  NOT NULL COMMENT 'namespace of the table to be cleaned up',
  `table_name`        VARCHAR(256)  NOT NULL COMMENT 'name of the table to be cleaned up',
  `metadata_location` MEDIUMTEXT   NOT NULL COMMENT 'location of the table metadata file to purge',
  `file_io_impl`      VARCHAR(256)  NOT NULL COMMENT 'FileIO implementation class used to access the table files',
  `file_io_props`     MEDIUMTEXT    NOT NULL COMMENT 'JSON-encoded FileIO properties',
  `state`             VARCHAR(16)   NOT NULL COMMENT 'PENDING|RUNNING|SUCCEEDED|FAILED',
  `attempts`          INT(10)       NOT NULL DEFAULT 0 COMMENT 'number of processing attempts made so far',
  `last_error`        VARCHAR(2048) NULL COMMENT 'truncated reason for the most recent failure, NULL until a job fails',
  `heartbeat_at`      BIGINT(20)    NOT NULL DEFAULT 0 COMMENT 'last heartbeat from the worker, 0 when not running',
  `created_by`        VARCHAR(128)  NOT NULL COMMENT 'principal that requested the drop (audit)',
  `updated_at`        BIGINT(20)    NOT NULL COMMENT 'last state change, drives poll ordering and old finished-job cleanup',
  PRIMARY KEY (`id`),
  KEY `idx_state_updated` (`state`, `updated_at`),
  KEY `idx_object` (`catalog_id`, `namespace`(255), `table_name`(128), `state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'async Iceberg table cleanup jobs';
