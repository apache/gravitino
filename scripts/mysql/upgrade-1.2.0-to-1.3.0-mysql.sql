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

CREATE INDEX idx_user_meta_name_del_upd
    ON user_meta (metalake_id, user_name, deleted_at, updated_at);
CREATE INDEX idx_role_meta_del_upd
    ON role_meta (role_id, deleted_at, updated_at);
CREATE INDEX idx_owner_meta_del_upd_obj
    ON owner_meta (deleted_at, updated_at, metadata_object_id);

-- Entity name->id mutation tracking (eventual consistency -- entity change poller)
CREATE TABLE IF NOT EXISTS `entity_change_log` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `metalake_name` VARCHAR(128)    NOT NULL,
  `entity_type`   VARCHAR(32)     NOT NULL COMMENT 'METALAKE | CATALOG | SCHEMA | TABLE | FILESET | TOPIC | MODEL | VIEW',
  `entity_full_name` VARCHAR(512) NOT NULL COMMENT 'Dot-separated full name of the affected entity. For ALTER, stores the old name. For DROP, stores the entity name.',
  `operate_type`  TINYINT UNSIGNED NOT NULL COMMENT 'Operate type code: 1=ALTER, 2=DROP, 3=INSERT. Codes are stable and never re-used.',
  `created_at`    BIGINT          NOT NULL COMMENT 'timestamp of the change in millis',
  PRIMARY KEY (`id`),
  INDEX `idx_ecl_created_at` (`created_at`)
) COMMENT 'Append-only log of entity structural changes. One row per affected entity per operation. The entity change poller reads this table to drive targeted invalidation of metadataIdCache on HA peer nodes. Rows older than the retention window (default 1 h) are pruned periodically.';

-- add audit_info as nullable first for MySQL 5.7 compatibility
ALTER TABLE `view_meta`
    ADD COLUMN `audit_info` MEDIUMTEXT COMMENT 'view audit info' AFTER `schema_id`;

-- backfill existing rows before enforcing NOT NULL
UPDATE `view_meta`
    SET `audit_info` = '{}'
    WHERE `audit_info` IS NULL;

ALTER TABLE `view_meta`
    MODIFY COLUMN `audit_info` MEDIUMTEXT NOT NULL COMMENT 'view audit info' AFTER `schema_id`;

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
