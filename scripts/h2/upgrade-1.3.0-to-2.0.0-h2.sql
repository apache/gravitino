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

ALTER TABLE `table_meta`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE UNIQUE INDEX IF NOT EXISTS `uk_tm_deletion` ON `table_meta` (`deletion_id`);
CREATE INDEX IF NOT EXISTS `idx_tm_deleted_action`
    ON `table_meta` (`deleted_at`, `deletion_id`);

CREATE TABLE IF NOT EXISTS `entity_deletion` (
  `deletion_id`              VARCHAR(64)   NOT NULL COMMENT 'opaque identifier for one active deletion generation',
  `state`                    VARCHAR(16)   NOT NULL COMMENT 'DELETED|PURGING',
  `retention_expires_at`     BIGINT        NOT NULL COMMENT 'fixed recovery deadline in milliseconds',
  `purge_job_id`             VARCHAR(64)   DEFAULT NULL COMMENT 'batch purge job that claimed this generation',
  PRIMARY KEY (`deletion_id`),
  KEY `idx_entity_deletion_gc` (`state`, `retention_expires_at`, `deletion_id`),
  KEY `idx_entity_deletion_purge_job` (`purge_job_id`, `deletion_id`)
) COMMENT='active deletion lifecycle actions';
