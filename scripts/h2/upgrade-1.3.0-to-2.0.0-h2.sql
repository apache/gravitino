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

ALTER TABLE `tag_meta` ADD COLUMN `allowed_values` CLOB DEFAULT NULL COMMENT 'tag allowed values as a JSON string array, NULL allows any value, [] allows no value' AFTER `properties`;

ALTER TABLE `tag_relation_meta` DROP INDEX `uk_ti_mi_del`;

ALTER TABLE `tag_relation_meta` ADD COLUMN `tag_value` VARCHAR(256) NOT NULL DEFAULT '' COMMENT 'tag assignment value, empty string means no value' AFTER `metadata_object_type`;

ALTER TABLE `idp_group_meta` ADD COLUMN `group_comment` VARCHAR(1024) DEFAULT '' COMMENT 'idp group comment' AFTER `group_name`;

CREATE UNIQUE INDEX IF NOT EXISTS `uk_ti_mi_mo_tv_del` ON `tag_relation_meta` (`tag_id`, `metadata_object_id`, `metadata_object_type`, `tag_value`, `deleted_at`);
CREATE INDEX IF NOT EXISTS `idx_tid_value` ON `tag_relation_meta` (`tag_id`, `tag_value`);

ALTER TABLE `job_run_meta` ADD COLUMN `job_started_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job started at' AFTER `job_run_status`;
