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

ALTER TABLE `view_meta`
    ADD COLUMN `audit_info` MEDIUMTEXT NOT NULL COMMENT 'view audit info' AFTER `schema_id`;

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
    `security_mode` VARCHAR(32) NOT NULL DEFAULT 'DEFINER' COMMENT 'DEFINER or INVOKER',
    `representations` MEDIUMTEXT NOT NULL COMMENT 'view representations (JSON array)',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'view version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'view version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_vid_ver_del` (`view_id`, `version`, `deleted_at`),
    KEY `idx_vvmid` (`metalake_id`),
    KEY `idx_vvcid` (`catalog_id`),
    KEY `idx_vvsid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'view version info';
