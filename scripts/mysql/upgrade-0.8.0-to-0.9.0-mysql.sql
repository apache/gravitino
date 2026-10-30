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

-- MySQL has no ADD COLUMN IF NOT EXISTS. Guard the unique-key conversion so
-- a failed upgrade can be restarted from the beginning: skip exactly the
-- completed steps and fail on any other unexpected schema state.

-- using default 'unknown' to fill in the new column for compatibility
SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fileset_version_info' AND column_name = 'storage_location_name'),
    'SELECT 1',
    'ALTER TABLE `fileset_version_info` ADD COLUMN `storage_location_name` VARCHAR(256) NOT NULL DEFAULT ''unknown'' COMMENT ''fileset storage location name'' AFTER `properties`');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl = IF(NOT EXISTS(SELECT 1 FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'fileset_version_info' AND index_name = 'uk_fid_ver_del'),
    'SELECT 1',
    'ALTER TABLE `fileset_version_info` DROP INDEX `uk_fid_ver_del`');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.statistics
    WHERE table_schema = DATABASE() AND table_name = 'fileset_version_info' AND index_name = 'uk_fid_ver_sto_del'),
    'SELECT 1',
    'ALTER TABLE `fileset_version_info` ADD CONSTRAINT `uk_fid_ver_sto_del` UNIQUE KEY (`fileset_id`, `version`, `storage_location_name`, `deleted_at`)');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- remove the default value for storage_location_name
SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema = DATABASE() AND table_name = 'fileset_version_info' AND column_name = 'storage_location_name' AND column_default IS NOT NULL),
    'ALTER TABLE `fileset_version_info` ALTER COLUMN `storage_location_name` DROP DEFAULT',
    'SELECT 1');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
