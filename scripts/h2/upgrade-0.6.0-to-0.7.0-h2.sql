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
CREATE TABLE IF NOT EXISTS `table_column_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id',
    `table_version` INT UNSIGNED NOT NULL COMMENT 'table version',
    `column_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'column id',
    `column_name` VARCHAR(128) NOT NULL COMMENT 'column name',
    `column_position` INT UNSIGNED NOT NULL COMMENT 'column position, starting from 0',
    `column_type` CLOB NOT NULL COMMENT 'column type',
    `column_comment` VARCHAR(256) DEFAULT '' COMMENT 'column comment',
    `column_nullable` TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'column nullable, 0 is not nullable, 1 is nullable',
    `column_auto_increment` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'column auto increment, 0 is not auto increment, 1 is auto increment',
    `column_default_value` CLOB DEFAULT NULL COMMENT 'column default value',
    `column_op_type` TINYINT(1) NOT NULL COMMENT 'column operation type, 1 is create, 2 is update, 3 is delete',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'column deleted at',
    `audit_info` CLOB NOT NULL COMMENT 'column audit info',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_tid_ver_cid_del` (`table_id`, `table_version`, `column_id`, `deleted_at`),
    KEY `idx_tcmid` (`metalake_id`),
    KEY `idx_tccid` (`catalog_id`),
    KEY `idx_tcsid` (`schema_id`)
) ENGINE=InnoDB;
