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

CREATE TABLE IF NOT EXISTS `table_version_info` (
    `table_id`        BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id',
    `format`          VARCHAR(64) NOT NULL COMMENT 'table format, such as Lance, Iceberg and so on',
    `properties`      MEDIUMTEXT DEFAULT NULL COMMENT 'table properties',
    `partitioning`  MEDIUMTEXT DEFAULT NULL COMMENT 'table partition info',
    `distribution` MEDIUMTEXT DEFAULT NULL COMMENT 'table distribution info',
    `sort_orders` MEDIUMTEXT DEFAULT NULL COMMENT 'table sort order info',
    `indexes`      MEDIUMTEXT DEFAULT NULL COMMENT 'table index info',
    `comment`   MEDIUMTEXT DEFAULT NULL COMMENT 'table comment',
    `version` BIGINT(20) UNSIGNED COMMENT 'table current version',
    `deleted_at`      BIGINT(20) UNSIGNED DEFAULT 0 COMMENT 'table deletion timestamp, 0 means not deleted',
    PRIMARY KEY (table_id),
    UNIQUE KEY `uk_table_id_deleted_at` (`table_id`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'table detail information including format, location, properties, partition, distribution, sort order, index and so on';
