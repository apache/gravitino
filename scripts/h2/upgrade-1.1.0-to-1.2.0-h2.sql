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

CREATE TABLE IF NOT EXISTS `function_meta` (
    `function_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'function id',
    `function_name` VARCHAR(128) NOT NULL COMMENT 'function name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `function_type` VARCHAR(64) NOT NULL COMMENT 'function type',
    `deterministic` TINYINT(1) DEFAULT 1 COMMENT 'whether the function result is deterministic',
    `function_current_version` INT UNSIGNED DEFAULT 1 COMMENT 'function current version',
    `function_latest_version` INT UNSIGNED DEFAULT 1 COMMENT 'function latest version',
    `audit_info` CLOB NOT NULL COMMENT 'function audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'function deleted at',
    PRIMARY KEY (`function_id`),
    UNIQUE KEY `uk_sid_fun_del` (`schema_id`, `function_name`, `deleted_at`),
    KEY `idx_funmid` (`metalake_id`),
    KEY `idx_funcid` (`catalog_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `function_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `function_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'function id',
    `version` INT UNSIGNED NOT NULL COMMENT 'function version',
    `function_comment` CLOB DEFAULT NULL COMMENT 'function version comment',
    `definitions` CLOB NOT NULL COMMENT 'function definitions details',
    `audit_info` CLOB NOT NULL COMMENT 'function version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'function version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_funid_ver_del` (`function_id`, `version`, `deleted_at`),
    KEY `idx_funvmid` (`metalake_id`),
    KEY `idx_funvcid` (`catalog_id`),
    KEY `idx_funvsid` (`schema_id`)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS `view_meta` (
    `view_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'view id',
    `view_name` VARCHAR(128) NOT NULL COMMENT 'view name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'view current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'view last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'view deleted at',
    PRIMARY KEY (`view_id`),
    UNIQUE KEY `uk_sid_vn_del` (`schema_id`, `view_name`, `deleted_at`),
    KEY `idx_vemid` (`metalake_id`),
    KEY `idx_vecid` (`catalog_id`)
) ENGINE=InnoDB;

-- Add partition statistics storage support
CREATE TABLE IF NOT EXISTS partition_statistic_meta (
    table_id BIGINT NOT NULL COMMENT 'table id from table_meta',
    partition_name VARCHAR(1024) NOT NULL COMMENT 'partition name',
    statistic_name VARCHAR(128) NOT NULL COMMENT 'statistic name',
    statistic_value CLOB NOT NULL COMMENT 'statistic value as JSON',
    audit_info CLOB NOT NULL COMMENT 'audit information as JSON',
    created_at BIGINT NOT NULL COMMENT 'creation timestamp in milliseconds',
    updated_at BIGINT NOT NULL COMMENT 'last update timestamp in milliseconds',
    PRIMARY KEY (table_id, partition_name, statistic_name)
);

CREATE INDEX IF NOT EXISTS idx_table_partition ON partition_statistic_meta(table_id, partition_name);

-- Add optimizer metrics storage tables
CREATE TABLE IF NOT EXISTS `table_metrics` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `table_identifier` VARCHAR(1024) NOT NULL COMMENT 'normalized table identifier',
    `metric_name` VARCHAR(1024) NOT NULL COMMENT 'metric name',
    `table_partition` VARCHAR(1024) DEFAULT NULL COMMENT 'normalized partition identifier',
    `metric_ts` BIGINT(20) NOT NULL COMMENT 'metric timestamp in epoch seconds',
    `metric_value` VARCHAR(1024) NOT NULL COMMENT 'metric value payload',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='optimizer table metrics';

CREATE TABLE IF NOT EXISTS `job_metrics` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `job_identifier` VARCHAR(1024) NOT NULL COMMENT 'normalized job identifier',
    `metric_name` VARCHAR(1024) NOT NULL COMMENT 'metric name',
    `metric_ts` BIGINT(20) NOT NULL COMMENT 'metric timestamp in epoch seconds',
    `metric_value` VARCHAR(1024) NOT NULL COMMENT 'metric value payload',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB COMMENT='optimizer job metrics';

CREATE INDEX IF NOT EXISTS `idx_table_metrics_metric_ts` ON `table_metrics`(`metric_ts`);
CREATE INDEX IF NOT EXISTS `idx_job_metrics_metric_ts` ON `job_metrics`(`metric_ts`);
CREATE INDEX IF NOT EXISTS `idx_table_metrics_composite`
  ON `table_metrics`(`table_identifier`, `table_partition`, `metric_ts`);
CREATE INDEX IF NOT EXISTS `idx_job_metrics_identifier_metric_ts`
  ON `job_metrics`(`job_identifier`, `metric_ts`);
