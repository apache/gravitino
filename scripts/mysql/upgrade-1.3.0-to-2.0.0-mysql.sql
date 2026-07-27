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

ALTER TABLE `user_meta`
    ADD COLUMN `external_id` VARCHAR(256) DEFAULT NULL COMMENT 'external identifier from an upstream identity system' AFTER `metalake_id`,
    ADD COLUMN `enabled` TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'whether the user is enabled, 0 is disabled, 1 is enabled' AFTER `external_id`;

ALTER TABLE `group_meta`
    ADD COLUMN `external_id` VARCHAR(256) DEFAULT NULL COMMENT 'external identifier from an upstream identity system' AFTER `metalake_id`;

CREATE UNIQUE INDEX `uk_mid_ueid_del` ON `user_meta` (`metalake_id`, `external_id`, `deleted_at`);
CREATE UNIQUE INDEX `uk_mid_geid_del` ON `group_meta` (`metalake_id`, `external_id`, `deleted_at`);

ALTER TABLE `table_column_version_info`
    MODIFY COLUMN `column_comment` VARCHAR(4096) DEFAULT '' COMMENT 'column comment';

ALTER TABLE `table_meta`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_tm_deletion` ON `table_meta` (`deletion_id`);

ALTER TABLE `table_version_info`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_tvi_table_deletion`
    ON `table_version_info` (`table_id`, `deletion_id`);

ALTER TABLE `table_column_version_info`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'column deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_tcvi_table_deletion`
    ON `table_column_version_info` (`table_id`, `deletion_id`);

ALTER TABLE `owner_meta`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_owner_object_deletion`
    ON `owner_meta` (`metadata_object_id`, `metadata_object_type`, `deletion_id`);

ALTER TABLE `role_meta_securable_object`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_securable_object_deletion`
    ON `role_meta_securable_object` (`metadata_object_id`, `type`, `deletion_id`);

ALTER TABLE `tag_relation_meta`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_tag_relation_deletion`
    ON `tag_relation_meta` (`deletion_id`, `metadata_object_type`, `metadata_object_id`);

ALTER TABLE `policy_relation_meta`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_policy_relation_deletion`
    ON `policy_relation_meta` (`metadata_object_id`, `metadata_object_type`, `deletion_id`);

ALTER TABLE `statistic_meta`
    ADD COLUMN `deletion_id` VARCHAR(64) DEFAULT NULL COMMENT 'table deletion generation identifier' AFTER `deleted_at`;
CREATE INDEX `idx_statistic_object_deletion`
    ON `statistic_meta` (`metadata_object_id`, `metadata_object_type`, `deletion_id`);

CREATE TABLE IF NOT EXISTS `entity_deletion` (
  `deletion_id`              VARCHAR(64)    NOT NULL COMMENT 'opaque identifier for one deletion generation',
  `entity_type`              VARCHAR(32)    NOT NULL COMMENT 'entity type, TABLE in the Iceberg REST implementation',
  `entity_id`                BIGINT(20)     UNSIGNED NOT NULL COMMENT 'immutable source entity id',
  `entity_version`           BIGINT(20)     UNSIGNED NOT NULL COMMENT 'source entity version captured at deletion',
  `metalake_id`              BIGINT(20)     UNSIGNED NOT NULL COMMENT 'immutable owning metalake id',
  `catalog_id`               BIGINT(20)     UNSIGNED NOT NULL COMMENT 'immutable owning catalog id',
  `parent_id`                BIGINT(20)     UNSIGNED NOT NULL COMMENT 'immutable immediate parent id, schema id for a table',
  `namespace_snapshot`       VARCHAR(512)   NOT NULL COMMENT 'namespace snapshot used for routing and audit',
  `entity_name_snapshot`     VARCHAR(128)   NOT NULL COMMENT 'entity name captured at deletion',
  `active_name_key`          VARCHAR(64)    DEFAULT NULL COMMENT 'unique name reservation while deletion is active',
  `state`                    VARCHAR(16)    NOT NULL COMMENT 'DELETED|RESTORED|PURGING|PURGED',
  `revision`                 BIGINT(20)     UNSIGNED NOT NULL DEFAULT 0 COMMENT 'optimistic lifecycle revision',
  `deleted_at`               BIGINT(20)     UNSIGNED NOT NULL COMMENT 'deletion timestamp in milliseconds',
  `retention_expires_at`     BIGINT(20)     UNSIGNED DEFAULT NULL COMMENT 'fixed recovery deadline, NULL means immediate nonrecoverable cleanup',
  `deleted_by`               VARCHAR(128)   NOT NULL COMMENT 'actor that requested deletion',
  `purge_requested`          TINYINT(1)     NOT NULL COMMENT 'original Iceberg REST purgeRequested value for audit',
  `purge_job_type`           VARCHAR(64)    NOT NULL COMMENT 'durable purge executor type',
  `purge_job_id`             VARCHAR(64)    DEFAULT NULL COMMENT 'batch purge job that claimed this generation',
  `cleanup_status`           VARCHAR(16)    DEFAULT NULL COMMENT 'PENDING|RUNNING|FAILED|SUCCEEDED',
  `cleanup_attempt_count`    INT(10)        UNSIGNED NOT NULL DEFAULT 0 COMMENT 'number of cleanup attempts',
  `cleanup_last_error`       VARCHAR(2048)  DEFAULT NULL COMMENT 'sanitized most recent cleanup error',
  `accepted_restore_etag`    VARCHAR(192)   DEFAULT NULL COMMENT 'deletion action ETag accepted by successful UNDROP',
  `request_id`               VARCHAR(128)   DEFAULT NULL COMMENT 'originating request id',
  `correlation_id`           VARCHAR(128)   DEFAULT NULL COMMENT 'lifecycle correlation id',
  `restored_at`              BIGINT(20)     UNSIGNED DEFAULT NULL COMMENT 'successful restore timestamp in milliseconds',
  `purged_at`                BIGINT(20)     UNSIGNED DEFAULT NULL COMMENT 'successful purge timestamp in milliseconds',
  `updated_at`               BIGINT(20)     UNSIGNED NOT NULL COMMENT 'last lifecycle update timestamp in milliseconds',
  PRIMARY KEY (`deletion_id`),
  UNIQUE KEY `uk_entity_deletion_active_name` (`active_name_key`),
  KEY `idx_entity_deletion_entity_history` (`entity_type`, `entity_id`, `deleted_at`, `deletion_id`),
  KEY `idx_entity_deletion_gc` (`state`, `retention_expires_at`, `deletion_id`),
  KEY `idx_entity_deletion_purge_job` (`purge_job_id`, `cleanup_status`, `deletion_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'durable deletion lifecycle actions and terminal receipts';

CREATE TABLE IF NOT EXISTS `entity_deletion_audit` (
  `audit_id`                 VARCHAR(64)    NOT NULL COMMENT 'audit event identifier',
  `deletion_id`              VARCHAR(64)    NOT NULL COMMENT 'deletion generation identifier',
  `entity_type`              VARCHAR(32)    NOT NULL COMMENT 'entity type',
  `entity_id`                BIGINT(20)     UNSIGNED NOT NULL COMMENT 'immutable source entity id',
  `event_type`               VARCHAR(64)    NOT NULL COMMENT 'lifecycle event type',
  `action_revision`          BIGINT(20)     UNSIGNED DEFAULT NULL COMMENT 'action revision associated with the event',
  `prior_state`              VARCHAR(16)    DEFAULT NULL COMMENT 'prior lifecycle state',
  `new_state`                VARCHAR(16)    DEFAULT NULL COMMENT 'new lifecycle state',
  `prior_cleanup_status`     VARCHAR(16)    DEFAULT NULL COMMENT 'prior cleanup status',
  `new_cleanup_status`       VARCHAR(16)    DEFAULT NULL COMMENT 'new cleanup status',
  `purge_job_id`             VARCHAR(64)    DEFAULT NULL COMMENT 'associated purge job id',
  `lease_epoch`              BIGINT(20)     UNSIGNED DEFAULT NULL COMMENT 'purge worker fencing epoch',
  `actor`                    VARCHAR(128)   NOT NULL COMMENT 'request actor or worker identity',
  `request_id`               VARCHAR(128)   DEFAULT NULL COMMENT 'request id',
  `correlation_id`           VARCHAR(128)   NOT NULL COMMENT 'lifecycle correlation id',
  `reason_code`              VARCHAR(64)    DEFAULT NULL COMMENT 'bounded machine-readable reason',
  `reason`                   VARCHAR(2048)  DEFAULT NULL COMMENT 'sanitized reason without credentials or secrets',
  `created_at`               BIGINT(20)     UNSIGNED NOT NULL COMMENT 'event timestamp in milliseconds',
  PRIMARY KEY (`audit_id`),
  KEY `idx_entity_deletion_audit_action` (`deletion_id`, `created_at`, `audit_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'append-only deletion lifecycle audit events';
