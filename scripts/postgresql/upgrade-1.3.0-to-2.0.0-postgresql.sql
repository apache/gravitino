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

ALTER TABLE user_meta ADD COLUMN IF NOT EXISTS external_id VARCHAR(256) DEFAULT NULL;
ALTER TABLE user_meta ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE group_meta ADD COLUMN IF NOT EXISTS external_id VARCHAR(256) DEFAULT NULL;

COMMENT ON COLUMN user_meta.external_id IS 'external identifier from an upstream identity system';
COMMENT ON COLUMN user_meta.enabled IS 'whether the user is enabled, 0 is disabled, 1 is enabled';
COMMENT ON COLUMN group_meta.external_id IS 'external identifier from an upstream identity system';

CREATE UNIQUE INDEX IF NOT EXISTS uk_mid_ueid_del ON user_meta (metalake_id, external_id, deleted_at);
CREATE UNIQUE INDEX IF NOT EXISTS uk_mid_geid_del ON group_meta (metalake_id, external_id, deleted_at);

ALTER TABLE table_column_version_info
    ALTER COLUMN column_comment TYPE VARCHAR(4096);

ALTER TABLE table_meta ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE table_version_info ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE table_column_version_info ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE owner_meta ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE role_meta_securable_object ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE tag_relation_meta ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE policy_relation_meta ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
ALTER TABLE statistic_meta ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);

COMMENT ON COLUMN table_meta.deletion_id IS 'table deletion generation identifier';
COMMENT ON COLUMN table_version_info.deletion_id IS 'table deletion generation identifier';
COMMENT ON COLUMN table_column_version_info.deletion_id IS 'column deletion generation identifier';
COMMENT ON COLUMN owner_meta.deletion_id IS 'table deletion generation identifier';
COMMENT ON COLUMN role_meta_securable_object.deletion_id IS 'table deletion generation identifier';
COMMENT ON COLUMN tag_relation_meta.deletion_id IS 'table deletion generation identifier';
COMMENT ON COLUMN policy_relation_meta.deletion_id IS 'table deletion generation identifier';
COMMENT ON COLUMN statistic_meta.deletion_id IS 'table deletion generation identifier';

CREATE INDEX IF NOT EXISTS idx_tm_deletion ON table_meta (deletion_id);
CREATE INDEX IF NOT EXISTS idx_tvi_table_deletion
    ON table_version_info (table_id, deletion_id);
CREATE INDEX IF NOT EXISTS idx_tcvi_table_deletion
    ON table_column_version_info (table_id, deletion_id);
CREATE INDEX IF NOT EXISTS idx_owner_object_deletion
    ON owner_meta (metadata_object_id, metadata_object_type, deletion_id);
CREATE INDEX IF NOT EXISTS idx_securable_object_deletion
    ON role_meta_securable_object (metadata_object_id, type, deletion_id);
CREATE INDEX IF NOT EXISTS idx_tag_relation_deletion
    ON tag_relation_meta (deletion_id, metadata_object_type, metadata_object_id);
CREATE INDEX IF NOT EXISTS idx_policy_relation_deletion
    ON policy_relation_meta (metadata_object_id, metadata_object_type, deletion_id);
CREATE INDEX IF NOT EXISTS idx_statistic_object_deletion
    ON statistic_meta (metadata_object_id, metadata_object_type, deletion_id);

CREATE TABLE IF NOT EXISTS entity_deletion (
    deletion_id VARCHAR(64) NOT NULL PRIMARY KEY,
    entity_type VARCHAR(32) NOT NULL,
    entity_id BIGINT NOT NULL,
    entity_version BIGINT NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    namespace_snapshot VARCHAR(512) NOT NULL,
    entity_name_snapshot VARCHAR(128) NOT NULL,
    active_name_key VARCHAR(64),
    state VARCHAR(16) NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    deleted_at BIGINT NOT NULL,
    retention_expires_at BIGINT,
    deleted_by VARCHAR(128) NOT NULL,
    purge_requested BOOLEAN NOT NULL,
    purge_job_type VARCHAR(64) NOT NULL,
    purge_job_id VARCHAR(64),
    cleanup_status VARCHAR(16),
    cleanup_attempt_count INT NOT NULL DEFAULT 0,
    cleanup_last_error VARCHAR(2048),
    accepted_restore_etag VARCHAR(192),
    request_id VARCHAR(128),
    correlation_id VARCHAR(128),
    restored_at BIGINT,
    purged_at BIGINT,
    updated_at BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_entity_deletion_active_name
    ON entity_deletion (active_name_key);
CREATE INDEX IF NOT EXISTS idx_entity_deletion_entity_history
    ON entity_deletion (entity_type, entity_id, deleted_at, deletion_id);
CREATE INDEX IF NOT EXISTS idx_entity_deletion_gc
    ON entity_deletion (state, retention_expires_at, deletion_id);
CREATE INDEX IF NOT EXISTS idx_entity_deletion_purge_job
    ON entity_deletion (purge_job_id, cleanup_status, deletion_id);
COMMENT ON TABLE entity_deletion IS 'durable deletion lifecycle actions and terminal receipts';
COMMENT ON COLUMN entity_deletion.deletion_id IS 'opaque identifier for one deletion generation';
COMMENT ON COLUMN entity_deletion.entity_type IS 'entity type, TABLE in the Iceberg REST implementation';
COMMENT ON COLUMN entity_deletion.entity_id IS 'immutable source entity id';
COMMENT ON COLUMN entity_deletion.entity_version IS 'source entity version captured at deletion';
COMMENT ON COLUMN entity_deletion.metalake_id IS 'immutable owning metalake id';
COMMENT ON COLUMN entity_deletion.catalog_id IS 'immutable owning catalog id';
COMMENT ON COLUMN entity_deletion.parent_id IS 'immutable immediate parent id, schema id for a table';
COMMENT ON COLUMN entity_deletion.namespace_snapshot IS 'namespace snapshot used for routing and audit';
COMMENT ON COLUMN entity_deletion.entity_name_snapshot IS 'entity name captured at deletion';
COMMENT ON COLUMN entity_deletion.active_name_key IS 'unique name reservation while deletion is active';
COMMENT ON COLUMN entity_deletion.state IS 'DELETED | RESTORED | PURGING | PURGED';
COMMENT ON COLUMN entity_deletion.revision IS 'optimistic lifecycle revision';
COMMENT ON COLUMN entity_deletion.deleted_at IS 'deletion timestamp in milliseconds';
COMMENT ON COLUMN entity_deletion.retention_expires_at IS 'fixed recovery deadline, NULL means immediate nonrecoverable cleanup';
COMMENT ON COLUMN entity_deletion.deleted_by IS 'actor that requested deletion';
COMMENT ON COLUMN entity_deletion.purge_requested IS 'original Iceberg REST purgeRequested value for audit';
COMMENT ON COLUMN entity_deletion.purge_job_type IS 'durable purge executor type';
COMMENT ON COLUMN entity_deletion.purge_job_id IS 'batch purge job that claimed this generation';
COMMENT ON COLUMN entity_deletion.cleanup_status IS 'PENDING | RUNNING | FAILED | SUCCEEDED';
COMMENT ON COLUMN entity_deletion.cleanup_attempt_count IS 'number of cleanup attempts';
COMMENT ON COLUMN entity_deletion.cleanup_last_error IS 'sanitized most recent cleanup error';
COMMENT ON COLUMN entity_deletion.accepted_restore_etag IS 'deletion action ETag accepted by successful UNDROP';
COMMENT ON COLUMN entity_deletion.request_id IS 'originating request id';
COMMENT ON COLUMN entity_deletion.correlation_id IS 'lifecycle correlation id';
COMMENT ON COLUMN entity_deletion.restored_at IS 'successful restore timestamp in milliseconds';
COMMENT ON COLUMN entity_deletion.purged_at IS 'successful purge timestamp in milliseconds';
COMMENT ON COLUMN entity_deletion.updated_at IS 'last lifecycle update timestamp in milliseconds';

CREATE TABLE IF NOT EXISTS entity_deletion_audit (
    audit_id VARCHAR(64) NOT NULL PRIMARY KEY,
    deletion_id VARCHAR(64) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    entity_id BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    action_revision BIGINT,
    prior_state VARCHAR(16),
    new_state VARCHAR(16),
    prior_cleanup_status VARCHAR(16),
    new_cleanup_status VARCHAR(16),
    purge_job_id VARCHAR(64),
    lease_epoch BIGINT,
    actor VARCHAR(128) NOT NULL,
    request_id VARCHAR(128),
    correlation_id VARCHAR(128) NOT NULL,
    reason_code VARCHAR(64),
    reason VARCHAR(2048),
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entity_deletion_audit_action
    ON entity_deletion_audit (deletion_id, created_at, audit_id);
COMMENT ON TABLE entity_deletion_audit IS 'append-only deletion lifecycle audit events';
COMMENT ON COLUMN entity_deletion_audit.audit_id IS 'audit event identifier';
COMMENT ON COLUMN entity_deletion_audit.deletion_id IS 'deletion generation identifier';
COMMENT ON COLUMN entity_deletion_audit.entity_type IS 'entity type';
COMMENT ON COLUMN entity_deletion_audit.entity_id IS 'immutable source entity id';
COMMENT ON COLUMN entity_deletion_audit.event_type IS 'lifecycle event type';
COMMENT ON COLUMN entity_deletion_audit.action_revision IS 'action revision associated with the event';
COMMENT ON COLUMN entity_deletion_audit.prior_state IS 'prior lifecycle state';
COMMENT ON COLUMN entity_deletion_audit.new_state IS 'new lifecycle state';
COMMENT ON COLUMN entity_deletion_audit.prior_cleanup_status IS 'prior cleanup status';
COMMENT ON COLUMN entity_deletion_audit.new_cleanup_status IS 'new cleanup status';
COMMENT ON COLUMN entity_deletion_audit.purge_job_id IS 'associated purge job id';
COMMENT ON COLUMN entity_deletion_audit.lease_epoch IS 'purge worker fencing epoch';
COMMENT ON COLUMN entity_deletion_audit.actor IS 'request actor or worker identity';
COMMENT ON COLUMN entity_deletion_audit.request_id IS 'request id';
COMMENT ON COLUMN entity_deletion_audit.correlation_id IS 'lifecycle correlation id';
COMMENT ON COLUMN entity_deletion_audit.reason_code IS 'bounded machine-readable reason';
COMMENT ON COLUMN entity_deletion_audit.reason IS 'sanitized reason without credentials or secrets';
COMMENT ON COLUMN entity_deletion_audit.created_at IS 'event timestamp in milliseconds';
