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

COMMENT ON COLUMN table_meta.deletion_id IS 'table deletion generation identifier';

CREATE UNIQUE INDEX IF NOT EXISTS uk_tm_deletion ON table_meta (deletion_id);
CREATE INDEX IF NOT EXISTS idx_tm_deleted_action ON table_meta (deleted_at, deletion_id);

CREATE TABLE IF NOT EXISTS entity_deletion (
    deletion_id VARCHAR(64) NOT NULL PRIMARY KEY,
    state VARCHAR(16) NOT NULL,
    retention_expires_at BIGINT NOT NULL,
    purge_job_id VARCHAR(64)
);
CREATE INDEX IF NOT EXISTS idx_entity_deletion_gc
    ON entity_deletion (state, retention_expires_at, deletion_id);
CREATE INDEX IF NOT EXISTS idx_entity_deletion_purge_job
    ON entity_deletion (purge_job_id, deletion_id);
COMMENT ON TABLE entity_deletion IS 'active deletion lifecycle actions';
COMMENT ON COLUMN entity_deletion.deletion_id IS 'opaque identifier for one active deletion generation';
COMMENT ON COLUMN entity_deletion.state IS 'DELETED | PURGING';
COMMENT ON COLUMN entity_deletion.retention_expires_at IS 'fixed recovery deadline in milliseconds';
COMMENT ON COLUMN entity_deletion.purge_job_id IS 'batch purge job that claimed this generation';

ALTER TABLE iceberg_cleanup_job
    ADD COLUMN IF NOT EXISTS table_id BIGINT,
    ADD COLUMN IF NOT EXISTS deletion_id VARCHAR(64);
CREATE UNIQUE INDEX IF NOT EXISTS uk_icj_deletion ON iceberg_cleanup_job (deletion_id);
COMMENT ON COLUMN iceberg_cleanup_job.table_id IS 'immutable retained table id, NULL for legacy immediate-purge jobs';
COMMENT ON COLUMN iceberg_cleanup_job.deletion_id IS 'opaque retained deletion generation, NULL for legacy immediate-purge jobs';

ALTER TABLE iceberg_cleanup_job
    ADD COLUMN IF NOT EXISTS manifests_total BIGINT,
    ADD COLUMN IF NOT EXISTS manifests_done BIGINT;
COMMENT ON COLUMN iceberg_cleanup_job.manifests_total IS 'advisory number of manifests discovered, NULL before progress is reported';
COMMENT ON COLUMN iceberg_cleanup_job.manifests_done IS 'advisory number of manifests processed, NULL before progress is reported';
