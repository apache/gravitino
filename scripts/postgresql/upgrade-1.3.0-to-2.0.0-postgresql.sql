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





-- Preserve policy_relation_meta from pre-2.0 installations, including its existing data.
-- The 2.0 server no longer reads direct object-policy assignments from this table.

ALTER TABLE table_column_version_info
    ALTER COLUMN column_comment TYPE VARCHAR(4096);

ALTER TABLE model_meta ADD COLUMN IF NOT EXISTS current_version INT NOT NULL DEFAULT 1;
ALTER TABLE model_meta ADD COLUMN IF NOT EXISTS last_version INT NOT NULL DEFAULT 1;
COMMENT ON COLUMN model_meta.current_version IS 'model current version';
COMMENT ON COLUMN model_meta.last_version IS 'model last allocated version';

ALTER TABLE tag_meta ADD COLUMN IF NOT EXISTS allowed_values TEXT DEFAULT NULL;
COMMENT ON COLUMN tag_meta.allowed_values IS 'tag allowed values as a JSON string array, NULL allows any value, [] allows no value';

ALTER TABLE tag_relation_meta ADD COLUMN IF NOT EXISTS tag_value VARCHAR(256) NOT NULL DEFAULT '';
COMMENT ON COLUMN tag_relation_meta.tag_value IS 'tag assignment value, empty string means no value';

ALTER TABLE idp_user_meta ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE;
COMMENT ON COLUMN idp_user_meta.enabled IS 'whether the user is enabled, 0 is disabled, 1 is enabled';

ALTER TABLE idp_group_meta ADD COLUMN IF NOT EXISTS group_comment VARCHAR(1024) DEFAULT '';
COMMENT ON COLUMN idp_group_meta.group_comment IS 'idp group comment';

ALTER TABLE idp_user_meta ADD COLUMN IF NOT EXISTS audit_info TEXT NOT NULL DEFAULT '{}';
COMMENT ON COLUMN idp_user_meta.audit_info IS 'idp user audit info';

ALTER TABLE idp_group_meta ADD COLUMN IF NOT EXISTS audit_info TEXT NOT NULL DEFAULT '{}';
COMMENT ON COLUMN idp_group_meta.audit_info IS 'idp group audit info';

ALTER TABLE idp_user_group_rel ADD COLUMN IF NOT EXISTS audit_info TEXT NOT NULL DEFAULT '{}';
COMMENT ON COLUMN idp_user_group_rel.audit_info IS 'idp user group relation audit info';

ALTER TABLE tag_relation_meta DROP CONSTRAINT IF EXISTS tag_relation_meta_tag_id_metadata_object_id_metadata_object_key;

CREATE UNIQUE INDEX IF NOT EXISTS uk_ti_mi_mo_tv_del ON tag_relation_meta (tag_id, metadata_object_id, metadata_object_type, tag_value, deleted_at);
CREATE INDEX IF NOT EXISTS tag_relation_meta_idx_tag_id_value ON tag_relation_meta (tag_id, tag_value);

ALTER TABLE job_run_meta ADD COLUMN IF NOT EXISTS job_started_at BIGINT NOT NULL DEFAULT 0;
COMMENT ON COLUMN job_run_meta.job_started_at IS 'job run started at';

CREATE TABLE IF NOT EXISTS policy_tag_relation_meta (
    id BIGSERIAL NOT NULL,
    policy_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    selector TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (policy_id, tag_id, deleted_at)
);

CREATE INDEX IF NOT EXISTS policy_tag_relation_meta_idx_tag_id ON policy_tag_relation_meta (tag_id);
COMMENT ON TABLE policy_tag_relation_meta IS 'policy tag relation';
COMMENT ON COLUMN policy_tag_relation_meta.id IS 'auto increment id';
COMMENT ON COLUMN policy_tag_relation_meta.policy_id IS 'policy id';
COMMENT ON COLUMN policy_tag_relation_meta.tag_id IS 'tag id';
COMMENT ON COLUMN policy_tag_relation_meta.selector IS 'policy tag selector JSON, NULL matches tag presence';
COMMENT ON COLUMN policy_tag_relation_meta.audit_info IS 'policy tag relation audit info';
COMMENT ON COLUMN policy_tag_relation_meta.current_version IS 'policy tag relation current version';
COMMENT ON COLUMN policy_tag_relation_meta.last_version IS 'policy tag relation last version';
COMMENT ON COLUMN policy_tag_relation_meta.deleted_at IS 'policy tag relation deleted at';

ALTER TABLE job_run_meta ADD COLUMN IF NOT EXISTS runtime_job_template TEXT DEFAULT NULL;
COMMENT ON COLUMN job_run_meta.runtime_job_template IS 'job run runtime job template';

CREATE TABLE IF NOT EXISTS semantic_model_meta (
    semantic_model_id BIGINT NOT NULL,
    semantic_model_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (semantic_model_id),
    UNIQUE (schema_id, semantic_model_name, deleted_at)
);

CREATE INDEX IF NOT EXISTS semantic_model_meta_idx_metalake_id ON semantic_model_meta (metalake_id);
CREATE INDEX IF NOT EXISTS semantic_model_meta_idx_catalog_id ON semantic_model_meta (catalog_id);
COMMENT ON TABLE semantic_model_meta IS 'semantic model metadata';

COMMENT ON COLUMN semantic_model_meta.semantic_model_id IS 'semantic model id';
COMMENT ON COLUMN semantic_model_meta.semantic_model_name IS 'semantic model name';
COMMENT ON COLUMN semantic_model_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN semantic_model_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN semantic_model_meta.schema_id IS 'schema id';
COMMENT ON COLUMN semantic_model_meta.audit_info IS 'semantic model identity audit info';
COMMENT ON COLUMN semantic_model_meta.current_version IS 'current version';
COMMENT ON COLUMN semantic_model_meta.last_version IS 'last allocated version';
COMMENT ON COLUMN semantic_model_meta.deleted_at IS 'semantic model deleted at';

CREATE TABLE IF NOT EXISTS semantic_model_version_info (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    semantic_model_id BIGINT NOT NULL,
    version INT NOT NULL,
    semantic_model_name VARCHAR(128) NOT NULL,
    semantic_model_comment TEXT DEFAULT NULL,
    datasets TEXT NOT NULL,
    relationships TEXT DEFAULT NULL,
    metrics TEXT DEFAULT NULL,
    ai_context TEXT DEFAULT NULL,
    custom_extensions TEXT DEFAULT NULL,
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (semantic_model_id, version, deleted_at)
);

CREATE INDEX IF NOT EXISTS semantic_model_version_info_idx_metalake_id ON semantic_model_version_info (metalake_id);
CREATE INDEX IF NOT EXISTS semantic_model_version_info_idx_catalog_id ON semantic_model_version_info (catalog_id);
CREATE INDEX IF NOT EXISTS semantic_model_version_info_idx_schema_id ON semantic_model_version_info (schema_id);
COMMENT ON TABLE semantic_model_version_info IS 'semantic model version information';

COMMENT ON COLUMN semantic_model_version_info.id IS 'auto increment id';
COMMENT ON COLUMN semantic_model_version_info.metalake_id IS 'metalake id';
COMMENT ON COLUMN semantic_model_version_info.catalog_id IS 'catalog id';
COMMENT ON COLUMN semantic_model_version_info.schema_id IS 'schema id';
COMMENT ON COLUMN semantic_model_version_info.semantic_model_id IS 'semantic model id';
COMMENT ON COLUMN semantic_model_version_info.version IS 'semantic model version';
COMMENT ON COLUMN semantic_model_version_info.semantic_model_name IS 'semantic model name snapshot';
COMMENT ON COLUMN semantic_model_version_info.semantic_model_comment IS 'semantic model comment snapshot';
COMMENT ON COLUMN semantic_model_version_info.datasets IS 'dataset definitions snapshot (JSON)';
COMMENT ON COLUMN semantic_model_version_info.relationships IS 'relationship definitions snapshot (JSON)';
COMMENT ON COLUMN semantic_model_version_info.metrics IS 'metric definitions snapshot (JSON)';
COMMENT ON COLUMN semantic_model_version_info.ai_context IS 'model AI context snapshot (JSON)';
COMMENT ON COLUMN semantic_model_version_info.custom_extensions IS 'model custom extensions snapshot (JSON)';
COMMENT ON COLUMN semantic_model_version_info.properties IS 'semantic model properties snapshot (JSON)';
COMMENT ON COLUMN semantic_model_version_info.audit_info IS 'semantic model version audit info';
COMMENT ON COLUMN semantic_model_version_info.deleted_at IS 'version deleted at';

CREATE TABLE IF NOT EXISTS scheduled_expand_tasks (
    task_name VARCHAR(100) NOT NULL,
    task_instance VARCHAR(100) NOT NULL,
    task_data BYTEA,
    execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
    picked BOOLEAN NOT NULL,
    picked_by VARCHAR(50),
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    version BIGINT NOT NULL,
    consecutive_failures INT,
    last_success TIMESTAMP WITH TIME ZONE,
    last_failure TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (task_name, task_instance)
);
CREATE INDEX IF NOT EXISTS scheduled_expand_tasks_execution_time_idx ON scheduled_expand_tasks (execution_time);
CREATE INDEX IF NOT EXISTS scheduled_expand_tasks_last_heartbeat_idx ON scheduled_expand_tasks (last_heartbeat);
COMMENT ON TABLE scheduled_expand_tasks IS 'table maintenance expand schedule metadata';
COMMENT ON COLUMN scheduled_expand_tasks.task_name IS 'task name';
COMMENT ON COLUMN scheduled_expand_tasks.task_instance IS 'task instance';
COMMENT ON COLUMN scheduled_expand_tasks.task_data IS 'serialized task data';
COMMENT ON COLUMN scheduled_expand_tasks.execution_time IS 'next execution time';
COMMENT ON COLUMN scheduled_expand_tasks.picked IS 'whether the task is currently picked';
COMMENT ON COLUMN scheduled_expand_tasks.picked_by IS 'scheduler instance that picked the task';
COMMENT ON COLUMN scheduled_expand_tasks.last_heartbeat IS 'last heartbeat from the picker';
COMMENT ON COLUMN scheduled_expand_tasks.version IS 'optimistic lock version';
COMMENT ON COLUMN scheduled_expand_tasks.consecutive_failures IS 'consecutive failure count';
COMMENT ON COLUMN scheduled_expand_tasks.last_success IS 'last successful execution time';
COMMENT ON COLUMN scheduled_expand_tasks.last_failure IS 'last failed execution time';

CREATE TABLE IF NOT EXISTS scheduled_cron_tasks (
    task_name VARCHAR(100) NOT NULL,
    task_instance VARCHAR(100) NOT NULL,
    task_data BYTEA,
    execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
    picked BOOLEAN NOT NULL,
    picked_by VARCHAR(50),
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    version BIGINT NOT NULL,
    consecutive_failures INT,
    last_success TIMESTAMP WITH TIME ZONE,
    last_failure TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (task_name, task_instance)
);
CREATE INDEX IF NOT EXISTS scheduled_cron_tasks_execution_time_idx ON scheduled_cron_tasks (execution_time);
CREATE INDEX IF NOT EXISTS scheduled_cron_tasks_last_heartbeat_idx ON scheduled_cron_tasks (last_heartbeat);
COMMENT ON TABLE scheduled_cron_tasks IS 'table maintenance cron schedule metadata';
COMMENT ON COLUMN scheduled_cron_tasks.task_name IS 'task name';
COMMENT ON COLUMN scheduled_cron_tasks.task_instance IS 'task instance';
COMMENT ON COLUMN scheduled_cron_tasks.task_data IS 'serialized task data';
COMMENT ON COLUMN scheduled_cron_tasks.execution_time IS 'next execution time';
COMMENT ON COLUMN scheduled_cron_tasks.picked IS 'whether the task is currently picked';
COMMENT ON COLUMN scheduled_cron_tasks.picked_by IS 'scheduler instance that picked the task';
COMMENT ON COLUMN scheduled_cron_tasks.last_heartbeat IS 'last heartbeat from the picker';
COMMENT ON COLUMN scheduled_cron_tasks.version IS 'optimistic lock version';
COMMENT ON COLUMN scheduled_cron_tasks.consecutive_failures IS 'consecutive failure count';
COMMENT ON COLUMN scheduled_cron_tasks.last_success IS 'last successful execution time';
COMMENT ON COLUMN scheduled_cron_tasks.last_failure IS 'last failed execution time';

CREATE TABLE IF NOT EXISTS scheduled_commit_tasks (
    task_name VARCHAR(100) NOT NULL,
    task_instance VARCHAR(100) NOT NULL,
    task_data BYTEA,
    execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
    picked BOOLEAN NOT NULL,
    picked_by VARCHAR(50),
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    version BIGINT NOT NULL,
    consecutive_failures INT,
    last_success TIMESTAMP WITH TIME ZONE,
    last_failure TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (task_name, task_instance)
);
CREATE INDEX IF NOT EXISTS scheduled_commit_tasks_execution_time_idx ON scheduled_commit_tasks (execution_time);
CREATE INDEX IF NOT EXISTS scheduled_commit_tasks_last_heartbeat_idx ON scheduled_commit_tasks (last_heartbeat);
COMMENT ON TABLE scheduled_commit_tasks IS 'table maintenance commit schedule metadata';
COMMENT ON COLUMN scheduled_commit_tasks.task_name IS 'task name';
COMMENT ON COLUMN scheduled_commit_tasks.task_instance IS 'task instance';
COMMENT ON COLUMN scheduled_commit_tasks.task_data IS 'serialized task data';
COMMENT ON COLUMN scheduled_commit_tasks.execution_time IS 'next execution time';
COMMENT ON COLUMN scheduled_commit_tasks.picked IS 'whether the task is currently picked';
COMMENT ON COLUMN scheduled_commit_tasks.picked_by IS 'scheduler instance that picked the task';
COMMENT ON COLUMN scheduled_commit_tasks.last_heartbeat IS 'last heartbeat from the picker';
COMMENT ON COLUMN scheduled_commit_tasks.version IS 'optimistic lock version';
COMMENT ON COLUMN scheduled_commit_tasks.consecutive_failures IS 'consecutive failure count';
COMMENT ON COLUMN scheduled_commit_tasks.last_success IS 'last successful execution time';
COMMENT ON COLUMN scheduled_commit_tasks.last_failure IS 'last failed execution time';

CREATE TABLE IF NOT EXISTS table_maintenance_action (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT,
    table_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    before_snapshot_id BIGINT,
    after_snapshot_id BIGINT,
    finished_at BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tma_job_id ON table_maintenance_action (job_id);
-- Soft slot: finished_at=0 is exclusive per table/policy (one in-flight finish).
CREATE UNIQUE INDEX IF NOT EXISTS uk_tma_table_policy_finished ON table_maintenance_action (table_id, policy_id, finished_at);
CREATE INDEX IF NOT EXISTS idx_tma_before_sid ON table_maintenance_action (table_id, before_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_tma_after_sid ON table_maintenance_action (table_id, after_snapshot_id);
CREATE INDEX IF NOT EXISTS idx_tma_created_at ON table_maintenance_action (created_at);
COMMENT ON TABLE table_maintenance_action IS 'table maintenance action metadata';
COMMENT ON COLUMN table_maintenance_action.id IS 'auto increment id';
COMMENT ON COLUMN table_maintenance_action.job_id IS 'job run id, maps to job_run_meta.job_run_id';
COMMENT ON COLUMN table_maintenance_action.table_id IS 'table id from table_meta';
COMMENT ON COLUMN table_maintenance_action.policy_id IS 'policy id from policy_meta';
COMMENT ON COLUMN table_maintenance_action.before_snapshot_id IS 'Iceberg snapshot id before maintenance, null until sampled';
COMMENT ON COLUMN table_maintenance_action.after_snapshot_id IS 'Iceberg snapshot id after maintenance, null while pending';
COMMENT ON COLUMN table_maintenance_action.finished_at IS 'finish millis, 0 = soft in-flight slot (one per table/policy)';
COMMENT ON COLUMN table_maintenance_action.created_at IS 'creation timestamp in milliseconds';

CREATE TABLE IF NOT EXISTS table_snapshot_metrics (
    id BIGSERIAL PRIMARY KEY,
    table_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    metrics_value TEXT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uk_tid_pid_sid ON table_snapshot_metrics (table_id, policy_id, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_tsm_created_at ON table_snapshot_metrics (created_at);
COMMENT ON TABLE table_snapshot_metrics IS 'table snapshot metrics metadata';
COMMENT ON COLUMN table_snapshot_metrics.id IS 'auto increment id';
COMMENT ON COLUMN table_snapshot_metrics.table_id IS 'table id from table_meta';
COMMENT ON COLUMN table_snapshot_metrics.policy_id IS 'policy id from policy_meta';
COMMENT ON COLUMN table_snapshot_metrics.snapshot_id IS 'Iceberg snapshot id';
COMMENT ON COLUMN table_snapshot_metrics.metrics_value IS 'metrics value as JSON';
COMMENT ON COLUMN table_snapshot_metrics.created_at IS 'creation timestamp in milliseconds';

-- Merge duplicate live owners left by concurrent assignments: the newest live row
-- (largest id) wins, and older ones are soft-deleted.
UPDATE owner_meta
    SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT),
        updated_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)
    WHERE deleted_at = 0
      AND id < (
        SELECT MAX(d.id) FROM owner_meta d
        WHERE d.deleted_at = 0
          AND d.metadata_object_id = owner_meta.metadata_object_id
          AND d.metadata_object_type = owner_meta.metadata_object_type
      );

-- Separate the optimistic-concurrency token from the history version for fileset and policy.
-- Until now current_version served as both: it is the join key into *_version_info and the value
-- the CAS compares, so every alter had to advance it and write a snapshot even when nothing in
-- that snapshot changed. occ_version takes over the CAS; current_version again advances only when
-- the stored snapshot changes. The default is the whole backfill, because occ_version is only ever
-- compared against itself on the same row.
ALTER TABLE fileset_meta ADD COLUMN IF NOT EXISTS occ_version INT NOT NULL DEFAULT 1;
COMMENT ON COLUMN fileset_meta.occ_version IS 'fileset optimistic concurrency version';

ALTER TABLE policy_meta ADD COLUMN IF NOT EXISTS occ_version INT NOT NULL DEFAULT 1;
COMMENT ON COLUMN policy_meta.occ_version IS 'policy optimistic concurrency version';

CREATE TABLE IF NOT EXISTS domain_meta (
    domain_id BIGINT NOT NULL,
    domain_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    domain_comment VARCHAR(256) DEFAULT '',
    domain_status VARCHAR(16) NOT NULL DEFAULT 'DRAFT',
    backing_tag_id BIGINT DEFAULT NULL,
    backing_tag_value VARCHAR(256) DEFAULT NULL,
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (domain_id),
    UNIQUE (metalake_id, domain_name, deleted_at)
);
CREATE INDEX IF NOT EXISTS domain_meta_idx_backing_tag ON domain_meta (metalake_id, backing_tag_id, backing_tag_value, deleted_at);
COMMENT ON TABLE domain_meta IS 'domain metadata';
COMMENT ON COLUMN domain_meta.domain_id IS 'domain id';
COMMENT ON COLUMN domain_meta.domain_name IS 'domain name';
COMMENT ON COLUMN domain_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN domain_meta.domain_comment IS 'domain comment';
COMMENT ON COLUMN domain_meta.domain_status IS 'domain status, DRAFT or PUBLISHED';
COMMENT ON COLUMN domain_meta.backing_tag_id IS 'id of the backing tag, NULL if the domain has none';
COMMENT ON COLUMN domain_meta.backing_tag_value IS 'value of the backing tag, NULL binds the tag without a value';
COMMENT ON COLUMN domain_meta.properties IS 'domain properties';
COMMENT ON COLUMN domain_meta.audit_info IS 'domain audit info';
COMMENT ON COLUMN domain_meta.current_version IS 'domain current version';
COMMENT ON COLUMN domain_meta.last_version IS 'domain last version';
COMMENT ON COLUMN domain_meta.deleted_at IS 'domain deleted at';

CREATE TABLE IF NOT EXISTS domain_rel (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    metalake_id BIGINT NOT NULL,
    parent_id BIGINT NOT NULL,
    child_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    CONSTRAINT uk_dr_ci_del UNIQUE (child_id, deleted_at)
);
CREATE INDEX IF NOT EXISTS domain_rel_idx_parent ON domain_rel (parent_id, deleted_at);
CREATE INDEX IF NOT EXISTS domain_rel_idx_metalake ON domain_rel (metalake_id, deleted_at);
COMMENT ON TABLE domain_rel IS 'domain parent relation';
COMMENT ON COLUMN domain_rel.id IS 'auto increment id';
COMMENT ON COLUMN domain_rel.metalake_id IS 'metalake id';
COMMENT ON COLUMN domain_rel.parent_id IS 'parent domain id';
COMMENT ON COLUMN domain_rel.child_id IS 'sub-domain id';
COMMENT ON COLUMN domain_rel.audit_info IS 'domain relation audit info';
COMMENT ON COLUMN domain_rel.current_version IS 'domain relation current version';
COMMENT ON COLUMN domain_rel.last_version IS 'domain relation last version';
COMMENT ON COLUMN domain_rel.deleted_at IS 'domain relation deleted at';
