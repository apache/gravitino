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

ALTER TABLE tag_meta ADD COLUMN IF NOT EXISTS allowed_values TEXT DEFAULT NULL;
COMMENT ON COLUMN tag_meta.allowed_values IS 'tag allowed values as a JSON string array, NULL allows any value, [] allows no value';

ALTER TABLE tag_relation_meta ADD COLUMN IF NOT EXISTS tag_value VARCHAR(256) NOT NULL DEFAULT '';
COMMENT ON COLUMN tag_relation_meta.tag_value IS 'tag assignment value, empty string means no value';

ALTER TABLE idp_group_meta ADD COLUMN IF NOT EXISTS group_comment VARCHAR(1024) DEFAULT '';
COMMENT ON COLUMN idp_group_meta.group_comment IS 'idp group comment';

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
    tombstone_id BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (policy_id, tag_id, tombstone_id)
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
COMMENT ON COLUMN policy_tag_relation_meta.tombstone_id IS 'unique discriminator for deleted policy tag relations';
