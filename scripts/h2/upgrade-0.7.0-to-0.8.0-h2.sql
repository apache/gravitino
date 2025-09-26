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
ALTER TABLE role_meta_securable_object ALTER COLUMN privilege_names CLOB;
ALTER TABLE role_meta_securable_object ALTER COLUMN privilege_conditions CLOB;

CREATE TABLE IF NOT EXISTS model_meta (
    model_id BIGINT NOT NULL,
    model_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    model_comment CLOB DEFAULT NULL,
    model_properties CLOB DEFAULT NULL,
    model_latest_version INT DEFAULT 0,
    audit_info CLOB NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (model_id),
    UNIQUE KEY uk_sid_mn_del (schema_id, model_name, deleted_at)
);

CREATE INDEX IF NOT EXISTS idx_mmid ON model_meta (metalake_id);
CREATE INDEX IF NOT EXISTS idx_mcid ON model_meta (catalog_id);
COMMENT ON TABLE model_meta IS 'model meta';
COMMENT ON COLUMN model_meta.model_id IS 'model id';
COMMENT ON COLUMN model_meta.model_name IS 'model name';
COMMENT ON COLUMN model_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN model_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN model_meta.schema_id IS 'schema id';
COMMENT ON COLUMN model_meta.model_comment IS 'model comment';
COMMENT ON COLUMN model_meta.model_properties IS 'model properties';
COMMENT ON COLUMN model_meta.model_latest_version IS 'model latest version';
COMMENT ON COLUMN model_meta.audit_info IS 'model audit info';
COMMENT ON COLUMN model_meta.deleted_at IS 'model deleted at';

CREATE TABLE IF NOT EXISTS model_version_info (
    id BIGINT NOT NULL AUTO_INCREMENT,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    model_id BIGINT NOT NULL,
    version INT NOT NULL,
    model_version_comment CLOB DEFAULT NULL,
    model_version_properties CLOB DEFAULT NULL,
    model_version_uri CLOB NOT NULL,
    audit_info CLOB NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_mid_ver_del (model_id, version, deleted_at)
);

CREATE INDEX IF NOT EXISTS idx_vmid ON model_version_info (metalake_id);
CREATE INDEX IF NOT EXISTS idx_vcid ON model_version_info (catalog_id);
CREATE INDEX IF NOT EXISTS idx_vsid ON model_version_info (schema_id);
COMMENT ON TABLE model_version_info IS 'model version info';
COMMENT ON COLUMN model_version_info.id IS 'auto increment id';
COMMENT ON COLUMN model_version_info.metalake_id IS 'metalake id';
COMMENT ON COLUMN model_version_info.catalog_id IS 'catalog id';
COMMENT ON COLUMN model_version_info.schema_id IS 'schema id';
COMMENT ON COLUMN model_version_info.model_id IS 'model id';
COMMENT ON COLUMN model_version_info.version IS 'model version';
COMMENT ON COLUMN model_version_info.model_version_comment IS 'model version comment';
COMMENT ON COLUMN model_version_info.model_version_properties IS 'model version properties';
COMMENT ON COLUMN model_version_info.model_version_uri IS 'model storage uri';
COMMENT ON COLUMN model_version_info.audit_info IS 'model version audit info';
COMMENT ON COLUMN model_version_info.deleted_at IS 'model version deleted at';

CREATE TABLE IF NOT EXISTS model_version_alias_rel (
    id BIGINT NOT NULL AUTO_INCREMENT,
    model_id BIGINT NOT NULL,
    model_version INT NOT NULL,
    model_version_alias VARCHAR(128) NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uk_mi_mva_del (model_id, model_version_alias, deleted_at)
);

CREATE INDEX IF NOT EXISTS idx_mva ON model_version_alias_rel (model_version_alias);
COMMENT ON TABLE model_version_alias_rel IS 'model version alias relation';
COMMENT ON COLUMN model_version_alias_rel.id IS 'auto increment id';
COMMENT ON COLUMN model_version_alias_rel.model_id IS 'model id';
COMMENT ON COLUMN model_version_alias_rel.model_version IS 'model version';
COMMENT ON COLUMN model_version_alias_rel.model_version_alias IS 'model version alias';
COMMENT ON COLUMN model_version_alias_rel.deleted_at IS 'model version alias deleted at';