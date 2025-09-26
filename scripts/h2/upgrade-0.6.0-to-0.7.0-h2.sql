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
CREATE TABLE IF NOT EXISTS table_column_version_info (
    id BIGINT NOT NULL AUTO_INCREMENT,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    table_version INT NOT NULL,
    column_id BIGINT NOT NULL,
    column_name VARCHAR(128) NOT NULL,
    column_position INT NOT NULL,
    column_type CLOB NOT NULL,
    column_comment VARCHAR(256) DEFAULT '',
    column_nullable TINYINT NOT NULL DEFAULT 1,
    column_auto_increment TINYINT NOT NULL DEFAULT 0,
    column_default_value CLOB DEFAULT NULL,
    column_op_type TINYINT NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    audit_info CLOB NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tid_ver_cid_del (table_id, table_version, column_id, deleted_at)
);


CREATE INDEX IF NOT EXISTS idx_tcmid ON table_column_version_info (metalake_id);
CREATE INDEX IF NOT EXISTS idx_tccid ON table_column_version_info (catalog_id);
CREATE INDEX IF NOT EXISTS idx_tcsid ON table_column_version_info (schema_id);
COMMENT ON TABLE table_column_version_info IS 'table column version info';
COMMENT ON COLUMN table_column_version_info.id IS 'auto increment id';
COMMENT ON COLUMN table_column_version_info.metalake_id IS 'metalake id';
COMMENT ON COLUMN table_column_version_info.catalog_id IS 'catalog id';
COMMENT ON COLUMN table_column_version_info.schema_id IS 'schema id';
COMMENT ON COLUMN table_column_version_info.table_id IS 'table id';
COMMENT ON COLUMN table_column_version_info.table_version IS 'table version';
COMMENT ON COLUMN table_column_version_info.column_id IS 'column id';
COMMENT ON COLUMN table_column_version_info.column_name IS 'column name';
COMMENT ON COLUMN table_column_version_info.column_position IS 'column position, starting from 0';
COMMENT ON COLUMN table_column_version_info.column_type IS 'column type';
COMMENT ON COLUMN table_column_version_info.column_comment IS 'column comment';
COMMENT ON COLUMN table_column_version_info.column_nullable IS 'column nullable, 0 is not nullable, 1 is nullable';
COMMENT ON COLUMN table_column_version_info.column_auto_increment IS 'column auto increment, 0 is not auto increment, 1 is auto increment';
COMMENT ON COLUMN table_column_version_info.column_default_value IS 'column default value';
COMMENT ON COLUMN table_column_version_info.column_op_type IS 'column operation type, 1 is create, 2 is update, 3 is delete';
COMMENT ON COLUMN table_column_version_info.deleted_at IS 'column deleted at';
COMMENT ON COLUMN table_column_version_info.audit_info IS 'column audit info';