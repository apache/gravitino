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

-- Note: Database and schema creation is not included in this script. Please create the database and
-- schema before running this script. for example in psql:
-- CREATE DATABASE example_db;
-- \c example_db
-- CREATE SCHEMA example_schema;
-- set search_path to example_schema;


CREATE TABLE IF NOT EXISTS metalake_meta (
    metalake_id BIGINT NOT NULL,
    metalake_name VARCHAR(128) NOT NULL,
    metalake_comment VARCHAR(256) DEFAULT '',
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (metalake_id),
    UNIQUE (metalake_name, deleted_at)
    );
COMMENT ON TABLE metalake_meta IS 'metalake metadata';

COMMENT ON COLUMN metalake_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN metalake_meta.metalake_name IS 'metalake name';
COMMENT ON COLUMN metalake_meta.metalake_comment IS 'metalake comment';
COMMENT ON COLUMN metalake_meta.properties IS 'metalake properties';
COMMENT ON COLUMN metalake_meta.audit_info IS 'metalake audit info';
COMMENT ON COLUMN metalake_meta.schema_version IS 'metalake schema version info';
COMMENT ON COLUMN metalake_meta.current_version IS 'metalake current version';
COMMENT ON COLUMN metalake_meta.last_version IS 'metalake last version';
COMMENT ON COLUMN metalake_meta.deleted_at IS 'metalake deleted at';


CREATE TABLE IF NOT EXISTS catalog_meta (
    catalog_id BIGINT NOT NULL,
    catalog_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    type VARCHAR(64) NOT NULL,
    provider VARCHAR(64) NOT NULL,
    catalog_comment VARCHAR(256) DEFAULT '',
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (catalog_id),
    UNIQUE (metalake_id, catalog_name, deleted_at)
    );

COMMENT ON TABLE catalog_meta IS 'catalog metadata';

COMMENT ON COLUMN catalog_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN catalog_meta.catalog_name IS 'catalog name';
COMMENT ON COLUMN catalog_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN catalog_meta.type IS 'catalog type';
COMMENT ON COLUMN catalog_meta.provider IS 'catalog provider';
COMMENT ON COLUMN catalog_meta.catalog_comment IS 'catalog comment';
COMMENT ON COLUMN catalog_meta.properties IS 'catalog properties';
COMMENT ON COLUMN catalog_meta.audit_info IS 'catalog audit info';
COMMENT ON COLUMN catalog_meta.current_version IS 'catalog current version';
COMMENT ON COLUMN catalog_meta.last_version IS 'catalog last version';
COMMENT ON COLUMN catalog_meta.deleted_at IS 'catalog deleted at';


CREATE TABLE IF NOT EXISTS schema_meta (
    schema_id BIGINT NOT NULL,
    schema_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_comment VARCHAR(256) DEFAULT '',
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (schema_id),
    UNIQUE (catalog_id, schema_name, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_metalake_id ON schema_meta (metalake_id);
COMMENT ON TABLE schema_meta IS 'schema metadata';

COMMENT ON COLUMN schema_meta.schema_id IS 'schema id';
COMMENT ON COLUMN schema_meta.schema_name IS 'schema name';
COMMENT ON COLUMN schema_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN schema_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN schema_meta.schema_comment IS 'schema comment';
COMMENT ON COLUMN schema_meta.properties IS 'schema properties';
COMMENT ON COLUMN schema_meta.audit_info IS 'schema audit info';
COMMENT ON COLUMN schema_meta.current_version IS 'schema current version';
COMMENT ON COLUMN schema_meta.last_version IS 'schema last version';
COMMENT ON COLUMN schema_meta.deleted_at IS 'schema deleted at';


CREATE TABLE IF NOT EXISTS table_meta (
    table_id BIGINT NOT NULL,
    table_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (table_id),
    UNIQUE (schema_id, table_name, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_metalake_id ON table_meta (metalake_id);
CREATE INDEX IF NOT EXISTS idx_catalog_id ON table_meta (catalog_id);
COMMENT ON TABLE table_meta IS 'table metadata';

COMMENT ON COLUMN table_meta.table_id IS 'table id';
COMMENT ON COLUMN table_meta.table_name IS 'table name';
COMMENT ON COLUMN table_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN table_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN table_meta.schema_id IS 'schema id';
COMMENT ON COLUMN table_meta.audit_info IS 'table audit info';
COMMENT ON COLUMN table_meta.current_version IS 'table current version';
COMMENT ON COLUMN table_meta.last_version IS 'table last version';
COMMENT ON COLUMN table_meta.deleted_at IS 'table deleted at';

CREATE TABLE IF NOT EXISTS table_column_version_info (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    table_version INT NOT NULL,
    column_id BIGINT NOT NULL,
    column_name VARCHAR(128) NOT NULL,
    column_position INT NOT NULL,
    column_type TEXT NOT NULL,
    column_comment VARCHAR(256) DEFAULT '',
    column_nullable SMALLINT NOT NULL DEFAULT 1,
    column_auto_increment SMALLINT NOT NULL DEFAULT 0,
    column_default_value TEXT DEFAULT NULL,
    column_op_type SMALLINT NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    audit_info TEXT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (table_id, table_version, column_id, deleted_at)
);
CREATE INDEX idx_mid ON table_column_version_info (metalake_id);
CREATE INDEX idx_cid ON table_column_version_info (catalog_id);
CREATE INDEX idx_sid ON table_column_version_info (schema_id);
COMMENT ON TABLE table_column_version_info IS 'table column version information';

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


CREATE TABLE IF NOT EXISTS fileset_meta (
    fileset_id BIGINT NOT NULL,
    fileset_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    type VARCHAR(64) NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (fileset_id),
    UNIQUE (schema_id, fileset_name, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_metalake_id ON fileset_meta (metalake_id);
CREATE INDEX IF NOT EXISTS idx_catalog_id ON fileset_meta (catalog_id);
COMMENT ON TABLE fileset_meta IS 'fileset metadata';

COMMENT ON COLUMN fileset_meta.fileset_id IS 'fileset id';
COMMENT ON COLUMN fileset_meta.fileset_name IS 'fileset name';
COMMENT ON COLUMN fileset_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN fileset_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN fileset_meta.schema_id IS 'schema id';
COMMENT ON COLUMN fileset_meta.type IS 'fileset type';
COMMENT ON COLUMN fileset_meta.audit_info IS 'fileset audit info';
COMMENT ON COLUMN fileset_meta.current_version IS 'fileset current version';
COMMENT ON COLUMN fileset_meta.last_version IS 'fileset last version';
COMMENT ON COLUMN fileset_meta.deleted_at IS 'fileset deleted at';


CREATE TABLE IF NOT EXISTS fileset_version_info (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    fileset_id BIGINT NOT NULL,
    version INT NOT NULL,
    fileset_comment VARCHAR(256) DEFAULT '',
    properties TEXT DEFAULT NULL,
    storage_location TEXT NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (fileset_id, version, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_metalake_id ON fileset_version_info (metalake_id);
CREATE INDEX IF NOT EXISTS idx_catalog_id ON fileset_version_info (catalog_id);
CREATE INDEX IF NOT EXISTS idx_schema_id ON fileset_version_info (schema_id);
COMMENT ON TABLE fileset_version_info IS 'fileset version information';

COMMENT ON COLUMN fileset_version_info.id IS 'auto increment id';
COMMENT ON COLUMN fileset_version_info.metalake_id IS 'metalake id';
COMMENT ON COLUMN fileset_version_info.catalog_id IS 'catalog id';
COMMENT ON COLUMN fileset_version_info.schema_id IS 'schema id';
COMMENT ON COLUMN fileset_version_info.fileset_id IS 'fileset id';
COMMENT ON COLUMN fileset_version_info.version IS 'fileset info version';
COMMENT ON COLUMN fileset_version_info.fileset_comment IS 'fileset comment';
COMMENT ON COLUMN fileset_version_info.properties IS 'fileset properties';
COMMENT ON COLUMN fileset_version_info.storage_location IS 'fileset storage location';
COMMENT ON COLUMN fileset_version_info.deleted_at IS 'fileset deleted at';


CREATE TABLE IF NOT EXISTS topic_meta (
    topic_id BIGINT NOT NULL,
    topic_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    comment VARCHAR(256) DEFAULT '',
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id),
    UNIQUE (schema_id, topic_name, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_metalake_id ON topic_meta (metalake_id);
CREATE INDEX IF NOT EXISTS idx_catalog_id ON topic_meta (catalog_id);
COMMENT ON TABLE topic_meta IS 'topic metadata';

COMMENT ON COLUMN topic_meta.topic_id IS 'topic id';
COMMENT ON COLUMN topic_meta.topic_name IS 'topic name';
COMMENT ON COLUMN topic_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN topic_meta.catalog_id IS 'catalog id';
COMMENT ON COLUMN topic_meta.schema_id IS 'schema id';
COMMENT ON COLUMN topic_meta.comment IS 'topic comment';
COMMENT ON COLUMN topic_meta.properties IS 'topic properties';
COMMENT ON COLUMN topic_meta.audit_info IS 'topic audit info';
COMMENT ON COLUMN topic_meta.current_version IS 'topic current version';
COMMENT ON COLUMN topic_meta.last_version IS 'topic last version';
COMMENT ON COLUMN topic_meta.deleted_at IS 'topic deleted at';


CREATE TABLE IF NOT EXISTS user_meta (
    user_id BIGINT NOT NULL,
    user_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id),
    UNIQUE (metalake_id, user_name, deleted_at)
    );
COMMENT ON TABLE user_meta IS 'user metadata';

COMMENT ON COLUMN user_meta.user_id IS 'user id';
COMMENT ON COLUMN user_meta.user_name IS 'username';
COMMENT ON COLUMN user_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN user_meta.audit_info IS 'user audit info';
COMMENT ON COLUMN user_meta.current_version IS 'user current version';
COMMENT ON COLUMN user_meta.last_version IS 'user last version';
COMMENT ON COLUMN user_meta.deleted_at IS 'user deleted at';

CREATE TABLE IF NOT EXISTS role_meta (
    role_id BIGINT NOT NULL,
    role_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (role_id),
    UNIQUE (metalake_id, role_name, deleted_at)
    );

COMMENT ON TABLE role_meta IS 'role metadata';

COMMENT ON COLUMN role_meta.role_id IS 'role id';
COMMENT ON COLUMN role_meta.role_name IS 'role name';
COMMENT ON COLUMN role_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN role_meta.properties IS 'role properties';
COMMENT ON COLUMN role_meta.audit_info IS 'role audit info';
COMMENT ON COLUMN role_meta.current_version IS 'role current version';
COMMENT ON COLUMN role_meta.last_version IS 'role last version';
COMMENT ON COLUMN role_meta.deleted_at IS 'role deleted at';


CREATE TABLE IF NOT EXISTS role_meta_securable_object (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    role_id BIGINT NOT NULL,
    metadata_object_id BIGINT NOT NULL,
    type  VARCHAR(128) NOT NULL,
    privilege_names VARCHAR(256) NOT NULL,
    privilege_conditions VARCHAR(256) NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
    );

CREATE INDEX IF NOT EXISTS idx_role_id ON role_meta_securable_object (role_id);
COMMENT ON TABLE role_meta_securable_object IS 'role to securable object relation metadata';

COMMENT ON COLUMN role_meta_securable_object.id IS 'auto increment id';
COMMENT ON COLUMN role_meta_securable_object.role_id IS 'role id';
COMMENT ON COLUMN role_meta_securable_object.metadata_object_id IS 'The entity id of securable object';
COMMENT ON COLUMN role_meta_securable_object.type IS 'securable object type';
COMMENT ON COLUMN role_meta_securable_object.privilege_names IS 'securable object privilege names';
COMMENT ON COLUMN role_meta_securable_object.privilege_conditions IS 'securable object privilege conditions';
COMMENT ON COLUMN role_meta_securable_object.current_version IS 'securable object current version';
COMMENT ON COLUMN role_meta_securable_object.last_version IS 'securable object last version';
COMMENT ON COLUMN role_meta_securable_object.deleted_at IS 'securable object deleted at';


CREATE TABLE IF NOT EXISTS user_role_rel (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    user_id BIGINT NOT NULL,
    role_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (user_id, role_id, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_user_id ON user_role_rel (user_id);
COMMENT ON TABLE user_role_rel IS 'user role relation metadata';

COMMENT ON COLUMN user_role_rel.id IS 'auto increment id';
COMMENT ON COLUMN user_role_rel.user_id IS 'user id';
COMMENT ON COLUMN user_role_rel.role_id IS 'role id';
COMMENT ON COLUMN user_role_rel.audit_info IS 'relation audit info';
COMMENT ON COLUMN user_role_rel.current_version IS 'relation current version';
COMMENT ON COLUMN user_role_rel.last_version IS 'relation last version';
COMMENT ON COLUMN user_role_rel.deleted_at IS 'relation deleted at';


CREATE TABLE IF NOT EXISTS group_meta (
    group_id BIGINT NOT NULL,
    group_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (group_id),
    UNIQUE (metalake_id, group_name, deleted_at)
    );
COMMENT ON TABLE group_meta IS 'group metadata';

COMMENT ON COLUMN group_meta.group_id IS 'group id';
COMMENT ON COLUMN group_meta.group_name IS 'group name';
COMMENT ON COLUMN group_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN group_meta.audit_info IS 'group audit info';
COMMENT ON COLUMN group_meta.current_version IS 'group current version';
COMMENT ON COLUMN group_meta.last_version IS 'group last version';
COMMENT ON COLUMN group_meta.deleted_at IS 'group deleted at';


CREATE TABLE IF NOT EXISTS group_role_rel (
    id BIGSERIAL NOT NULL,
    group_id BIGINT NOT NULL,
    role_id BIGINT NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (group_id, role_id, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_group_id ON group_role_rel (group_id);
COMMENT ON TABLE group_role_rel IS 'relation between group and role';
COMMENT ON COLUMN group_role_rel.id IS 'auto increment id';
COMMENT ON COLUMN group_role_rel.group_id IS 'group id';
COMMENT ON COLUMN group_role_rel.role_id IS 'role id';
COMMENT ON COLUMN group_role_rel.audit_info IS 'relation audit info';
COMMENT ON COLUMN group_role_rel.current_version IS 'relation current version';
COMMENT ON COLUMN group_role_rel.last_version IS 'relation last version';
COMMENT ON COLUMN group_role_rel.deleted_at IS 'relation deleted at';

CREATE TABLE IF NOT EXISTS tag_meta (
    tag_id BIGINT NOT NULL,
    tag_name VARCHAR(128) NOT NULL,
    metalake_id BIGINT NOT NULL,
    tag_comment VARCHAR(256) DEFAULT '',
    properties TEXT DEFAULT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tag_id),
    UNIQUE (metalake_id, tag_name, deleted_at)
    );

COMMENT ON TABLE tag_meta IS 'tag metadata';

COMMENT ON COLUMN tag_meta.tag_id IS 'tag id';
COMMENT ON COLUMN tag_meta.tag_name IS 'tag name';
COMMENT ON COLUMN tag_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN tag_meta.tag_comment IS 'tag comment';
COMMENT ON COLUMN tag_meta.properties IS 'tag properties';
COMMENT ON COLUMN tag_meta.audit_info IS 'tag audit info';


CREATE TABLE IF NOT EXISTS tag_relation_meta (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    tag_id BIGINT NOT NULL,
    metadata_object_id BIGINT NOT NULL,
    metadata_object_type VARCHAR(64) NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (tag_id, metadata_object_id, metadata_object_type, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_tag_id ON tag_relation_meta (tag_id);
CREATE INDEX IF NOT EXISTS idx_metadata_object_id ON tag_relation_meta (metadata_object_id);
COMMENT ON TABLE tag_relation_meta IS 'tag metadata object relation';
COMMENT ON COLUMN tag_relation_meta.id IS 'auto increment id';
COMMENT ON COLUMN tag_relation_meta.tag_id IS 'tag id';
COMMENT ON COLUMN tag_relation_meta.metadata_object_id IS 'metadata object id';
COMMENT ON COLUMN tag_relation_meta.metadata_object_type IS 'metadata object type';
COMMENT ON COLUMN tag_relation_meta.audit_info IS 'tag relation audit info';
COMMENT ON COLUMN tag_relation_meta.current_version IS 'tag relation current version';
COMMENT ON COLUMN tag_relation_meta.last_version IS 'tag relation last version';
COMMENT ON COLUMN tag_relation_meta.deleted_at IS 'tag relation deleted at';

CREATE TABLE IF NOT EXISTS owner_meta (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY,
    metalake_id BIGINT NOT NULL,
    owner_id BIGINT NOT NULL,
    owner_type VARCHAR(64) NOT NULL,
    metadata_object_id BIGINT NOT NULL,
    metadata_object_type VARCHAR(64) NOT NULL,
    audit_info TEXT NOT NULL,
    current_version INT NOT NULL DEFAULT 1,
    last_version INT NOT NULL DEFAULT 1,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (owner_id, metadata_object_id, metadata_object_type, deleted_at)
    );

CREATE INDEX IF NOT EXISTS idx_owner_id ON owner_meta (owner_id);
CREATE INDEX IF NOT EXISTS idx_metadata_object_id ON owner_meta (metadata_object_id);
COMMENT ON TABLE owner_meta IS 'owner relation';
COMMENT ON COLUMN owner_meta.id IS 'auto increment id';
COMMENT ON COLUMN owner_meta.metalake_id IS 'metalake id';
COMMENT ON COLUMN owner_meta.owner_id IS 'owner id';
COMMENT ON COLUMN owner_meta.owner_type IS 'owner type';
COMMENT ON COLUMN owner_meta.metadata_object_id IS 'metadata object id';
COMMENT ON COLUMN owner_meta.metadata_object_type IS 'metadata object type';
COMMENT ON COLUMN owner_meta.audit_info IS 'owner relation audit info';
COMMENT ON COLUMN owner_meta.current_version IS 'owner relation current version';
COMMENT ON COLUMN owner_meta.last_version IS 'owner relation last version';
COMMENT ON COLUMN owner_meta.deleted_at IS 'owner relation deleted at';

