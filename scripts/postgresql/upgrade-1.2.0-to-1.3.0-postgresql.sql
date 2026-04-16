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

ALTER TABLE role_meta  ADD COLUMN updated_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE user_meta  ADD COLUMN updated_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE group_meta ADD COLUMN updated_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE owner_meta ADD COLUMN updated_at BIGINT NOT NULL DEFAULT 0;

CREATE INDEX idx_user_meta_name_del_upd ON user_meta (metalake_id, user_name, deleted_at, updated_at);
CREATE INDEX idx_group_meta_del_upd ON group_meta (group_id, deleted_at, updated_at);
CREATE INDEX idx_role_meta_del_upd ON role_meta (role_id, deleted_at, updated_at);
CREATE INDEX idx_owner_meta_obj_del_upd ON owner_meta (metadata_object_id, deleted_at, updated_at);

UPDATE role_meta  SET updated_at = 1 WHERE updated_at = 0 AND deleted_at = 0;
UPDATE user_meta  SET updated_at = 1 WHERE updated_at = 0 AND deleted_at = 0;
UPDATE group_meta SET updated_at = 1 WHERE updated_at = 0 AND deleted_at = 0;
UPDATE owner_meta SET updated_at = 1 WHERE updated_at = 0 AND deleted_at = 0;

CREATE TABLE IF NOT EXISTS group_user_rel (
    id BIGSERIAL PRIMARY KEY,
    group_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    deleted_at BIGINT NOT NULL DEFAULT 0,
    UNIQUE (group_id, user_id, deleted_at)
);

CREATE TABLE IF NOT EXISTS entity_change_log (
    id BIGSERIAL PRIMARY KEY,
    metalake_name VARCHAR(128) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    full_name VARCHAR(512) NOT NULL,
    operate_type VARCHAR(16) NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ecl_created_at ON entity_change_log(created_at);
