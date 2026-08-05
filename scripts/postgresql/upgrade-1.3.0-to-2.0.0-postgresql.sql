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
