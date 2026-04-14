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

-- Add version columns for Phase 2 version-validated auth cache

ALTER TABLE role_meta
    ADD COLUMN securable_objects_version INT NOT NULL DEFAULT 1;

COMMENT ON COLUMN role_meta.securable_objects_version IS
    'Incremented atomically with any privilege grant/revoke for this role';

ALTER TABLE user_meta
    ADD COLUMN role_grants_version INT NOT NULL DEFAULT 1;

COMMENT ON COLUMN user_meta.role_grants_version IS
    'Incremented atomically with any role assignment/revocation for this user';

ALTER TABLE group_meta
    ADD COLUMN role_grants_version INT NOT NULL DEFAULT 1;

COMMENT ON COLUMN group_meta.role_grants_version IS
    'Incremented atomically with any role assignment/revocation for this group';
