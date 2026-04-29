--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
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

-- Role privilege tracking (strong consistency -- Step 3 version check)
ALTER TABLE `role_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any privilege grant/revoke for this role.
             JcasbinAuthorizer compares db.updated_at vs cached updated_at per request
             to decide whether to reload JCasbin policies for this role.';

-- User role assignment tracking (strong consistency -- Step 1a version check)
ALTER TABLE `user_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any role assign/revoke for this user.
             JcasbinAuthorizer compares db.updated_at vs cached updated_at per request
             to decide whether to reload the user-role mapping.';

-- Group role assignment tracking (strong consistency -- Step 1b version check)
ALTER TABLE `group_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any role assign/revoke for this group.
             JcasbinAuthorizer compares db.updated_at vs cached updated_at per request
             to decide whether to reload the group-role mapping.';

-- Ownership mutation tracking (eventual consistency -- owner change poller)
ALTER TABLE `owner_meta`
    ADD COLUMN `updated_at` BIGINT NOT NULL DEFAULT 0
    COMMENT 'Set to currentTimeMillis() on any ownership transfer.
             The owner change poller reads updated_at > maxSeen to find changed rows
             and invalidates only the specific metadataObjectIds in ownerRelCache.';

-- Covering indexes for high-frequency read predicates
CREATE INDEX idx_user_meta_name_del_upd
    ON user_meta (metalake_id, user_name, deleted_at, updated_at);
CREATE INDEX idx_group_meta_del_upd
    ON group_meta (group_id, deleted_at, updated_at);
CREATE INDEX idx_role_meta_del_upd
    ON role_meta (role_id, deleted_at, updated_at);
CREATE INDEX idx_owner_meta_obj_del_upd
    ON owner_meta (metadata_object_id, deleted_at, updated_at);
CREATE INDEX idx_owner_meta_del_upd_obj
    ON owner_meta (deleted_at, updated_at, metadata_object_id);

-- Entity name->id mutation tracking (eventual consistency -- entity change poller)
CREATE TABLE IF NOT EXISTS `entity_change_log` (
  `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `metalake_name` VARCHAR(128)    NOT NULL,
  `entity_type`   VARCHAR(32)     NOT NULL COMMENT 'METALAKE | CATALOG | SCHEMA | TABLE | FILESET | TOPIC | MODEL | VIEW',
  `full_name`     VARCHAR(512)    NOT NULL COMMENT 'Dot-separated full name of the affected entity. For RENAME, stores the OLD name (the stale key to invalidate). For DROP/ALTER, the entity name.',
  `operate_type`  TINYINT UNSIGNED NOT NULL COMMENT 'Operate type code: 1=RENAME, 2=DROP, 3=INSERT. Codes are stable and never re-used.',
  `created_at`    BIGINT          NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `idx_ecl_created_at` (`created_at`)
) COMMENT 'Append-only log of entity structural changes. One row per affected entity per operation. The entity change poller reads this table to drive targeted invalidation of metadataIdCache on HA peer nodes. Rows older than the retention window (default 1 h) are pruned periodically.';
