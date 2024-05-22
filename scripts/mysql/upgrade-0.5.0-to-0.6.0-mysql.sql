--
-- Copyright 2024 Datastrato Pvt Ltd.
-- This software is licensed under the Apache License version 2.
--

-- <issue-3099: Store role, user, and group under {metalake}.system.{role|user|group} in relation storage>
ALTER TABLE `user_meta`
    ADD COLUMN `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id' AFTER `metalake_id`;
ALTER TABLE `user_meta`
    ADD COLUMN `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id' AFTER `catalog_id`;

ALTER TABLE `group_meta`
    ADD COLUMN `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id' AFTER `metalake_id`;
ALTER TABLE `group_meta`
    ADD COLUMN `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id' AFTER `catalog_id`;

ALTER TABLE `role_meta`
    ADD COLUMN `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id' AFTER `metalake_id`;
ALTER TABLE `role_meta`
    ADD COLUMN `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id' AFTER `catalog_id`;

-- todo: insert system catalog, user, group and role schema
