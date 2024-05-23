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

-- todo: initialize the system catalog and schema, and update user, group and role meta table with the catalog and schema id
-- SELECT metalake_id INTO @metalake_id FROM metalake_meta;
-- WHILE @metalake_id IS NOT NULL DO
-- insert into catalog_meta (catalog_id, metalake_id, catalog_name...) values (@random_id, @metalake_id, 'system', ...)
-- insert into schema_meta (schema_id, metalake_id, catalog_id, schema_name...) values (@random_id, @metalake_id, @catalog_id, 'user' ...)
-- insert into schema_meta (schema_id, metalake_id, catalog_id, schema_name...) values (@random_id, @metalake_id, @catalog_id, 'group' ...)
-- insert into schema_meta (schema_id, metalake_id, catalog_id, schema_name...) values (@random_id, @metalake_id, @catalog_id, 'role' ...)
-- update user_meta set catalog_id = @catalog_id, schema_id = @schema_id_user where metalake_id = @metalake_id;
-- update group_meta set catalog_id = @catalog_id, schema_id = @schema_id_group where metalake_id = @metalake_id;
-- update role_meta set catalog_id = @catalog_id, schema_id = @schema_id_role where metalake_id = @metalake_id;
