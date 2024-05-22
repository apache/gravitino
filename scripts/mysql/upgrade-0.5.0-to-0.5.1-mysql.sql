--
-- Copyright 2024 Datastrato Pvt Ltd.
-- This software is licensed under the Apache License version 2.
--

ALTER TABLE `role_meta`
    ADD COLUMN `securable_objects` VARCHAR(2048) NOT NULL COMMENT 'securable objects' AFTER `properties`;

ALTER TABLE `role_meta` DROP COLUMN `securable_object_full_name`;
ALTER TABLE `role_meta` DROP COLUMN `securable_object_type`;
ALTER TABLE `role_meta` DROP COLUMN `privileges`;
ALTER TABLE `role_meta` DROP COLUMN `privilege_conditions`;
