--
-- Copyright 2024 Datastrato Pvt Ltd.
-- This software is licensed under the Apache License version 2.
--
ALTER TABLE `role_meta` MODIFY COLUMN `securable_object_full_name` VARCHAR(256) NOT NULL DEFAULT '' COMMENT 'deprecated';
ALTER TABLE `role_meta` MODIFY COLUMN `securable_object_type` VARCHAR(32) NOT NULL DEFAULT '' COMMENT 'deprecated';
ALTER TABLE `role_meta` MODIFY COLUMN `privileges`  VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'deprecated';
ALTER TABLE `role_meta` MODIFY COLUMN `privilege_conditions` VARCHAR(64) NOT NULL DEFAULT '' COMMENT 'deprecated';

CREATE TABLE IF NOT EXISTS `role_meta_securable_object` (
    `id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'securable object id',
    `role_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'role id',
    `entity_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'The entity id of securable object',
    `type`  VARCHAR(128) NOT NULL COMMENT 'securable object type',
    `privilege_names` VARCHAR(256) NOT NULL COMMENT 'securable object privilege names',
    `privilege_conditions` VARCHAR(256) NOT NULL COMMENT 'securable object privilege conditions',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'securable objectcurrent version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'securable object last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'securable object deleted at',
    PRIMARY KEY (`id`),
    KEY `idx_obj_rid` (`role_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'securable object meta';
