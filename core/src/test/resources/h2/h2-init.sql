--
-- Copyright 2024 Datastrato Pvt Ltd.
-- This software is licensed under the Apache License version 2.
--

CREATE TABLE IF NOT EXISTS `metalake_meta` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `metalake_name` VARCHAR(128) NOT NULL COMMENT 'metalake name',
    `metalake_comment` VARCHAR(256) DEFAULT '' COMMENT 'metalake comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'metalake properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'metalake audit info',
    `schema_version` MEDIUMTEXT NOT NULL COMMENT 'metalake schema version info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'metalake current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'metalake last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 NULL COMMENT 'metalake deleted at',
    PRIMARY KEY (metalake_id),
    CONSTRAINT uk_mn_del UNIQUE (metalake_name, deleted_at)
) ENGINE = InnoDB;