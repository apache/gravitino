--
-- Copyright 2024 Datastrato Pvt Ltd.
-- This software is licensed under the Apache License version 2.
--

CREATE TABLE IF NOT EXISTS `metalake_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `metalake_name` VARCHAR(128) NOT NULL COMMENT 'metalake name',
    `metalake_comment` VARCHAR(256) DEFAULT '' COMMENT 'metalake comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'metalake properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'metalake audit info',
    `schema_version` TEXT NOT NULL COMMENT 'metalake schema version info',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_mn` (`metalake_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'metalake metadata';