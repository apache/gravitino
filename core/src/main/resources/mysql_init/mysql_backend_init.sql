CREATE DATABASE IF NOT EXISTS `gravitino_meta` DEFAULT CHARACTER SET utf8mb4;
USE `gravitino_meta`;
CREATE TABLE IF NOT EXISTS `metalake_meta`
(
    `id` bigint(20) unsigned NOT NULL COMMENT 'metalake id',
    `metalake_name`  varchar(128) NOT NULL COMMENT 'metalake name',
    `metalake_comment` varchar(256) DEFAULT '' COMMENT 'metalake comment',
    `properties` mediumtext DEFAULT NULL COMMENT 'metalake properties',
    `audit_info` mediumtext NOT NULL COMMENT 'metalake audit info',
    `schema_version` text NOT NULL COMMENT 'metalake schema version info',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_mn` (`metalake_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'metalake metadata';