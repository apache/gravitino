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

CREATE TABLE IF NOT EXISTS `metalake_meta` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `metalake_name` VARCHAR(128) NOT NULL COMMENT 'metalake name',
    `metalake_comment` VARCHAR(256) DEFAULT '' COMMENT 'metalake comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'metalake properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'metalake audit info',
    `schema_version` MEDIUMTEXT NOT NULL COMMENT 'metalake schema version info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'metalake current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'metalake last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'metalake deleted at',
    PRIMARY KEY (`metalake_id`),
    UNIQUE KEY `uk_mn_del` (`metalake_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'metalake metadata';

CREATE TABLE IF NOT EXISTS `catalog_meta` (
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `catalog_name` VARCHAR(128) NOT NULL COMMENT 'catalog name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `type` VARCHAR(64) NOT NULL COMMENT 'catalog type',
    `provider` VARCHAR(64) NOT NULL COMMENT 'catalog provider',
    `catalog_comment` VARCHAR(256) DEFAULT '' COMMENT 'catalog comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'catalog properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'catalog audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'catalog current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'catalog last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'catalog deleted at',
    PRIMARY KEY (`catalog_id`),
    UNIQUE KEY `uk_mid_cn_del` (`metalake_id`, `catalog_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'catalog metadata';

CREATE TABLE IF NOT EXISTS `schema_meta` (
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `schema_name` VARCHAR(128) NOT NULL COMMENT 'schema name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_comment` VARCHAR(256) DEFAULT '' COMMENT 'schema comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'schema properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'schema audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'schema current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'schema last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'schema deleted at',
    PRIMARY KEY (`schema_id`),
    UNIQUE KEY `uk_cid_sn_del` (`catalog_id`, `schema_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'schema metadata';

CREATE TABLE IF NOT EXISTS `table_meta` (
    `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id',
    `table_name` VARCHAR(128) NOT NULL COMMENT 'table name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'table audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'table current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'table last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'table deleted at',
    PRIMARY KEY (`table_id`),
    UNIQUE KEY `uk_sid_tn_del` (`schema_id`, `table_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'table metadata';

CREATE TABLE IF NOT EXISTS `table_column_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id',
    `table_version` INT UNSIGNED NOT NULL COMMENT 'table version',
    `column_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'column id',
    `column_name` VARCHAR(128) NOT NULL COMMENT 'column name',
    `column_position` INT UNSIGNED NOT NULL COMMENT 'column position, starting from 0',
    `column_type` TEXT NOT NULL COMMENT 'column type',
    `column_comment` VARCHAR(256) DEFAULT '' COMMENT 'column comment',
    `column_nullable` TINYINT(1) NOT NULL DEFAULT 1 COMMENT 'column nullable, 0 is not nullable, 1 is nullable',
    `column_auto_increment` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'column auto increment, 0 is not auto increment, 1 is auto increment',
    `column_default_value` TEXT DEFAULT NULL COMMENT 'column default value',
    `column_op_type` TINYINT(1) NOT NULL COMMENT 'column operation type, 1 is create, 2 is update, 3 is delete',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'column deleted at',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'column audit info',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_tid_ver_cid_del` (`table_id`, `table_version`, `column_id`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`),
    KEY `idx_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'table column version info';

CREATE TABLE IF NOT EXISTS `fileset_meta` (
    `fileset_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'fileset id',
    `fileset_name` VARCHAR(128) NOT NULL COMMENT 'fileset name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `type` VARCHAR(64) NOT NULL COMMENT 'fileset type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'fileset audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'fileset current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'fileset last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'fileset deleted at',
    PRIMARY KEY (`fileset_id`),
    UNIQUE KEY `uk_sid_fn_del` (`schema_id`, `fileset_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'fileset metadata';

CREATE TABLE IF NOT EXISTS `fileset_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `fileset_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'fileset id',
    `version` INT UNSIGNED NOT NULL COMMENT 'fileset info version',
    `fileset_comment` VARCHAR(256) DEFAULT '' COMMENT 'fileset comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'fileset properties',
    `storage_location_name` VARCHAR(256) NOT NULL DEFAULT 'default' COMMENT 'fileset storage location name',
    `storage_location` MEDIUMTEXT NOT NULL COMMENT 'fileset storage location',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'fileset deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_fid_ver_sto_del` (`fileset_id`, `version`, `storage_location_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`),
    KEY `idx_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'fileset version info';

CREATE TABLE IF NOT EXISTS `topic_meta` (
    `topic_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'topic id',
    `topic_name` VARCHAR(128) NOT NULL COMMENT 'topic name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `comment` VARCHAR(256) DEFAULT '' COMMENT 'topic comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'topic properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'topic audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'topic current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'topic last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'topic deleted at',
    PRIMARY KEY (`topic_id`),
    UNIQUE KEY `uk_sid_tn_del` (`schema_id`, `topic_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'topic metadata';

CREATE TABLE IF NOT EXISTS `user_meta` (
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'user id',
    `user_name` VARCHAR(128) NOT NULL COMMENT 'username',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'user audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'user current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'user last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'user deleted at',
    PRIMARY KEY (`user_id`),
    UNIQUE KEY `uk_mid_us_del` (`metalake_id`, `user_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'user metadata';

CREATE TABLE IF NOT EXISTS `role_meta` (
    `role_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'role id',
    `role_name` VARCHAR(128) NOT NULL COMMENT 'role name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'schema properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'role audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'role current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'role last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'role deleted at',
    PRIMARY KEY (`role_id`),
    UNIQUE KEY `uk_mid_rn_del` (`metalake_id`, `role_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'role metadata';

CREATE TABLE IF NOT EXISTS `role_meta_securable_object` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `role_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'role id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'The entity id of securable object',
    `type`  VARCHAR(128) NOT NULL COMMENT 'securable object type',
    `privilege_names` TEXT(81920) NOT NULL COMMENT 'securable object privilege names',
    `privilege_conditions` TEXT(81920) NOT NULL COMMENT 'securable object privilege conditions',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'securable objectcurrent version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'securable object last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'securable object deleted at',
    PRIMARY KEY (`id`),
    KEY `idx_obj_rid` (`role_id`),
    KEY `idx_obj_eid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'securable object meta';

CREATE TABLE IF NOT EXISTS `user_role_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `user_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'user id',
    `role_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'role id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ui_ri_del` (`user_id`, `role_id`, `deleted_at`),
    KEY `idx_rid` (`role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'user role relation';

CREATE TABLE IF NOT EXISTS `group_meta` (
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'group id',
    `group_name` VARCHAR(128) NOT NULL COMMENT 'group name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'group audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'group current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'group last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'group deleted at',
    PRIMARY KEY (`group_id`),
    UNIQUE KEY `uk_mid_gr_del` (`metalake_id`, `group_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'group metadata';

CREATE TABLE IF NOT EXISTS `group_role_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `group_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'group id',
    `role_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'role id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_gi_ri_del` (`group_id`, `role_id`, `deleted_at`),
    KEY `idx_rid` (`group_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'group role relation';

CREATE TABLE IF NOT EXISTS `tag_meta` (
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `tag_name` VARCHAR(128) NOT NULL COMMENT 'tag name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `tag_comment` VARCHAR(256) DEFAULT '' COMMENT 'tag comment',
    `properties` MEDIUMTEXT DEFAULT NULL COMMENT 'tag properties',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'tag audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag deleted at',
    PRIMARY KEY (`tag_id`),
    UNIQUE KEY `uk_mi_tn_del` (`metalake_id`, `tag_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'tag metadata';

CREATE TABLE IF NOT EXISTS `tag_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `tag_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'tag id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'tag relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'tag relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'tag relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ti_mi_mo_del` (`tag_id`, `metadata_object_id`, `metadata_object_type`, `deleted_at`),
    KEY `idx_tid` (`tag_id`),
    KEY `idx_mid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'tag metadata object relation';

CREATE TABLE IF NOT EXISTS `owner_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `owner_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'owner id',
    `owner_type` VARCHAR(64) NOT NULL COMMENT 'owner type',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'owner relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'owner relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'owner relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'owner relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_ow_me_del` (`owner_id`, `metadata_object_id`, `metadata_object_type`,`deleted_at`),
    KEY `idx_oid` (`owner_id`),
    KEY `idx_meid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'owner relation';

CREATE TABLE IF NOT EXISTS `model_meta` (
    `model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'model id',
    `model_name` VARCHAR(128) NOT NULL COMMENT 'model name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `model_comment` TEXT DEFAULT NULL COMMENT 'model comment',
    `model_properties` MEDIUMTEXT DEFAULT NULL COMMENT 'model properties',
    `model_latest_version` INT UNSIGNED DEFAULT 0 COMMENT 'model latest version',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'model audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'model deleted at',
    PRIMARY KEY (`model_id`),
    UNIQUE KEY `uk_sid_mn_del` (`schema_id`, `model_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'model metadata';

CREATE TABLE IF NOT EXISTS `model_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `schema_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'schema id',
    `model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'model id',
    `version` INT UNSIGNED NOT NULL COMMENT 'model version',
    `model_version_comment` TEXT DEFAULT NULL COMMENT 'model version comment',
    `model_version_properties` MEDIUMTEXT DEFAULT NULL COMMENT 'model version properties',
    `model_version_uri_name` VARCHAR(256) NOT NULL COMMENT 'model version uri name',
    `model_version_uri` TEXT NOT NULL COMMENT 'model storage uri',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'model version audit info',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'model version deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_mid_ver_uri_del` (`model_id`, `version`, `model_version_uri_name`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`),
    KEY `idx_cid` (`catalog_id`),
    KEY `idx_sid` (`schema_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'model version info';

CREATE TABLE IF NOT EXISTS `model_version_alias_rel` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `model_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'model id',
    `model_version` INT UNSIGNED NOT NULL COMMENT 'model version',
    `model_version_alias` VARCHAR(128) NOT NULL COMMENT 'model version alias',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'model version alias deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_mi_mva_del` (`model_id`, `model_version_alias`, `deleted_at`),
    KEY `idx_mva` (`model_version_alias`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'model_version_alias_rel';

CREATE TABLE IF NOT EXISTS `policy_meta` (
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `policy_name` VARCHAR(128) NOT NULL COMMENT 'policy name',
    `policy_type` VARCHAR(64) NOT NULL COMMENT 'policy type',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'policy audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy deleted at',
    PRIMARY KEY (`policy_id`),
    UNIQUE KEY `uk_mi_pn_del` (`metalake_id`, `policy_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy metadata';

CREATE TABLE IF NOT EXISTS `policy_version_info` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `version` INT UNSIGNED NOT NULL COMMENT 'policy info version',
    `policy_comment` TEXT DEFAULT NULL COMMENT 'policy info comment',
    `enabled` TINYINT(1) DEFAULT 1 COMMENT 'whether the policy is enabled, 0 is disabled, 1 is enabled',
    `content` MEDIUMTEXT DEFAULT NULL COMMENT 'policy content',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_pod_ver_del` (`policy_id`, `version`, `deleted_at`),
    KEY `idx_mid` (`metalake_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy version info';

CREATE TABLE IF NOT EXISTS `policy_relation_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'policy relation audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy relation current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'policy relation last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'policy relation deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_pi_mi_mo_del` (`policy_id`, `metadata_object_id`, `metadata_object_type`, `deleted_at`),
    KEY `idx_pid` (`policy_id`),
    KEY `idx_mid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'policy metadata object relation';

CREATE TABLE IF NOT EXISTS `statistic_meta` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'auto increment id',
    `statistic_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'statistic id',
    `statistic_name` VARCHAR(128) NOT NULL COMMENT 'statistic name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `statistic_value` MEDIUMTEXT NOT NULL COMMENT 'statistic value',
    `metadata_object_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metadata object id',
    `metadata_object_type` VARCHAR(64) NOT NULL COMMENT 'metadata object type',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'statistic audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'statistic current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'statistic last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'statistic deleted at',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_si_mi_mo_del` (`statistic_name`, `metadata_object_id`, `deleted_at`),
    KEY `idx_stid` (`statistic_id`),
    KEY `idx_moid` (`metadata_object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'statistic metadata';

CREATE TABLE IF NOT EXISTS `job_template_meta` (
    `job_template_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job template id',
    `job_template_name` VARCHAR(128) NOT NULL COMMENT 'job template name',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `job_template_comment` TEXT DEFAULT NULL COMMENT 'job template comment',
    `job_template_content` MEDIUMTEXT NOT NULL COMMENT 'job template content',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'job template audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job template current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job template last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job template deleted at',
    PRIMARY KEY (`job_template_id`),
    UNIQUE KEY `uk_mid_jtn_del` (`metalake_id`, `job_template_name`, `deleted_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'job template metadata';

CREATE TABLE IF NOT EXISTS `job_run_meta` (
    `job_run_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job run id',
    `job_template_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job template id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `job_execution_id` varchar(256) NOT NULL COMMENT 'job execution id',
    `job_run_status` varchar(64) NOT NULL COMMENT 'job run status',
    `job_finished_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job finished at',
    `audit_info` MEDIUMTEXT NOT NULL COMMENT 'job run audit info',
    `current_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job run current version',
    `last_version` INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'job run last version',
    `deleted_at` BIGINT(20) UNSIGNED NOT NULL DEFAULT 0 COMMENT 'job run deleted at',
    PRIMARY KEY (`job_run_id`),
    UNIQUE KEY `uk_mid_jei_del` (`metalake_id`, `job_execution_id`, `deleted_at`),
    KEY `idx_job_template_id` (`job_template_id`),
    KEY `idx_job_execution_id` (`job_execution_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT 'job run metadata';
