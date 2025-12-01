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

CREATE TABLE commit_metrics_report (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp BIGINT NOT NULL COMMENT 'Timestamp in milliseconds',
    namespace VARCHAR(1024) NOT NULL COMMENT 'Namespace of the table',
    table_name VARCHAR(255) NOT NULL COMMENT 'Table name',
    snapshot_id BIGINT NOT NULL COMMENT 'Snapshot identifier',
    sequence_number BIGINT NOT NULL COMMENT 'Sequence number',
    operation VARCHAR(50) NOT NULL COMMENT 'Operation type (APPEND, OVERWRITE, etc)',
    added_data_files BIGINT DEFAULT 0 COMMENT 'Number of added data files',
    removed_data_files BIGINT DEFAULT 0 COMMENT 'Number of removed data files',
    total_data_files BIGINT DEFAULT 0 COMMENT 'Total number of data files',
    added_delete_files BIGINT DEFAULT 0 COMMENT 'Number of added delete files',
    added_equality_delete_files BIGINT DEFAULT 0 COMMENT 'Number of added equality delete files',
    added_positional_delete_files BIGINT DEFAULT 0 COMMENT 'Number of added positional delete files',
    removed_delete_files BIGINT DEFAULT 0 COMMENT 'Number of removed delete files',
    removed_equality_delete_files BIGINT DEFAULT 0 COMMENT 'Number of removed equality delete files',
    removed_positional_delete_files BIGINT DEFAULT 0 COMMENT 'Number of removed positional delete files',
    total_delete_files BIGINT DEFAULT 0 COMMENT 'Total number of delete files',
    added_records BIGINT DEFAULT 0 COMMENT 'Number of added records',
    removed_records BIGINT DEFAULT 0 COMMENT 'Number of removed records',
    total_records BIGINT DEFAULT 0 COMMENT 'Total number of records',
    added_files_size_in_bytes BIGINT DEFAULT 0 COMMENT 'Size of added files in bytes',
    removed_files_size_in_bytes BIGINT DEFAULT 0 COMMENT 'Size of removed files in bytes',
    total_files_size_in_bytes BIGINT DEFAULT 0 COMMENT 'Total file size in bytes',
    added_positional_deletes BIGINT DEFAULT 0 COMMENT 'Number of added positional deletes',
    removed_positional_deletes BIGINT DEFAULT 0 COMMENT 'Number of removed positional deletes',
    total_positional_deletes BIGINT DEFAULT 0 COMMENT 'Total number of positional deletes',
    added_equality_deletes BIGINT DEFAULT 0 COMMENT 'Number of added equality deletes',
    removed_equality_deletes BIGINT DEFAULT 0 COMMENT 'Number of removed equality deletes',
    total_equality_deletes BIGINT DEFAULT 0 COMMENT 'Total number of equality deletes',
    manifests_created BIGINT DEFAULT 0 COMMENT 'Number of manifests created',
    manifests_replaced BIGINT DEFAULT 0 COMMENT 'Number of manifests replaced',
    manifests_kept BIGINT DEFAULT 0 COMMENT 'Number of manifests kept',
    manifest_entries_processed BIGINT DEFAULT 0 COMMENT 'Number of manifest entries processed',
    added_dvs BIGINT DEFAULT 0 COMMENT 'Number of added delete vectors',
    removed_dvs BIGINT DEFAULT 0 COMMENT 'Number of removed delete vectors',
    total_duration_ms BIGINT DEFAULT 0 COMMENT 'Total operation duration in milliseconds',
    attempts BIGINT DEFAULT 1 COMMENT 'Number of attempts',
    metadata CLOB COMMENT 'Additional metadata in JSON format',
    KEY `idx_commit_report` (`timestamp`, `namespace`, `table_name`)
) ENGINE = InnoDB;

CREATE TABLE scan_metrics_report (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp BIGINT NOT NULL COMMENT 'Timestamp in milliseconds',
    namespace VARCHAR(1024) NOT NULL COMMENT 'Namespace of the table',
    table_name VARCHAR(255) NOT NULL COMMENT 'Table name',
    snapshot_id BIGINT COMMENT 'Snapshot identifier',
    schema_id INT COMMENT 'Schema identifier',
    filter CLOB COMMENT 'Filter condition applied during scan',
    metadata CLOB COMMENT 'Additional metadata in JSON format',
    projected_field_ids CLOB COMMENT 'List of projected field IDs',
    projected_field_names CLOB COMMENT 'List of projected field names',
    equality_delete_files BIGINT DEFAULT 0 COMMENT 'Number of equality delete files',
    indexed_delete_files BIGINT DEFAULT 0 COMMENT 'Number of indexed delete files',
    positional_delete_files BIGINT DEFAULT 0 COMMENT 'Number of positional delete files',
    result_data_files BIGINT DEFAULT 0 COMMENT 'Number of data files processed',
    result_delete_files BIGINT DEFAULT 0 COMMENT 'Number of delete files processed',
    scanned_data_manifests BIGINT DEFAULT 0 COMMENT 'Number of data manifests scanned',
    scanned_delete_manifests BIGINT DEFAULT 0 COMMENT 'Number of delete manifests scanned',
    skipped_data_files BIGINT DEFAULT 0 COMMENT 'Number of data files skipped',
    skipped_data_manifests BIGINT DEFAULT 0 COMMENT 'Number of data manifests skipped',
    skipped_delete_files BIGINT DEFAULT 0 COMMENT 'Number of delete files skipped',
    skipped_delete_manifests BIGINT DEFAULT 0 COMMENT 'Number of delete manifests skipped',
    total_data_manifests BIGINT DEFAULT 0 COMMENT 'Total number of data manifests',
    total_delete_file_size_in_bytes BIGINT DEFAULT 0 COMMENT 'Total size of delete files in bytes',
    total_delete_manifests BIGINT DEFAULT 0 COMMENT 'Total number of delete manifests',
    total_file_size_in_bytes BIGINT DEFAULT 0 COMMENT 'Total file size in bytes',
    total_planning_duration BIGINT DEFAULT 0 COMMENT 'Total planning duration in milliseconds',
    KEY `idx_scan_report` (`timestamp`, `namespace`, `table_name`)
) ENGINE = InnoDB;
