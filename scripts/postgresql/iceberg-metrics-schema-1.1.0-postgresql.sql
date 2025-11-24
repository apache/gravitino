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

-- Note: Database and schema creation is not included in this script. Please create the database and
-- schema before running this script. for example in psql:
-- CREATE DATABASE example_db;
-- \c example_db
-- CREATE SCHEMA example_schema;
-- set search_path to example_schema;


CREATE TABLE commit_metrics_report (
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    namespace VARCHAR(1024) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    snapshot_id BIGINT NOT NULL,
    sequence_number BIGINT NOT NULL,
    operation VARCHAR(50) NOT NULL,
    added_data_files BIGINT DEFAULT 0,
    removed_data_files BIGINT DEFAULT 0,
    total_data_files BIGINT DEFAULT 0,
    added_delete_files BIGINT DEFAULT 0,
    added_equality_delete_files BIGINT DEFAULT 0,
    added_positional_delete_files BIGINT DEFAULT 0,
    removed_delete_files BIGINT DEFAULT 0,
    removed_equality_delete_files BIGINT DEFAULT 0,
    removed_positional_delete_files BIGINT DEFAULT 0,
    total_delete_files BIGINT DEFAULT 0,
    added_records BIGINT DEFAULT 0,
    removed_records BIGINT DEFAULT 0,
    total_records BIGINT DEFAULT 0,
    added_files_size_in_bytes BIGINT DEFAULT 0,
    removed_files_size_in_bytes BIGINT DEFAULT 0,
    total_files_size_in_bytes BIGINT DEFAULT 0,
    added_positional_deletes BIGINT DEFAULT 0,
    removed_positional_deletes BIGINT DEFAULT 0,
    total_positional_deletes BIGINT DEFAULT 0,
    added_equality_deletes BIGINT DEFAULT 0,
    removed_equality_deletes BIGINT DEFAULT 0,
    total_equality_deletes BIGINT DEFAULT 0,
    manifests_created BIGINT DEFAULT 0,
    manifests_replaced BIGINT DEFAULT 0,
    manifests_kept BIGINT DEFAULT 0,
    manifest_entries_processed BIGINT DEFAULT 0,
    added_dvs BIGINT DEFAULT 0,
    removed_dvs BIGINT DEFAULT 0,
    total_duration_ms BIGINT DEFAULT 0,
    attempts BIGINT DEFAULT 1,
    metadata TEXT
);

CREATE INDEX idx_commit_report ON commit_metrics_report (timestamp, namespace, table_name);

COMMENT ON TABLE commit_metrics_report IS 'Table for storing commit metrics information';
COMMENT ON COLUMN commit_metrics_report.timestamp IS 'Timestamp in milliseconds';
COMMENT ON COLUMN commit_metrics_report.namespace IS 'Namespace of the table';
COMMENT ON COLUMN commit_metrics_report.table_name IS 'Table name';
COMMENT ON COLUMN commit_metrics_report.snapshot_id IS 'Snapshot identifier';
COMMENT ON COLUMN commit_metrics_report.operation IS 'Operation type (APPEND, OVERWRITE, etc)';
COMMENT ON COLUMN commit_metrics_report.metadata IS 'Additional metadata in JSON format';
COMMENT ON COLUMN commit_metrics_report.total_duration_ms IS 'Total operation duration in milliseconds';
COMMENT ON COLUMN commit_metrics_report.attempts IS 'Number of attempts';
COMMENT ON COLUMN commit_metrics_report.added_data_files IS 'Number of added data files';
COMMENT ON COLUMN commit_metrics_report.removed_data_files IS 'Number of removed data files';
COMMENT ON COLUMN commit_metrics_report.total_data_files IS 'Total number of data files';
COMMENT ON COLUMN commit_metrics_report.added_delete_files IS 'Number of added delete files';
COMMENT ON COLUMN commit_metrics_report.added_equality_delete_files IS 'Number of added equality delete files';
COMMENT ON COLUMN commit_metrics_report.added_positional_delete_files IS 'Number of added positional delete files';
COMMENT ON COLUMN commit_metrics_report.removed_delete_files IS 'Number of removed delete files';
COMMENT ON COLUMN commit_metrics_report.removed_equality_delete_files IS 'Number of removed equality delete files';
COMMENT ON COLUMN commit_metrics_report.removed_positional_delete_files IS 'Number of removed positional delete files';
COMMENT ON COLUMN commit_metrics_report.total_delete_files IS 'Total number of delete files';
COMMENT ON COLUMN commit_metrics_report.added_records IS 'Number of added records';
COMMENT ON COLUMN commit_metrics_report.removed_records IS 'Number of removed records';
COMMENT ON COLUMN commit_metrics_report.total_records IS 'Total number of records';
COMMENT ON COLUMN commit_metrics_report.added_files_size_in_bytes IS 'Size of added files in bytes';
COMMENT ON COLUMN commit_metrics_report.removed_files_size_in_bytes IS 'Size of removed files in bytes';
COMMENT ON COLUMN commit_metrics_report.total_files_size_in_bytes IS 'Total file size in bytes';
COMMENT ON COLUMN commit_metrics_report.added_positional_deletes IS 'Number of added positional deletes';
COMMENT ON COLUMN commit_metrics_report.removed_positional_deletes IS 'Number of removed positional deletes';
COMMENT ON COLUMN commit_metrics_report.total_positional_deletes IS 'Total number of positional deletes';
COMMENT ON COLUMN commit_metrics_report.added_equality_deletes IS 'Number of added equality deletes';
COMMENT ON COLUMN commit_metrics_report.removed_equality_deletes IS 'Number of removed equality deletes';
COMMENT ON COLUMN commit_metrics_report.total_equality_deletes IS 'Total number of equality deletes';
COMMENT ON COLUMN commit_metrics_report.manifests_created IS 'Number of manifests created';
COMMENT ON COLUMN commit_metrics_report.manifests_replaced IS 'Number of manifests replaced';
COMMENT ON COLUMN commit_metrics_report.manifests_kept IS 'Number of manifests kept';
COMMENT ON COLUMN commit_metrics_report.manifest_entries_processed IS 'Number of manifest entries processed';
COMMENT ON COLUMN commit_metrics_report.added_dvs IS 'Number of added delete vectors';
COMMENT ON COLUMN commit_metrics_report.removed_dvs IS 'Number of removed delete vectors';
COMMENT ON COLUMN commit_metrics_report.total_duration_ms IS 'Total operation duration in milliseconds';
COMMENT ON COLUMN commit_metrics_report.attempts IS 'Number of attempts';
COMMENT ON COLUMN commit_metrics_report.metadata IS 'Additional metadata in JSON format';


CREATE TABLE scan_metrics_report (
    id BIGSERIAL PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    namespace VARCHAR(1024) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    snapshot_id BIGINT,
    schema_id BIGINT,
    filter TEXT,
    metadata TEXT,
    projected_field_ids TEXT,
    projected_field_names TEXT,
    equality_delete_files BIGINT DEFAULT 0,
    indexed_delete_files BIGINT DEFAULT 0,
    positional_delete_files BIGINT DEFAULT 0,
    result_data_files BIGINT DEFAULT 0,
    result_delete_files BIGINT DEFAULT 0,
    scanned_data_manifests BIGINT DEFAULT 0,
    scanned_delete_manifests BIGINT DEFAULT 0,
    skipped_data_files BIGINT DEFAULT 0,
    skipped_data_manifests BIGINT DEFAULT 0,
    skipped_delete_files BIGINT DEFAULT 0,
    skipped_delete_manifests BIGINT DEFAULT 0,
    total_data_manifests BIGINT DEFAULT 0,
    total_delete_file_size_in_bytes BIGINT DEFAULT 0,
    total_delete_manifests BIGINT DEFAULT 0,
    total_file_size_in_bytes BIGINT DEFAULT 0,
    total_planning_duration BIGINT DEFAULT 0
);

CREATE INDEX idx_scan_report ON scan_metrics_report (timestamp, namespace, table_name);

COMMENT ON TABLE scan_metrics_report IS 'Table for storing scan metrics information';
COMMENT ON COLUMN scan_metrics_report.timestamp IS 'Timestamp in milliseconds';
COMMENT ON COLUMN scan_metrics_report.namespace IS 'Namespace of the table';
COMMENT ON COLUMN scan_metrics_report.table_name IS 'Table name';
COMMENT ON COLUMN scan_metrics_report.snapshot_id IS 'Snapshot identifier';
COMMENT ON COLUMN scan_metrics_report.schema_id IS 'Schema identifier';
COMMENT ON COLUMN scan_metrics_report.filter IS 'Filter condition applied during scan';
COMMENT ON COLUMN scan_metrics_report.metadata IS 'Additional metadata in JSON format';
COMMENT ON COLUMN scan_metrics_report.projected_field_ids IS 'List of projected field IDs';
COMMENT ON COLUMN scan_metrics_report.projected_field_names IS 'List of projected field names';
COMMENT ON COLUMN scan_metrics_report.equality_delete_files IS 'Number of equality delete files';
COMMENT ON COLUMN scan_metrics_report.indexed_delete_files IS 'Number of indexed delete files';
COMMENT ON COLUMN scan_metrics_report.positional_delete_files IS 'Number of positional delete files';
COMMENT ON COLUMN scan_metrics_report.result_data_files IS 'Number of data files processed';
COMMENT ON COLUMN scan_metrics_report.result_delete_files IS 'Number of delete files processed';
COMMENT ON COLUMN scan_metrics_report.scanned_data_manifests IS 'Number of data manifests scanned';
COMMENT ON COLUMN scan_metrics_report.scanned_delete_manifests IS 'Number of delete manifests scanned';
COMMENT ON COLUMN scan_metrics_report.skipped_data_files IS 'Number of data files skipped';
COMMENT ON COLUMN scan_metrics_report.skipped_data_manifests IS 'Number of data manifests skipped';
COMMENT ON COLUMN scan_metrics_report.skipped_delete_files IS 'Number of delete files skipped';
COMMENT ON COLUMN scan_metrics_report.skipped_delete_manifests IS 'Number of delete manifests skipped';
COMMENT ON COLUMN scan_metrics_report.total_data_manifests IS 'Total number of data manifests';
COMMENT ON COLUMN scan_metrics_report.total_delete_file_size_in_bytes IS 'Total size of delete files in bytes';
COMMENT ON COLUMN scan_metrics_report.total_delete_manifests IS 'Total number of delete manifests';
COMMENT ON COLUMN scan_metrics_report.total_file_size_in_bytes IS 'Total file size in bytes';
COMMENT ON COLUMN scan_metrics_report.total_planning_duration IS 'Total planning duration in milliseconds';
