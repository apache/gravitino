-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
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

-- Test Iceberg snapshot maintenance procedures via Gravitino connector

CREATE SCHEMA IF NOT EXISTS gt_snapshot_test;

CREATE TABLE gt_snapshot_test.maintenance_table (
    id int,
    name varchar
);

-- Insert data to create snapshots
INSERT INTO gt_snapshot_test.maintenance_table VALUES (1, 'alice');

INSERT INTO gt_snapshot_test.maintenance_table VALUES (2, 'bob');

INSERT INTO gt_snapshot_test.maintenance_table VALUES (3, 'charlie');

-- Verify we have multiple snapshots
SELECT count(*) >= 3 FROM "gt_snapshot_test"."maintenance_table$snapshots";

-- Test expire_snapshots procedure (delegation to inner Iceberg connector).
-- Note: remove_orphan_files, optimize, and rewrite_manifests procedures
-- return non-deterministic statistics rows (file counts vary by run), so
-- they are covered by Java unit tests instead of this SQL integration test.
ALTER TABLE gt_snapshot_test.maintenance_table EXECUTE expire_snapshots;

-- Verify table data is still intact after expire_snapshots
SELECT count(*) FROM gt_snapshot_test.maintenance_table;

-- Cleanup
DROP TABLE gt_snapshot_test.maintenance_table;

DROP SCHEMA gt_snapshot_test;
