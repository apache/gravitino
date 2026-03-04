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

CREATE TABLE IF NOT EXISTS table_version_info (
    table_id        BIGINT NOT NULL,
    format          VARCHAR(64),
    properties      TEXT,
    partitioning  TEXT,
    distribution TEXT,
    sort_orders TEXT,
    indexes      TEXT,
    "comment"   TEXT,
    version BIGINT,
    deleted_at      BIGINT DEFAULT 0,
    UNIQUE (table_id, version, deleted_at)
);
COMMENT ON TABLE table_version_info                  IS 'table detail information including format, location, properties, partition, distribution, sort order, index and so on';
COMMENT ON COLUMN table_version_info.table_id        IS 'table id';
COMMENT ON COLUMN table_version_info.format          IS 'table format, such as Lance, Iceberg and so on, it will be null if it is not a lakehouse table';
COMMENT ON COLUMN table_version_info.properties      IS 'table properties';
COMMENT ON COLUMN table_version_info.partitioning    IS 'table partition info';
COMMENT on COLUMN table_version_info.distribution    IS 'table distribution info';
COMMENT ON COLUMN table_version_info.sort_orders     IS 'table sort order info';
COMMENT ON COLUMN table_version_info.indexes         IS 'table index info';
COMMENT ON COLUMN table_version_info."comment"       IS 'table comment';
COMMENT ON COLUMN table_version_info.version         IS 'table current version';
COMMENT ON COLUMN table_version_info.deleted_at      IS 'table deletion timestamp, 0 means not deleted';
