/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */


CREATE TABLE IF NOT EXISTS table_version_info (
                                             table_id        BIGINT PRIMARY KEY,
                                             format          VARCHAR(64) NOT NULL,
    location        VARCHAR(512) NOT NULL,
    "external"      BOOLEAN NOT NULL DEFAULT FALSE,
    properties      TEXT,
    partition_info  TEXT,
    distribution_info TEXT,
    sort_order_info TEXT,
    index_info      TEXT,
    table_comment   TEXT,
    current_version BIGINT,
    deleted_at      BIGINT DEFAULT 0
    );
COMMENT ON TABLE table_version_info IS 'table detail information including format, location, properties, partition, distribution, sort order, index and so on';
COMMENT ON COLUMN table_version_info.table_id        IS 'table id';
COMMENT ON COLUMN table_version_info.format          IS 'table format, such as Lance, Iceberg and so on';
COMMENT ON COLUMN table_version_info.location        IS 'table storage location';
COMMENT ON COLUMN table_version_info."external"      IS 'whether the table is external table';
COMMENT ON COLUMN table_version_info.properties      IS 'table properties';
COMMENT ON COLUMN table_version_info.partition_info  IS 'table partition info';
COMMENT on COLUMN table_version_info.distribution_info IS 'table distribution info';
COMMENT ON COLUMN table_version_info.sort_order_info IS 'table sort order info';
COMMENT ON COLUMN table_version_info.index_info      IS 'table index info';
COMMENT ON COLUMN table_version_info.table_comment   IS 'table comment';
COMMENT ON COLUMN table_version_info.current_version IS 'table current version';
COMMENT ON COLUMN table_version_info.deleted_at      IS 'table deletion timestamp, 0 means not deleted';
