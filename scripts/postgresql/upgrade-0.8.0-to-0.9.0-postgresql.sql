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

-- using default 'unknown' to fill in the new column for compatibility
ALTER TABLE fileset_version_info ADD COLUMN storage_location_name VARCHAR(256) NOT NULL DEFAULT 'unknown';
COMMENT ON COLUMN fileset_version_info.storage_location_name IS 'fileset storage location name';
ALTER TABLE fileset_version_info DROP CONSTRAINT fileset_version_info_fileset_id_version_deleted_at_key;
ALTER TABLE fileset_version_info ADD CONSTRAINT uk_fid_ver_sto_del UNIQUE (fileset_id, version, storage_location_name, deleted_at);
-- remove the default value for storage_location_name
ALTER TABLE fileset_version_info ALTER COLUMN storage_location_name DROP DEFAULT;