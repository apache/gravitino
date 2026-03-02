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

ALTER TABLE metalake_meta ALTER COLUMN metalake_comment TYPE TEXT;
ALTER TABLE catalog_meta ALTER COLUMN catalog_comment TYPE TEXT;
ALTER TABLE schema_meta ALTER COLUMN schema_comment TYPE TEXT;
ALTER TABLE fileset_version_info ALTER COLUMN fileset_comment TYPE TEXT;
ALTER TABLE topic_meta ALTER COLUMN comment TYPE TEXT;
ALTER TABLE tag_meta ALTER COLUMN tag_comment TYPE TEXT;
ALTER TABLE table_column_version_info ALTER COLUMN column_comment TYPE TEXT;
