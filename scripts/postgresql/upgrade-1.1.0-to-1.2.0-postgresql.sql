--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
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

CREATE INDEX IF NOT EXISTS catalog_meta_idx_name_da ON catalog_meta (catalog_name, deleted_at);
CREATE INDEX IF NOT EXISTS schema_meta_idx_name_da ON schema_meta (schema_name, deleted_at);
CREATE INDEX IF NOT EXISTS table_meta_idx_name_da ON table_meta (table_name, deleted_at);
CREATE INDEX IF NOT EXISTS fileset_meta_idx_name_da ON fileset_meta (fileset_name, deleted_at);
CREATE INDEX IF NOT EXISTS model_meta_idx_name_da ON model_meta (model_name, deleted_at);
CREATE INDEX IF NOT EXISTS topic_meta_idx_name_da ON topic_meta (topic_name, deleted_at);
