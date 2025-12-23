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
alter table `catalog_meta` add index catalog_meta_idx_name_da (catalog_name, deleted_at);
alter table `schema_meta` add index schema_meta_idx_name_da (catalog_name, deleted_at);
alter table `table_meta` add index table_meta_idx_name_da (table_name, deleted_at);
alter table `fileset_meta` add index fileset_meta_idx_name_da (fileset_name, deleted_at);
alter table `model_meta` add index model_meta_idx_name_da (model_name, deleted_at);
alter table `topic_meta` add index topic_meta_idx_name_da (topic_name, deleted_at);
