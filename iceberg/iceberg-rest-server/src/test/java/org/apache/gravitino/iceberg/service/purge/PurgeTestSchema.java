/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.iceberg.service.purge;

final class PurgeTestSchema {

  static final String H2_CREATE =
      "CREATE TABLE IF NOT EXISTS `iceberg_cleanup_job` ("
          + "`id` BIGINT AUTO_INCREMENT,"
          + "`metalake_name` VARCHAR(128) NOT NULL,"
          + "`catalog_name` VARCHAR(128) NOT NULL,"
          + "`namespace` VARCHAR(512) NOT NULL,"
          + "`table_name` VARCHAR(256) NOT NULL,"
          + "`metadata_location` VARCHAR(1024) NOT NULL,"
          + "`file_io_impl` VARCHAR(256) NOT NULL,"
          + "`file_io_props` CLOB NOT NULL,"
          + "`state` VARCHAR(16) NOT NULL,"
          + "`attempts` INT NOT NULL DEFAULT 0,"
          + "`last_error` VARCHAR(2048) NULL,"
          + "`heartbeat_at` BIGINT NULL,"
          + "`created_by` VARCHAR(128) NOT NULL,"
          + "`updated_at` BIGINT NOT NULL,"
          + "PRIMARY KEY (`id`)"
          + ");"
          + "CREATE INDEX IF NOT EXISTS `idx_state_updated` ON `iceberg_cleanup_job`"
          + " (`state`, `updated_at`);"
          + "CREATE INDEX IF NOT EXISTS `idx_object` ON `iceberg_cleanup_job`"
          + " (`catalog_name`, `namespace`, `table_name`, `state`);";

  private PurgeTestSchema() {}
}
