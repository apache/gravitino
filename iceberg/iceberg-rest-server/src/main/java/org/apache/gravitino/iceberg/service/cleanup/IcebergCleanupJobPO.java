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

package org.apache.gravitino.iceberg.service.cleanup;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Persistence object for one {@code iceberg_cleanup_job} row. Mirrors the table columns and is
 * mapped by {@link IcebergCleanupJobMapper}; {@link IcebergCleanupJobStore} converts it to and from
 * the {@link IcebergCleanupJob} domain object. Instances are created through the generated {@code
 * builder()} (or populated by MyBatis via field reflection through the private no-arg constructor)
 * and expose no setters, so they are effectively immutable once built.
 */
@Getter
@Builder(setterPrefix = "with")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergCleanupJobPO {

  private Long id;
  private String metalakeName;
  private String catalogName;
  private String namespace;
  private String tableName;
  private String metadataLocation;
  private String fileIOImpl;
  private String fileIOProps;
  private String state;
  private Integer attempts;
  private String lastError;
  private Long heartbeatAt;
  private String createdBy;
  private Long updatedAt;
}
