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

package org.apache.gravitino.iceberg.service.cleanup.po;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupJob;
import org.apache.gravitino.iceberg.service.cleanup.mapper.IcebergCleanupJobMapper;
import org.apache.gravitino.json.JsonUtils;

/**
 * Persistence object for one {@code iceberg_cleanup_job} row. Mirrors the table columns and is
 * mapped by {@link IcebergCleanupJobMapper}, and converts to and from the {@link IcebergCleanupJob}
 * domain object via {@link #fromCleanupJob} and {@link #toCleanupJob}. Instances are created
 * through the generated {@code builder()} (or populated by MyBatis via field reflection through the
 * private no-arg constructor) and expose no setters, so they are effectively immutable once built.
 */
@Getter
@Builder(setterPrefix = "with")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergCleanupJobPO {

  private Long id;
  private Long catalogId;
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

  /**
   * Builds a new PENDING row from a domain job.
   *
   * @param job the domain job to persist
   * @param id the generated row id
   * @param now the enqueue timestamp (initial {@code updated_at})
   * @return the persistence object
   */
  public static IcebergCleanupJobPO fromCleanupJob(IcebergCleanupJob job, long id, long now) {
    return builder()
        .withId(id)
        .withCatalogId(job.catalogId())
        .withNamespace(job.namespace())
        .withTableName(job.tableName())
        .withMetadataLocation(job.metadataLocation())
        .withFileIOImpl(job.fileIOImpl())
        .withFileIOProps(propertiesToJson(job.fileIOProperties()))
        .withState(IcebergCleanupJob.State.PENDING.name())
        .withAttempts(0)
        .withLastError(null)
        .withHeartbeatAt(0L)
        .withCreatedBy(job.createdBy())
        .withUpdatedAt(now)
        .build();
  }

  /**
   * Converts this row to the domain job. Only the columns that are immutable after enqueue are
   * carried over; mutable bookkeeping columns (state, attempts, heartbeat, etc.) live only in the
   * row.
   *
   * @return the domain job
   */
  public IcebergCleanupJob toCleanupJob() {
    return new IcebergCleanupJob(
        id,
        catalogId,
        namespace,
        tableName,
        metadataLocation,
        fileIOImpl,
        jsonToProperties(fileIOProps),
        createdBy);
  }

  private static String propertiesToJson(Map<String, String> props) {
    try {
      return JsonUtils.objectMapper().writeValueAsString(props);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to serialize fileIOProperties", e);
    }
  }

  private static Map<String, String> jsonToProperties(String json) {
    try {
      return JsonUtils.objectMapper().readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to deserialize fileIOProperties", e);
    }
  }
}
