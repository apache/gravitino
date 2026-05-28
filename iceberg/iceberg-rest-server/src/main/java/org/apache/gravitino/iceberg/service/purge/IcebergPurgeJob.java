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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Immutable description of a table to purge. Carries exactly the fields the request path supplies
 * at enqueue and the worker reads back to delete files. Mutable progress (state, attempts,
 * heartbeat) lives only in the {@code iceberg_cleanup_job} row, managed by {@link
 * IcebergPurgeJobStore}; the {@link State} enum here names those row states.
 */
@Getter
@Accessors(fluent = true)
public class IcebergPurgeJob {

  /** Lifecycle states of an {@code iceberg_cleanup_job} row. */
  public enum State {
    /** Awaiting a worker; also the state a transiently-failed job returns to. */
    PENDING,

    /** Taken by a worker that is actively deleting files. */
    RUNNING,

    /** Every reachable file was deleted or already gone. */
    SUCCEEDED,

    /** Retries exhausted or a non-retryable failure; some files may remain undeleted. */
    FAILED;

    /**
     * Whether the job has finished, i.e. reached a final state that a worker no longer acts on.
     *
     * @return {@code true} for {@link #SUCCEEDED} and {@link #FAILED}
     */
    public boolean isFinished() {
      return this == SUCCEEDED || this == FAILED;
    }
  }

  private final long id;
  private final String metalakeName;
  private final String catalogName;
  private final String namespace;
  private final String tableName;
  private final String metadataLocation;
  private final String fileIOImpl;
  private final Map<String, String> fileIOProperties;
  private final String createdBy;

  /**
   * Creates a purge job description.
   *
   * @param id row id, or {@code 0} before persistence
   * @param metalakeName owning metalake
   * @param catalogName Iceberg catalog name
   * @param namespace table namespace (dotted)
   * @param tableName table name
   * @param metadataLocation the dropped table's {@code metadata.json} location
   * @param fileIOImpl FileIO implementation class to reconstruct in the worker
   * @param fileIOProperties properties to reconstruct the FileIO, snapshotted at enqueue
   * @param createdBy principal that requested the drop
   */
  public IcebergPurgeJob(
      long id,
      String metalakeName,
      String catalogName,
      String namespace,
      String tableName,
      String metadataLocation,
      String fileIOImpl,
      Map<String, String> fileIOProperties,
      String createdBy) {
    this.id = id;
    this.metalakeName = metalakeName;
    this.catalogName = catalogName;
    this.namespace = namespace;
    this.tableName = tableName;
    this.metadataLocation = metadataLocation;
    this.fileIOImpl = fileIOImpl;
    this.fileIOProperties = ImmutableMap.copyOf(fileIOProperties);
    this.createdBy = createdBy;
  }
}
