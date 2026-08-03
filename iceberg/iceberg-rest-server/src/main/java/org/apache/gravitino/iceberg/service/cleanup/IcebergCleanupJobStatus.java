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

import javax.annotation.Nullable;

/** Safe status projection for one Iceberg cleanup job. */
public final class IcebergCleanupJobStatus {

  private final long id;
  private final IcebergCleanupJob.State state;
  private final int attempts;
  @Nullable private final Long manifestsTotal;
  @Nullable private final Long manifestsDone;
  private final long updatedAt;

  /**
   * Creates a cleanup status projection.
   *
   * @param id job id
   * @param state durable job state
   * @param attempts processing attempts
   * @param manifestsTotal advisory manifest total, or {@code null} before progress is reported
   * @param manifestsDone advisory completed manifest count, or {@code null} before progress is
   *     reported
   * @param updatedAt last state or heartbeat update time
   */
  public IcebergCleanupJobStatus(
      long id,
      IcebergCleanupJob.State state,
      int attempts,
      @Nullable Long manifestsTotal,
      @Nullable Long manifestsDone,
      long updatedAt) {
    this.id = id;
    this.state = state;
    this.attempts = attempts;
    this.manifestsTotal = manifestsTotal;
    this.manifestsDone = manifestsDone;
    this.updatedAt = updatedAt;
  }

  /**
   * @return job id
   */
  public long id() {
    return id;
  }

  /**
   * @return durable job state
   */
  public IcebergCleanupJob.State state() {
    return state;
  }

  /**
   * @return processing attempts
   */
  public int attempts() {
    return attempts;
  }

  /**
   * @return advisory manifest total, or {@code null} before progress is reported
   */
  @Nullable
  public Long manifestsTotal() {
    return manifestsTotal;
  }

  /**
   * @return advisory completed manifest count, or {@code null} before progress is reported
   */
  @Nullable
  public Long manifestsDone() {
    return manifestsDone;
  }

  /**
   * @return last state or heartbeat update time
   */
  public long updatedAt() {
    return updatedAt;
  }
}
