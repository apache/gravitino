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
package org.apache.gravitino.maintenance.storage.service;

import java.util.Objects;

/**
 * Immutable occupancy claim returned by acquire.
 *
 * <p>Callers must pass {@link #createdAt()} to {@link TableMaintenanceActionMetaService#bindJobId}
 * without re-reading {@code created_at} from the row.
 *
 * <p>{@link #id()} is the durable claim token for OccupancyGate follow-ups: make {@code runJob}
 * idempotent on this id (or outbox / reconcile-before-reclaim). Binding {@code job_id} alone cannot
 * undo a submit that already succeeded after a crash between {@code runJob} and bind.
 */
public final class OccupancyClaim {

  private final long id;
  private final long createdAt;

  /**
   * @param id {@code table_maintenance_action.id}
   * @param createdAt claim lease start written at acquire / reclaim time
   */
  public OccupancyClaim(long id, long createdAt) {
    this.id = id;
    this.createdAt = createdAt;
  }

  /**
   * @return action row id
   */
  public long id() {
    return id;
  }

  /**
   * @return {@code created_at} observed when this claim was acquired
   */
  public long createdAt() {
    return createdAt;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OccupancyClaim)) {
      return false;
    }
    OccupancyClaim that = (OccupancyClaim) o;
    return id == that.id && createdAt == that.createdAt;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, createdAt);
  }

  @Override
  public String toString() {
    return "OccupancyClaim{id=" + id + ", createdAt=" + createdAt + '}';
  }
}
