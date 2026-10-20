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
package org.apache.gravitino.maintenance.scheduler.cron;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * db-scheduler {@code task_data} for {@code cron-job} rows. One table for {@code table:...}
 * instance ids; many tables for batched snapshot-expiry {@code batch:...} instance ids (§5.2.2).
 */
public final class CronJobData implements Serializable {

  private static final long serialVersionUID = 1L;

  private final ArrayList<Long> tableIds;

  /**
   * @param tableIds table entity ids covered by this cron-job
   */
  public CronJobData(List<Long> tableIds) {
    this.tableIds = new ArrayList<>(tableIds == null ? List.of() : tableIds);
  }

  /**
   * @return unmodifiable table ids
   */
  public List<Long> tableIds() {
    return Collections.unmodifiableList(tableIds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CronJobData)) {
      return false;
    }
    CronJobData that = (CronJobData) o;
    return Objects.equals(tableIds, that.tableIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableIds);
  }

  @Override
  public String toString() {
    return "CronJobData{tableIds=" + tableIds + '}';
  }
}
