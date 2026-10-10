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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Evaluate triggers for TMS ({@code onCommit} and/or {@code crontab}). */
public final class TableMaintenanceSchedule {

  private final boolean onCommit;
  @Nullable private final String crontab;

  /**
   * Creates a schedule.
   *
   * @param onCommit whether IRC commit should enqueue maintenance
   * @param crontab Quartz-style cron expression for expand/cron path, or null
   */
  public TableMaintenanceSchedule(Boolean onCommit, @Nullable String crontab) {
    this.onCommit = onCommit != null && onCommit;
    this.crontab = StringUtils.isBlank(crontab) ? null : crontab.trim();
  }

  /**
   * Parses a schedule from a generic map (for example policy JSON).
   *
   * @param schedule map under the {@code schedule} key
   * @return parsed schedule
   */
  public static TableMaintenanceSchedule fromMap(Map<String, Object> schedule) {
    Preconditions.checkNotNull(schedule, "schedule cannot be null");
    Object onCommit = schedule.get("onCommit");
    Object crontab = schedule.get("crontab");
    Boolean onCommitFlag = onCommit == null ? null : Boolean.valueOf(String.valueOf(onCommit));
    String crontabExpr = crontab == null ? null : String.valueOf(crontab);
    return new TableMaintenanceSchedule(onCommitFlag, crontabExpr);
  }

  /**
   * @return whether commit-triggered maintenance is enabled
   */
  public boolean onCommit() {
    return onCommit;
  }

  /**
   * @return crontab expression when set
   */
  @Nullable
  public String crontab() {
    return crontab;
  }

  /**
   * Validates this schedule for the given maintenance task type.
   *
   * @param taskType maintenance task type
   */
  public void validate(TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    if (onCommit && !taskType.onCommitAllowed()) {
      throw new IllegalArgumentException(
          String.format(
              "schedule.onCommit=true is not allowed for maintenance task type %s",
              taskType.configKey()));
    }
    if (!onCommit && crontab == null) {
      throw new IllegalArgumentException(
          "schedule must set onCommit and/or crontab for automated maintenance");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TableMaintenanceSchedule)) {
      return false;
    }
    TableMaintenanceSchedule that = (TableMaintenanceSchedule) o;
    return onCommit == that.onCommit && Objects.equals(crontab, that.crontab);
  }

  @Override
  public int hashCode() {
    return Objects.hash(onCommit, crontab);
  }

  @Override
  public String toString() {
    return "TableMaintenanceSchedule{onCommit=" + onCommit + ", crontab=" + crontab + '}';
  }
}
