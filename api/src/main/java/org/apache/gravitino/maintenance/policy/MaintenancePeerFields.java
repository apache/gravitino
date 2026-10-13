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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.policy.PolicyContent;

/**
 * Optional TMS fields stored in the policy content JSON document alongside task content; not
 * members of Iceberg*Content classes.
 *
 * <p>This type is not {@link PolicyContent}. It is a SerDe, DTO, and {@link
 * org.apache.gravitino.meta.PolicyEntity} carrier only.
 */
public final class MaintenancePeerFields {

  /** JSON property for {@link MaintenanceTrigger}. */
  public static final String SCHEDULE_JSON_KEY = "schedule";

  /** JSON property for minimum interval between finished jobs. */
  public static final String MIN_INTERVAL_MS_JSON_KEY = "minIntervalMs";

  /** JSON property for Spark / IRC client options. */
  public static final String JOB_OPTIONS_JSON_KEY = "jobOptions";

  /** JSON property for Gravitino updater options. */
  public static final String UPDATE_OPTIONS_JSON_KEY = "updateOptions";

  private static final MaintenancePeerFields EMPTY =
      new MaintenancePeerFields(null, null, Collections.emptyMap(), Collections.emptyMap());

  @Nullable private final MaintenanceTrigger schedule;
  @Nullable private final Long minIntervalMs;
  private final Map<String, String> jobOptions;
  private final Map<String, String> updateOptions;

  private MaintenancePeerFields(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      Map<String, String> jobOptions,
      Map<String, String> updateOptions) {
    this.schedule = schedule;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = jobOptions;
    this.updateOptions = updateOptions;
  }

  /**
   * Creates peer fields from optional values.
   *
   * @param schedule evaluate trigger, or null
   * @param minIntervalMs cooldown ms, or null
   * @param jobOptions Spark / IRC options, or null
   * @param updateOptions updater options, or null
   * @return peer fields instance
   */
  public static MaintenancePeerFields of(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    Map<String, String> normalizedJobOptions = copyOptions(jobOptions);
    Map<String, String> normalizedUpdateOptions = copyOptions(updateOptions);
    if (schedule == null
        && minIntervalMs == null
        && normalizedJobOptions.isEmpty()
        && normalizedUpdateOptions.isEmpty()) {
      return EMPTY;
    }
    return new MaintenancePeerFields(
        schedule, minIntervalMs, normalizedJobOptions, normalizedUpdateOptions);
  }

  /**
   * @return shared empty peer fields
   */
  public static MaintenancePeerFields empty() {
    return EMPTY;
  }

  /**
   * @return evaluate trigger, or {@code null} when unset
   */
  @Nullable
  public MaintenanceTrigger schedule() {
    return schedule;
  }

  /**
   * @return cooldown after last finished job, or {@code null}
   */
  @Nullable
  public Long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return Spark / IRC client properties; never {@code null}
   */
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  /**
   * @return Gravitino updater options; never {@code null}
   */
  public Map<String, String> updateOptions() {
    return updateOptions;
  }

  /**
   * Whether any TMS peer field is set.
   *
   * @return {@code true} when schedule, interval, or either options map is present
   */
  public boolean hasAnyField() {
    return schedule != null
        || minIntervalMs != null
        || !jobOptions.isEmpty()
        || !updateOptions.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MaintenancePeerFields)) {
      return false;
    }
    MaintenancePeerFields that = (MaintenancePeerFields) o;
    return Objects.equals(schedule, that.schedule)
        && Objects.equals(minIntervalMs, that.minIntervalMs)
        && Objects.equals(jobOptions, that.jobOptions)
        && Objects.equals(updateOptions, that.updateOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schedule, minIntervalMs, jobOptions, updateOptions);
  }

  private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(options));
  }
}
