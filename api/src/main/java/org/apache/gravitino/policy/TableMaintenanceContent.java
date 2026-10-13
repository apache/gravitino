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
package org.apache.gravitino.policy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.policy.MaintenanceTrigger;
import org.apache.gravitino.maintenance.policy.TableMaintenancePolicies;

/**
 * Base {@link PolicyContent} for built-in Iceberg table maintenance policies, carrying optional TMS
 * peer fields in the same JSON document as task thresholds.
 */
public abstract class TableMaintenanceContent implements PolicyContent {

  @Nullable private MaintenanceTrigger schedule;

  @Nullable private Long minIntervalMs;

  private Map<String, String> jobOptions = Collections.emptyMap();

  private Map<String, String> updateOptions = Collections.emptyMap();

  /** Default constructor for Jackson deserialization only. */
  protected TableMaintenanceContent() {}

  /**
   * @param schedule optional evaluate trigger
   * @param minIntervalMs optional cooldown between finished jobs
   * @param jobOptions Spark / IRC client options
   * @param updateOptions Gravitino updater options
   */
  protected TableMaintenanceContent(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    setPeerFields(schedule, minIntervalMs, jobOptions, updateOptions);
  }

  /**
   * @return evaluate trigger, or {@code null} when unset
   */
  @Nullable
  public MaintenanceTrigger schedule() {
    return schedule;
  }

  /**
   * @return minimum interval between finished jobs in milliseconds, or {@code null} when unset
   */
  @Nullable
  public Long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return Spark / IRC client options; never {@code null}
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

  @Override
  public void validate() throws IllegalArgumentException {
    PolicyContent.super.validate();
  }

  /**
   * Whether any TMS peer field is set on this content.
   *
   * @return {@code true} when schedule, interval, or either options map is present
   */
  public boolean hasPeerFields() {
    return TableMaintenancePolicies.hasAnyMaintenanceField(
        schedule, minIntervalMs, jobOptions, updateOptions);
  }

  /**
   * Returns a copy of this content with the given peer fields.
   *
   * @param schedule evaluate trigger
   * @param minIntervalMs cooldown ms
   * @param jobOptions job options map
   * @param updateOptions updater options map
   * @return content with peer fields applied
   */
  public abstract TableMaintenanceContent withPeerFields(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions);

  /**
   * Validates peer fields for the given built-in policy type.
   *
   * @param policyType built-in policy type
   */
  protected void validatePolicyContentDefaults(Policy.BuiltInType policyType) {
    TableMaintenancePolicies.validate(policyType, this);
  }

  /**
   * @param other peer-bearing content to compare against
   * @return whether peer fields are equal
   */
  protected boolean peerFieldsEquals(TableMaintenanceContent other) {
    return Objects.equals(schedule, other.schedule)
        && Objects.equals(minIntervalMs, other.minIntervalMs)
        && Objects.equals(jobOptions, other.jobOptions)
        && Objects.equals(updateOptions, other.updateOptions);
  }

  /**
   * @return hash code contribution from peer fields
   */
  protected int peerFieldsHashCode() {
    return Objects.hash(schedule, minIntervalMs, jobOptions, updateOptions);
  }

  private void setPeerFields(
      @Nullable MaintenanceTrigger schedule,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    this.schedule = schedule;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = copyOptions(jobOptions);
    this.updateOptions = copyOptions(updateOptions);
  }

  private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(options));
  }
}
