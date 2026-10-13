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

import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.maintenance.policy.TableMaintenancePolicies;

/**
 * Base {@link PolicyContent} for built-in Iceberg table maintenance policies.
 *
 * <p>Carries optional shared execution peer fields ({@link TimeTrigger}, {@link EventTrigger},
 * {@code minIntervalMs}, {@code jobOptions}, {@code updateOptions}) in the same JSON document as
 * task thresholds.
 */
public abstract class TableMaintenanceContent implements PolicyContent {

  @Nullable private TimeTrigger timeTrigger;

  @Nullable private EventTrigger eventTrigger;

  @Nullable private Long minIntervalMs;

  private Map<String, String> jobOptions = PolicyExecutionFields.copyOptions(null);

  private Map<String, String> updateOptions = PolicyExecutionFields.copyOptions(null);

  /** Default constructor for Jackson deserialization only. */
  protected TableMaintenanceContent() {}

  /**
   * @param timeTrigger optional time trigger
   * @param eventTrigger optional event trigger
   * @param minIntervalMs optional cooldown between finished jobs
   * @param jobOptions job / client options
   * @param updateOptions updater options
   */
  protected TableMaintenanceContent(
      @Nullable TimeTrigger timeTrigger,
      @Nullable EventTrigger eventTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    setPeerFields(timeTrigger, eventTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  @Override
  @Nullable
  public TimeTrigger timeTrigger() {
    return timeTrigger;
  }

  @Override
  @Nullable
  public EventTrigger eventTrigger() {
    return eventTrigger;
  }

  @Override
  @Nullable
  public Long minIntervalMs() {
    return minIntervalMs;
  }

  @Override
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  @Override
  public Map<String, String> updateOptions() {
    return updateOptions;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    PolicyContent.super.validate();
  }

  /**
   * Whether any shared execution peer field is set on this content.
   *
   * @return {@code true} when a trigger, interval, or either options map is present
   */
  public boolean hasPeerFields() {
    return PolicyExecutionFields.hasAny(
        timeTrigger, eventTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  /**
   * Returns a copy of this content with the given peer fields.
   *
   * @param timeTrigger time trigger
   * @param eventTrigger event trigger
   * @param minIntervalMs cooldown ms
   * @param jobOptions job options map
   * @param updateOptions updater options map
   * @return content with peer fields applied
   */
  public abstract TableMaintenanceContent withPeerFields(
      @Nullable TimeTrigger timeTrigger,
      @Nullable EventTrigger eventTrigger,
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
    return Objects.equals(timeTrigger, other.timeTrigger)
        && Objects.equals(eventTrigger, other.eventTrigger)
        && Objects.equals(minIntervalMs, other.minIntervalMs)
        && Objects.equals(jobOptions, other.jobOptions)
        && Objects.equals(updateOptions, other.updateOptions);
  }

  /**
   * @return hash code contribution from peer fields
   */
  protected int peerFieldsHashCode() {
    return Objects.hash(timeTrigger, eventTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  private void setPeerFields(
      @Nullable TimeTrigger timeTrigger,
      @Nullable EventTrigger eventTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    this.timeTrigger = timeTrigger;
    this.eventTrigger = eventTrigger;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = PolicyExecutionFields.copyOptions(jobOptions);
    this.updateOptions = PolicyExecutionFields.copyOptions(updateOptions);
  }
}
