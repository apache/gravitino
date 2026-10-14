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
import org.apache.commons.lang3.StringUtils;

/**
 * Immutable execution peer fields for executable policy contents: triggers, cooldown, and option
 * bags.
 *
 * <p>JSON property names remain the flat peer keys on policy content ({@code timeTrigger}, {@code
 * commitTrigger}, {@code minIntervalMs}, {@code jobOptions}, {@code updateOptions}). This type is
 * the in-memory composition for {@link ExecutablePolicyContent}.
 */
public final class PolicyExecutionSpec {

  /** Spec with every peer unset. */
  public static final PolicyExecutionSpec EMPTY =
      new PolicyExecutionSpec(null, null, null, null, null);

  @Nullable private final String timeTrigger;
  @Nullable private final Boolean commitTrigger;
  @Nullable private final Long minIntervalMs;
  private final Map<String, String> jobOptions;
  private final Map<String, String> updateOptions;

  private PolicyExecutionSpec(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    this.timeTrigger = StringUtils.isBlank(timeTrigger) ? null : timeTrigger;
    this.commitTrigger = commitTrigger;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = PolicyExecutionFields.copyOptions(jobOptions);
    this.updateOptions = PolicyExecutionFields.copyOptions(updateOptions);
  }

  /**
   * Creates an immutable execution spec.
   *
   * @param timeTrigger optional cron expression
   * @param commitTrigger optional commit-trigger flag
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions optional job options
   * @param updateOptions optional update options
   * @return immutable spec
   */
  public static PolicyExecutionSpec of(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    if (!PolicyExecutionFields.hasAny(
        timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions)) {
      return EMPTY;
    }
    return new PolicyExecutionSpec(
        timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  /**
   * @return empty spec
   */
  public static PolicyExecutionSpec empty() {
    return EMPTY;
  }

  /**
   * @return cron expression, or {@code null} when unset
   */
  @Nullable
  public String timeTrigger() {
    return timeTrigger;
  }

  /**
   * @return {@code true} when commit-triggered evaluation is enabled, otherwise {@code null} or
   *     {@code false}
   */
  @Nullable
  public Boolean commitTrigger() {
    return commitTrigger;
  }

  /**
   * @return minimum interval in milliseconds, or {@code null} when unset
   */
  @Nullable
  public Long minIntervalMs() {
    return minIntervalMs;
  }

  /**
   * @return job options; never {@code null}
   */
  public Map<String, String> jobOptions() {
    return jobOptions;
  }

  /**
   * @return update options; never {@code null}
   */
  public Map<String, String> updateOptions() {
    return updateOptions;
  }

  /**
   * @return {@code true} when any peer is set
   */
  public boolean hasAny() {
    return PolicyExecutionFields.hasAny(
        timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  /** Validates shared peer constraints (cooldown and option maps). */
  public void validate() {
    PolicyExecutionFields.validate(
        timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PolicyExecutionSpec)) {
      return false;
    }
    PolicyExecutionSpec that = (PolicyExecutionSpec) other;
    return Objects.equals(timeTrigger, that.timeTrigger)
        && Objects.equals(commitTrigger, that.commitTrigger)
        && Objects.equals(minIntervalMs, that.minIntervalMs)
        && Objects.equals(jobOptions, that.jobOptions)
        && Objects.equals(updateOptions, that.updateOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  @Override
  public String toString() {
    // Never render option map values; credentials may appear before sensitive-key rejection.
    return "PolicyExecutionSpec{"
        + "timeTrigger='"
        + timeTrigger
        + '\''
        + ", commitTrigger="
        + commitTrigger
        + ", minIntervalMs="
        + minIntervalMs
        + ", jobOptionsKeys="
        + jobOptions.keySet()
        + ", updateOptionsKeys="
        + updateOptions.keySet()
        + '}';
  }
}
