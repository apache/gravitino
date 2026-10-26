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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * Immutable execution peer fields for executable policy contents: triggers, cooldown, and option
 * bags.
 *
 * <p>This type is Jackson-independent in-memory composition for {@link ExecutablePolicyContent}.
 * Wire and persistence JSON keep the flat peer keys ({@code timeTrigger}, {@code commitTrigger},
 * {@code minIntervalMs}, {@code jobOptions}, {@code updaterOptions}); {@code common} maps those
 * keys to and from this value object via custom Jackson serdes (same idea as {@code
 * DistributionSerializer}).
 *
 * <p>Shared peer constraints are checked in {@link #of}; type-specific rules (for example rejecting
 * {@code commitTrigger} for orphan cleanup) belong on the concrete content or the server
 * create/alter path.
 *
 * <p>{@code timeTrigger} cron syntax is <strong>not</strong> validated here. The scheduler
 * registration path (db-scheduler / runtime cron parser) is the source of truth; validating with a
 * regex or a second library in {@code api} would drift from that runtime and pull scheduler
 * dependencies into the public API module.
 */
@Evolving
public final class PolicyExecutionSpec {

  /** Spec with every peer unset. */
  public static final PolicyExecutionSpec EMPTY =
      new PolicyExecutionSpec(null, null, null, ImmutableMap.of(), ImmutableMap.of());

  @Nullable private final String timeTrigger;
  @Nullable private final Boolean commitTrigger;
  @Nullable private final Long minIntervalMs;
  private final Map<String, String> jobOptions;
  private final Map<String, String> updaterOptions;

  private PolicyExecutionSpec(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      Map<String, String> jobOptions,
      Map<String, String> updaterOptions) {
    this.timeTrigger = timeTrigger;
    this.commitTrigger = commitTrigger;
    this.minIntervalMs = minIntervalMs;
    this.jobOptions = jobOptions;
    this.updaterOptions = updaterOptions;
  }

  /**
   * Creates an immutable execution spec.
   *
   * @param timeTrigger optional cron expression
   * @param commitTrigger optional commit-trigger flag; any non-null value (including {@code false})
   *     is retained
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions optional job options
   * @param updaterOptions optional updater options
   * @return immutable spec, or {@link #EMPTY} when every peer is unset
   * @throws IllegalArgumentException if {@code minIntervalMs} is negative or an option map has a
   *     blank key or value
   */
  public static PolicyExecutionSpec of(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updaterOptions) {
    String normalizedTimeTrigger = StringUtils.isBlank(timeTrigger) ? null : timeTrigger;
    Map<String, String> normalizedJobOptions = copyOptions(jobOptions);
    Map<String, String> normalizedUpdaterOptions = copyOptions(updaterOptions);
    if (minIntervalMs != null) {
      Preconditions.checkArgument(minIntervalMs >= 0, "minIntervalMs must be >= 0");
    }
    validateOptionMap(normalizedJobOptions, "jobOptions");
    validateOptionMap(normalizedUpdaterOptions, "updaterOptions");
    if (!hasAny(
        normalizedTimeTrigger,
        commitTrigger,
        minIntervalMs,
        normalizedJobOptions,
        normalizedUpdaterOptions)) {
      return EMPTY;
    }
    return new PolicyExecutionSpec(
        normalizedTimeTrigger,
        commitTrigger,
        minIntervalMs,
        normalizedJobOptions,
        normalizedUpdaterOptions);
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
   * @return commit-trigger flag when explicitly supplied (including {@code false}), or {@code null}
   *     when unset
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
   * @return updater options; never {@code null}
   */
  public Map<String, String> updaterOptions() {
    return updaterOptions;
  }

  /**
   * @return {@code true} when any peer is set
   */
  public boolean hasAny() {
    return hasAny(timeTrigger, commitTrigger, minIntervalMs, jobOptions, updaterOptions);
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
        && Objects.equals(updaterOptions, that.updaterOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeTrigger, commitTrigger, minIntervalMs, jobOptions, updaterOptions);
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
        + ", updaterOptionsKeys="
        + updaterOptions.keySet()
        + '}';
  }

  /**
   * Whether any execution peer field is set.
   *
   * <p>Every non-null {@code commitTrigger} (including {@code false}) counts as explicitly
   * supplied, so JSON round-trips and {@code equals}/{@code hashCode} do not depend on other peers.
   */
  private static boolean hasAny(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      Map<String, String> jobOptions,
      Map<String, String> updaterOptions) {
    return StringUtils.isNotBlank(timeTrigger)
        || commitTrigger != null
        || minIntervalMs != null
        || !jobOptions.isEmpty()
        || !updaterOptions.isEmpty();
  }

  private static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return ImmutableMap.of();
    }
    return ImmutableMap.copyOf(options);
  }

  private static void validateOptionMap(Map<String, String> options, String mapName) {
    if (options.isEmpty()) {
      return;
    }
    options.forEach(
        (key, value) -> {
          Preconditions.checkArgument(StringUtils.isNotBlank(key), "%s key is blank", mapName);
          Preconditions.checkArgument(
              StringUtils.isNotBlank(value), "%s '%s' must have non-empty value", mapName, key);
        });
  }
}
