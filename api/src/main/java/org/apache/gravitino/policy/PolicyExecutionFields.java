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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Optional execution peer fields shared by policy contents: triggers, cooldown, and option bags.
 *
 * <p>JSON property names are stable API keys for policy content documents.
 */
public final class PolicyExecutionFields {

  /** JSON property for {@link TimeTrigger}. */
  public static final String TIME_TRIGGER_JSON_KEY = "timeTrigger";

  /** JSON property for {@link CommitTrigger}. */
  public static final String COMMIT_TRIGGER_JSON_KEY = "commitTrigger";

  /** JSON property for minimum interval between finished runs. */
  public static final String MIN_INTERVAL_MS_JSON_KEY = "minIntervalMs";

  /** JSON property for job / client options. */
  public static final String JOB_OPTIONS_JSON_KEY = "jobOptions";

  /** JSON property for updater / control-plane options. */
  public static final String UPDATE_OPTIONS_JSON_KEY = "updateOptions";

  private PolicyExecutionFields() {}

  /**
   * Whether any execution peer field is set.
   *
   * @param timeTrigger optional time trigger
   * @param commitTrigger optional commit trigger
   * @param minIntervalMs cooldown ms
   * @param jobOptions job options map
   * @param updateOptions update options map
   * @return {@code true} when any field is present
   */
  public static boolean hasAny(
      @Nullable TimeTrigger timeTrigger,
      @Nullable CommitTrigger commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    return timeTrigger != null
        || commitTrigger != null
        || minIntervalMs != null
        || (jobOptions != null && !jobOptions.isEmpty())
        || (updateOptions != null && !updateOptions.isEmpty());
  }

  /**
   * Validates common execution peer fields (trigger shape, cooldown, option maps).
   *
   * @param timeTrigger optional time trigger
   * @param commitTrigger optional commit trigger
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions job options (may be empty)
   * @param updateOptions update options (may be empty)
   */
  public static void validate(
      @Nullable TimeTrigger timeTrigger,
      @Nullable CommitTrigger commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    if (!hasAny(timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions)) {
      return;
    }
    if (timeTrigger != null) {
      timeTrigger.validate();
    }
    if (commitTrigger != null) {
      commitTrigger.validate();
    }
    if (minIntervalMs != null) {
      Preconditions.checkArgument(minIntervalMs >= 0, "minIntervalMs must be >= 0");
    }
    validateOptionMap(jobOptions, "jobOptions");
    validateOptionMap(updateOptions, "updateOptions");
  }

  /**
   * Merges option maps from a nearer policy over a farther policy (farther first, nearer wins).
   *
   * @param farther options from a more distant attachment
   * @param nearer options from a nearer attachment
   * @return merged map
   */
  public static Map<String, String> mergeOptions(
      Map<String, String> farther, Map<String, String> nearer) {
    if ((farther == null || farther.isEmpty()) && (nearer == null || nearer.isEmpty())) {
      return Collections.emptyMap();
    }
    Map<String, String> merged = new LinkedHashMap<>();
    if (farther != null) {
      merged.putAll(farther);
    }
    if (nearer != null) {
      merged.putAll(nearer);
    }
    return ImmutableMap.copyOf(merged);
  }

  /**
   * Defensive copy for option maps stored on policy content.
   *
   * @param options input map, or null
   * @return immutable copy, never null
   */
  public static Map<String, String> copyOptions(@Nullable Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(new LinkedHashMap<>(options));
  }

  private static void validateOptionMap(Map<String, String> options, String mapName) {
    if (options == null || options.isEmpty()) {
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
