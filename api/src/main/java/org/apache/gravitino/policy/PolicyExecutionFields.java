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
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Optional execution peer fields shared by policy contents: triggers, cooldown, and option bags.
 *
 * <p>JSON property names are stable API keys. Trigger peers are scalars: {@code timeTrigger} is a
 * cron string and {@code commitTrigger} is a boolean.
 */
public final class PolicyExecutionFields {

  /** JSON property for the Quartz cron string. */
  public static final String TIME_TRIGGER_JSON_KEY = "timeTrigger";

  /** JSON property for the commit-trigger boolean. */
  public static final String COMMIT_TRIGGER_JSON_KEY = "commitTrigger";

  /** JSON property for minimum interval between finished runs. */
  public static final String MIN_INTERVAL_MS_JSON_KEY = "minIntervalMs";

  /** JSON property for job / client options. */
  public static final String JOB_OPTIONS_JSON_KEY = "jobOptions";

  /** JSON property for updater / control-plane options. */
  public static final String UPDATE_OPTIONS_JSON_KEY = "updateOptions";

  /** Allowed charset / named tokens per Quartz cron field (shape only, not full Quartz parse). */
  private static final Pattern CRON_FIELD =
      Pattern.compile(
          "(?i)^(?:\\*|/|-|,|\\?|#|[0-9]|L|W|"
              + "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|"
              + "SUN|MON|TUE|WED|THU|FRI|SAT)+$");

  private PolicyExecutionFields() {}

  /**
   * Whether any execution peer field is set.
   *
   * @param timeTrigger optional cron expression
   * @param commitTrigger optional commit flag ({@code true} counts as set)
   * @param minIntervalMs cooldown ms
   * @param jobOptions job options map
   * @param updateOptions update options map
   * @return {@code true} when any field is present
   */
  public static boolean hasAny(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    return StringUtils.isNotBlank(timeTrigger)
        || Boolean.TRUE.equals(commitTrigger)
        || minIntervalMs != null
        || (jobOptions != null && !jobOptions.isEmpty())
        || (updateOptions != null && !updateOptions.isEmpty());
  }

  /**
   * Validates common execution peer fields (cron shape, cooldown, option maps).
   *
   * @param timeTrigger optional cron expression
   * @param commitTrigger optional commit flag
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions job options (may be empty)
   * @param updateOptions update options (may be empty)
   */
  public static void validate(
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger,
      @Nullable Long minIntervalMs,
      @Nullable Map<String, String> jobOptions,
      @Nullable Map<String, String> updateOptions) {
    if (!hasAny(timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions)) {
      return;
    }
    if (StringUtils.isNotBlank(timeTrigger)) {
      validateCronExpression(timeTrigger.trim());
    }
    if (minIntervalMs != null) {
      Preconditions.checkArgument(minIntervalMs >= 0, "minIntervalMs must be >= 0");
    }
    validateOptionMap(jobOptions, "jobOptions");
    validateOptionMap(updateOptions, "updateOptions");
  }

  /**
   * Validates Quartz cron field count and allowed tokens (shape check, not full Quartz parse).
   *
   * @param expression trimmed cron expression
   */
  public static void validateCronExpression(String expression) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(expression), "timeTrigger must not be blank");
    String[] fields = expression.trim().split("\\s+");
    Preconditions.checkArgument(
        fields.length == 6 || fields.length == 7,
        "timeTrigger must be a Quartz cron with 6 fields "
            + "(seconds minutes hours day-of-month month day-of-week) or 7 fields "
            + "(optional year), but got %s field(s): '%s'",
        fields.length,
        expression);
    for (int i = 0; i < fields.length; i++) {
      Preconditions.checkArgument(
          CRON_FIELD.matcher(fields[i]).matches(),
          "timeTrigger field %s ('%s') is not a valid Quartz cron token "
              + "(digits, *, ,, -, /, ?, L, W, #, or month/day-of-week names)",
          i + 1,
          fields[i]);
    }
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
