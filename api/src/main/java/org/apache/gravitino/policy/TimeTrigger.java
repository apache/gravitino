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
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Time-based policy trigger expressed as a Quartz cron.
 *
 * <p>{@code cronExpression} has either 6 fields ({@code seconds minutes hours day-of-month month
 * day-of-week}) or 7 fields (optional trailing {@code year}). Time zone is the server default; this
 * type does not carry a zone id.
 *
 * <p>{@link #validateCronExpression(String)} is a <strong>shape check</strong> only: field count
 * plus allowed Quartz tokens. It does not enforce Quartz value ranges or cross-field rules.
 */
public final class TimeTrigger implements Trigger {

  /** Allowed charset / named tokens per Quartz cron field (shape only, not full Quartz parse). */
  private static final Pattern CRON_FIELD =
      Pattern.compile(
          "(?i)^(?:\\*|/|-|,|\\?|#|[0-9]|L|W|"
              + "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|"
              + "SUN|MON|TUE|WED|THU|FRI|SAT)+$");

  private final String cronExpression;

  /** Default constructor for Jackson field-based deserialization only. */
  private TimeTrigger() {
    this(null);
  }

  /**
   * Creates a time trigger.
   *
   * @param cronExpression Quartz cron expression
   */
  public TimeTrigger(@Nullable String cronExpression) {
    this.cronExpression = StringUtils.isBlank(cronExpression) ? null : cronExpression.trim();
  }

  /**
   * @return Quartz cron expression when set
   */
  @Nullable
  public String cronExpression() {
    return cronExpression;
  }

  /**
   * Validates that this trigger carries a well-shaped cron expression.
   *
   * @throws IllegalArgumentException if the expression is missing or malformed
   */
  @Override
  public void validate() {
    Preconditions.checkArgument(
        cronExpression != null, "timeTrigger.cronExpression must be set when timeTrigger is set");
    validateCronExpression(cronExpression);
  }

  /**
   * Validates Quartz cron field count and allowed tokens (shape check, not full Quartz parse).
   *
   * @param expression trimmed cron expression
   */
  public static void validateCronExpression(String expression) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(expression), "cronExpression must not be blank");
    String[] fields = expression.trim().split("\\s+");
    Preconditions.checkArgument(
        fields.length == 6 || fields.length == 7,
        "cronExpression must be a Quartz cron with 6 fields "
            + "(seconds minutes hours day-of-month month day-of-week) or 7 fields "
            + "(optional year), but got %s field(s): '%s'",
        fields.length,
        expression);
    for (int i = 0; i < fields.length; i++) {
      Preconditions.checkArgument(
          CRON_FIELD.matcher(fields[i]).matches(),
          "cronExpression field %s ('%s') is not a valid Quartz cron token "
              + "(digits, *, ,, -, /, ?, L, W, #, or month/day-of-week names)",
          i + 1,
          fields[i]);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TimeTrigger)) {
      return false;
    }
    TimeTrigger that = (TimeTrigger) o;
    return Objects.equals(cronExpression, that.cronExpression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cronExpression);
  }

  @Override
  public String toString() {
    return "TimeTrigger{cronExpression=" + cronExpression + '}';
  }
}
