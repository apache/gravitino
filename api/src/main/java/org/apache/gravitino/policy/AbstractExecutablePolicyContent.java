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
 * Base for built-in Iceberg maintenance policy contents that may carry optional execution peer
 * fields.
 *
 * <p>These peers are intentionally not on {@link PolicyContent}: row-filter, column-mask, and
 * custom contents have no meaningful time/commit triggers, cooldown, or job/update option bags.
 * Callers that need peers should use {@code instanceof AbstractExecutablePolicyContent} (or a
 * concrete maintenance content type).
 *
 * <p>Subclasses keep strategy-only constructors. Callers attach execution fields through {@link
 * #withExecutionFields}.
 */
public abstract class AbstractExecutablePolicyContent implements PolicyContent {

  @Nullable private String timeTrigger;
  @Nullable private Boolean commitTrigger;
  @Nullable private Long minIntervalMs;
  private Map<String, String> jobOptions = PolicyExecutionFields.copyOptions(null);
  private Map<String, String> updateOptions = PolicyExecutionFields.copyOptions(null);

  /** Creates content with unset execution fields. */
  protected AbstractExecutablePolicyContent() {}

  /**
   * Attaches optional execution fields. Strategy subclasses do not take these in constructors.
   *
   * @param timeTrigger optional cron expression
   * @param commitTrigger optional commit-trigger flag
   * @param minIntervalMs optional cooldown ms
   * @param jobOptions optional job options
   * @param updateOptions optional update options
   * @param <T> concrete content type
   * @return this instance
   */
  @SuppressWarnings("unchecked")
  public <T extends AbstractExecutablePolicyContent> T withExecutionFields(
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
    return (T) this;
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
   * Validates universal {@link PolicyContent} rules plus shared execution-peer checks.
   *
   * <p>Does not enforce type-specific maintenance contracts (for example orphan cleanup forbidding
   * {@code commitTrigger}, or cron parsing). Those belong on concrete subclasses or the server
   * create/alter path with the real {@link Policy.BuiltInType}.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    PolicyContent.super.validate();
    PolicyExecutionFields.validate(
        timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  /**
   * Whether execution fields match.
   *
   * @param that other content
   * @return true when execution fields are equal
   */
  protected boolean executionFieldsEqual(AbstractExecutablePolicyContent that) {
    return Objects.equals(timeTrigger, that.timeTrigger)
        && Objects.equals(commitTrigger, that.commitTrigger)
        && Objects.equals(minIntervalMs, that.minIntervalMs)
        && Objects.equals(jobOptions, that.jobOptions)
        && Objects.equals(updateOptions, that.updateOptions);
  }

  /**
   * @return hash of execution fields
   */
  protected int executionFieldsHashCode() {
    return Objects.hash(timeTrigger, commitTrigger, minIntervalMs, jobOptions, updateOptions);
  }

  /**
   * Appends execution fields to a subclass {@code toString} body.
   *
   * <p>Callers that already wrote subclass fields should append {@code ", "} before invoking this
   * method.
   *
   * @param sb builder already containing an opening brace and any subclass fields
   */
  protected void appendExecutionFields(StringBuilder sb) {
    sb.append("timeTrigger='")
        .append(timeTrigger)
        .append('\'')
        .append(", commitTrigger=")
        .append(commitTrigger)
        .append(", minIntervalMs=")
        .append(minIntervalMs)
        .append(", jobOptions=")
        .append(jobOptions)
        .append(", updateOptions=")
        .append(updateOptions);
  }
}
