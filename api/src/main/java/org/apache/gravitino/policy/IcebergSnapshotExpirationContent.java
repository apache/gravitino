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
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;

/**
 * Configuration for Iceberg snapshot expiration on an optimizer invocation.
 *
 * <p>Maps to {@code builtin-iceberg-expire-snapshots} job options ({@code older_than} is derived
 * from {@link #olderThanDays()} by the job adapter; {@code retain_last} and {@code stream_results}
 * are forwarded).
 */
public class IcebergSnapshotExpirationContent extends AbstractExecutablePolicyContent {
  /** Property key for strategy type. */
  public static final String STRATEGY_TYPE_KEY = "strategy.type";
  /** Property key for the job template. */
  public static final String JOB_TEMPLATE_NAME_KEY = "job.template-name";
  /** Rule key for the trigger expression. */
  public static final String TRIGGER_EXPR_KEY = "trigger-expr";
  /** Rule key for the score expression. */
  public static final String SCORE_EXPR_KEY = "score-expr";
  /** Prefix for policy rules forwarded as job options. */
  public static final String JOB_OPTIONS_PREFIX = "job.options.";
  /** Retention age option key shared with the job adapter. */
  public static final String OLDER_THAN_DAYS_KEY = "olderThanDays";
  /** Retain-last option key shared with the job adapter. */
  public static final String RETAIN_LAST_KEY = "retainLast";
  /** Stream-results option key shared with the job adapter. */
  public static final String STREAM_RESULTS_KEY = "streamResults";
  /** Maximum retention, approximately 100 years. */
  public static final long MAX_OLDER_THAN_DAYS = 36500;
  /** Strategy type for snapshot expiration. */
  public static final String STRATEGY_TYPE_VALUE = "iceberg-snapshot-expiration";
  /** Built-in job template name for expire snapshots. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-expire-snapshots";
  /** Default retention in days (Iceberg expire_snapshots default: five days ago). */
  public static final long DEFAULT_OLDER_THAN_DAYS = 5;
  /** Default number of recent snapshots to keep. */
  public static final long DEFAULT_RETAIN_LAST = 1;
  /** Default stream-results flag. */
  public static final boolean DEFAULT_STREAM_RESULTS = false;

  private final long olderThanDays;
  private final long retainLast;
  private final boolean streamResults;

  /** Creates content with built-in defaults. */
  public IcebergSnapshotExpirationContent() {
    this(DEFAULT_OLDER_THAN_DAYS, DEFAULT_RETAIN_LAST, DEFAULT_STREAM_RESULTS);
  }

  IcebergSnapshotExpirationContent(
      @Nullable Long olderThanDays, @Nullable Long retainLast, @Nullable Boolean streamResults) {
    this.olderThanDays = olderThanDays == null ? DEFAULT_OLDER_THAN_DAYS : olderThanDays;
    this.retainLast = retainLast == null ? DEFAULT_RETAIN_LAST : retainLast;
    this.streamResults = streamResults == null ? DEFAULT_STREAM_RESULTS : streamResults;
  }

  /**
   * @return minimum age of snapshots eligible for expiration, in days
   */
  public long olderThanDays() {
    return olderThanDays;
  }

  /**
   * @return minimum number of recent snapshots to keep regardless of age
   */
  public long retainLast() {
    return retainLast;
  }

  /**
   * @return whether the expire job should stream intermediate delete results
   */
  public boolean streamResults() {
    return streamResults;
  }

  @Override
  public Set<MetadataObject.Type> supportedObjectTypes() {
    return ImmutableSet.of(
        MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE);
  }

  @Override
  public Map<String, String> properties() {
    return ImmutableMap.of(
        STRATEGY_TYPE_KEY, STRATEGY_TYPE_VALUE, JOB_TEMPLATE_NAME_KEY, JOB_TEMPLATE_NAME_VALUE);
  }

  @Override
  public Map<String, Object> rules() {
    Map<String, Object> rules = new LinkedHashMap<>();
    // Eligibility is evaluated when the optimizer is invoked. Scheduling is owned by the caller.
    rules.put(TRIGGER_EXPR_KEY, "true");
    rules.put(SCORE_EXPR_KEY, "1");
    rules.put(JOB_OPTIONS_PREFIX + OLDER_THAN_DAYS_KEY, olderThanDays);
    rules.put(JOB_OPTIONS_PREFIX + RETAIN_LAST_KEY, retainLast);
    rules.put(JOB_OPTIONS_PREFIX + STREAM_RESULTS_KEY, streamResults);
    return Collections.unmodifiableMap(rules);
  }

  /**
   * Validates retention age before storing a policy or submitting a job.
   *
   * @param days minimum snapshot age in days
   * @throws IllegalArgumentException if days is outside the supported range
   */
  public static void validateOlderThanDays(long days) {
    Preconditions.checkArgument(
        days >= 1 && days <= MAX_OLDER_THAN_DAYS,
        "olderThanDays must be between 1 and %s",
        MAX_OLDER_THAN_DAYS);
  }

  /**
   * Validates retain-last before storing a policy or submitting a job.
   *
   * @param retainLast number of recent snapshots to keep
   * @throws IllegalArgumentException if retainLast is not a positive long
   */
  public static void validateRetainLast(long retainLast) {
    Preconditions.checkArgument(retainLast >= 1, "retainLast must be a positive long");
  }

  @Override
  public void validate() {
    PolicyContent.super.validate();
    validateOlderThanDays(olderThanDays);
    validateRetainLast(retainLast);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergSnapshotExpirationContent)) {
      return false;
    }
    IcebergSnapshotExpirationContent that = (IcebergSnapshotExpirationContent) other;
    return olderThanDays == that.olderThanDays
        && retainLast == that.retainLast
        && streamResults == that.streamResults;
  }

  @Override
  public int hashCode() {
    return Objects.hash(olderThanDays, retainLast, streamResults);
  }

  @Override
  public String toString() {
    return "IcebergSnapshotExpirationContent{"
        + "olderThanDays="
        + olderThanDays
        + ", retainLast="
        + retainLast
        + ", streamResults="
        + streamResults
        + '}';
  }
}
