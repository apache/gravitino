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
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;

/** Configuration for table-level Iceberg orphan cleanup on an optimizer invocation. */
public class IcebergOrphanFileRemovalContent extends AbstractExecutablePolicyContent {
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
  /** Retention option key shared with the job adapter. */
  public static final String OLDER_THAN_DAYS_KEY = "olderThanDays";
  /** Scan location option key shared with the job adapter. */
  public static final String LOCATION_KEY = "location";
  /** Preview mode option key shared with the job adapter. */
  public static final String DRY_RUN_KEY = "dryRun";
  /** Maximum retention, approximately 100 years, to keep cleanup timestamps practical. */
  public static final long MAX_OLDER_THAN_DAYS = 36500;
  /** Strategy type for orphan cleanup. */
  public static final String STRATEGY_TYPE_VALUE = "iceberg-orphan-file-removal";
  /** Existing Spark job template used for cleanup. */
  public static final String JOB_TEMPLATE_NAME_VALUE = "builtin-iceberg-remove-orphan-files";
  /** Default retention in days. */
  public static final long DEFAULT_OLDER_THAN_DAYS = 3;
  /** Default execution mode. */
  public static final boolean DEFAULT_DRY_RUN = false;
  /** TMS S1 metric: orphan files removed (or found in dry-run). */
  public static final String REMOVED_FILE_NUMBER_METRIC = "custom-removed-file-number";
  /** TMS S1 metric: bytes of orphan files removed. */
  public static final String REMOVED_FILE_SIZE_METRIC = "custom-removed-file-size";

  private final long olderThanDays;
  @Nullable private final String location;
  private final boolean dryRun;

  private IcebergOrphanFileRemovalContent() {
    this(DEFAULT_OLDER_THAN_DAYS, null, DEFAULT_DRY_RUN, PolicyExecutionSpec.empty());
  }

  IcebergOrphanFileRemovalContent(long olderThanDays, @Nullable String location, boolean dryRun) {
    this(olderThanDays, location, dryRun, PolicyExecutionSpec.empty());
  }

  IcebergOrphanFileRemovalContent(
      long olderThanDays,
      @Nullable String location,
      boolean dryRun,
      @Nullable PolicyExecutionSpec executionSpec) {
    super(executionSpec);
    this.olderThanDays = olderThanDays;
    this.location = location;
    this.dryRun = dryRun;
  }

  /**
   * @return minimum age of files eligible for removal, in days
   */
  public long olderThanDays() {
    return olderThanDays;
  }

  /**
   * @return optional scan location within the table, or null for the table location
   */
  @Nullable
  public String location() {
    return location;
  }

  /**
   * @return whether candidates should only be listed
   */
  public boolean dryRun() {
    return dryRun;
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
    rules.put(JOB_OPTIONS_PREFIX + DRY_RUN_KEY, dryRun);
    if (location != null) {
      rules.put(JOB_OPTIONS_PREFIX + LOCATION_KEY, location);
    }
    return Collections.unmodifiableMap(rules);
  }

  /**
   * Validates retention before storing a policy or submitting a job.
   *
   * @param days minimum file age in days
   * @throws IllegalArgumentException if days is outside the supported range
   */
  public static void validateOlderThanDays(long days) {
    Preconditions.checkArgument(
        days >= 1 && days <= MAX_OLDER_THAN_DAYS,
        "olderThanDays must be between 1 and %s",
        MAX_OLDER_THAN_DAYS);
  }

  /**
   * Validates orphan-scan location syntax shared by policy write-time checks and job submission.
   *
   * <p>Mirrors {@code OrphanFileLocationUtils.normalizeLocation} lexical rules so values that would
   * fail {@link URI#create(String)} or absolute-path checks are rejected when the policy is stored,
   * not only when a cleanup job later runs.
   *
   * @param location non-null scan location
   * @return parsed URI after syntax checks
   * @throws IllegalArgumentException if the location is blank, ambiguous, or not a usable URI
   */
  public static URI parseAndValidateLocation(String location) {
    Preconditions.checkArgument(StringUtils.isNotBlank(location), "location must not be blank");
    Preconditions.checkArgument(
        location.equals(StringUtils.strip(location.trim())),
        "location must not have leading or trailing whitespace");
    Preconditions.checkArgument(
        !location.contains(".."), "location must not contain '..' path segments: %s", location);
    Preconditions.checkArgument(
        !location.contains("\\"), "location must not contain '\\': %s", location);
    Preconditions.checkArgument(
        !location.contains("%"), "location must not contain percent-encoding: %s", location);
    final URI uri;
    try {
      uri = URI.create(location);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("location must be a valid URI: " + location, e);
    }
    Preconditions.checkArgument(
        uri.getQuery() == null
            && uri.getFragment() == null
            && uri.getPath() != null
            && uri.getPath().startsWith("/"),
        "location must be an absolute path without query or fragment: %s",
        location);
    return uri;
  }

  @Override
  public void validate() {
    super.validate();
    Preconditions.checkArgument(
        !Boolean.TRUE.equals(executionSpec().commitTrigger()),
        "commitTrigger is not allowed for orphan file removal");
    validateOlderThanDays(olderThanDays);
    if (location != null) {
      parseAndValidateLocation(location);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergOrphanFileRemovalContent)) {
      return false;
    }
    IcebergOrphanFileRemovalContent that = (IcebergOrphanFileRemovalContent) other;
    return executionFieldsEqual(that)
        && olderThanDays == that.olderThanDays
        && dryRun == that.dryRun
        && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(executionFieldsHashCode(), olderThanDays, location, dryRun);
  }
}
