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
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Base for built-in Iceberg maintenance policy contents that carry optional execution peers.
 *
 * <p>Peers are supplied at construction as an immutable {@link PolicyExecutionSpec}. Subclasses
 * keep strategy fields separate and pass the spec through {@link
 * #AbstractExecutablePolicyContent(PolicyExecutionSpec)}. Wire and persistence JSON keep peer keys
 * flat; mapping to and from those keys is handled in {@code common} (custom Jackson serdes), not by
 * flattening the five fields onto this type.
 */
public abstract class AbstractExecutablePolicyContent implements ExecutablePolicyContent {

  /** Flat jobConf key consumed by {@code builtin-iceberg-maintenance-task}. */
  public static final String JOB_OPTION_WAREHOUSE_LOCATION = "warehouse_location";

  /** Short alias routed to {@link #JOB_OPTION_WAREHOUSE_LOCATION} by TMS jobConf builder. */
  public static final String JOB_OPTION_WAREHOUSE = "warehouse";

  /** Flat jobConf key for Iceberg REST / JDBC catalog URI. */
  public static final String JOB_OPTION_CATALOG_URI = "catalog_uri";

  /** Short alias routed to {@link #JOB_OPTION_CATALOG_URI}. */
  public static final String JOB_OPTION_URI = "uri";

  /** Flat jobConf key for catalog type ({@code rest}, {@code jdbc}, …). */
  public static final String JOB_OPTION_CATALOG_TYPE = "catalog_type";

  /** Short alias routed to {@link #JOB_OPTION_CATALOG_TYPE}. */
  public static final String JOB_OPTION_TYPE = "type";

  private final PolicyExecutionSpec executionSpec;

  /**
   * Creates content with the given execution peers.
   *
   * @param executionSpec immutable peers; {@code null} is treated as {@link
   *     PolicyExecutionSpec#empty()}
   */
  protected AbstractExecutablePolicyContent(@Nullable PolicyExecutionSpec executionSpec) {
    this.executionSpec = executionSpec == null ? PolicyExecutionSpec.empty() : executionSpec;
  }

  /** Creates content with unset execution peers. */
  protected AbstractExecutablePolicyContent() {
    this(PolicyExecutionSpec.empty());
  }

  @Override
  public PolicyExecutionSpec executionSpec() {
    return executionSpec;
  }

  /**
   * Validates universal {@link PolicyContent} rules and TMS submit-time {@code jobOptions}.
   *
   * <p>Shared execution-peer constraints are enforced when constructing {@link
   * PolicyExecutionSpec}. When {@code timeTrigger} is set or {@code commitTrigger} is {@code true},
   * cron/commit submit will run {@code builtin-iceberg-maintenance-task}; require the catalog
   * connection keys that template placeholders expect from policy {@code jobOptions} (TMS fills
   * table/policy ids later). Type-specific contracts (for example orphan forbidding {@code
   * commitTrigger}) belong on concrete subclasses.
   */
  @Override
  public void validate() throws IllegalArgumentException {
    ExecutablePolicyContent.super.validate();
    validateJobOptionsForScheduledSubmit(executionSpec());
  }

  /**
   * Requires catalog connection keys in {@code jobOptions} when the policy can schedule Spark
   * maintenance.
   *
   * <p>Accepted aliases match {@code MaintenanceJobConfBuilder} routing: {@code warehouse} / {@code
   * warehouse_location}, {@code uri} / {@code catalog_uri}, {@code type} / {@code catalog_type}.
   *
   * @param spec execution peers
   * @throws IllegalArgumentException when a scheduled policy is missing required jobOptions
   */
  static void validateJobOptionsForScheduledSubmit(PolicyExecutionSpec spec) {
    boolean willSchedule =
        StringUtils.isNotBlank(spec.timeTrigger()) || Boolean.TRUE.equals(spec.commitTrigger());
    if (!willSchedule) {
      return;
    }
    Map<String, String> jobOptions = spec.jobOptions();
    requireJobOption(
        jobOptions,
        JOB_OPTION_WAREHOUSE_LOCATION,
        JOB_OPTION_WAREHOUSE,
        "jobOptions must set '"
            + JOB_OPTION_WAREHOUSE_LOCATION
            + "' (or '"
            + JOB_OPTION_WAREHOUSE
            + "') when timeTrigger or commitTrigger schedules maintenance Spark jobs");
    requireJobOption(
        jobOptions,
        JOB_OPTION_CATALOG_URI,
        JOB_OPTION_URI,
        "jobOptions must set '"
            + JOB_OPTION_CATALOG_URI
            + "' (or '"
            + JOB_OPTION_URI
            + "') when timeTrigger or commitTrigger schedules maintenance Spark jobs");
    requireJobOption(
        jobOptions,
        JOB_OPTION_CATALOG_TYPE,
        JOB_OPTION_TYPE,
        "jobOptions must set '"
            + JOB_OPTION_CATALOG_TYPE
            + "' (or '"
            + JOB_OPTION_TYPE
            + "') when timeTrigger or commitTrigger schedules maintenance Spark jobs");
  }

  private static void requireJobOption(
      Map<String, String> jobOptions, String primary, String alias, String message) {
    boolean present =
        StringUtils.isNotBlank(jobOptions.get(primary))
            || StringUtils.isNotBlank(jobOptions.get(alias));
    Preconditions.checkArgument(present, message);
  }

  /**
   * Whether execution fields match.
   *
   * @param that other content
   * @return true when execution fields are equal
   */
  protected boolean executionFieldsEqual(AbstractExecutablePolicyContent that) {
    return Objects.equals(executionSpec(), that.executionSpec());
  }

  /**
   * @return hash of execution fields
   */
  protected int executionFieldsHashCode() {
    return Objects.hashCode(executionSpec());
  }

  /**
   * Appends execution fields to a subclass {@code toString} body.
   *
   * <p>Option map <em>values</em> are never rendered (keys only) so accidental credentials are not
   * exposed through logs or exception messages.
   *
   * <p>Callers that already wrote subclass fields should append {@code ", "} before invoking this
   * method.
   *
   * @param sb builder already containing an opening brace and any subclass fields
   */
  protected void appendExecutionFields(StringBuilder sb) {
    PolicyExecutionSpec spec = executionSpec();
    sb.append("timeTrigger='")
        .append(spec.timeTrigger())
        .append('\'')
        .append(", commitTrigger=")
        .append(spec.commitTrigger())
        .append(", minIntervalMs=")
        .append(spec.minIntervalMs())
        .append(", jobOptionsKeys=")
        .append(spec.jobOptions().keySet())
        .append(", updaterOptionsKeys=")
        .append(spec.updaterOptions().keySet());
  }
}
