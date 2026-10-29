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
package org.apache.gravitino.maintenance.policy;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionSpec;

/**
 * Server-side checks for Iceberg maintenance executable policies on create/alter.
 *
 * <p>Kept in {@code core} (invoked from {@code PolicyManager}) so policies are validated whether or
 * not {@code plugins/maintenance} is loaded. Catalog {@code jobOptions} keys match TMS {@code
 * MaintenanceJobConfBuilder} placeholders / short aliases; they are not part of the public {@code
 * api} content contract.
 */
public final class MaintenanceExecutablePolicyValidator {

  /** Flat jobConf key consumed by {@code builtin-iceberg-maintenance-task}. */
  public static final String JOB_OPTION_WAREHOUSE_LOCATION = "warehouse_location";

  /** Short alias routed to {@link #JOB_OPTION_WAREHOUSE_LOCATION}. */
  public static final String JOB_OPTION_WAREHOUSE = "warehouse";

  /** Flat jobConf key for Iceberg REST / JDBC catalog URI. */
  public static final String JOB_OPTION_CATALOG_URI = "catalog_uri";

  /** Short alias routed to {@link #JOB_OPTION_CATALOG_URI}. */
  public static final String JOB_OPTION_URI = "uri";

  /** Flat jobConf key for catalog type ({@code rest}, {@code jdbc}, …). */
  public static final String JOB_OPTION_CATALOG_TYPE = "catalog_type";

  /** Short alias routed to {@link #JOB_OPTION_CATALOG_TYPE}. */
  public static final String JOB_OPTION_TYPE = "type";

  private MaintenanceExecutablePolicyValidator() {}

  /**
   * Validates maintenance-specific create/alter rules for {@code type} / {@code content}.
   *
   * <p>No-op for non-maintenance built-ins. For maintenance types: rejects {@code commitTrigger} on
   * orphan cleanup; when {@code timeTrigger} is set or {@code commitTrigger} is {@code true},
   * requires catalog connection keys in {@code jobOptions}.
   *
   * @param type built-in policy type from the create/alter request
   * @param content policy content
   * @throws IllegalArgumentException when a maintenance contract is violated
   */
  public static void validate(Policy.BuiltInType type, PolicyContent content) {
    if (!MaintenancePolicyDefinitions.isMaintenance(type)) {
      return;
    }
    Preconditions.checkArgument(
        content instanceof ExecutablePolicyContent,
        "maintenance policy type %s requires ExecutablePolicyContent, got %s",
        type,
        content == null ? null : content.getClass().getName());
    PolicyExecutionSpec spec = ((ExecutablePolicyContent) content).executionSpec();
    if (type == Policy.BuiltInType.ICEBERG_ORPHAN_FILE_REMOVAL) {
      Preconditions.checkArgument(
          !Boolean.TRUE.equals(spec.commitTrigger()),
          "commitTrigger is not allowed for orphan file removal");
    }
    validateJobOptionsForScheduledSubmit(spec);
  }

  /**
   * Requires catalog connection keys in {@code jobOptions} when Spark maintenance can be scheduled.
   *
   * <p>Accepted aliases: {@code warehouse} / {@code warehouse_location}, {@code uri} / {@code
   * catalog_uri}, {@code type} / {@code catalog_type}.
   *
   * @param spec execution peers
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
}
