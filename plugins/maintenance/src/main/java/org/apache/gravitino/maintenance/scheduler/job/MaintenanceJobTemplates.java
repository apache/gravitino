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
package org.apache.gravitino.maintenance.scheduler.job;

import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.policy.Policy;

/**
 * Shared TMS job template name and jobConf keys for cron and commit submits.
 *
 * <p>TMS always submits {@link #ICEBERG_MAINTENANCE_TEMPLATE}; standalone {@code builtin-iceberg-*}
 * templates remain for CLI / optimizer.
 */
public final class MaintenanceJobTemplates {

  /**
   * Built-in template name for the Iceberg maintenance Spark job. Must match {@code
   * IcebergMaintenanceJob.NAME} ({@code builtin-iceberg-maintenance-task}).
   */
  public static final String ICEBERG_MAINTENANCE_TEMPLATE =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /**
   * JobConf key for the ordered maintenance work list (JSON array). Each element carries {@link
   * #TABLE_ID_KEY}, {@link #POLICY_TYPE_KEY}, {@link #POLICY_ID_KEY}, {@link #ACTION_ID_KEY},
   * {@link #PARAMS_KEY}, and {@link #RULES_KEY}.
   */
  public static final String TASKS_KEY = "tasks";

  /** {@code tasks[]} field: table entity id. */
  public static final String TABLE_ID_KEY = "table_id";

  /** {@code tasks[]} field: {@link Policy.BuiltInType#policyType()}. */
  public static final String POLICY_TYPE_KEY = "policy_type";

  /** {@code tasks[]} field: policy entity id. */
  public static final String POLICY_ID_KEY = "policy_id";

  /** {@code tasks[]} field: soft-slot action id (stamped at submit). */
  public static final String ACTION_ID_KEY = "action_id";

  /** {@code tasks[]} field: procedure placeholders (catalog/table + CALL flags). */
  public static final String PARAMS_KEY = "params";

  /**
   * {@code tasks[]} field: gate rules object. Also used at schedule-time as a per-task parameter
   * holding JSON map of {@code policy_type} → rules before submit projection.
   */
  public static final String RULES_KEY = "rules";

  /**
   * Deprecated flat dispatch key retained for schedule-time parameter maps and tests. Runtime Spark
   * submits use {@link #TASKS_KEY} instead. Values are {@link Policy.BuiltInType#policyType()}.
   */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /**
   * Deprecated flat policy-id list; runtime Spark submits embed {@link #POLICY_ID_KEY} on each
   * {@link #TASKS_KEY} element.
   */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /** JobConf key for overlaid updater options (JSON object string). */
  public static final String UPDATER_OPTIONS_JOB_CONF_KEY = "updater_options";

  private MaintenanceJobTemplates() {}

  /**
   * @param builtInType policy type
   * @return whether TMS Iceberg maintenance jobs handle this type
   */
  public static boolean isIcebergMaintenance(Policy.BuiltInType builtInType) {
    if (builtInType == null) {
      return false;
    }
    switch (builtInType) {
      case ICEBERG_COMPACTION:
      case ICEBERG_REWRITE_MANIFESTS:
      case ICEBERG_SNAPSHOT_EXPIRATION:
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        return true;
      default:
        return false;
    }
  }
}
