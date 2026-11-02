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
package org.apache.gravitino.maintenance.scheduler.job.cron;

import java.util.Optional;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.policy.Policy;

/**
 * Maps maintenance {@link Policy.BuiltInType} to the cron Spark job template and procedure kind.
 *
 * <p>Cron always submits {@link #ICEBERG_MAINTENANCE_TEMPLATE}; standalone {@code
 * builtin-iceberg-*} templates remain for CLI / optimizer.
 */
public final class MaintenanceJobTemplates {

  /**
   * Built-in template name for the Iceberg maintenance Spark job. Must match {@code
   * IcebergMaintenanceJob.NAME} ({@code builtin-iceberg-maintenance-task}).
   */
  public static final String ICEBERG_MAINTENANCE_TEMPLATE =
      JobTemplateProvider.BUILTIN_NAME_PREFIX + "iceberg-maintenance-task";

  /**
   * JobConf key for procedure dispatch inside the maintenance template. One wire name, or a
   * comma-separated commit chain. Required by {@link #ICEBERG_MAINTENANCE_TEMPLATE}.
   */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /**
   * JobConf key for policy entity ids parallel to {@link #MAINTENANCE_TYPES_KEY} (comma-separated).
   * Required so Spark callbacks can attribute soft slots by {@code policyId}.
   */
  public static final String POLICY_IDS_KEY = "policy_ids";

  /**
   * JobConf key for {@code table_maintenance_action.action_id} values parallel to {@link
   * #POLICY_IDS_KEY} (comma-separated). Injected at submit after soft-slot acquire.
   */
  public static final String ACTION_IDS_KEY = "action_ids";

  /** JobConf key for overlaid updater options (JSON object string). */
  public static final String UPDATER_OPTIONS_JOB_CONF_KEY = "updater_options";

  /**
   * JobConf key for enqueue-time policy {@code rules()}, JSON map of wire maintenance type → rules
   * object. Consumed in-Spark for trigger-expr recommend (no Gravitino round-trip).
   */
  public static final String RULES_KEY = "rules";

  private MaintenanceJobTemplates() {}

  /**
   * @param builtInType maintenance policy type
   * @return cron job template name when known
   */
  public static Optional<String> templateName(Policy.BuiltInType builtInType) {
    if (maintenanceType(builtInType).isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(ICEBERG_MAINTENANCE_TEMPLATE);
  }

  /**
   * @param builtInType maintenance policy type
   * @return wire value for {@link #MAINTENANCE_TYPES_KEY}
   */
  public static Optional<String> maintenanceType(Policy.BuiltInType builtInType) {
    if (builtInType == null) {
      return Optional.empty();
    }
    switch (builtInType) {
      case ICEBERG_COMPACTION:
        return Optional.of("rewrite_data_files");
      case ICEBERG_REWRITE_MANIFESTS:
        return Optional.of("rewrite_manifests");
      case ICEBERG_SNAPSHOT_EXPIRATION:
        return Optional.of("expire_snapshots");
      case ICEBERG_ORPHAN_FILE_REMOVAL:
        return Optional.of("remove_orphan_files");
      default:
        return Optional.empty();
    }
  }
}
