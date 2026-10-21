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
package org.apache.gravitino.maintenance.scheduler.cron;

import java.util.Optional;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.policy.Policy;

/**
 * Maps maintenance {@link Policy.BuiltInType} to the TMS cron Spark job template and procedure
 * kind.
 *
 * <p>Cron always submits {@link #TMS_ICEBERG_MAINTENANCE_TEMPLATE}; standalone {@code
 * builtin-iceberg-*} templates remain for CLI / optimizer.
 */
public final class MaintenanceJobTemplates {

  /**
   * Built-in template registered by {@code TmsIcebergMaintenanceJob} in the jobs module. Uses the
   * shared {@link JobTemplateProvider} constant so the name cannot drift from the jobs module.
   */
  public static final String TMS_ICEBERG_MAINTENANCE_TEMPLATE =
      JobTemplateProvider.BUILTIN_TMS_ICEBERG_MAINTENANCE;

  /** JobConf key for procedure dispatch inside the TMS template. */
  public static final String MAINTENANCE_TYPE_KEY = "maintenance_type";

  /**
   * JobConf key for a comma-separated commit-chain of wire types (§5.2.3). When set, Spark runs
   * each type in order inside one Job.
   */
  public static final String MAINTENANCE_TYPES_KEY = "maintenance_types";

  /** JobConf key for overlaid updater options (JSON object string). */
  public static final String UPDATER_OPTIONS_JOB_CONF_KEY = "updater_options";

  /** JobConf key carrying the TMS table entity id for in-job follow-ups. */
  public static final String TMS_TABLE_ID_KEY = "tms_table_id";

  /** JobConf key carrying the TMS policy entity id for in-job follow-ups. */
  public static final String TMS_POLICY_ID_KEY = "tms_policy_id";

  private MaintenanceJobTemplates() {}

  /**
   * @param builtInType maintenance policy type
   * @return TMS cron job template name when known
   */
  public static Optional<String> templateName(Policy.BuiltInType builtInType) {
    if (maintenanceType(builtInType).isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(TMS_ICEBERG_MAINTENANCE_TEMPLATE);
  }

  /**
   * @param builtInType maintenance policy type
   * @return wire value for {@link #MAINTENANCE_TYPE_KEY}
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
